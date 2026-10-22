#!/usr/bin/env python3
"""Echo service that follows the chat convention of chatui (docs/celaut/chat.md).

One session for each instance. The history is in memory. The answer to a message is
the text of the message. The service accepts image and audio files (revision 3), keeps
them and gives them back, but it never reads their content: the answer has no
attachment and does not depend on a file.
"""

import base64
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

MAX_MESSAGES = 10_000
MAX_TEXT = 16_384
MAX_BODY = 24 << 20
MAX_FILE = 4 << 20
MAX_FILES = 4
MAX_ANSWER = 1 << 20  # body of the answer to a read (chat.md revision 4)
MAX_FILE_BYTES = 64 << 20  # all files of the history; above it, history_full
ACCEPTS = ["image", "audio"]
MIME_KINDS = {"image/jpeg": "image", "image/png": "image",
              "audio/mpeg": "audio", "audio/wav": "audio"}


class Chat:
    """The history of the conversation. `count` is the `seq` of the next message."""

    def __init__(self, max_messages=MAX_MESSAGES):
        self.messages = []
        self.files = {}  # seq -> list of the bytes of each attachment
        self.file_bytes = 0
        self.max_messages = max_messages
        self.lock = threading.Lock()  # the data, held for a short time
        self.sending = threading.Lock()  # one send in progress; never held by a read

    @property
    def count(self):
        return len(self.messages)

    def add(self, role, text, files=()):
        message = {"seq": self.count, "role": role, "text": text}
        if files:
            message["attachments"] = [f["description"] for f in files]
            self.files[message["seq"]] = [f["data"] for f in files]
            self.file_bytes += sum(len(f["data"]) for f in files)
        self.messages.append(message)
        return message

    def answer(self, text):
        """The echo of the text only. The files are not an input."""
        return text


ATTACHMENT = re.compile("/chat/messages/([^/]+)/attachments/([^/]+)")


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _digits(value):
    return re.fullmatch("[0-9]+", value) is not None


def _read_files(body):
    """Check step 1 of the files. Return (files, None) or (None, reason).

    The bytes are decoded to be kept and to know `size`. They are not interpreted.
    """
    items = body.get("attachments")
    if items is None:
        return [], None
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_FILES:
        return None, "attachments needs 1 to %d files" % MAX_FILES
    files = []
    for item in items:
        if (not isinstance(item, dict) or not isinstance(item.get("mime"), str)
                or not isinstance(item.get("data"), str)
                or not isinstance(item.get("name", ""), str)):
            return None, "a file needs a mime and data"
        try:
            data = base64.b64decode(item["data"], validate=True)
        except ValueError:
            return None, "the data of a file is not valid base64"
        description = {"mime": item["mime"], "size": len(data)}
        if "name" in item:
            description["name"] = item["name"]
        files.append({"description": description, "data": data})
    return files, None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    chat = None

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status, error, message=None, **extra):
        body = {"error": error, **extra}
        if message:
            body["message"] = message
        self._send(status, body)

    def _method_not_allowed(self, allow="GET, POST"):
        length = self.headers.get("Content-Length", "")
        if length.isdigit() and int(length) <= MAX_BODY:
            self.rfile.read(int(length))  # keep the connection in step
        data = b'{"error": "method_not_allowed"}'
        self.send_response(405)
        self.send_header("Allow", allow)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _other_method(self):
        path = urlsplit(self.path).path
        if path == "/chat/messages":
            return self._method_not_allowed()
        if ATTACHMENT.fullmatch(path):
            return self._method_not_allowed("GET")
        self._error(404, "not_found")

    do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = _other_method

    def _attachment(self, seq, index):
        if not (_digits(seq) and _digits(index)):
            return self._error(400, "bad_request", "seq and index are not valid")
        with self.chat.lock:
            files = self.chat.files.get(int(seq), [])
            if int(index) >= len(files):
                return self._error(404, "not_found")
            description = self.chat.messages[int(seq)]["attachments"][int(index)]
            data = files[int(index)]
        self.send_response(200)
        self.send_header("Content-Type", description["mime"])
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _page(self, start):
        """The messages from `start`, as many as fit in MAX_ANSWER, at least one.

        The caller holds the data lock. The size is that of the JSON that _send makes.
        """
        header = len(json.dumps({"count": self.chat.count, "accepts": ACCEPTS,
                                 "messages": []}).encode())
        used = header
        page = []
        for message in self.chat.messages[start:]:
            used += len(json.dumps(message).encode()) + 2  # ", " between messages
            if page and used > MAX_ANSWER:
                break
            page.append(message)
        return page

    def do_GET(self):
        url = urlsplit(self.path)
        found = ATTACHMENT.fullmatch(url.path)
        if found:
            return self._attachment(*found.groups())
        if url.path != "/chat/messages":
            return self._error(404, "not_found")
        start = 0
        query = parse_qs(url.query, keep_blank_values=True)
        if "from" in query:
            raw = query["from"][0]
            if not _digits(raw):
                return self._error(400, "bad_request", "from is not valid")
            start = int(raw)
        with self.chat.lock:
            if start > self.chat.count:
                return self._error(400, "bad_request", "from is more than count")
            self._send(200, {"count": self.chat.count, "accepts": ACCEPTS,
                             "messages": self._page(start)})

    def do_POST(self):
        path = urlsplit(self.path).path
        if ATTACHMENT.fullmatch(path):
            return self._method_not_allowed("GET")
        if path != "/chat/messages":
            return self._error(404, "not_found")
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            length = -1
        if not 0 <= length <= MAX_BODY:
            if length > MAX_BODY:
                # Read and drop the body, so the connection stays in step. A very
                # large body is not read: the connection closes after the answer.
                if length <= 4 * MAX_BODY:
                    self.connection.settimeout(2)
                    try:
                        remaining = length
                        while remaining:
                            chunk = self.rfile.read(min(remaining, 1 << 20))
                            if not chunk:
                                break
                            remaining -= len(chunk)
                    except OSError:
                        self.close_connection = True
                    finally:
                        self.connection.settimeout(None)
                else:
                    self.close_connection = True
            return self._error(400, "bad_request", "the body size is not valid")
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except ValueError:
            body = None
        if (not isinstance(body, dict) or not _is_int(body.get("expect"))
                or body["expect"] < 0 or not isinstance(body.get("text"), str)):
            return self._error(400, "bad_request",
                               "the body needs an integer expect and a text")
        files, reason = _read_files(body)
        if reason or not (body["text"] or files):
            return self._error(400, "bad_request",
                               reason or "the text is empty and there is no file")
        if (len(body["text"].encode()) > MAX_TEXT
                or any(f["description"]["size"] > MAX_FILE for f in files)):
            return self._error(400, "bad_request", "the text or a file is too big")
        if any(f["description"]["mime"] not in MIME_KINDS for f in files):
            return self._error(415, "unsupported_media",
                               "accepted: " + ", ".join(MIME_KINDS))
        for f in files:
            f["description"] = {"kind": MIME_KINDS[f["description"]["mime"]],
                                **f["description"]}
        if not self.chat.sending.acquire(blocking=False):
            return self._error(503, "busy")
        try:
            with self.chat.lock:
                if body["expect"] != self.chat.count:
                    return self._error(409, "out_of_sync", count=self.chat.count)
                if (self.chat.count + 2 > self.chat.max_messages
                        or self.chat.file_bytes + sum(len(f["data"]) for f in files)
                        > MAX_FILE_BYTES):
                    return self._error(413, "history_full",
                                       "start a new instance for a new conversation")
            answer = self.chat.answer(body["text"])  # a slow answer holds no data lock
            with self.chat.lock:
                user = self.chat.add("user", body["text"], files)
                reply = self.chat.add("assistant", answer)
                result = {"count": self.chat.count, "messages": [user, reply]}
            self._send(200, result)
        finally:
            self.chat.sending.release()


def make_server(host="0.0.0.0", port=8080, chat=None):
    handler = type("ChatHandler", (Handler,), {"chat": chat or Chat()})
    return ThreadingHTTPServer((host, port), handler)


if __name__ == "__main__":
    make_server(port=int(os.environ.get("PORT", "8080"))).serve_forever()
