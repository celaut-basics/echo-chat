#!/usr/bin/env python3
"""Echo service that follows the chat convention of chatui (docs/celaut/chat.md).

One session for each instance. The history is in memory. The answer to a message is
the text of the message.
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

MAX_MESSAGES = 10_000
MAX_BODY = 1 << 20


class Chat:
    """The history of the conversation. `count` is the `seq` of the next message."""

    def __init__(self, max_messages=MAX_MESSAGES):
        self.messages = []
        self.max_messages = max_messages
        self.lock = threading.Lock()

    @property
    def count(self):
        return len(self.messages)

    def add(self, role, text):
        message = {"seq": self.count, "role": role, "text": text}
        self.messages.append(message)
        return message

    def answer(self, text):
        return text


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


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

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path != "/chat/messages":
            return self._error(404, "not_found")
        start = 0
        if "from" in parse_qs(url.query):
            raw = parse_qs(url.query)["from"][0]
            if not raw.isdigit():
                return self._error(400, "bad_request", "from is not valid")
            start = int(raw)
        with self.chat.lock:
            if start > self.chat.count:
                return self._error(400, "bad_request", "from is more than count")
            self._send(200, {"count": self.chat.count,
                             "messages": self.chat.messages[start:]})

    def do_POST(self):
        if urlsplit(self.path).path != "/chat/messages":
            return self._error(404, "not_found")
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            length = -1
        if not 0 <= length <= MAX_BODY:
            return self._error(400, "bad_request", "the body size is not valid")
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except ValueError:
            body = None
        if (not isinstance(body, dict) or not _is_int(body.get("expect"))
                or body["expect"] < 0 or not isinstance(body.get("text"), str)
                or not body["text"]):
            return self._error(400, "bad_request",
                               "the body needs an integer expect and a text")
        if not self.chat.lock.acquire(blocking=False):
            return self._error(503, "busy")
        try:
            if body["expect"] != self.chat.count:
                return self._error(409, "out_of_sync", count=self.chat.count)
            if self.chat.count + 2 > self.chat.max_messages:
                return self._error(413, "history_full",
                                   "start a new instance for a new conversation")
            user = self.chat.add("user", body["text"])
            reply = self.chat.add("assistant", self.chat.answer(body["text"]))
            self._send(200, {"count": self.chat.count, "messages": [user, reply]})
        finally:
            self.chat.lock.release()


def make_server(host="0.0.0.0", port=8080, chat=None):
    handler = type("ChatHandler", (Handler,), {"chat": chat or Chat()})
    return ThreadingHTTPServer((host, port), handler)


if __name__ == "__main__":
    make_server(port=int(os.environ.get("PORT", "8080"))).serve_forever()
