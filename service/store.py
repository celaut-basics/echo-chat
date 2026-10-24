"""The conversation of the echo service: the history, the files and the answers.

chat.md revision 6. Everything that changes the history, and the event that tells about the
change, happen under one lock. So a `Connect` stream that joins sees a replay and then the
live events, with none lost and none sent twice (section 6.5, step 3).

The answer to a user message is its text. A message can start with commands, to make the
echo behave as a conformance fixture (see `parse_commands`).
"""

import hashlib
import os
import queue
import re
import threading
import time
import uuid

from . import chat_pb2 as pb

SHA3_256_TYPE = hashlib.sha3_256(b"").digest()  # chat.md section 6.4
ACCEPTED_MIME_TYPES = ["image/jpeg", "image/png", "audio/mpeg", "audio/wav"]

MAX_TEXT = 16_384
MAX_REQUEST = 128 << 10
MAX_FILE = 4 << 20
MAX_FILES = 4
MAX_FILE_BYTES = 64 << 20  # all files that the service keeps
READ_LIMIT = 1000  # the most messages of one GetMessages
KEY = re.compile("[A-Za-z0-9_-]{1,64}")
STATUS = pb.MessageStatus

COMMANDS = ("stream", "reasoning", "pin", "fail", "attach", "delay", "pending", "reject", "echo")
REJECTS = ("service_error", "busy", "history_full")


class Reject(Exception):
    """A call that fails. `status` is a name of `grpc.StatusCode`, `reason` goes in
    the trailer `chat-reason` (chat.md section 9)."""

    def __init__(self, status, reason, message=""):
        super().__init__(message or reason)
        self.status, self.reason, self.message = status, reason, message or reason


def bad_request(message):
    return Reject("INVALID_ARGUMENT", "bad_request", message)


def normalize_mime(mime):
    return mime.split(";")[0].strip().lower()


def sha3(data):
    return hashlib.sha3_256(data).digest()


def text_of(content):
    return "".join(p.text.text for p in content if p.WhichOneof("kind") == "text")


def parse_commands(text):
    """Split the leading commands from the text. Return (commands, rest).

    A command is a word that starts with `/` at the start of the text, followed by a
    space or the end. The user message keeps the whole text. The answer is `rest`.
    A leading word that is not a command stays in the text. `/echo` ends the commands,
    so `/echo /stream` echoes `/stream`.

    | Command | Effect |
    |---|---|
    | `/stream[=MS]` | The answer grows word by word, one word each MS milliseconds (50) |
    | `/reasoning` | As `/stream`, with `reasoning` before the text |
    | `/pin` or `/pin=late` | The answer is pinned when it is made, or after it is final |
    | `/fail` | As `/stream`, but the answer stops after some words with `FAILED` |
    | `/attach` | The answer has the attachments of the user message |
    | `/delay=MS` | The answer starts MS milliseconds after the service takes the message |
    | `/pending` | The answer is `PENDING` from the send until it starts |
    | `/reject=REASON` | The send fails with `service_error`, `busy` or `history_full` |
    """
    commands = {}
    rest = text
    while rest.startswith("/"):
        token, space, tail = rest.partition(" ")
        name, equals, value = token[1:].partition("=")
        if name in ("stream", "delay"):
            if equals or name == "delay":
                if not (value.isascii() and value.isdigit()):
                    break
                value = int(value)
        elif name == "pin":
            if equals and value != "late":
                break
        elif name == "reject":
            if value not in REJECTS:
                break
        elif name not in COMMANDS or equals:
            break
        commands[name] = value if equals else True
        rest = tail if space else ""
        if name == "echo":
            break
    return commands, rest


class Chat:
    def __init__(self, max_messages=None, max_queue=None, stream=None, delay_ms=None):
        env = os.environ.get
        self.max_messages = int(env("ECHO_MAX_MESSAGES", 10_000)) if max_messages is None else max_messages
        self.max_queue = int(env("ECHO_MAX_QUEUE", 64)) if max_queue is None else max_queue
        # ECHO_STREAM: "" (whole answers), "text" or "reasoning" for every answer.
        self.stream = env("ECHO_STREAM", "") if stream is None else stream
        self.delay_ms = int(env("ECHO_DELAY_MS", 50)) if delay_ms is None else delay_ms
        self.created_at = self._now()
        self.messages = []  # sequence n is messages[n - 1]
        self.by_id = {}
        self.keys = {}  # idempotency_key -> message_id
        self.files = {}  # SHA3-256 value -> bytes
        self.file_bytes = 0
        self.subscribers = []
        self.pending = 0  # user messages that wait for an answer
        self.lock = threading.Lock()
        self.jobs = queue.Queue()
        self.worker = threading.Thread(target=self._work, daemon=True)
        self.worker.start()

    def close(self):
        self.jobs.put(None)

    @staticmethod
    def _now():
        return time.time_ns() // 1_000_000

    @property
    def latest(self):
        return len(self.messages)

    # The next methods run with the lock held.

    def _touch(self, message):
        message.updated_at = max(self._now(), message.updated_at)

    def _publish(self, **event):
        event = pb.ServerEvent(**event)
        for q in self.subscribers:
            q.put(event)

    def _add(self, **fields):
        now = self._now()
        message = pb.Message(message_id=uuid.uuid4().hex, sequence=self.latest + 1,
                             created_at=now, updated_at=now, **fields)
        self.messages.append(message)
        self.by_id[message.message_id] = message
        self._publish(message_created=pb.MessageCreated(message=message))
        return message

    def _set_status(self, message, status):
        message.status = status
        self._touch(message)
        self._publish(message_status_changed=pb.MessageStatusChanged(
            message_id=message.message_id, status=status, updated_at=message.updated_at))

    def _pin(self, message, pinned):
        if message.pinned == pinned:
            return
        message.pinned = pinned
        self._touch(message)
        self._publish(message_pinned_changed=pb.MessagePinnedChanged(
            message_id=message.message_id, pinned=pinned, updated_at=message.updated_at))

    def sha3_of(self, ref):
        """The SHA3-256 hash of a FileReference, or a Reject."""
        for h in ref.hashes:
            if h.HasField("type") and h.type == SHA3_256_TYPE:
                if len(h.value) != 32:
                    raise bad_request("a SHA3-256 hash has 32 bytes")
                return h.value
        raise bad_request("a file needs a hash of type SHA3-256")

    # Calls.

    def capabilities(self):
        return pb.Capabilities(accepted_mime_types=ACCEPTED_MIME_TYPES)

    def conversation(self):
        return pb.Conversation(conversation_id="", created_at=self.created_at, archived=False)

    def send(self, request):
        """chat.md section 6.1. Return (user message, True if it is a repeat)."""
        if request.conversation_id:
            raise Reject("NOT_FOUND", "not_found", "there is only the main conversation")
        # Step 1.
        if not request.content:
            raise bad_request("content has no part")
        attachments = []
        for part in request.content:
            kind = part.WhichOneof("kind")
            if kind not in ("text", "image", "audio"):
                raise bad_request("a part needs a kind that a client can send")
            if kind != "text":
                attachments.append((kind, getattr(part, kind).file))
        if request.HasField("idempotency_key") and not KEY.fullmatch(request.idempotency_key):
            raise bad_request("idempotency_key has 1 to 64 characters of A-Za-z0-9_-")
        text = text_of(request.content)
        if not text and not attachments:
            raise bad_request("the text is empty and there is no attachment")
        values = []
        for kind, ref in attachments:
            if not ref.HasField("mime_type") or not ref.HasField("size_bytes"):
                raise bad_request("a file needs mime_type and size_bytes")
            mime = normalize_mime(ref.mime_type)
            if (mime.startswith("image/") and kind != "image") or (
                    mime.startswith("audio/") and kind != "audio"):
                raise bad_request("the kind of the part does not agree with mime_type")
            values.append(self.sha3_of(ref))
        if len(attachments) > MAX_FILES:
            raise bad_request("a message has %d attachments or less" % MAX_FILES)
        # Step 2.
        if (len(text.encode()) > MAX_TEXT or request.ByteSize() > MAX_REQUEST
                or any(ref.size_bytes > MAX_FILE for _, ref in attachments)):
            raise Reject("INVALID_ARGUMENT", "too_large", "the text, the request or a file is too large")
        # Step 3.
        for _, ref in attachments:
            if normalize_mime(ref.mime_type) not in ACCEPTED_MIME_TYPES:
                raise Reject("INVALID_ARGUMENT", "unsupported_media",
                             "accepted: " + ", ".join(ACCEPTED_MIME_TYPES))
        commands, rest = parse_commands(text)
        with self.lock:
            # Step 4.
            if request.HasField("reply_to_message_id") and request.reply_to_message_id not in self.by_id:
                raise Reject("NOT_FOUND", "not_found", "reply_to_message_id is not a message")
            # Step 5.
            if any(value not in self.files for value in values):
                raise Reject("FAILED_PRECONDITION", "file_missing", "upload the files first")
            # Step 6.
            if request.HasField("idempotency_key") and request.idempotency_key in self.keys:
                known = self.by_id[self.keys[request.idempotency_key]]
                return pb.Message.FromString(known.SerializeToString()), True
            # Steps 7 and 8. Each message that waits adds its answer.
            if commands.get("reject") == "service_error":
                raise Reject("INTERNAL", "service_error", "the service cannot take the message")
            if commands.get("reject") == "history_full" or \
                    self.latest + self.pending + 2 > self.max_messages:
                raise Reject("RESOURCE_EXHAUSTED", "history_full", "start a new instance")
            if commands.get("reject") == "busy" or self.pending >= self.max_queue:
                raise Reject("UNAVAILABLE", "busy", "try again later")
            content = [pb.ContentPart.FromString(p.SerializeToString()) for p in request.content]
            for part in content:  # the media type has the form of section 5.1
                kind = part.WhichOneof("kind")
                if kind != "text":
                    ref = getattr(part, kind).file
                    ref.mime_type = normalize_mime(ref.mime_type)
            user = self._add(content=content, status=STATUS.MESSAGE_STATUS_COMPLETED,
                             reply_to_message_id=request.reply_to_message_id
                             if request.HasField("reply_to_message_id") else None,
                             idempotency_key=request.idempotency_key
                             if request.HasField("idempotency_key") else None)
            self.pending += 1
            if user.HasField("idempotency_key"):
                self.keys[user.idempotency_key] = user.message_id
            job = {"user": user.message_id, "text": rest, "commands": commands, "answer": None}
            if "pending" in commands:
                job["answer"] = self._add(
                    status=STATUS.MESSAGE_STATUS_PENDING, from_service=True,
                    reply_to_message_id=user.message_id,
                    pinned=commands.get("pin") is True).message_id
            self.jobs.put(job)
            return pb.Message.FromString(user.SerializeToString()), False

    def get_messages(self, request):
        """chat.md section 6.2. Return a list of messages."""
        if request.conversation_id:
            raise Reject("NOT_FOUND", "not_found", "there is only the main conversation")
        if request.limit and request.last:
            raise bad_request("limit and last cannot go together")
        with self.lock:
            if request.after_sequence > self.latest:
                raise bad_request("after_sequence is more than latest")
            first = request.after_sequence + 1
            end = self.latest if not request.before_sequence else min(self.latest, request.before_sequence - 1)
            if end < first:
                return []
            if request.last:
                first = max(first, end - min(request.last, READ_LIMIT) + 1)
            else:
                end = min(end, first + (request.limit or READ_LIMIT) - 1, first + READ_LIMIT - 1)
            return [pb.Message.FromString(m.SerializeToString()) for m in self.messages[first - 1:end]]

    def set_pinned(self, request):
        if request.conversation_id:
            raise Reject("NOT_FOUND", "not_found", "there is only the main conversation")
        with self.lock:
            message = self.by_id.get(request.message_id)
            if message is None:
                raise Reject("NOT_FOUND", "not_found", "there is no such message")
            self._pin(message, request.pinned)
            return pb.Message.FromString(message.SerializeToString())

    def subscribe(self, events, after_sequence):
        """Put the replay and `SubscriptionReady` in `events`, then the live events."""
        with self.lock:
            if after_sequence > self.latest:
                raise bad_request("after_sequence is more than latest")
            for message in self.messages[after_sequence:]:
                events.put(pb.ServerEvent(message_created=pb.MessageCreated(message=message)))
            events.put(pb.ServerEvent(subscription_ready=pb.SubscriptionReady(
                latest_sequence=self.latest)))
            self.subscribers.append(events)

    def unsubscribe(self, events):
        with self.lock:
            if events in self.subscribers:
                self.subscribers.remove(events)

    def repeat(self, events, message):
        """The `MessageCreated` of a send that was a repeat goes to the stream of the repeat."""
        events.put(pb.ServerEvent(message_created=pb.MessageCreated(message=message)))

    def check_acknowledge(self, last_sequence):
        with self.lock:
            if last_sequence > self.latest:
                raise bad_request("last_sequence is more than latest")

    # Files (section 6.4).

    def has_file(self, value):
        with self.lock:
            return value in self.files

    def get_file(self, value):
        with self.lock:
            return self.files.get(value)

    def put_file(self, value, data):
        with self.lock:
            if value not in self.files:
                if self.file_bytes + len(data) > MAX_FILE_BYTES:
                    raise Reject("RESOURCE_EXHAUSTED", "history_full", "the files are too many")
                self.files[value] = data
                self.file_bytes += len(data)

    # The answers.

    def _work(self):
        while True:
            job = self.jobs.get()
            if job is None:
                return
            try:
                self._answer(job)
            except Exception as error:  # an answer must end (section 5.2)
                self._fail(job, "the echo failed: %r" % (error,))
            finally:
                with self.lock:
                    self.pending -= 1

    def _fail(self, job, error):
        with self.lock:
            answer = self.by_id.get(job["answer"] or "")
            if answer is None:
                answer = self._add(from_service=True, reply_to_message_id=job["user"],
                                   status=STATUS.MESSAGE_STATUS_IN_PROGRESS)
            if answer.status in (STATUS.MESSAGE_STATUS_PENDING, STATUS.MESSAGE_STATUS_IN_PROGRESS):
                answer.metadata["error"] = error
                self._set_status(answer, STATUS.MESSAGE_STATUS_FAILED)

    def _answer(self, job):
        commands, text = job["commands"], job["text"]
        if commands.get("delay"):
            time.sleep(commands["delay"] / 1000)
        streaming = (any(c in commands for c in ("stream", "reasoning", "fail"))
                     or self.stream in ("text", "reasoning"))
        reasoning = "reasoning" in commands or self.stream == "reasoning"
        pinned = commands.get("pin") is True
        with self.lock:
            user = self.by_id[job["user"]]
            parts = [p for p in user.content if p.WhichOneof("kind") != "text"] \
                if "attach" in commands else []
            if not streaming:
                content = [pb.ContentPart(text=pb.TextContent(text=text))] + parts
                if job["answer"]:
                    answer = self.by_id[job["answer"]]
                    answer.content.extend(content)
                    self._set_status(answer, STATUS.MESSAGE_STATUS_COMPLETED)
                else:
                    answer = self._add(content=content, from_service=True, pinned=pinned,
                                       reply_to_message_id=user.message_id,
                                       status=STATUS.MESSAGE_STATUS_COMPLETED)
                answer_id = answer.message_id
            else:
                if job["answer"]:
                    answer = self.by_id[job["answer"]]
                    answer.content.extend(parts)
                    self._set_status(answer, STATUS.MESSAGE_STATUS_IN_PROGRESS)
                else:
                    answer = self._add(content=parts, from_service=True, pinned=pinned,
                                       reply_to_message_id=user.message_id,
                                       status=STATUS.MESSAGE_STATUS_IN_PROGRESS)
                answer_id = answer.message_id
        if streaming:
            self._stream(answer_id, text, commands, reasoning)
        if commands.get("pin") == "late":
            with self.lock:
                self._pin(self.by_id[answer_id], True)

    def _delta(self, answer_id, text="", reasoning=""):
        with self.lock:
            answer = self.by_id[answer_id]
            if text:
                if answer.content and answer.content[-1].WhichOneof("kind") == "text":
                    answer.content[-1].text.text += text
                else:
                    answer.content.add().text.text = text
            answer.reasoning += reasoning
            self._publish(message_delta=pb.MessageDelta(
                message_id=answer_id, text_append=text, reasoning_append=reasoning))

    def _stream(self, answer_id, text, commands, reasoning):
        pause = commands["stream"] if isinstance(commands.get("stream"), int) else self.delay_ms
        words = re.findall(r"\S+\s*|\s+", text)
        if reasoning:
            for piece in ("Reading the message. ", "It has %d word(s). " % len(words),
                          "I will echo it."):
                time.sleep(pause / 1000)
                self._delta(answer_id, reasoning=piece)
        if "fail" in commands:
            words = words[:max(1, len(words) // 2)]
        for word in words:
            time.sleep(pause / 1000)
            self._delta(answer_id, text=word)
        with self.lock:
            answer = self.by_id[answer_id]
            if "fail" in commands:
                answer.metadata["error"] = "the answer failed because the message has /fail"
                self._set_status(answer, STATUS.MESSAGE_STATUS_FAILED)
            else:
                self._set_status(answer, STATUS.MESSAGE_STATUS_COMPLETED)
