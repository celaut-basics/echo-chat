#!/usr/bin/env python3
"""Conformance tests of chat.md revision 6, section 11.1, for any service of the interface.

    python3 -m conformance.runner HOST:PORT [--package service] [--profile echo|none]
                                  [--history-limit N] [--queue-limit N] [--only 6,7,20]

Run it on a NEW instance: tests 3 to 6 need an empty history. The runner is a gRPC client
over h2c. It uses the stubs of chat.proto from the Python package `--package`
(`<package>.chat_pb2` and `<package>.chat_pb2_grpc`).

A test that needs a way to make the service slow, stream or fail needs a profile: the text
prefixes that make the service do it (section 11.1: "A service can fail on request only if
it has a way to make it fail"). `--profile echo` has the commands of echo-chat. With
`--profile none` those tests are skipped. A test that does not apply to the service
(for example test 42 on a service that has `supports_fork` false) is skipped too.
Exit status: 0 if no test failed.

To reuse it for another service, give a profile for it (see `Profile`). The tests use only
the calls of chat.proto.
"""

import argparse
import hashlib
import importlib
import os
import queue
import socket
import struct
import sys
import threading
import time
import zlib
from concurrent import futures
from dataclasses import dataclass

import grpc

SHA3 = hashlib.sha3_256(b"").digest()


@dataclass
class Profile:
    """Prefixes of a text that make the service do something. None: it cannot."""
    stream: str = None  # the answer grows in deltas, slowly enough to see it
    slow: str = None  # as `stream`, and slow: the answer takes about 2 seconds
    reasoning: str = None  # as `stream`, with reasoning
    pin: str = None  # the answer is pinned when it is made
    fail: str = None  # the answer ends with FAILED
    reject_take: str = None  # the send fails with INTERNAL service_error
    delay: str = None  # the answer starts 400 ms after the send (to fill the queue)
    attach: str = None  # the answer has the attachments of the message


PROFILES = {
    "none": Profile(),
    "echo": Profile(stream="/stream=20 ", slow="/stream=400 ", reasoning="/reasoning /stream=20 ",
                    pin="/pin ", fail="/fail ", reject_take="/reject=service_error ",
                    delay="/delay=400 ", attach="/attach "),
}


class Skip(Exception):
    pass


class Failed(AssertionError):
    pass


def check(condition, message="check failed"):
    if not condition:
        raise Failed(message)


def png(extra=0):
    """A valid PNG of one pixel. `extra` bytes of a text chunk make it larger."""
    def chunk(kind, data):
        body = kind + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body))
    image = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    if extra:
        image += chunk(b"tEXt", b"x\x00" + os.urandom(extra))
    return image + chunk(b"IDAT", zlib.compress(b"\x00\x00\x00\x00")) + chunk(b"IEND", b"")


class Stream:
    """A `Connect` stream. `events` has the server events, then None when the call ends."""

    def __init__(self, stub, pb):
        self.pb = pb
        self.out = queue.Queue()
        self.events = queue.Queue()
        self.error = None

        def requests():
            while True:
                item = self.out.get()
                if item is None:
                    return
                yield item

        self.call = stub.Connect(requests())

        def read():
            try:
                for event in self.call:
                    self.events.put(event)
            except grpc.RpcError as error:
                self.error = error
            self.events.put(None)

        threading.Thread(target=read, daemon=True).start()

    def send(self, event_id=None, **event):
        self.out.put(self.pb.ClientEvent(event_id=event_id, **event))

    def get(self, timeout=10):
        try:
            return self.events.get(timeout=timeout)
        except queue.Empty:
            raise Failed("no event in %s seconds" % timeout)

    def quiet(self, timeout=0.5):
        """True if no event comes in `timeout` seconds."""
        try:
            event = self.events.get(timeout=timeout)
        except queue.Empty:
            return True
        self.events.put(event)
        return False

    def until(self, done, timeout=15):
        """The events up to the first event for which `done(event)` is true."""
        seen, end = [], time.time() + timeout
        while time.time() < end:
            event = self.get(max(0.1, end - time.time()))
            check(event is not None, "the stream ended: %s" % self.status())
            seen.append(event)
            if done(event):
                return seen
        raise Failed("the stream did not give the event")

    def status(self):
        return (self.error.code(), reason(self.error)) if self.error else None

    def close(self):
        self.out.put(None)
        self.call.cancel()


def reason(error):
    for key, value in error.trailing_metadata() or ():
        if key == "chat-reason":
            return value
    return None


FINAL = ("MESSAGE_STATUS_COMPLETED", "MESSAGE_STATUS_FAILED", "MESSAGE_STATUS_CANCELLED")


class Conformance:
    def __init__(self, target, package="service", profile=None, history_limit=None,
                 queue_limit=None):
        self.pb = importlib.import_module(package + ".chat_pb2")
        self.rpc = importlib.import_module(package + ".chat_pb2_grpc")
        self.target = target
        self.profile = profile or Profile()
        self.history_limit, self.queue_limit = history_limit, queue_limit
        self.channel = grpc.insecure_channel(target)
        self.chat = self.rpc.ChatServiceStub(self.channel)
        self.files = self.rpc.FileServiceStub(self.channel)
        self.s = {}  # what a test keeps for the next tests
        self.timeout = 30

    # Helpers.

    def status_name(self, message):
        return self.pb.MessageStatus.Name(message.status)

    def text(self, text):
        return self.pb.ContentPart(text=self.pb.TextContent(text=text))

    def ref(self, data, mime="image/png", **fields):
        return self.pb.FileReference(
            hashes=[self.pb.Hash(type=SHA3, value=hashlib.sha3_256(data).digest())],
            mime_type=mime, size_bytes=len(data), **fields)

    def image(self, ref):
        return self.pb.ContentPart(image=self.pb.ImageContent(file=ref))

    def request(self, *content, **fields):
        return self.pb.SendMessageRequest(
            content=[self.text(c) if isinstance(c, str) else c for c in content], **fields)

    def send(self, *content, key=None, **fields):
        return self.chat.SendMessage(self.request(*content, idempotency_key=key, **fields),
                                     timeout=self.timeout)

    def fails(self, call, status, why=None):
        """Check that `call()` ends with `status` and the reason `why`."""
        try:
            call()
        except grpc.RpcError as error:
            check(error.code() == status, "status %s, expected %s" % (error.code(), status))
            if why:
                check(reason(error) == why, "reason %r, expected %r" % (reason(error), why))
            return error
        raise Failed("the call worked, expected %s %s" % (status, why))

    def send_fails(self, request, status, why):
        before = self.latest()
        self.fails(lambda: self.chat.SendMessage(request, timeout=self.timeout), status, why)
        check(self.latest() == before, "latest changed")

    def read(self, **fields):
        return list(self.chat.GetMessages(self.pb.GetMessagesRequest(**fields), timeout=self.timeout))

    def all_messages(self):
        out = []
        while True:
            page = self.read(after_sequence=out[-1].sequence if out else 0)
            if not page:
                return out
            out += page

    def latest(self):
        last = self.read(last=1)
        return last[0].sequence if last else 0

    def get(self, message_id):
        found = [m for m in self.all_messages() if m.message_id == message_id]
        check(found, "the message is not in the history")
        return found[0]

    def answer_of(self, user, final=True, timeout=30):
        """Wait for the answer to `user`, as section 11.1 says."""
        end = time.time() + timeout
        while time.time() < end:
            for m in self.read(after_sequence=user.sequence):
                if m.from_service and m.reply_to_message_id == user.message_id and (
                        not final or self.status_name(m) in FINAL):
                    return m
            time.sleep(0.2)
        raise Failed("no answer to the message")

    def settle(self, timeout=60):
        """Wait until each user message has a final answer."""
        end = time.time() + timeout
        while time.time() < end:
            messages = self.all_messages()
            answered = {m.reply_to_message_id for m in messages
                        if m.from_service and self.status_name(m) in FINAL}
            if all(m.message_id in answered for m in messages if not m.from_service):
                return
            time.sleep(0.2)
        raise Failed("the answers are not final")

    def body(self, message):
        return "".join(p.text.text for p in message.content if p.WhichOneof("kind") == "text")

    def stream(self, after=0, channel=None):
        stub = self.chat if channel is None else self.rpc.ChatServiceStub(channel)
        stream = Stream(stub, self.pb)
        stream.send(subscribe=self.pb.SubscribeRequest(after_sequence=after))
        return stream

    def ready(self, stream):
        """The replay and `SubscriptionReady` of a new stream: (messages, latest)."""
        events = stream.until(lambda e: e.WhichOneof("event") == "subscription_ready")
        check(all(e.WhichOneof("event") == "message_created" for e in events[:-1]),
              "the replay has an event that is not MessageCreated")
        return [e.message_created.message for e in events[:-1]], events[-1].subscription_ready.latest_sequence

    def answer_events(self, stream, user_id, timeout=30):
        """The events of the answer to `user_id`, up to its final state."""
        answer = {}

        def done(e):
            kind = e.WhichOneof("event")
            if kind == "message_created":
                m = e.message_created.message
                if m.from_service and m.reply_to_message_id == user_id:
                    answer["id"] = m.message_id
                    return self.status_name(m) in FINAL
            if kind == "message_status_changed" and e.message_status_changed.message_id == answer.get("id"):
                return self.pb.MessageStatus.Name(e.message_status_changed.status) in FINAL
            return False

        return stream.until(done, timeout)

    def apply(self, events, answer_id):
        """Rebuild the text and the reasoning of a message from MessageCreated and deltas."""
        text, reasoning, status = "", "", None
        for e in events:
            kind = e.WhichOneof("event")
            if kind == "message_created" and e.message_created.message.message_id == answer_id:
                m = e.message_created.message
                text, reasoning, status = self.body(m), m.reasoning, self.status_name(m)
            elif kind == "message_delta" and e.message_delta.message_id == answer_id:
                text += e.message_delta.text_append
                reasoning += e.message_delta.reasoning_append
            elif kind == "message_status_changed" and e.message_status_changed.message_id == answer_id:
                status = self.pb.MessageStatus.Name(e.message_status_changed.status)
        return text, reasoning, status

    def need(self, *names):
        for name in names:
            if getattr(self.profile, name) is None:
                raise Skip("the profile has no way to make the service: " + name)

    def caps(self):
        return self.chat.GetCapabilities(self.pb.GetCapabilitiesRequest(), timeout=self.timeout)

    def accepts(self, mime):
        return mime in self.caps().accepted_mime_types

    def upload(self, data, mime="image/png", chunk=64 << 10, **fields):
        ref = self.ref(data, mime, **fields)

        def requests():
            yield self.pb.UploadFileRequest(file=ref)
            for start in range(0, len(data), chunk):
                yield self.pb.UploadFileRequest(chunk=data[start:start + chunk])

        return self.files.UploadFile(requests(), timeout=120).file

    def query(self, *hashes):
        requests = (self.pb.QueryFilesRequest(hash=h) for h in hashes)
        return list(self.files.QueryFiles(requests, timeout=self.timeout))

    def available(self, data):
        (response,) = self.query(self.pb.Hash(type=SHA3, value=hashlib.sha3_256(data).digest()))
        return response.available

    def download(self, hash_):
        return list(self.files.DownloadFile(self.pb.DownloadFileRequest(hash=hash_), timeout=120))

    def need_attachments(self, mime="image/png"):
        if not self.accepts(mime):
            raise Skip("the service does not accept " + mime)

    def need_fresh_png(self):
        if "png" not in self.s:
            self.t51()
        return self.s["png"]

    # The tests. Each method is named `t<number>` and has its row of section 11.1 in the doc.

    def t1(self):
        self.s["caps0"] = self.caps()
        host, port = self.target.rsplit(":", 1)
        with socket.create_connection((host.strip("[]"), int(port)), timeout=5) as sock:
            sock.sendall(b"GET /chat HTTP/1.1\r\nHost: %b\r\n\r\n" % host.encode())
            sock.settimeout(2)
            try:
                answer = sock.recv(4096)
            except OSError:
                answer = b""
        check(not answer.startswith(b"HTTP/1.1 2"), "the service answered HTTP/1.1 with success")
        check(self.caps() == self.s["caps0"], "the service stopped")

    def t2(self):
        caps = self.s.get("caps0") or self.caps()
        types = list(caps.accepted_mime_types)
        check(all(t == t.lower() and ";" not in t for t in types), "a type is not lower case")
        if any(t.startswith("image/") for t in types):
            check({"image/jpeg", "image/png"} <= set(types), "image types")
        if any(t.startswith("audio/") for t in types):
            check({"audio/mpeg", "audio/wav"} <= set(types), "audio types")
        self.s["caps0"] = caps

    def t3(self):
        check(self.read(after_sequence=0) == [], "the history is not empty: run on a new instance")

    def t4(self):
        stream = self.stream()
        first = stream.get()
        check(first.WhichOneof("event") == "subscription_ready", "the first event is not SubscriptionReady")
        check(first.subscription_ready.latest_sequence == 0)
        stream.close()

    def t5(self):
        self.fails(lambda: self.read(after_sequence=1), grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t6(self):
        m = self.send("one", key="t6")
        check(m.sequence == 1 and not m.from_service and self.status_name(m) == "MESSAGE_STATUS_COMPLETED")
        check(self.body(m) == "one" and m.idempotency_key == "t6" and m.message_id)
        self.s["t6"] = m

    def t2_after_a_send(self):
        check(self.caps() == self.s["caps0"], "GetCapabilities changed")

    def t7(self):
        a = self.answer_of(self.s["t6"])
        check(a.from_service and a.sequence > 1)
        check(self.status_name(a) in ("MESSAGE_STATUS_COMPLETED", "MESSAGE_STATUS_FAILED"))
        self.s["a7"] = a

    def t8(self):
        before = self.latest()
        again = self.send("one", key="t6")
        check((again.message_id, again.sequence) == (self.s["t6"].message_id, 1))
        time.sleep(0.5)
        check(self.latest() == before, "latest changed")
        answers = [m for m in self.all_messages() if m.reply_to_message_id == again.message_id]
        check(len(answers) == 1, "a second answer started")

    def t9(self):
        with futures.ThreadPoolExecutor(2) as pool:
            jobs = [pool.submit(self.send, "nine-%d" % i, key="t9-%d" % i) for i in range(2)]
            users = [j.result() for j in jobs]
        check(users[0].sequence != users[1].sequence)
        for user in users:
            check(self.answer_of(user).from_service)
        self.settle()

    def t10(self):
        self.need("slow")
        started = time.time()
        first = self.send(self.profile.slow + "a b c d e")
        second = self.send("ten", key="t10")
        check(time.time() - started < 2, "a send waited for the answer")
        check(self.answer_of(first).from_service)
        check(self.answer_of(second).from_service)

    def t11(self):
        (m,) = self.read(after_sequence=1, limit=1)
        check(m.sequence == 2)

    def t12(self):
        check(self.read(after_sequence=self.latest()) == [])

    def t12a(self):
        latest = self.latest()
        check([m.sequence for m in self.read(last=2)] == [latest - 1, latest])

    def t12b(self):
        check([m.sequence for m in self.read(before_sequence=3)] == [1, 2])

    def t12c(self):
        self.fails(lambda: self.read(limit=1, last=1), grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t13(self):
        pb = self.pb
        cases = [pb.SendMessageRequest(),
                 self.request(""),
                 pb.SendMessageRequest(content=[pb.ContentPart()]),
                 self.request("x", idempotency_key="a b"),
                 self.request("x", idempotency_key="k" * 65),
                 self.request("x", idempotency_key="")]
        for request in cases:
            self.send_fails(request, grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t14(self):
        text = "  x  é ✓"
        m = self.send(text)
        check(self.body(m) == text)
        check(self.body(self.get(m.message_id)) == text)

    def t15(self):
        texts = [["a" * 16384], ["\n" * 16384], ["b" * 8192, "c" * 8192]]
        for parts in texts:
            m = self.send(*parts)
            check(self.body(m) == "".join(parts), "the text changed")
            check(self.body(self.get(m.message_id)) == "".join(parts))

    def t16(self):
        raw = self.request("sixteen").SerializeToString() + b"\x98\x06\x01"  # field 99, varint
        call = self.channel.unary_unary("/ChatService/SendMessage", request_serializer=lambda b: b,
                                        response_deserializer=self.pb.Message.FromString)
        check(self.body(call(raw, timeout=self.timeout)) == "sixteen")

    def t17(self):
        self.send_fails(self.request("x", reply_to_message_id="nothing"),
                        grpc.StatusCode.NOT_FOUND, "not_found")

    def t18(self):
        if self.caps().supports_fork:
            raise Skip("supports_fork is true")
        self.send_fails(self.request("x", conversation_id="other"), grpc.StatusCode.NOT_FOUND, "not_found")

    def t19(self):
        a = self.s["a7"]
        one, two = self.get(a.message_id), self.get(a.message_id)
        check(one == two and one.updated_at == two.updated_at)

    def _stream_test(self, prefix, reasoning=False):
        stream = self.stream(self.latest())
        _, latest = self.ready(stream)
        user = self.send((prefix or "") + "one two three four", key=None)
        events = self.answer_events(stream, user.message_id)
        kinds = [e.WhichOneof("event") for e in events]
        check(kinds[0] == "message_created" and events[0].message_created.message.message_id == user.message_id,
              "the first event is not the user message")
        check(events[1].WhichOneof("event") == "message_created", "the second event is not the answer")
        answer_id = events[1].message_created.message.message_id
        check(set(kinds[2:]) <= {"message_delta", "message_status_changed"})
        text, thought, status = self.apply(events, answer_id)
        final = self.get(answer_id)
        check(status == "MESSAGE_STATUS_COMPLETED" == self.status_name(final), "the status is not COMPLETED")
        check(text == self.body(final), "the text of the events is not the text of the read")
        check(thought == final.reasoning, "the reasoning of the events is not the reasoning of the read")
        if reasoning:
            check(final.reasoning and final.reasoning not in text, "no reasoning, or the reasoning is in the text")
            check(any(e.WhichOneof("event") == "message_delta" and e.message_delta.reasoning_append
                      for e in events), "no reasoning_append")
        check(self.latest() == latest + 2, "latest is not the sequence of the answer")
        stream.close()

    def t20(self):
        self._stream_test(self.profile.stream)

    def t21(self):
        self.need("reasoning")
        self._stream_test(self.profile.reasoning, reasoning=True)

    def t22(self):
        self.settle()
        stream = self.stream()
        messages, latest = self.ready(stream)
        every = self.all_messages()
        check([m.sequence for m in messages] == list(range(1, latest + 1)) and latest == self.latest())
        check(messages == every, "the replay is not the final state")
        stream.close()

    def t23(self):
        self.need("slow")
        user = self.send(self.profile.slow + "a b c d e f")
        stream = self.stream()
        events = self.answer_events(stream, user.message_id)
        answer_id = [e.message_created.message.message_id for e in events
                     if e.WhichOneof("event") == "message_created"
                     and e.message_created.message.reply_to_message_id == user.message_id][0]
        text, _, status = self.apply(events, answer_id)
        check(text == self.body(self.get(answer_id)), "the events do not make the final text")
        check(status == "MESSAGE_STATUS_COMPLETED")
        stream.close()

    def t24(self):
        stream = Stream(self.chat, self.pb)
        stream.send(event_id="e1", send_message=self.request("x", idempotency_key="t24"))
        while True:
            event = stream.get()
            if event is None:
                break
        check(stream.status() == (grpc.StatusCode.FAILED_PRECONDITION, "bad_state"), str(stream.status()))

    def _session(self):
        """The stream of tests 25 to 27g and 44 to 45, with no event that waits in it."""
        if "st" not in self.s:
            self.s["st"] = self.stream(self.latest())
            self.ready(self.s["st"])
        self.settle()
        while not self.s["st"].quiet(0.3):
            self.s["st"].get()
        return self.s["st"]

    def t25(self):
        st = self._session()
        st.send(event_id="s2", subscribe=self.pb.SubscribeRequest())
        e = st.get()
        check(e.WhichOneof("event") == "error" and e.error.code == "bad_state" and e.error.event_id == "s2")
        user = self.send("after the second subscribe")
        self.answer_events(st, user.message_id)
        check(st.error is None, "the stream ended")

    def t26(self):
        st = self._session()
        st.send(event_id="e1", send_message=self.request("twenty-six", idempotency_key="k26"))
        first = st.get()
        check(first.WhichOneof("event") == "message_created")
        user = first.message_created.message
        check(user.idempotency_key == "k26" and not user.from_service)
        self.s["k26"] = user
        events = self.answer_events(st, user.message_id)
        check(all(e.WhichOneof("event") != "error" for e in events))

    def _protocol_error(self, code, event_id, **event):
        st = self._session()
        st.send(event_id=event_id, **event)
        e = st.get()
        check(e.WhichOneof("event") == "error", "no ProtocolError: %s" % e.WhichOneof("event"))
        check(e.error.code == code, "code %r, expected %r" % (e.error.code, code))
        if event_id is not None:
            check(e.error.event_id == event_id, "event_id %r" % e.error.event_id)
        check(st.error is None, "the stream ended")

    def t27(self):
        before = self.latest()
        self._protocol_error("bad_request", "e2", send_message=self.pb.SendMessageRequest())
        check(self.latest() == before)

    def t27a(self):
        before = self.latest()
        self._protocol_error("bad_request", "e1", send_message=self.request("again", idempotency_key="k27a"))
        check(self.latest() == before)

    def t27b(self):
        self._protocol_error("bad_request", "x" * 65, send_message=self.request("long id"))

    def t27c(self):
        st, before = self._session(), self.latest()
        st.send(event_id="e7", send_message=self.request("twenty-six", idempotency_key="k26"))
        e = st.get()
        check(e.WhichOneof("event") == "message_created")
        same = e.message_created.message
        check((same.message_id, same.sequence) == (self.s["k26"].message_id, self.s["k26"].sequence))
        check(st.quiet(), "an event came: a second answer started")
        check(self.latest() == before, "latest changed")

    def t27d(self):
        self.need_attachments()
        pdf = self.pb.FileReference(hashes=[self.pb.Hash(type=SHA3, value=b"\0" * 32)],
                                    mime_type="application/pdf", size_bytes=1)
        self._protocol_error("unsupported_media", "e3", send_message=self.request(self.image(pdf)))

    def t27e(self):
        self.settle()
        st, before = self._session(), self.all_messages()
        st.send(acknowledge=self.pb.AcknowledgeEvent(last_sequence=self.latest()))
        check(st.quiet(), "an event came")
        self.s["acked"] = self.latest()
        check(self.all_messages() == before, "the history changed")

    def t27f(self):
        self._protocol_error("bad_request", "e4",
                             acknowledge=self.pb.AcknowledgeEvent(last_sequence=self.latest() + 1))

    def t27g(self):
        st, before = self._session(), self.all_messages()
        st.send(acknowledge=self.pb.AcknowledgeEvent(last_sequence=max(0, self.s.get("acked", 1) - 1)))
        check(st.quiet(), "an event came")
        check(self.all_messages() == before)

    def t28(self):
        stream = self.stream(self.latest() + 1)
        while stream.get() is not None:
            pass
        check(stream.status() == (grpc.StatusCode.INVALID_ARGUMENT, "bad_request"), str(stream.status()))

    def t29(self):
        self.need("fail")
        user = self.send(self.profile.fail + "one two three four")
        check(self.status_name(self.answer_of(user)) == "MESSAGE_STATUS_FAILED")
        after = self.send("after the failure")
        check(self.status_name(self.answer_of(after)) == "MESSAGE_STATUS_COMPLETED")

    def t30(self):
        self.need("reject_take")
        self.send_fails(self.request(self.profile.reject_take + "x"), grpc.StatusCode.INTERNAL, "service_error")

    def t33(self):
        a = self.s["a7"]
        before = self.get(a.message_id)
        stream = self.stream(self.latest())
        self.ready(stream)
        m = self.chat.SetMessagePinned(self.pb.SetMessagePinnedRequest(message_id=a.message_id, pinned=True))
        check(m.pinned and m.updated_at >= before.updated_at)
        e = stream.until(lambda e: e.WhichOneof("event") == "message_pinned_changed")[-1].message_pinned_changed
        check(e.message_id == a.message_id and e.pinned)
        self.s["pinned"] = m
        stream.close()

    def t34(self):
        u = self.s["t6"]
        m = self.chat.SetMessagePinned(self.pb.SetMessagePinnedRequest(message_id=u.message_id, pinned=True))
        check(m.pinned and m.content == u.content and m.status == u.status)

    def t35(self):
        a = self.s["pinned"]
        stream = self.stream(self.latest())
        self.ready(stream)
        m = self.chat.SetMessagePinned(self.pb.SetMessagePinnedRequest(message_id=a.message_id, pinned=True))
        check(m.pinned and m.updated_at == a.updated_at, "updated_at changed")
        check(stream.quiet(), "an event came")
        stream.close()

    def t36(self):
        a = self.s["pinned"]
        m = self.chat.SetMessagePinned(self.pb.SetMessagePinnedRequest(message_id=a.message_id, pinned=False))
        check(not m.pinned and m.updated_at >= a.updated_at)
        check(not self.get(a.message_id).pinned)
        self.chat.SetMessagePinned(self.pb.SetMessagePinnedRequest(
            message_id=self.s["t6"].message_id, pinned=False))

    def t37(self):
        self.fails(lambda: self.chat.SetMessagePinned(
            self.pb.SetMessagePinnedRequest(message_id="nothing", pinned=True)), grpc.StatusCode.NOT_FOUND)

    def t38(self):
        self.need("slow")
        user = self.send(self.profile.slow + "a b c d e f")
        answer = self.answer_of(user, final=False)
        m = self.chat.SetMessagePinned(self.pb.SetMessagePinnedRequest(message_id=answer.message_id, pinned=True))
        check(m.pinned)
        final = self.answer_of(user)
        check(final.pinned and self.body(final).split() == ["a", "b", "c", "d", "e", "f"],
              "the answer did not keep growing, or is not pinned")

    def t39(self):
        self.need("pin")
        stream = self.stream(self.latest())
        self.ready(stream)
        user = self.send(self.profile.pin + "pinned")
        events = self.answer_events(stream, user.message_id)
        created = [e.message_created.message for e in events if e.WhichOneof("event") == "message_created"][1]
        check(created.pinned, "MessageCreated is not pinned")
        check(self.answer_of(user).pinned and self.get(created.message_id).pinned)
        stream.close()

    def t40(self):
        c = self.chat.GetConversation(self.pb.GetConversationRequest())
        check(c.conversation_id == "" and c.created_at > 0 and not c.archived)

    def t41(self):
        if self.caps().supports_fork:
            raise Skip("supports_fork is true")
        self.fails(lambda: self.chat.ForkConversation(self.pb.ForkConversationRequest(
            forked_from_message_id=self.s["t6"].message_id)), grpc.StatusCode.UNIMPLEMENTED)

    def t42(self):
        if not self.caps().supports_fork:
            raise Skip("supports_fork is false")
        c = self.chat.ForkConversation(self.pb.ForkConversationRequest(
            forked_from_message_id=self.s["t6"].message_id))
        check(c.conversation_id)
        check(self.read(conversation_id=c.conversation_id)[0].sequence == 1)

    def t43(self):
        if not self.caps().supports_cancel:
            raise Skip("supports_cancel is false")
        self.need("slow")
        stream = self.stream(self.latest())
        self.ready(stream)
        user = self.send(self.profile.slow + "a b c d e f")
        answer = self.answer_of(user, final=False)
        stream.send(event_id="c43", cancel_message=self.pb.CancelMessageRequest(message_id=answer.message_id))
        stream.until(lambda e: e.WhichOneof("event") == "message_status_changed" and
                     self.pb.MessageStatus.Name(e.message_status_changed.status) == "MESSAGE_STATUS_CANCELLED")
        stream.close()

    def t44(self):
        if not self.caps().supports_cancel:
            raise Skip("supports_cancel is false")
        st = self._session()
        st.send(event_id="e5", cancel_message=self.pb.CancelMessageRequest(message_id=self.s["a7"].message_id))
        e = st.get()
        check(e.WhichOneof("event") == "error" and e.error.code == "bad_state" and e.error.event_id == "e5")

    def t45(self):
        if self.caps().supports_cancel:
            raise Skip("supports_cancel is true")
        self._protocol_error("unimplemented", "e6",
                             cancel_message=self.pb.CancelMessageRequest(message_id=self.s["a7"].message_id))

    def t46(self):
        channel = grpc.insecure_channel(self.target)
        stream = self.stream(channel=channel)
        self.ready(stream)
        stub = self.rpc.ChatServiceStub(channel)
        for _ in range(5):
            stub.GetCapabilities(self.pb.GetCapabilitiesRequest(), timeout=5)
        check(stream.error is None, "the stream ended")
        stream.close()
        channel.close()

    def t47(self):
        channel = grpc.insecure_channel(self.target)
        stream = self.stream(channel=channel)
        self.ready(stream)
        stub = self.rpc.ChatServiceStub(channel)
        calls = [stub.GetCapabilities.future(self.pb.GetCapabilitiesRequest(), timeout=10) for _ in range(7)]
        check(all(c.result() is not None for c in calls) and stream.error is None)
        stream.close()
        channel.close()

    def t48(self):
        self.need_attachments()
        a, b = (self.pb.Hash(type=SHA3, value=hashlib.sha3_256(os.urandom(8)).digest()),
                self.pb.Hash(value=hashlib.sha3_256(os.urandom(8)).digest()))
        first, second = self.query(a, b)
        check(first.hash.type == SHA3 and second.hash.type == SHA3 and not first.available and not second.available)
        check((first.hash.value, second.hash.value) == (a.value, b.value), "the order changed")

    def t49(self):
        self.need_attachments()
        self.fails(lambda: self.query(self.pb.Hash(value=b"\0" * 32)),
                   grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t50(self):
        self.need_attachments()
        (r,) = self.query(self.pb.Hash(type=b"unknown algorithm", value=b"\0" * 16))
        check(not r.available)

    def t51(self):
        self.need_attachments("image/png")
        data = png(150_000)
        ref = self.upload(data, filename="t51.png")
        check(any(h.type == SHA3 and h.value == hashlib.sha3_256(data).digest() for h in ref.hashes))
        check(all(h.HasField("type") for h in ref.hashes) and ref.size_bytes == len(data))
        self.s["png"], self.s["png_ref"] = data, ref

    def t52(self):
        check(self.available(self.need_fresh_png()))

    def t53(self):
        data = self.need_fresh_png()
        chunks = self.download(self.pb.Hash(type=SHA3, value=hashlib.sha3_256(data).digest()))
        check(all(len(c.chunk) <= 64 << 10 for c in chunks), "a chunk is above 64 KiB")
        check(b"".join(c.chunk for c in chunks) == data and len(chunks) > 1)

    def t54(self):
        self.need_attachments()
        data = png(100)
        ref = self.ref(data)
        ref.hashes[0].value = hashlib.sha3_256(b"another").digest()
        self.fails(lambda: self.files.UploadFile(iter([self.pb.UploadFileRequest(file=ref),
                                                       self.pb.UploadFileRequest(chunk=data)])),
                   grpc.StatusCode.INVALID_ARGUMENT, "hash_mismatch")
        check(not self.available(b"another"))

    def t55(self):
        self.need_attachments()
        data = png(100)
        ref = self.ref(data)
        ref.size_bytes += 1
        self.fails(lambda: self.files.UploadFile(iter([self.pb.UploadFileRequest(file=ref),
                                                       self.pb.UploadFileRequest(chunk=data)])),
                   grpc.StatusCode.INVALID_ARGUMENT, "hash_mismatch")
        check(not self.available(data))

    def t56(self):
        self.need_attachments()
        data, up = png(100), self.pb.UploadFileRequest
        self.fails(lambda: self.files.UploadFile(iter([up(chunk=data), up(file=self.ref(data))])),
                   grpc.StatusCode.INVALID_ARGUMENT, "bad_request")
        self.fails(lambda: self.files.UploadFile(iter([up(file=self.ref(data)), up(chunk=data[:10]),
                                                       up(file=self.ref(data))])),
                   grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t57(self):
        data = b"%PDF-1.4 not an image"
        ref = self.ref(data, "application/pdf")
        self.fails(lambda: self.files.UploadFile(iter([self.pb.UploadFileRequest(file=ref),
                                                       self.pb.UploadFileRequest(chunk=data)])),
                   grpc.StatusCode.INVALID_ARGUMENT, "unsupported_media")
        check(not self.available(data))

    def t58(self):
        self.need_attachments()
        data = png(0) + b"\0" * ((4 << 20) - len(png(0)))
        check(self.upload(data).size_bytes == 4 << 20)

    def t59(self):
        self.need_attachments()
        data = png(0) + b"\1" * ((4 << 20) + 1 - len(png(0)))
        ref = self.ref(data)
        self.fails(lambda: self.files.UploadFile(iter([self.pb.UploadFileRequest(file=ref)] + [
            self.pb.UploadFileRequest(chunk=data[s:s + (64 << 10)]) for s in range(0, len(data), 64 << 10)])),
                   grpc.StatusCode.INVALID_ARGUMENT, "too_large")
        check(not self.available(data))

    def t60(self):
        data = self.need_fresh_png()
        check(self.upload(data, filename="t51.png") == self.s["png_ref"])

    def t61(self):
        self.need_fresh_png()
        m = self.send(self.image(self.s["png_ref"]))
        (part,) = m.content
        check(part.WhichOneof("kind") == "image" and part.image.file.mime_type == "image/png")
        check(part.image.file.hashes[0].value == self.s["png_ref"].hashes[0].value)
        check(part.image.file.size_bytes == len(self.s["png"]))
        self.s["t61"] = m

    def t62(self):
        self.need_fresh_png()
        if "t61" not in self.s:
            self.t61()
        h = self.get(self.s["t61"].message_id).content[0].image.file.hashes[0]
        check(b"".join(c.chunk for c in self.download(h)) == self.s["png"])

    def t63(self):
        self.need_attachments()
        ghost = self.ref(os.urandom(32))
        self.send_fails(self.request(self.image(ghost)), grpc.StatusCode.FAILED_PRECONDITION, "file_missing")

    def t64(self):
        self.need_attachments()
        pdf = self.ref(b"pdf", "application/pdf")
        self.send_fails(self.request(self.image(pdf)), grpc.StatusCode.INVALID_ARGUMENT, "unsupported_media")
        wav = self.ref(b"wav", "audio/wav")
        self.send_fails(self.request(self.image(wav)), grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t65(self):
        self.need_attachments()
        ref = self.ref(self.need_fresh_png())
        self.send_fails(self.request(*[self.image(ref)] * 5), grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t66(self):
        if self.caps().accepted_mime_types:
            raise Skip("the service accepts attachments")
        self.send_fails(self.request(self.image(self.ref(b"x"))), grpc.StatusCode.INVALID_ARGUMENT,
                        "unsupported_media")

    def t67(self):
        types = self.caps().accepted_mime_types
        other = [m for m in ("image/jpeg", "audio/mpeg") if m not in types and
                 not any(t.split("/")[0] == m.split("/")[0] for t in types)]
        if not types or not other:
            raise Skip("the service accepts each kind")
        part = self.image(self.ref(b"x", other[0])) if other[0].startswith("image") else \
            self.pb.ContentPart(audio=self.pb.AudioContent(file=self.ref(b"x", other[0])))
        self.send_fails(self.request(part), grpc.StatusCode.INVALID_ARGUMENT, "unsupported_media")

    def t68(self):
        self.need_fresh_png()
        ref = self.pb.FileReference(hashes=self.s["png_ref"].hashes, mime_type="IMAGE/PNG",
                                    size_bytes=len(self.s["png"]))
        m = self.send(self.image(ref))
        check(m.content[0].image.file.mime_type == "image/png")

    def t69(self):
        self.need_attachments()
        refs = [self.upload(png(0) + os.urandom((4 << 20) - len(png(0)))) for _ in range(4)]
        check(all(r.size_bytes == 4 << 20 for r in refs))
        m = self.send(*[self.image(r) for r in refs])
        check(len(m.content) == 4)

    def t70(self):
        self.need_attachments("audio/wav")
        data = b"RIFF" + os.urandom(2000)
        ref = self.upload(data, "audio/wav", filename="voice.wav")
        m = self.send(self.pb.ContentPart(audio=self.pb.AudioContent(file=ref, duration_ms=1234)))
        check(m.content[0].audio.duration_ms == 1234)
        check(self.get(m.message_id).content[0].audio.duration_ms == 1234)

    def t71(self):
        self.need_attachments()
        self.fails(lambda: self.download(self.pb.Hash(type=SHA3, value=os.urandom(32))), grpc.StatusCode.NOT_FOUND)
        self.fails(lambda: self.download(self.pb.Hash(value=os.urandom(32))),
                   grpc.StatusCode.INVALID_ARGUMENT, "bad_request")

    def t72(self):
        self.need("attach")
        self.need_fresh_png()
        user = self.send(self.text(self.profile.attach + "back"), self.image(self.s["png_ref"]))
        answer = self.answer_of(user)
        parts = [p for p in answer.content if p.WhichOneof("kind") == "image"]
        check(parts, "the answer has no attachment")
        check(b"".join(c.chunk for c in self.download(parts[0].image.file.hashes[0])) == self.s["png"])

    def t73(self):
        raise Skip("the service implements FileService")

    def t74(self):
        call = self.channel.unary_unary("/ChatService/NoSuchMethod", request_serializer=lambda b: b)
        self.fails(lambda: call(b"", timeout=5), grpc.StatusCode.UNIMPLEMENTED)

    def t32(self):
        """The queue is full (optional)."""
        self.need("delay")
        if not self.queue_limit:
            raise Skip("give --queue-limit")
        self.settle()
        busy = None
        for i in range(self.queue_limit + 4):
            before = self.latest()
            try:
                self.send(self.profile.delay + "queue %d" % i)
            except grpc.RpcError as error:
                busy = error
                check(error.code() == grpc.StatusCode.UNAVAILABLE and reason(error) == "busy",
                      "%s %s" % (error.code(), reason(error)))
                check(self.latest() == before, "latest changed")
                break
        check(busy is not None, "the queue never was full")
        self.settle()

    def t31(self):
        """The history is full (optional)."""
        if not self.history_limit:
            raise Skip("give --history-limit")
        self.settle()
        for _ in range(self.history_limit):
            before = self.latest()
            try:
                self.send("fill")
            except grpc.RpcError as error:
                check(error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED and reason(error) == "history_full",
                      "%s %s" % (error.code(), reason(error)))
                check(self.latest() == before, "latest changed")
                return
        raise Failed("the history never was full")

    def t2_last(self):
        check(self.caps() == self.s["caps0"], "GetCapabilities changed during the tests")

    ORDER = ("1 2 3 4 5 6 2_after_a_send 7 8 9 10 11 12 12a 12b 12c 13 14 15 16 17 18 19 20 21 22 23 "
             "24 25 26 27 27a 27b 27c 27d 27e 27f 27g 28 29 30 33 34 35 36 37 38 39 40 41 42 43 44 45 "
             "46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 "
             "32 31 2_last").split()

    def run(self, only=None, out=sys.stdout):
        results = {"ok": 0, "FAIL": 0, "skip": 0}
        for name in self.ORDER:
            if only and name.replace("_after_a_send", "").replace("_last", "") not in only:
                continue
            try:
                getattr(self, "t" + name)()
                state = "ok"
            except Skip as why:
                state = "skip"
                detail = str(why)
            except (Failed, grpc.RpcError, KeyError, IndexError, ValueError) as error:
                state = "FAIL"
                detail = error.__class__.__name__ + ": " + (
                    "%s %s" % (error.code(), reason(error)) if isinstance(error, grpc.RpcError)
                    else str(error))
            results[state] += 1
            print("%-5s test %-16s %s" % (state, name.replace("_", " "),
                                          detail if state != "ok" else ""), file=out, flush=True)
        for key in ("st",):
            if key in self.s:
                self.s[key].close()
        print("%(ok)d passed, %(FAIL)d failed, %(skip)d skipped" % results, file=out)
        return results["FAIL"] == 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("target", help="HOST:PORT of the h2c port of the service")
    parser.add_argument("--package", default="service", help="package of chat_pb2 (default: service)")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="none")
    parser.add_argument("--history-limit", type=int, help="messages that the instance holds (test 31)")
    parser.add_argument("--queue-limit", type=int, help="answers that can wait (test 32)")
    parser.add_argument("--only", help="comma-separated test numbers")
    args = parser.parse_args(argv)
    run = Conformance(args.target, args.package, PROFILES[args.profile], args.history_limit, args.queue_limit)
    return 0 if run.run(set(args.only.split(",")) if args.only else None) else 1


if __name__ == "__main__":
    sys.exit(main())
