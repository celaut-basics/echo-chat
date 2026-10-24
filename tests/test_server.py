"""Tests of the gRPC server of the echo service (chat.md revision 6).

`ConformanceTest` runs the conformance runner (conformance/runner.py, the tests of
section 11.1) against a server in this process. The other tests check what is particular
to the echo: the answer, the commands at the start of a text and the options of `Chat`.

Needs grpcio and protobuf (requirements-dev.txt).
"""
import hashlib
import io
import time
import unittest

import grpc

from conformance.runner import PROFILES, Conformance
from service import chat_pb2 as pb
from service import chat_pb2_grpc as rpc
from service.server import make_server
from service.store import Chat, parse_commands


class ServerCase(unittest.TestCase):
    chat_options = {}

    def setUp(self):
        self.server, port, self.chat = make_server("127.0.0.1", 0, Chat(**self.chat_options))
        self.server.start()
        self.channel = grpc.insecure_channel("127.0.0.1:%d" % port)
        self.stub = rpc.ChatServiceStub(self.channel)

    def tearDown(self):
        self.channel.close()
        self.server.stop(0)
        self.chat.close()

    def send(self, text, **fields):
        return self.stub.SendMessage(pb.SendMessageRequest(
            content=[pb.ContentPart(text=pb.TextContent(text=text))], **fields))

    def history(self, wait=0.0):
        time.sleep(wait)
        return list(self.stub.GetMessages(pb.GetMessagesRequest()))

    def wait_final(self, count):
        end = time.time() + 10
        while time.time() < end:
            messages = self.history()
            if len(messages) >= count and all(
                    m.status == pb.MESSAGE_STATUS_COMPLETED or m.status == pb.MESSAGE_STATUS_FAILED
                    for m in messages):
                return messages
            time.sleep(0.05)
        self.fail("the answers are not final")


def body(message):
    return "".join(p.text.text for p in message.content)


class ConformanceTest(unittest.TestCase):
    def test_the_conformance_tests_of_section_11_1(self):
        server, port, chat = make_server("127.0.0.1", 0, Chat(max_messages=600, max_queue=4))
        server.start()
        try:
            out = io.StringIO()
            run = Conformance("127.0.0.1:%d" % port, profile=PROFILES["echo"],
                              history_limit=600, queue_limit=4)
            passed = run.run(out=out)
            self.assertTrue(passed, out.getvalue())
            # The tests that do not apply to this service are skipped, and only those.
            skipped = {line.split()[2] for line in out.getvalue().splitlines() if line.startswith("skip")}
            self.assertEqual(skipped, {"42", "43", "44", "66", "67", "73"})
        finally:
            server.stop(0)
            chat.close()


class EchoTest(ServerCase):
    def test_the_answer_is_the_text_of_the_message_as_a_whole_answer(self):
        user = self.send("Hello", idempotency_key="k1")
        self.assertEqual((user.sequence, user.from_service, user.idempotency_key), (1, False, "k1"))
        user_message, answer = self.wait_final(2)
        self.assertEqual(user_message, user)
        self.assertEqual((answer.sequence, body(answer), answer.reply_to_message_id, answer.from_service),
                         (2, "Hello", user.message_id, True))
        self.assertEqual(answer.created_at, answer.updated_at)
        self.assertEqual(answer.reasoning, "")

    def test_the_same_key_gives_the_same_message_and_adds_nothing(self):
        first = self.send("one", idempotency_key="same")
        self.assertEqual(self.send("one", idempotency_key="same"), first)
        self.assertEqual(len(self.wait_final(2)), 2)

    def test_a_text_that_starts_with_a_word_that_is_not_a_command_is_echoed_whole(self):
        for text in ("/etc/passwd", "/stream=x a", "/pin=now", "/reject=no", " /stream a"):
            self.send(text)
        self.assertEqual([body(m) for m in self.wait_final(10)[1::2]],
                         ["/etc/passwd", "/stream=x a", "/pin=now", "/reject=no", " /stream a"])

    def test_echo_ends_the_commands(self):
        self.send("/echo /stream a b")
        self.assertEqual(body(self.wait_final(2)[1]), "/stream a b")

    def test_a_user_message_keeps_the_commands_in_its_text(self):
        user = self.send("/pin hello")
        self.assertEqual(body(user), "/pin hello")
        self.assertEqual(body(self.wait_final(2)[1]), "hello")

    def test_history_full_counts_the_answers_that_wait(self):
        self.chat.max_messages = 4
        self.send("/delay=300 one")
        self.send("two")
        with self.assertRaises(grpc.RpcError) as error:
            self.send("three")
        self.assertEqual(error.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
        self.assertEqual(len(self.wait_final(4)), 4)


class EchoStreamTest(ServerCase):
    chat_options = {"stream": "reasoning", "delay_ms": 1}

    def test_the_environment_makes_every_answer_stream_with_reasoning(self):
        self.send("one two  three")
        answer = self.wait_final(2)[1]
        self.assertEqual(body(answer), "one two  three")
        self.assertTrue(answer.reasoning)
        self.assertEqual(len(answer.content), 1)

    def test_pending_then_in_progress_then_final_with_one_event_each(self):
        events = self.subscribe_events()
        self.send("/pending /stream=100 a b")
        created = self.take(events, lambda e: e.WhichOneof("event") == "message_created", 2)
        self.assertEqual(created[1].message_created.message.status, pb.MESSAGE_STATUS_PENDING)
        changes = self.take(events, lambda e: e.WhichOneof("event") == "message_status_changed", 2)
        self.assertEqual([e.message_status_changed.status for e in changes],
                         [pb.MESSAGE_STATUS_IN_PROGRESS, pb.MESSAGE_STATUS_COMPLETED])

    def test_a_late_pin_is_an_event_after_the_answer_is_final(self):
        events = self.subscribe_events()
        self.send("/pin=late x")
        kinds = []
        for event in events:
            kinds.append(event.WhichOneof("event"))
            if kinds[-1] == "message_pinned_changed":
                break
        self.assertEqual(kinds[-3:], ["message_delta", "message_status_changed", "message_pinned_changed"])
        self.assertTrue(self.wait_final(2)[1].pinned)

    def test_a_failed_answer_keeps_what_it_made_and_tells_why(self):
        self.send("/fail one two three four")
        answer = self.wait_final(2)[1]
        self.assertEqual(answer.status, pb.MESSAGE_STATUS_FAILED)
        self.assertEqual(body(answer), "one two ")
        self.assertIn("/fail", answer.metadata["error"])

    def test_attachments_are_echoed_when_asked(self):
        data = b"\x89PNG\r\n\x1a\n" + b"x" * 10
        ref = pb.FileReference(hashes=[pb.Hash(type=hashlib.sha3_256(b"").digest(),
                                               value=hashlib.sha3_256(data).digest())],
                               mime_type="image/png", size_bytes=len(data))
        files = rpc.FileServiceStub(self.channel)
        files.UploadFile(iter([pb.UploadFileRequest(file=ref), pb.UploadFileRequest(chunk=data)]))
        part = pb.ContentPart(image=pb.ImageContent(file=ref))
        text = pb.ContentPart(text=pb.TextContent(text="/attach hi"))
        self.stub.SendMessage(pb.SendMessageRequest(content=[text, part]))
        answer = self.wait_final(2)[1]
        self.assertEqual([p.WhichOneof("kind") for p in answer.content], ["image", "text"])
        self.assertEqual(answer.content[0].image.file.hashes, ref.hashes)

    def subscribe_events(self):
        def gen():
            yield pb.ClientEvent(subscribe=pb.SubscribeRequest())
            time.sleep(30)

        call = self.stub.Connect(gen())
        self.addCleanup(call.cancel)
        first = next(call)
        self.assertEqual(first.WhichOneof("event"), "subscription_ready")
        return call

    def take(self, call, wanted, count):
        """The next `count` events of the stream for which `wanted(event)` is true."""
        found = []
        for event in call:
            if wanted(event):
                found.append(event)
                if len(found) == count:
                    return found
        self.fail("the stream ended")


class ParseCommandsTest(unittest.TestCase):
    def test_commands(self):
        self.assertEqual(parse_commands("/stream=20 /pin hello"), ({"stream": 20, "pin": True}, "hello"))
        self.assertEqual(parse_commands("/reasoning"), ({"reasoning": True}, ""))
        self.assertEqual(parse_commands("/delay=5 /pin=late /fail  two  spaces"),
                         ({"delay": 5, "pin": "late", "fail": True}, " two  spaces"))
        self.assertEqual(parse_commands("/reject=busy x"), ({"reject": "busy"}, "x"))
        self.assertEqual(parse_commands("plain /stream"), ({}, "plain /stream"))
        self.assertEqual(parse_commands("/delay x"), ({}, "/delay x"))
        self.assertEqual(parse_commands("/pin/stream x"), ({}, "/pin/stream x"))


if __name__ == "__main__":
    unittest.main()
