"""The cards slot of the echo service (chatui docs/celaut/cards.md, revision 1) and the
`/cards` command of the chat (chat.md section 5.8). Tests 1 to 9 of cards.md section 10.1,
and tests 91 and 92 of chat.md."""

import hashlib
import queue
import threading
import time
import unittest

import grpc

from service import cards_pb2 as cpb
from service import cards_pb2_grpc as crpc
from service import chat_pb2 as pb
from service import chat_pb2_grpc as rpc
from service.cards import SHA3_256_TYPE, make_cards_server
from service.server import make_server
from service.store import Chat


class CardsCase(unittest.TestCase):
    def setUp(self):
        self.logs = []
        self.cards_server, cards_port, self.deck = make_cards_server("127.0.0.1", 0)
        self.deck.log = self.logs.append
        self.server, port, self.chat = make_server(
            "127.0.0.1", 0, Chat(cards=self.deck, cards_port=cards_port))
        self.cards_port = cards_port
        self.cards_server.start()
        self.server.start()
        self.chat_channel = grpc.insecure_channel("127.0.0.1:%d" % port)
        self.channel = grpc.insecure_channel("127.0.0.1:%d" % cards_port)
        self.stub = crpc.CardsServiceStub(self.channel)
        self.files = rpc.FileServiceStub(self.channel)
        self.chat_stub = rpc.ChatServiceStub(self.chat_channel)

    def tearDown(self):
        self.channel.close()
        self.chat_channel.close()
        self.server.stop(0)
        self.cards_server.stop(0)
        self.chat.close()

    def say(self, text):
        self.chat_stub.SendMessage(pb.SendMessageRequest(
            content=[pb.ContentPart(text=pb.TextContent(text=text))]))
        end = time.time() + 10
        while time.time() < end:
            messages = list(self.chat_stub.GetMessages(pb.GetMessagesRequest()))
            if messages and messages[-1].from_service and \
                    messages[-1].status == pb.MESSAGE_STATUS_COMPLETED:
                return messages
            time.sleep(0.05)
        self.fail("no answer")

    def watch(self):
        """A Watch stream read by a thread: (events queue, call)."""
        events = queue.Queue()
        call = self.stub.Watch(cpb.WatchRequest())

        def read():
            try:
                for e in call:
                    events.put(e)
            except grpc.RpcError:
                pass
        threading.Thread(target=read, daemon=True).start()
        return events, call

    def act(self, item, action, key, deck="shopping-list", undo=False):
        return self.stub.SendAction(cpb.SendActionRequest(
            deck_id=deck, item_id=item, action_id=action, idempotency_key=key, undo=undo,
            acted_at=int(time.time() * 1000)))

    def test_91_92_cards_answers_with_a_slot_link_to_the_cards_slot(self):
        messages = self.say("/cards")
        answer = messages[-1]
        links = [p.slot_link for p in answer.content if p.WhichOneof("kind") == "slot_link"]
        self.assertEqual([(l.port, l.label) for l in links], [(self.cards_port, "Open the list")])
        self.assertIn("shopping list", answer.content[0].text.text)
        again = list(self.chat_stub.GetMessages(pb.GetMessagesRequest()))[-1]
        self.assertEqual(again, answer)

    def test_1_watch_first_event_is_the_deck(self):
        events, call = self.watch()
        first = events.get(timeout=5)
        self.assertEqual(first.WhichOneof("event"), "deck")
        self.assertEqual(first.deck.deck_id, "")  # no deck before /cards
        self.say("/cards")
        second = events.get(timeout=5)
        self.assertEqual(second.deck.deck_id, "shopping-list")
        self.assertGreater(second.version, first.version)
        self.assertEqual(len(second.deck.cards), 8)
        self.assertEqual({a.direction for a in second.deck.actions},
                         {cpb.DIRECTION_LEFT, cpb.DIRECTION_RIGHT})
        call.cancel()

    def test_2_3_4_decisions_are_ok_at_once_and_counted_once(self):
        self.say("/cards")
        events, call = self.watch()
        events.get(timeout=5)
        before = len(list(self.chat_stub.GetMessages(pb.GetMessagesRequest())))
        self.assertEqual(self.act("milk", "basket", "k1"), cpb.SendActionResponse())
        self.assertEqual(self.act("milk", "basket", "k1"), cpb.SendActionResponse())
        changed = events.get(timeout=5)
        self.assertEqual(changed.card_changed.card.decision, "basket")
        self.assertEqual(len(self.deck.taken), 1)
        self.assertEqual(self.deck.decisions, {"milk": "basket"})
        # No chat message for a decision.
        self.assertEqual(len(list(self.chat_stub.GetMessages(pb.GetMessagesRequest()))), before)
        call.cancel()

    def test_5_late_decision_is_ok_and_ignored(self):
        self.say("/cards")
        self.act("milk", "basket", "k1", deck="old-deck")
        self.act("no-such-item", "basket", "k2")
        self.assertEqual(self.deck.decisions, {})
        self.assertTrue(any("late" in line for line in self.logs))

    def test_6_bad_key_is_bad_request(self):
        self.say("/cards")
        with self.assertRaises(grpc.RpcError) as caught:
            self.act("milk", "basket", "")
        self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertIn(("cards-reason", "bad_request"), caught.exception.trailing_metadata())

    def test_7_undo_clears_the_decision(self):
        self.say("/cards")
        self.act("milk", "basket", "k1")
        self.act("milk", "basket", "k2", undo=True)
        self.assertEqual(self.deck.decisions, {})
        self.assertFalse(self.deck.deck.cards[0].HasField("decision"))

    def test_8_images_by_hash(self):
        self.say("/cards")
        card = self.deck.deck.cards[0]
        ref = card.image.file
        digest = ref.hashes[0].value
        data = b"".join(r.chunk for r in self.files.DownloadFile(pb.DownloadFileRequest(
            hash=pb.Hash(type=SHA3_256_TYPE, value=digest))))
        self.assertEqual(hashlib.sha3_256(data).digest(), digest)
        self.assertEqual(len(data), ref.size_bytes)
        self.assertTrue(data.startswith(b"\x89PNG"))

    def test_9_two_watches_and_refresh_and_new(self):
        self.say("/cards")
        a, call_a = self.watch()
        b, call_b = self.watch()
        a.get(timeout=5)
        b.get(timeout=5)
        self.say("/cards refresh")
        ea, eb = a.get(timeout=5), b.get(timeout=5)
        self.assertEqual(ea, eb)
        self.assertEqual(ea.deck.deck_id, "shopping-list")
        self.assertEqual(len(ea.deck.cards), 9)
        self.say("/cards new")
        self.assertEqual(a.get(timeout=5).deck.deck_id, "shopping-list-2")
        call_a.cancel()
        call_b.cancel()


if __name__ == "__main__":
    unittest.main()
