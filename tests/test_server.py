import http.client
import json
import threading
import unittest

from service.server import Chat, make_server


class ChatServerTest(unittest.TestCase):
    def setUp(self):
        self.chat = Chat(max_messages=6)
        self.server = make_server("127.0.0.1", 0, self.chat)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.conn = http.client.HTTPConnection(*self.server.server_address)

    def tearDown(self):
        self.conn.close()
        self.server.shutdown()
        self.server.server_close()

    def call(self, method, path, body=None, raw=None):
        data = raw if raw is not None else (None if body is None else json.dumps(body))
        self.conn.request(method, path, data, {"Content-Type": "application/json"})
        response = self.conn.getresponse()
        return response.status, json.loads(response.read())

    def send(self, expect, text):
        return self.call("POST", "/chat/messages", {"expect": expect, "text": text})

    def test_send_returns_user_and_echo(self):
        status, body = self.send(0, "Hello")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"count": 2, "messages": [
            {"seq": 0, "role": "user", "text": "Hello"},
            {"seq": 1, "role": "assistant", "text": "Hello"}]})

    def test_messages_are_numbered_in_order_on_one_connection(self):
        self.send(0, "one")
        _, body = self.send(2, "two")
        self.assertEqual([m["seq"] for m in body["messages"]], [2, 3])
        self.assertEqual(body["count"], 4)

    def test_out_of_sync_changes_nothing(self):
        self.send(0, "one")
        status, body = self.send(0, "one")
        self.assertEqual((status, body["error"], body["count"]), (409, "out_of_sync", 2))
        self.assertEqual(self.chat.count, 2)

    def test_retry_after_lost_answer_is_read_not_duplicated(self):
        self.send(0, "one")  # the client lost this answer
        _, history = self.call("GET", "/chat/messages?from=0")
        self.assertEqual([m["text"] for m in history["messages"]], ["one", "one"])
        self.assertEqual(history["count"], 2)

    def test_history_from(self):
        self.send(0, "one")
        self.send(2, "two")
        _, body = self.call("GET", "/chat/messages?from=3")
        self.assertEqual(body["count"], 4)
        self.assertEqual([m["seq"] for m in body["messages"]], [3])
        _, body = self.call("GET", "/chat/messages?from=4")
        self.assertEqual(body["messages"], [])

    def test_history_without_from_has_all(self):
        self.send(0, "one")
        _, body = self.call("GET", "/chat/messages")
        self.assertEqual(len(body["messages"]), 2)

    def test_history_from_more_than_count_is_bad_request(self):
        status, body = self.call("GET", "/chat/messages?from=1")
        self.assertEqual((status, body["error"]), (400, "bad_request"))
        status, _ = self.call("GET", "/chat/messages?from=x")
        self.assertEqual(status, 400)

    def test_bad_bodies(self):
        for body in [{"text": "a"}, {"expect": "0", "text": "a"}, {"expect": True, "text": "a"},
                     {"expect": -1, "text": "a"}, {"expect": 0}, {"expect": 0, "text": ""},
                     {"expect": 0, "text": 5}, [1]]:
            status, reply = self.call("POST", "/chat/messages", body)
            self.assertEqual((status, reply["error"]), (400, "bad_request"), body)
        status, _ = self.call("POST", "/chat/messages", raw="not json")
        self.assertEqual(status, 400)
        self.assertEqual(self.chat.count, 0)

    def test_history_full(self):
        for expect in (0, 2, 4):
            self.assertEqual(self.send(expect, "x")[0], 200)
        status, body = self.send(6, "x")
        self.assertEqual((status, body["error"]), (413, "history_full"))
        self.assertEqual(self.chat.count, 6)

    def test_busy_while_a_send_is_in_progress(self):
        self.chat.lock.acquire()
        try:
            status, body = self.send(0, "one")
        finally:
            self.chat.lock.release()
        self.assertEqual((status, body["error"]), (503, "busy"))
        self.assertEqual(self.chat.count, 0)

    def test_unknown_path_and_unknown_fields(self):
        self.assertEqual(self.call("GET", "/other")[0], 404)
        status, _ = self.call("POST", "/chat/messages",
                              {"expect": 0, "text": "a", "future": 1})
        self.assertEqual(status, 200)

    def test_utf8(self):
        _, body = self.send(0, "¿Qué tal? 你好")
        self.assertEqual(body["messages"][1]["text"], "¿Qué tal? 你好")


if __name__ == "__main__":
    unittest.main()
