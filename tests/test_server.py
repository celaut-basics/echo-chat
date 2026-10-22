import base64
import http.client
import json
import threading
import unittest

from service.server import Chat, make_server


class ServerCase(unittest.TestCase):
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



class ChatServerTest(ServerCase):
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


PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256))


def b64(data):
    return base64.b64encode(data).decode()


class AttachmentTest(ServerCase):
    """Revision 3, sections 5.1, 6.1, 6.2, 6.4 and tests 19 to 26 of section 10.1."""

    def setUp(self):
        super().setUp()
        self.chat.max_messages = 100

    def get(self, path):
        self.conn.request("GET", path)
        response = self.conn.getresponse()
        return response, response.read()

    def send_files(self, expect, files, text=""):
        return self.call("POST", "/chat/messages",
                         {"expect": expect, "text": text, "attachments": files})

    def png(self, **extra):
        return {"mime": "image/png", "data": b64(PNG), **extra}

    def test_19_accepts_is_in_each_history_answer(self):
        _, body = self.call("GET", "/chat/messages")
        self.assertEqual(body["accepts"], ["image", "audio"])
        self.send_files(0, [self.png()])
        _, body = self.call("GET", "/chat/messages?from=2")
        self.assertEqual(body["accepts"], ["image", "audio"])
        self.assertEqual(body["messages"], [])

    def test_20_send_only_a_file(self):
        status, body = self.send_files(0, [self.png()])
        self.assertEqual(status, 200)
        user, reply = body["messages"]
        self.assertEqual(user, {"seq": 0, "role": "user", "text": "", "attachments": [
            {"kind": "image", "mime": "image/png", "size": len(PNG)}]})
        self.assertNotIn("data", json.dumps(user))
        self.assertEqual(reply, {"seq": 1, "role": "assistant", "text": ""})

    def test_all_accepted_types_and_the_name(self):
        files = [{"mime": m, "name": "f" + str(i), "data": b64(bytes([i]) * 3)}
                 for i, m in enumerate(["image/jpeg", "image/png", "audio/mpeg", "audio/wav"])]
        status, body = self.send_files(0, files, "look")
        self.assertEqual(status, 200)
        self.assertEqual([(a["kind"], a["mime"], a["name"], a["size"])
                          for a in body["messages"][0]["attachments"]],
                         [("image", "image/jpeg", "f0", 3), ("image", "image/png", "f1", 3),
                          ("audio", "audio/mpeg", "f2", 3), ("audio", "audio/wav", "f3", 3)])
        _, history = self.call("GET", "/chat/messages")
        self.assertEqual(history["messages"][0]["attachments"],
                         body["messages"][0]["attachments"])

    def test_the_echo_ignores_the_content_of_the_files(self):
        _, body = self.send_files(0, [self.png(), {"mime": "audio/wav", "data": b64(b"RIFF")}],
                                  "hello")
        self.assertEqual(body["messages"][1], {"seq": 1, "role": "assistant", "text": "hello"})
        _, body = self.send_files(2, [{"mime": "image/jpeg", "data": b64(b"not a jpeg")}])
        self.assertEqual(body["messages"][1]["text"], "")

    def test_21_read_the_file_back(self):
        self.send_files(0, [self.png(), {"mime": "audio/wav", "data": b64(b"\x00\xff")}])
        response, data = self.get("/chat/messages/0/attachments/0")
        self.assertEqual((response.status, data), (200, PNG))
        self.assertEqual(response.getheader("Content-Type"), "image/png")
        self.assertEqual(response.getheader("Content-Length"), str(len(PNG)))
        response, data = self.get("/chat/messages/0/attachments/1")
        self.assertEqual((response.status, data), (200, b"\x00\xff"))
        self.assertEqual(response.getheader("Content-Type"), "audio/wav")

    def test_22_read_errors(self):
        self.send_files(0, [self.png()])
        for path, status in [("/chat/messages/0/attachments/1", 404),
                             ("/chat/messages/9999/attachments/0", 404),
                             ("/chat/messages/1/attachments/0", 404),
                             ("/chat/messages/x/attachments/0", 400),
                             ("/chat/messages/0/attachments/-1", 400),
                             ("/chat/messages/0/attachments/%C2%B2", 400)]:
            response, data = self.get(path)
            self.assertEqual(response.status, status, path)
            self.assertEqual(json.loads(data)["error"],
                             "not_found" if status == 404 else "bad_request")

    def test_23_unsupported_media(self):
        status, body = self.send_files(0, [self.png(), {"mime": "application/pdf",
                                                         "data": b64(b"%PDF")}])
        self.assertEqual((status, body["error"]), (415, "unsupported_media"))
        self.assertEqual(self.chat.count, 0)

    def test_24_bad_requests_change_nothing(self):
        too_big = {"mime": "image/png", "data": b64(b"\0" * ((4 << 20) + 1))}
        for body in [
                {"expect": 0, "text": "x", "attachments": [{"mime": "image/png",
                                                            "data": "not base64!"}]},
                {"expect": 0, "text": "x", "attachments": [self.png()] * 5},
                {"expect": 0, "text": ""},
                {"expect": 0, "text": "x", "attachments": [too_big]},
                {"expect": 0, "text": "x", "attachments": []},
                {"expect": 0, "text": "", "attachments": []},
                {"expect": 0, "text": "x", "attachments": "no"},
                {"expect": 0, "text": "x", "attachments": [{"mime": "image/png"}]},
                {"expect": 0, "text": "x", "attachments": [{"data": b64(PNG)}]},
                {"expect": 0, "text": "x", "attachments": [self.png(name=5)]}]:
            status, reply = self.call("POST", "/chat/messages", body)
            self.assertEqual((status, reply["error"]), (400, "bad_request"), str(body)[:80])
        self.assertEqual(self.chat.count, 0)

    def test_25_four_files_of_four_mib(self):
        big = b"\x01" * (4 << 20)
        files = [{"mime": "audio/mpeg", "data": b64(big)}] * 4
        status, body = self.send_files(0, files, "four")
        self.assertEqual(status, 200)
        self.assertEqual([a["size"] for a in body["messages"][0]["attachments"]], [4 << 20] * 4)
        response, data = self.get("/chat/messages/0/attachments/3")
        self.assertEqual((response.status, data == big), (200, True))

    def test_text_too_long_is_bad_request(self):
        self.assertEqual(self.send(0, "x" * 16_384)[0], 200)
        self.assertEqual(self.send(2, "x" * 16_385)[0], 400)

    def test_check_order_validation_before_sync(self):
        self.send(0, "one")
        status, body = self.send_files(0, [{"mime": "application/pdf", "data": b64(b"a")}])
        self.assertEqual((status, body["error"]), (415, "unsupported_media"))
        status, body = self.send_files(0, [self.png()])
        self.assertEqual((status, body["error"], body["count"]), (409, "out_of_sync", 2))
        self.assertEqual(self.chat.files, {})

    def test_history_full_when_the_files_use_all_the_room(self):
        self.chat.file_bytes = (64 << 20) - 1
        status, body = self.send_files(0, [self.png()])
        self.assertEqual((status, body["error"]), (413, "history_full"))
        self.assertEqual(self.chat.count, 0)

    def test_15_methods_and_paths(self):
        for method, path, allow in [("PUT", "/chat/messages", "GET, POST"),
                                    ("DELETE", "/chat/messages", "GET, POST"),
                                    ("POST", "/chat/messages/0/attachments/0", "GET"),
                                    ("PUT", "/chat/messages/0/attachments/0", "GET")]:
            self.conn.request(method, path, "{}", {"Content-Type": "application/json"})
            response = self.conn.getresponse()
            body = json.loads(response.read())
            self.assertEqual((response.status, body["error"], response.getheader("Allow")),
                             (405, "method_not_allowed", allow), (method, path))
        self.assertEqual(self.call("GET", "/chat/other")[0], 404)
        self.assertEqual(self.call("GET", "/chat/messages/0/other/0")[0], 404)


if __name__ == "__main__":
    unittest.main()
