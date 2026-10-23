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

    def send(self, text, **extra):
        return self.call("POST", "/chat/messages", {"text": text, **extra})


def user(seq, text, **extra):
    return {"seq": seq, "role": "user", "text": text, "status": "done", **extra}


def answer(seq, text, reply_to):
    return {"seq": seq, "role": "assistant", "text": text, "status": "done",
            "reply_to": reply_to}


class ChatServerTest(ServerCase):
    def test_send_returns_the_user_message_and_the_answer_is_in_the_history(self):
        status, body = self.send("Hello", cid="k1")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"count": 2, "message": user(0, "Hello", cid="k1")})
        _, history = self.call("GET", "/chat/messages")
        self.assertEqual(history["messages"], [user(0, "Hello", cid="k1"),
                                               answer(1, "Hello", 0)])

    def test_messages_are_numbered_in_order_on_one_connection(self):
        self.send("one")
        _, body = self.send("two")
        self.assertEqual(body["message"]["seq"], 2)
        self.assertEqual(body["count"], 4)
        _, history = self.call("GET", "/chat/messages")
        self.assertEqual([m["seq"] for m in history["messages"]], [0, 1, 2, 3])
        self.assertEqual([m["reply_to"] for m in history["messages"][1::2]], [0, 2])

    def test_the_same_cid_gives_the_same_message_and_adds_nothing(self):
        _, first = self.send("one", cid="same")
        status, again = self.send("one", cid="same")
        self.assertEqual(status, 200)
        self.assertEqual(again["message"], first["message"])
        self.assertEqual(self.chat.count, 2)

    def test_sends_at_the_same_time_are_all_taken(self):
        results = []
        self.chat.max_messages = 100

        def post(i):
            conn = http.client.HTTPConnection(*self.server.server_address)
            conn.request("POST", "/chat/messages", json.dumps({"cid": "c%d" % i, "text": "m"}))
            results.append(conn.getresponse().status)
            conn.close()

        threads = [threading.Thread(target=post, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        self.assertEqual(results, [200] * 8)
        _, history = self.call("GET", "/chat/messages")
        self.assertEqual(history["count"], 16)
        self.assertEqual([m["seq"] for m in history["messages"]], list(range(16)))
        for m in history["messages"][1::2]:  # each answer follows the message it answers
            self.assertEqual(m["reply_to"], m["seq"] - 1)

    def test_history_from(self):
        self.send("one")
        self.send("two")
        _, body = self.call("GET", "/chat/messages?from=3")
        self.assertEqual(body["count"], 4)
        self.assertEqual([m["seq"] for m in body["messages"]], [3])
        _, body = self.call("GET", "/chat/messages?from=4")
        self.assertEqual(body["messages"], [])

    def test_history_last_and_limit(self):
        self.chat.max_messages = 100
        for text in ("a", "b", "c"):
            self.send(text)
        _, body = self.call("GET", "/chat/messages?last=2")
        self.assertEqual((body["count"], [m["seq"] for m in body["messages"]]), (6, [4, 5]))
        _, body = self.call("GET", "/chat/messages?last=1000")
        self.assertEqual(len(body["messages"]), 6)
        _, body = self.call("GET", "/chat/messages?from=2&limit=1")
        self.assertEqual([m["seq"] for m in body["messages"]], [2])
        _, body = self.call("GET", "/chat/messages?limit=3")
        self.assertEqual([m["seq"] for m in body["messages"]], [0, 1, 2])

    def test_filters_that_are_not_valid_are_400(self):
        self.send("one")
        for query in ("from=1&last=1", "last=1&limit=1", "last=0", "limit=0", "last=a",
                      "limit=", "from=3", "last=-1"):
            status, body = self.call("GET", "/chat/messages?" + query)
            self.assertEqual((status, body["error"]), (400, "bad_request"), query)

    def test_history_without_filters_has_all(self):
        self.send("one")
        _, body = self.call("GET", "/chat/messages")
        self.assertEqual(len(body["messages"]), 2)

    def test_history_from_more_than_count_is_bad_request(self):
        status, body = self.call("GET", "/chat/messages?from=1")
        self.assertEqual((status, body["error"]), (400, "bad_request"))
        status, _ = self.call("GET", "/chat/messages?from=x")
        self.assertEqual(status, 400)

    def test_bad_bodies(self):
        for body in [{"expect": 0}, {"text": ""}, {"text": 5}, {"text": "a", "cid": 5},
                     {"text": "a", "cid": "a b"}, {"text": "a", "cid": ""},
                     {"text": "a", "cid": "x" * 65}, [1]]:
            status, reply = self.call("POST", "/chat/messages", body)
            self.assertEqual((status, reply["error"]), (400, "bad_request"), body)
        status, _ = self.call("POST", "/chat/messages", raw="not json")
        self.assertEqual(status, 400)
        self.assertEqual(self.chat.count, 0)

    def test_a_cid_of_64_characters_is_valid(self):
        self.assertEqual(self.send("a", cid="x" * 64)[0], 200)

    def test_history_full(self):
        for _ in range(3):
            self.assertEqual(self.send("x")[0], 200)
        status, body = self.send("x")
        self.assertEqual((status, body["error"]), (413, "history_full"))
        self.assertEqual(self.chat.count, 6)

    def test_history_full_does_not_stop_the_same_cid_from_giving_its_message(self):
        _, first = self.send("x", cid="a")
        for _ in range(2):
            self.send("x")
        self.assertEqual(self.send("x")[0], 413)
        status, again = self.send("x", cid="a")
        self.assertEqual((status, again["message"]), (200, first["message"]))

    def test_the_old_expect_field_is_ignored(self):
        status, body = self.call("POST", "/chat/messages", {"expect": 99, "text": "a"})
        self.assertEqual((status, body["message"]["seq"]), (200, 0))

    def test_unknown_path_and_unknown_fields(self):
        self.assertEqual(self.call("GET", "/other")[0], 404)
        status, _ = self.call("POST", "/chat/messages", {"text": "a", "future": 1})
        self.assertEqual(status, 200)

    def test_text_is_not_changed(self):
        _, body = self.send("  x  é ✓")
        self.assertEqual(body["message"]["text"], "  x  é ✓")

    def test_utf8(self):
        self.send("¿Qué tal? 你好")
        _, history = self.call("GET", "/chat/messages")
        self.assertEqual(history["messages"][1]["text"], "¿Qué tal? 你好")


PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256))


def b64(data):
    return base64.b64encode(data).decode()


class ConformanceFixesTest(ServerCase):
    """Tests 4 and 5, and the body above the limit (sections 4 and 6.2)."""

    def test_4_from_with_a_digit_that_is_not_ascii_is_400_and_keeps_the_connection(self):
        status, body = self.call("GET", "/chat/messages?from=%C2%B2")
        self.assertEqual((status, body["error"]), (400, "bad_request"))
        self.assertEqual(self.call("GET", "/chat/messages")[0], 200)

    def test_5_from_that_is_not_valid_is_400(self):
        for value in ("", "-1", "1.0", "a"):
            status, body = self.call("GET", "/chat/messages?from=" + value)
            self.assertEqual((status, body["error"]), (400, "bad_request"), value)

    def test_a_body_above_the_limit_is_400_and_the_connection_stays_in_step(self):
        from service.server import MAX_BODY
        status, body = self.call("POST", "/chat/messages", raw="x" * (MAX_BODY + 10))
        self.assertEqual((status, body["error"]), (400, "bad_request"))
        self.assertEqual(self.send("after")[0], 200)


class AttachmentTest(ServerCase):
    """Sections 5.1, 6.1, 6.2, 6.4 and the tests of the files of section 10.1."""

    def setUp(self):
        super().setUp()
        self.chat.max_messages = 100

    def get(self, path):
        self.conn.request("GET", path)
        response = self.conn.getresponse()
        return response, response.read()

    def send_files(self, files, text=""):
        return self.call("POST", "/chat/messages", {"text": text, "attachments": files})

    def history(self, query=""):
        return self.call("GET", "/chat/messages" + query)[1]

    def png(self, **extra):
        return {"mime": "image/png", "data": b64(PNG), **extra}

    def test_19_accepts_is_in_each_history_answer(self):
        _, body = self.call("GET", "/chat/messages")
        self.assertEqual(body["accepts"], ["image", "audio"])
        self.send_files([self.png()])
        _, body = self.call("GET", "/chat/messages?from=2")
        self.assertEqual(body["accepts"], ["image", "audio"])
        self.assertEqual(body["messages"], [])

    def test_a_read_answer_is_at_most_1_mib_and_the_client_reads_the_rest(self):
        text = "\U0001F600" * 4096  # 16 384 bytes; 12 bytes each in JSON: worst case
        for _ in range(40):
            self.chat.add("user", text)
        self.assertGreater(len(json.dumps(self.chat.messages)), 1 << 20)
        start, seen = 0, []
        while start < self.chat.count:
            self.conn.request("GET", "/chat/messages?from=%d" % start)
            response = self.conn.getresponse()
            raw = response.read()
            self.assertEqual(response.status, 200)
            self.assertLessEqual(len(raw), 1 << 20)
            body = json.loads(raw)
            self.assertEqual(body["count"], 40)
            self.assertEqual(body["accepts"], ["image", "audio"])
            self.assertGreaterEqual(len(body["messages"]), 1)
            seen += [m["seq"] for m in body["messages"]]
            start = seen[-1] + 1
        self.assertEqual(seen, list(range(40)))

    def test_20_send_only_a_file(self):
        status, body = self.send_files([self.png()])
        self.assertEqual(status, 200)
        self.assertEqual(body["message"], {
            "seq": 0, "role": "user", "text": "", "status": "done", "attachments": [
                {"kind": "image", "mime": "image/png", "size": len(PNG)}]})
        self.assertNotIn("data", json.dumps(body))
        self.assertEqual(self.history()["messages"][1],
                         {"seq": 1, "role": "assistant", "text": "", "status": "done",
                          "reply_to": 0})

    def test_all_accepted_types_and_the_name(self):
        files = [{"mime": m, "name": "f" + str(i), "data": b64(bytes([i]) * 3)}
                 for i, m in enumerate(["image/jpeg", "image/png", "audio/mpeg", "audio/wav"])]
        status, body = self.send_files(files, "look")
        self.assertEqual(status, 200)
        self.assertEqual([(a["kind"], a["mime"], a["name"], a["size"])
                          for a in body["message"]["attachments"]],
                         [("image", "image/jpeg", "f0", 3), ("image", "image/png", "f1", 3),
                          ("audio", "audio/mpeg", "f2", 3), ("audio", "audio/wav", "f3", 3)])
        self.assertEqual(self.history()["messages"][0]["attachments"],
                         body["message"]["attachments"])

    def test_the_echo_ignores_the_content_of_the_files(self):
        self.send_files([self.png(), {"mime": "audio/wav", "data": b64(b"RIFF")}], "hello")
        self.send_files([{"mime": "image/jpeg", "data": b64(b"not a jpeg")}])
        messages = self.history()["messages"]
        self.assertEqual(messages[1]["text"], "hello")
        self.assertNotIn("attachments", messages[1])
        self.assertEqual(messages[3]["text"], "")

    def test_21_read_the_file_back(self):
        self.send_files([self.png(), {"mime": "audio/wav", "data": b64(b"\x00\xff")}])
        response, data = self.get("/chat/messages/0/attachments/0")
        self.assertEqual((response.status, data), (200, PNG))
        self.assertEqual(response.getheader("Content-Type"), "image/png")
        self.assertEqual(response.getheader("Content-Length"), str(len(PNG)))
        response, data = self.get("/chat/messages/0/attachments/1")
        self.assertEqual((response.status, data), (200, b"\x00\xff"))
        self.assertEqual(response.getheader("Content-Type"), "audio/wav")

    def test_22_read_errors(self):
        self.send_files([self.png()])
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
        status, body = self.send_files([self.png(), {"mime": "application/pdf",
                                                         "data": b64(b"%PDF")}])
        self.assertEqual((status, body["error"]), (415, "unsupported_media"))
        self.assertEqual(self.chat.count, 0)

    def test_24_bad_requests_change_nothing(self):
        too_big = {"mime": "image/png", "data": b64(b"\0" * ((4 << 20) + 1))}
        for body in [
                {"text": "x", "attachments": [{"mime": "image/png",
                                                            "data": "not base64!"}]},
                {"text": "x", "attachments": [self.png()] * 5},
                {"text": ""},
                {"text": "x", "attachments": [too_big]},
                {"text": "x", "attachments": []},
                {"text": "", "attachments": []},
                {"text": "x", "attachments": "no"},
                {"text": "x", "attachments": [{"mime": "image/png"}]},
                {"text": "x", "attachments": [{"data": b64(PNG)}]},
                {"text": "x", "attachments": [self.png(name=5)]}]:
            status, reply = self.call("POST", "/chat/messages", body)
            self.assertEqual((status, reply["error"]), (400, "bad_request"), str(body)[:80])
        self.assertEqual(self.chat.count, 0)

    def test_25_four_files_of_four_mib(self):
        big = b"\x01" * (4 << 20)
        files = [{"mime": "audio/mpeg", "data": b64(big)}] * 4
        status, body = self.send_files(files, "four")
        self.assertEqual(status, 200)
        self.assertEqual([a["size"] for a in body["message"]["attachments"]], [4 << 20] * 4)
        response, data = self.get("/chat/messages/0/attachments/3")
        self.assertEqual((response.status, data == big), (200, True))

    def test_text_too_long_is_bad_request(self):
        self.assertEqual(self.send("x" * 16_384)[0], 200)
        self.assertEqual(self.send("x" * 16_385)[0], 400)

    def test_check_order_the_type_before_the_cid_and_the_room(self):
        self.send("one", cid="a")
        status, body = self.send_files([{"mime": "application/pdf", "data": b64(b"a")}])
        self.assertEqual((status, body["error"]), (415, "unsupported_media"))
        self.assertEqual(self.chat.files, {})

    def test_the_same_cid_with_files_adds_the_files_once(self):
        _, first = self.call("POST", "/chat/messages",
                             {"cid": "f1", "text": "", "attachments": [self.png()]})
        _, again = self.call("POST", "/chat/messages",
                             {"cid": "f1", "text": "", "attachments": [self.png()]})
        self.assertEqual(again["message"], first["message"])
        self.assertEqual(self.chat.count, 2)
        self.assertEqual(len(self.chat.files[0]), 1)

    def test_history_full_when_the_files_use_all_the_room(self):
        self.chat.file_bytes = (64 << 20) - 1
        status, body = self.send_files([self.png()])
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
