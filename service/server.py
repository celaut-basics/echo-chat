#!/usr/bin/env python3
"""Echo service of the chat interface of chatui (docs/celaut/chat.md, revision 6).

gRPC over HTTP/2 in clear text (h2c) on the port of the slot: `ChatService` and
`FileService` of chat.proto. The state is in `store.Chat`. Run it as
`python3 -m service.server` from the directory that holds `service/`.
"""

import os
import queue
import re
import sys
import threading
from concurrent import futures

import grpc

from . import chat_pb2 as pb
from . import chat_pb2_grpc as rpc
from .store import (ACCEPTED_MIME_TYPES, MAX_FILE, SHA3_256_TYPE, Chat, Reject, bad_request,
                    normalize_mime, sha3)

EVENT_ID = re.compile("[A-Za-z0-9_-]{1,64}")
CHUNK = 64 << 10  # the size of a chunk that the service sends
END = object()


def fail(context, error):
    """End the call with the status and the trailer `chat-reason` (section 9)."""
    context.set_trailing_metadata((("chat-reason", error.reason),))
    context.abort(getattr(grpc.StatusCode, error.status), error.message)


def protocol_error(error, event_id):
    return pb.ServerEvent(error=pb.ProtocolError(
        code=error.reason, message=error.message, event_id=event_id))


class ChatService(rpc.ChatServiceServicer):
    def __init__(self, chat):
        self.chat = chat

    def _call(self, context, function, *args):
        try:
            return function(*args)
        except Reject as error:
            fail(context, error)

    def GetCapabilities(self, request, context):
        return self.chat.capabilities()

    def GetConversation(self, request, context):
        if request.conversation_id:
            fail(context, Reject("NOT_FOUND", "not_found", "there is only the main conversation"))
        return self.chat.conversation()

    def ForkConversation(self, request, context):
        fail(context, Reject("UNIMPLEMENTED", "unimplemented", "supports_fork is false"))

    def SendMessage(self, request, context):
        return self._call(context, self.chat.send, request)[0]

    def GetMessages(self, request, context):
        yield from self._call(context, self.chat.get_messages, request)

    def SetMessagePinned(self, request, context):
        return self._call(context, self.chat.set_pinned, request)

    def Connect(self, request_iterator, context):
        events = queue.Queue()
        context.add_callback(lambda: events.put(END))  # the client went away
        seen = set()  # the event_id of each event, to find a repeat
        state = {"subscribed": False}

        def end(error):
            context.set_trailing_metadata((("chat-reason", error.reason),))
            context.set_code(getattr(grpc.StatusCode, error.status))
            context.set_details(error.message)
            events.put(END)

        def handle(event):
            kind = event.WhichOneof("event")
            if not state["subscribed"] and kind != "subscribe":
                raise Reject("FAILED_PRECONDITION", "bad_state", "the first event is not subscribe")
            if event.HasField("event_id"):
                if not EVENT_ID.fullmatch(event.event_id) or event.event_id in seen:
                    raise bad_request("event_id is not valid or is not new")
                seen.add(event.event_id)
            if kind == "subscribe":
                if state["subscribed"]:
                    raise Reject("FAILED_PRECONDITION", "bad_state", "the stream has a subscribe")
                if event.subscribe.conversation_id:
                    raise Reject("NOT_FOUND", "not_found", "there is only the main conversation")
                self.chat.subscribe(events, event.subscribe.after_sequence)
                state["subscribed"] = True
            elif kind == "send_message":
                message, repeat = self.chat.send(event.send_message)
                if repeat:
                    self.chat.repeat(events, message)
            elif kind == "acknowledge":
                # A hint: the service keeps no events for a stream, so it drops none.
                self.chat.check_acknowledge(event.acknowledge.last_sequence)
            elif kind == "cancel_message":
                raise Reject("UNIMPLEMENTED", "unimplemented", "supports_cancel is false")
            # Another event is of a newer revision: ignore it.

        def read():
            try:
                for event in request_iterator:
                    try:
                        handle(event)
                    except Reject as error:
                        if not state["subscribed"]:  # section 6.5, steps 1 and 2
                            return end(error)
                        events.put(protocol_error(
                            error, event.event_id if event.HasField("event_id") else None))
            except grpc.RpcError:
                pass
            events.put(END)  # the client ended its side: end with OK

        threading.Thread(target=read, daemon=True).start()
        try:
            while True:
                event = events.get()
                if event is END:
                    return
                yield event
        finally:
            self.chat.unsubscribe(events)


class FileService(rpc.FileServiceServicer):
    def __init__(self, chat):
        self.chat = chat

    def QueryFiles(self, request_iterator, context):
        kind = None  # the last explicit type of the stream
        for request in request_iterator:
            h = request.hash
            if h.HasField("type"):
                kind = h.type
            elif kind is None:
                fail(context, bad_request("the first hash of the stream needs a type"))
            known = kind == SHA3_256_TYPE and len(h.value) == 32 and self.chat.has_file(h.value)
            yield pb.QueryFilesResponse(hash=pb.Hash(type=kind, value=h.value), available=known)

    def UploadFile(self, request_iterator, context):
        first = next(request_iterator, None)
        if first is None or first.WhichOneof("part") != "file":
            fail(context, bad_request("the first message needs a file"))
        ref = first.file
        if not ref.HasField("mime_type") or not ref.HasField("size_bytes"):
            fail(context, bad_request("a file needs mime_type and size_bytes"))
        try:
            expected = self.chat.sha3_of(ref)
        except Reject as error:
            fail(context, error)
        mime = normalize_mime(ref.mime_type)
        if mime not in ACCEPTED_MIME_TYPES:
            fail(context, Reject("INVALID_ARGUMENT", "unsupported_media",
                                 "accepted: " + ", ".join(ACCEPTED_MIME_TYPES)))
        too_large = Reject("INVALID_ARGUMENT", "too_large", "the file is above %d bytes" % MAX_FILE)
        if ref.size_bytes > MAX_FILE:
            fail(context, too_large)
        data = bytearray()
        for request in request_iterator:
            if request.WhichOneof("part") != "chunk":
                fail(context, bad_request("only the first message has a file"))
            data += request.chunk
            if len(data) > MAX_FILE:
                fail(context, too_large)
        data = bytes(data)
        if len(data) != ref.size_bytes or sha3(data) != expected:
            fail(context, Reject("INVALID_ARGUMENT", "hash_mismatch",
                                 "the bytes do not have the size or the hash that the file gave"))
        try:
            self.chat.put_file(expected, data)
        except Reject as error:
            fail(context, error)
        return pb.UploadFileResponse(file=pb.FileReference(
            hashes=[pb.Hash(type=SHA3_256_TYPE, value=expected)], mime_type=mime,
            size_bytes=len(data), filename=ref.filename if ref.HasField("filename") else None))

    def DownloadFile(self, request, context):
        h = request.hash
        if not h.HasField("type"):
            fail(context, bad_request("the hash needs a type"))
        data = self.chat.get_file(h.value) if h.type == SHA3_256_TYPE else None
        if data is None:
            fail(context, Reject("NOT_FOUND", "not_found", "the service has no such file"))
        for start in range(0, len(data), CHUNK):
            yield pb.DownloadFileResponse(chunk=data[start:start + CHUNK])


class Unimplemented(grpc.GenericRpcHandler):
    """A call that chat.proto does not define ends with UNIMPLEMENTED and its reason."""

    def service(self, details):
        def handler(request, context):
            fail(context, Reject("UNIMPLEMENTED", "unimplemented", "the call is not defined"))
        return grpc.unary_unary_rpc_method_handler(handler)


def make_server(host="0.0.0.0", port=8080, chat=None):
    """Return (server, port, chat). The server is not started."""
    chat = chat or Chat()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=64),
        options=(("grpc.so_reuseport", 0),
                 ("grpc.max_receive_message_length", 4 << 20),
                 ("grpc.max_send_message_length", 1 << 20),
                 ("grpc.max_concurrent_streams", 100),
                 ("grpc.http2.max_ping_strikes", 0),  # never close for a PING
                 ("grpc.keepalive_permit_without_calls", 1)))
    rpc.add_ChatServiceServicer_to_server(ChatService(chat), server)
    rpc.add_FileServiceServicer_to_server(FileService(chat), server)
    server.add_generic_rpc_handlers((Unimplemented(),))  # after the two services
    return server, server.add_insecure_port("%s:%d" % (host, port)), chat


if __name__ == "__main__":
    server, port, _ = make_server(port=int(os.environ.get("PORT", "8080")))
    server.start()
    print("echo-chat: gRPC on port %d" % port, file=sys.stderr, flush=True)
    server.wait_for_termination()
