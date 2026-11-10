"""The deck of the echo service: the cards interface of chatui (docs/celaut/cards.md,
revision 1, proposal of issue #120).

gRPC over h2c on its own slot (CARDS_PORT, 8081): `CardsService` of cards.proto, and
`FileService.DownloadFile` of chat.proto for the images of the cards. The chat command
`/cards` makes the deck "shopping-list" and answers with a `slot_link` to this slot
(chat.md section 5.8). A decision has no answer and makes no chat message: the service
keeps it and writes it to its log.
"""

import hashlib
import os
import queue
import re
import struct
import sys
import threading
import zlib
from concurrent import futures

import grpc

from . import cards_pb2 as cpb
from . import cards_pb2_grpc as crpc
from . import chat_pb2 as pb
from . import chat_pb2_grpc as rpc

SHA3_256_TYPE = hashlib.sha3_256(b"").digest()  # chat.md section 6.4
KEY = re.compile("[A-Za-z0-9_-]{1,64}")
CHUNK = 64 << 10
END = object()

# The shopping list of issue #120: (item_id, title, text, group, colour of the image or None).
ITEMS = [
    ("milk", "Milk", "2 litres, semi-skimmed", "Dairy", (235, 240, 250)),
    ("eggs", "Eggs", "A box of 12", "Dairy", (240, 210, 160)),
    ("bread", "Bread", "Whole grain, sliced", "Bakery", (190, 140, 80)),
    ("apples", "Apples", "1 kg, any red kind", "Fruit and vegetables", (200, 40, 40)),
    ("tomatoes", "Tomatoes", "6, for the salad", "Fruit and vegetables", None),
    ("rice", "Rice", "Basmati, 1 kg", "Dry food", None),
    ("coffee", "Coffee", "Ground, 250 g", "Dry food", (90, 60, 40)),
    ("soap", "Soap", "For the dishes", "Home", None),
]
EXTRA = ("lemons", "Lemons", "3, added by the refresh", "Fruit and vegetables", (250, 220, 50))


def png(rgb, size=96):
    """A PNG of one colour: a small image that the service makes with no file."""
    row = b"\x00" + bytes(rgb) * size
    raw = row * size

    def chunk(kind, data):
        return (struct.pack(">I", len(data)) + kind + data
                + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF))
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))


class Deck:
    """The state of the slot: one deck, its version, the decisions and the images."""

    def __init__(self, log=None):
        self.lock = threading.Lock()
        self.version = 1
        self.deck = cpb.Deck()  # no deck until /cards
        self.files = {}  # SHA3-256 value -> bytes
        self.keys = set()
        self.decisions = {}  # item_id -> action_id
        self.taken = []  # every decision, in order (cards.md section 6.2)
        self.subscribers = []
        self.count = 0  # the decks made, for the ids of /cards new
        self.log = log or (lambda text: print(text, file=sys.stderr, flush=True))

    def _publish(self, **event):
        self.version += 1
        e = cpb.DeckEvent(version=self.version, deck_id=self.deck.deck_id, **event)
        for q in self.subscribers:
            q.put(e)

    def _card(self, item):
        item_id, title, text, group, rgb = item
        card = cpb.Card(item_id=item_id, title=title, text=text, group=group,
                        detail="%s for the shopping list. Swipe right when it is in the "
                               "basket, left for later." % title)
        if rgb is not None:
            data = png(rgb)
            digest = hashlib.sha3_256(data).digest()
            self.files[digest] = data
            card.image.CopyFrom(pb.ImageContent(
                file=pb.FileReference(hashes=[pb.Hash(type=SHA3_256_TYPE, value=digest)],
                                      mime_type="image/png", size_bytes=len(data)),
                alt_text="A picture of %s" % title.lower()))
        if item_id == "soap":
            card.buttons.add(action_id="remove", label="Remove", icon=cpb.ICON_DELETE,
                             tone=cpb.TONE_NEGATIVE)
            card.tone = cpb.TONE_WARNING
        if item_id == "coffee":
            card.layout = cpb.LAYOUT_IMAGE_FULL
        if item_id in self.decisions:
            card.decision = self.decisions[item_id]
        return card

    def make(self, how=""):
        """`/cards` (the deck, made once), `/cards refresh` (the same deck again, with one
        more item) or `/cards new` (a new deck). Return the deck id."""
        with self.lock:
            if how == "new" or not self.deck.deck_id:
                self.count += 1
                self.decisions.clear()
                deck_id = "shopping-list" if self.count == 1 else "shopping-list-%d" % self.count
            elif how != "refresh":
                return self.deck.deck_id
            else:
                deck_id = self.deck.deck_id
            items = list(ITEMS) + ([EXTRA] if how == "refresh" else [])
            self.deck = cpb.Deck(
                deck_id=deck_id, title="Shopping list", show_progress=True,
                end_text="Everything is in the basket. See you at the checkout.",
                actions=[
                    cpb.DeckAction(action_id="later", label="Not yet", icon=cpb.ICON_LATER,
                                   direction=cpb.DIRECTION_LEFT),
                    cpb.DeckAction(action_id="basket", label="In the basket", icon=cpb.ICON_CART,
                                   tone=cpb.TONE_POSITIVE, direction=cpb.DIRECTION_RIGHT),
                ],
                cards=[self._card(i) for i in items])
            self._publish(deck=self.deck)
            return deck_id

    def watch(self, events):
        with self.lock:
            events.put(cpb.DeckEvent(version=self.version, deck_id=self.deck.deck_id,
                                     deck=self.deck))
            self.subscribers.append(events)

    def unwatch(self, events):
        with self.lock:
            if events in self.subscribers:
                self.subscribers.remove(events)

    def act(self, r):
        """cards.md section 6.2. Return None, or the text of a bad_request."""
        if not KEY.fullmatch(r.idempotency_key):
            return "idempotency_key has 1 to 64 characters of A-Za-z0-9_-"
        if not (r.deck_id and r.item_id and r.action_id):
            return "deck_id, item_id and action_id are needed"
        with self.lock:
            if r.idempotency_key in self.keys:
                return None  # the same key: OK, no change
            self.keys.add(r.idempotency_key)
            self.taken.append(r)
            known = r.deck_id == self.deck.deck_id and any(
                c.item_id == r.item_id for c in self.deck.cards)
            self.log("cards: %s %s %s %s%s" % (r.deck_id, r.item_id,
                                               "undo" if r.undo else "->", r.action_id,
                                               "" if known else " (late, ignored)"))
            if not known:
                return None
            if r.undo:
                self.decisions.pop(r.item_id, None)
            else:
                self.decisions[r.item_id] = r.action_id
            for card in self.deck.cards:
                if card.item_id == r.item_id:
                    if r.undo:
                        card.ClearField("decision")
                    else:
                        card.decision = r.action_id
                    self._publish(card_changed=cpb.CardChanged(card=card))
        return None


class CardsService(crpc.CardsServiceServicer):
    def __init__(self, deck):
        self.deck = deck

    def Watch(self, request, context):
        events = queue.Queue()
        self.deck.watch(events)
        context.add_callback(lambda: events.put(END))
        try:
            while True:
                e = events.get()
                if e is END:
                    return
                yield e
        finally:
            self.deck.unwatch(events)

    def SendAction(self, request, context):
        error = self.deck.act(request)
        if error:
            context.set_trailing_metadata((("cards-reason", "bad_request"),))
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, error)
        return cpb.SendActionResponse()


class CardFiles(rpc.FileServiceServicer):
    """Only `DownloadFile`: the client of the cards does not upload (cards.md section 4)."""

    def __init__(self, deck):
        self.deck = deck

    def DownloadFile(self, request, context):
        h = request.hash
        if h.type != SHA3_256_TYPE or len(h.value) != 32:
            context.set_trailing_metadata((("chat-reason", "bad_request"),))
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "a SHA3-256 hash is needed")
        data = self.deck.files.get(h.value)
        if data is None:
            context.set_trailing_metadata((("chat-reason", "not_found"),))
            context.abort(grpc.StatusCode.NOT_FOUND, "no such file")
        for i in range(0, len(data), CHUNK):
            yield pb.DownloadFileResponse(chunk=data[i:i + CHUNK])


def make_cards_server(host="0.0.0.0", port=8081, deck=None):
    """Return (server, port, deck). The server is not started."""
    deck = deck or Deck()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=32),
        options=(("grpc.so_reuseport", 0),
                 ("grpc.max_send_message_length", 1 << 20),
                 ("grpc.max_concurrent_streams", 100),
                 ("grpc.http2.max_ping_strikes", 0),
                 ("grpc.keepalive_permit_without_calls", 1)))
    crpc.add_CardsServiceServicer_to_server(CardsService(deck), server)
    rpc.add_FileServiceServicer_to_server(CardFiles(deck), server)
    return server, server.add_insecure_port("%s:%d" % (host, port)), deck


CARDS_PORT = int(os.environ.get("CARDS_PORT", "8081"))
