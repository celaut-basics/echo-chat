# echo-chat

An echo service that follows the chat convention of chatui
([docs/celaut/chat.md](https://github.com/celaut-project/chatui/blob/master/docs/celaut/chat.md)).
It is the check of milestone M4: the answer to each message is the text of the message.

## API

One HTTP slot on `:8080` (the `PORT` variable changes it).

| Request | Meaning |
|---|---|
| `POST /chat/messages` with `{"expect": N, "text": "..."}` | Add a user message and its echo. `expect` is the `count` that the client believes the service has |
| `GET /chat/messages?from=N` | The messages with `seq` of N or more, and `count` |

Status codes: 200, 400 `bad_request`, 409 `out_of_sync` (with `count`), 413 `history_full`
(10 000 messages), 503 `busy`.

The history is in memory. One instance is one conversation.

## Declaration in the service interface

The slot must declare `tcp`, then the protocol stack `http`, then `chat`.
The Celaut spec of the service is not in this directory yet.

## Tests

    python3 -m unittest discover -s tests -t .
