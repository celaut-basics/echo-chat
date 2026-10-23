# echo-chat

An echo service that follows the chat convention of chatui
([docs/celaut/chat.md](https://github.com/celaut-project/chatui/blob/master/docs/celaut/chat.md)).
It is the check of milestone M4: the answer to each message is the text of the message.

It follows revision 5 of `chat.md` (2026-10-23, ASD-STE100), with attachments. A send replies at
once with the user message and the client reads the answer with `GET`. The echo is ready at
once, so each answer is a whole message with `status` `done`. The service does not stream.
The service ignores the content of files: it accepts, validates, keeps and gives
back each file, but it never reads or interprets a file. The answer is the echo
of `text` only and has no attachment.

## API

One HTTP slot on `:8080` (the `PORT` variable changes it).

| Request | Meaning |
|---|---|
| `POST /chat/messages` with `{"cid": "...", "text": "...", "attachments": [...]}` | Add a user message and its echo, and reply with `{"count", "message"}` (the user message). `cid` (1 to 64 characters of `A-Za-z0-9_-`) and `attachments` are optional. A second send with the same `cid` gives the same message and adds nothing. Many sends can come at the same time |
| `GET /chat/messages?from=N` | The messages with `seq` of N or more, `count` and `accepts` (`["image", "audio"]`). The body is 1 MiB or less: if the messages from N are more, the answer has fewer messages, in order from N, at least one. `count` is always the total |
| `GET /chat/messages?last=N` | The last N messages |
| `GET /chat/messages?from=N&limit=M` | At most M messages from N. `last` cannot go with `from` or `limit` (400) |
| `GET /chat/messages/{seq}/attachments/{index}` | The bytes of a file of a user message, with its `Content-Type` |

A message has `seq`, `role`, `text`, `status` (always `done` here), `attachments` (user
messages with files), `cid` (user messages sent with one) and `reply_to` (the answer: the `seq`
of the message that it answers).

Status codes: 200, 400 `bad_request`, 404 `not_found`, 405 `method_not_allowed`,
413 `history_full` (10 000 messages, or 64 MiB of files), 415 `unsupported_media`. The
service never gives 409 or 503: it needs no `expect`, and it never makes a message wait.

## Files

An attachment is `{"mime", "name"?, "data"}` with `data` in base64. The service accepts
`image/jpeg`, `image/png`, `audio/mpeg` and `audio/wav` (other types: 415), up to 4 files
of 4 MiB each and a body of 24 MiB (the minimum is 128 KiB for text only). `text` can be empty if there is a file. The user
message lists each file as `{kind, mime, name?, size}` without `data`. The service keeps
the files in memory and gives them back at the path above. The file is checked (base64,
size, type) and never read: the echo does not depend on it.

The history is in memory. One instance is one conversation.

## Declaration in the service interface

The slot must declare `tcp`, then the protocol stack `http`, then `chat`.
`amd64/.service/service.json` and `arm64/.service/service.json` declare it with
`protocol_stack` (bottom layer first):

```json
"api": [{"port": 8080, "transport": "tcp",
         "protocol_stack": [["http"], {"tags": ["chat"], "prose": "..."}]}]
```

`protocol_stack` in an api slot needs nodo PR
[#549](https://github.com/celaut-project/nodo/pull/549). A nodo without it
ignores the key and packs a slot with one empty protocol layer. chatui does not
show such a service as a chat service. After a pack, check the stack with
`nodo inspect <service id>` or in the `StartService` answer.

## Pack: one tree per architecture

A Celaut service has one architecture (`service.json` → `architecture`). So
this repo has one pack root for each architecture, as in
`celaut-basics/demo-service`:

```bash
nodo pack amd64          # linux/amd64
nodo pack arm64          # linux/arm64
```

```
amd64/  arm64/           pack roots
├── .service/            Dockerfile, service.json, pack_config.json (one set per arch)
└── service -> ../service
service/                 shared source
```

`nodo pack <dir>` reads only `<dir>/.service/` and copies `<dir>` to its
cache. The copy follows symlinks, so `service/` reaches each pack root. The
two `.service/` sets are real files. Only `architecture` and the first comment
of the Dockerfile differ. The `debian:bookworm-slim` pin is a multi-arch
index, so one digest serves both. Python 3.11 comes from a frozen
snapshot.debian.org archive at exact versions, so the pins do not rot.

The Dockerfile writes `/entrypoint.sh` (`init.entry_path`). It starts the
server on port 8080 as an unprivileged user (uid 10001). The last build step
starts the server and checks one echo, so an image that cannot answer does not
become a service id.

`service/` is one symlinked directory in each pack root, so every file in it is in
the pack.

The service needs no network. Resources: 128 MiB of memory at start
(256 MiB at most) and 256 MB of disk.

## Run on a node

```bash
nodo import <6hex>.celaut.bee     # or: nodo pack amd64
nodo execute <service id>         # prints the local address of the slot
curl -s -X POST -H 'Content-Type: application/json' \
     -d '{"cid": "first", "text": "Hello"}' http://<address>/chat/messages
curl -s http://<address>/chat/messages
```

The reply to the send:

```json
{"count": 2, "message": {"seq": 0, "role": "user", "text": "Hello", "status": "done", "cid": "first"}}
```

The read:

```json
{"count": 2, "accepts": ["image", "audio"],
 "messages": [{"seq": 0, "role": "user", "text": "Hello", "status": "done", "cid": "first"},
              {"seq": 1, "role": "assistant", "text": "Hello", "status": "done", "reply_to": 0}]}
```

Stop the instance with `nodo kill <instance>`. The conversation ends with it.

## Releases

A release has the packed service of each architecture that was executed on a
node, exported with `nodo export <service id> <dir>` as
`<first 6 hex of the id>.celaut.bee`. The release notes give the service id,
the nodo commit and the tests. Each asset passed an import on a node with a
clean registry. Import it with `nodo import <file>`.

## Tests

    python3 -m unittest discover -s tests -t .

`tests/test_server.py` tests the server, with the conformance tests of `chat.md` (sends
at the same time, `cid`, the filters `from`, `last` and `limit`, the files) and a test of the 1 MiB limit of a read. `tests/test_layout.py` and
`tests/test_manifest.py` check the pack roots without a node.
