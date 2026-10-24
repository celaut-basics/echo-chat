# echo-chat

An echo service of the chat interface of chatui
([docs/celaut/chat.md](https://github.com/celaut-project/chatui/blob/master/docs/celaut/chat.md)).
It is the check of milestone M4: the answer to each message is the text of the message.

It follows revision 6 of `chat.md` (2026-10-24, ASD-STE100): gRPC over HTTP/2 in clear text
(h2c), defined by [`proto/chat.proto`](proto/chat.proto). Revision 5 (HTTP/1.1 with JSON) is
gone: a client of revision 5 and this service do not work together. `chat.proto` has no
package, so the calls are `/ChatService/<Method>` and `/FileService/<Method>`.

By default the answer is a whole message with `status` `COMPLETED`. The service is also the
fixture of the conformance tests of `chat.md` (section 11.1) and of the tests of a client: a
text can start with commands that make the answer stream, think, be pinned, fail and so on
(see below). The service ignores the content of a file: it accepts, validates, keeps and
gives back each file, but it never reads or interprets one.

## API

One gRPC slot on `:8080` (the `PORT` variable changes it). The server speaks h2c, with no
TLS and no ALPN.

| Call | What it does here |
|---|---|
| `ChatService.GetCapabilities` | The four types `image/jpeg`, `image/png`, `audio/mpeg`, `audio/wav`. All limits are 0 (the defaults of `chat.md` section 7). `supports_fork` and `supports_cancel` are `false` |
| `ChatService.SendMessage` | The checks of section 6.1 in their order, then the user message (`sequence` from 1) and the answer in the background. A send with a known `idempotency_key` gives that message and adds nothing |
| `ChatService.GetMessages` | `after_sequence`, `before_sequence`, `limit` and `last`. The limit of the service is 1000 messages |
| `ChatService.Connect` | `subscribe` with replay and `SubscriptionReady`, `send_message`, `acknowledge` (checked, a hint), `event_id` and `ProtocolError`. `cancel_message` gets a `ProtocolError` `unimplemented` |
| `ChatService.SetMessagePinned` | Any message. `updated_at` and `MessagePinnedChanged` only change if the value changes |
| `ChatService.GetConversation` | The main conversation. A `conversation_id` that is not empty is `NOT_FOUND` |
| `ChatService.ForkConversation` | `UNIMPLEMENTED` |
| `FileService.QueryFiles`, `UploadFile`, `DownloadFile` | By SHA3-256, in memory (64 MiB in all). Chunks of 64 KiB. Up to 4 files of 4 MiB in one message |
| Any other call | `UNIMPLEMENTED` |

Each error is a gRPC status with the trailer `chat-reason` (section 9): `bad_request`,
`unsupported_media`, `too_large`, `hash_mismatch`, `not_found`, `file_missing`, `bad_state`,
`history_full` (10 000 messages, or 64 MiB of files), `busy`, `service_error`, `unimplemented`.
In `Connect` the same names are the `code` of a `ProtocolError`.

The history is in memory. One instance is one conversation. The service keeps each file
until the instance stops.

## The echo and its commands

The answer is the text of the user message, in one `TextContent`. As in revision 5, the answer
has no attachment, unless the message has the command `/attach`.

A text can start with commands, each one followed by a space. The user message keeps the
whole text. The answer is the rest. A leading word that is not a command stays in the text,
so `/etc/passwd` is echoed as it is. `/echo` ends the commands: `/echo /stream a` echoes
`/stream a`.

| Command | Effect |
|---|---|
| `/stream[=MS]` | The answer is `IN_PROGRESS` and grows word by word with a `MessageDelta` each MS milliseconds (default 50). The text of the events is equal to the final text |
| `/reasoning` | As `/stream`, with `reasoning_append` events before the text |
| `/pin` | The answer is pinned when it is made (`pinned` in `MessageCreated`) |
| `/pin=late` | The answer is pinned after it is final, with a `MessagePinnedChanged` |
| `/fail` | As `/stream`, but the answer stops after half of the words with `FAILED` and `metadata` `error` |
| `/attach` | The answer has the attachments of the user message (after the text in a whole answer, before it in a stream, because only the end of the text can grow) |
| `/delay=MS` | The answer starts MS milliseconds after the send. The service makes the answers one after the other, so a delay makes the queue grow |
| `/pending` | The answer is made at the send as `PENDING`, and is `IN_PROGRESS` or final when its turn comes |
| `/reject=REASON` | The send fails and changes nothing: `service_error` (`INTERNAL`), `busy` (`UNAVAILABLE`) or `history_full` (`RESOURCE_EXHAUSTED`) |

Environment variables (the service reads them at start):

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | The port of the slot |
| `ECHO_STREAM` | empty | `text` or `reasoning`: each answer streams, with no command |
| `ECHO_DELAY_MS` | `50` | The pause between two deltas |
| `ECHO_MAX_MESSAGES` | `10000` | The size of the history. A send gets `history_full` when the history has no room for the user message, its answer and the answers that wait |
| `ECHO_MAX_QUEUE` | `64` | The user messages that can wait for an answer. Above it, `busy` |

The service cannot cancel an answer: `cancel_message` gets `unimplemented`.

## Declaration in the service interface

The slot must declare `tcp`, then the protocol stack `http2`, `grpc`, `chat`, from the
bottom to the top (`chat.md` section 3). `amd64/.service/service.json` and
`arm64/.service/service.json` declare it with `protocol_stack` (bottom layer first):

```json
"api": [{"port": 8080, "transport": "tcp",
         "protocol_stack": [["http2"], ["grpc"], {"tags": ["chat"], "prose": "..."}]}]
```

`protocol_stack` in an api slot needs nodo PR
[#549](https://github.com/celaut-project/nodo/pull/549). A nodo without it
ignores the key and packs a slot with one empty protocol layer. chatui does not
show such a service as a chat service. After a pack, check the stack with
`nodo inspect <service id>` or in the `StartService` answer. A slot with the stack
`http`, `chat` (revision 5) does not count for a client of revision 6.

## The stubs

`service/chat_pb2.py` and `service/chat_pb2_grpc.py` are made from `proto/chat.proto`, a
copy of `docs/celaut/chat.proto` of chatui (revision 6). To make them again after a change
of the file:

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
PATH=$PWD/.venv/bin:$PATH scripts/gen_proto.sh
```

`requirements-dev.txt` has the versions that the image installs: `grpcio` 1.51.1 and
`protobuf` 4.21.12 (Debian bookworm: `python3-grpcio` 1.51.1 and `python3-protobuf` 3.21.12,
the same release). A newer `grpcio-tools` makes stubs that need a newer runtime than the image
has.

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
index, so one digest serves both. Python 3.11 and its gRPC library come from a
frozen snapshot.debian.org archive at exact versions, so the pins do not rot.

The Dockerfile writes `/entrypoint.sh` (`init.entry_path`). It starts the
server (`python3 -m service.server`, from `/`) on port 8080 as an unprivileged
user (uid 10001). The last build step starts the server and checks one echo
over h2c, so an image that cannot answer does not become a service id.

`service/` is one symlinked directory in each pack root, so every file in it is in
the pack.

The service needs no network. Besides the standard library it needs `python3-grpcio` and
`python3-protobuf`: the Dockerfile installs them from the same snapshot as the interpreter,
at exact versions, with each package that they need and that the base image does not have.
Resources: 128 MiB of memory at start (256 MiB at most) and 256 MB of disk.

## Run on a node

```bash
nodo import <6hex>.celaut.bee     # or: nodo pack amd64
nodo execute <service id>         # prints the local address of the slot
grpcurl -plaintext -proto proto/chat.proto -d '{}' <address> ChatService/GetCapabilities
grpcurl -plaintext -proto proto/chat.proto \
        -d '{"content": [{"text": {"text": "Hello"}}], "idempotency_key": "first"}' \
        <address> ChatService/SendMessage
grpcurl -plaintext -proto proto/chat.proto -d '{}' <address> ChatService/GetMessages
```

Stop the instance with `nodo kill <instance>`. The conversation ends with it.

To run it without a node: `PORT=8080 python3 -m service.server` (Python 3.11 with `grpcio`
and `protobuf`).

## Releases

A release has the packed service of each architecture that was executed on a
node, exported with `nodo export <service id> <dir>` as
`<first 6 hex of the id>.celaut.bee`. The release notes give the service id,
the nodo commit and the tests. Each asset passed an import on a node with a
clean registry. Import it with `nodo import <file>`.

## Tests

    python3 -m unittest discover -s tests -t .

Use the Python of a venv with `requirements-dev.txt`. `tests/test_server.py` runs the
conformance tests of `chat.md` section 11.1 (below) against a server in the same process, and
tests the commands and the options of the echo. `tests/test_layout.py` and
`tests/test_manifest.py` check the pack roots without a node.

### Conformance runner

`conformance/runner.py` is a gRPC client that runs the tests of section 11.1 against any
service of the interface, over h2c. Run it on a new instance (tests 3 to 6 need an empty
history):

    python3 -m conformance.runner HOST:PORT --profile echo --history-limit 600 --queue-limit 4

`--profile echo` has the commands of this service (the text prefixes that make it stream, fail
and so on). Another service gives its own `Profile`, or runs with `--profile none`: then the
tests that need a slow, streaming or failing service are skipped. The tests that do not apply
to the service (for example 42 when `supports_fork` is `false`) are skipped too.
`--history-limit` and `--queue-limit` are the sizes of the instance, for the optional tests 31
and 32 (start the echo with `ECHO_MAX_MESSAGES=600 ECHO_MAX_QUEUE=4`). `--package` is the
Python package that has `chat_pb2` (`service`). The exit status is 0 if no test failed. The
runner has no code of this service: it needs only the stubs and `grpcio`.

For this service the runner skips tests 42, 43, 44, 66, 67 and 73. They need a service that
forks, cancels, accepts text only, accepts only one kind of file, or has no `FileService`.
