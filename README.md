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

The service needs no network. Resources: 128 MiB of memory at start
(256 MiB at most) and 256 MB of disk.

## Run on a node

```bash
nodo import <6hex>.celaut.bee     # or: nodo pack amd64
nodo execute <service id>         # prints the local address of the slot
curl -s -X POST -H 'Content-Type: application/json' \
     -d '{"expect": 0, "text": "Hello"}' http://<address>/chat/messages
```

The answer:

```json
{"count": 2, "messages": [{"seq": 0, "role": "user", "text": "Hello"},
                          {"seq": 1, "role": "assistant", "text": "Hello"}]}
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

`tests/test_server.py` tests the server. `tests/test_layout.py` and
`tests/test_manifest.py` check the pack roots without a node.
