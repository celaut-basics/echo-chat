# linux/amd64 variant. nodo builds it with --opt platform=linux/amd64 (from
# service.json). arm64/.service/Dockerfile is the same file; only this comment
# is different.

# The base is pinned by the digest of the multi-arch index of
# `debian:bookworm-slim`, the same pin as the other celaut-basics services. One
# digest serves both architectures.
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

# apt reads a frozen snapshot of the Debian archive, not the live mirror. The live
# mirror keeps one version of each package, so an exact pin stops resolving after
# the next security update. A snapshot keeps every version at a fixed URL. The
# `grep` stops the build if the base image changes its sources file. To update,
# change DEBIAN_SNAPSHOT and the pins below together.
ARG DEBIAN_SNAPSHOT=20261004T000000Z
RUN sed -i \
      -e "s|^URIs: http://deb.debian.org/debian\$|URIs: http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}|" \
      -e "s|^URIs: http://deb.debian.org/debian-security\$|URIs: http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}|" \
      -e '/^URIs: /a Check-Valid-Until: no' \
      /etc/apt/sources.list.d/debian.sources \
    && [ "$(grep -c '^URIs: http://snapshot.debian.org/' /etc/apt/sources.list.d/debian.sources)" = 2 ]

# The interpreter and the gRPC library of Python (python3-grpcio 1.51.1 with
# python3-protobuf 3.21.12, which is PyPI protobuf 4.21.12: the versions of
# requirements-dev.txt that make the stubs of service/). Each package that these
# two need and that the base image does not have is pinned, at the same versions
# on amd64 and arm64 (the list is what `apt-get -s install` gives on the snapshot
# below, for the two architectures). `python3.11` is the interpreter. The `python3`
# package that python3-grpcio needs is pinned too, and the link is made again
# after it, relative, so the packer does not have to resolve it.
RUN apt-get -o Acquire::Retries=5 update \
    && apt-get -o Acquire::Retries=5 install --no-install-recommends -y \
        python3.11=3.11.2-6+deb12u8 \
        python3.11-minimal=3.11.2-6+deb12u8 \
        libpython3.11-stdlib=3.11.2-6+deb12u8 \
        libpython3.11-minimal=3.11.2-6+deb12u8 \
        python3=3.11.2-1+b1 \
        python3-minimal=3.11.2-1+b1 \
        libpython3-stdlib=3.11.2-1+b1 \
        python3-grpcio=1.51.1-3+b1 \
        python3-protobuf=3.21.12-3+deb12u1 \
        libprotobuf32=3.21.12-3+deb12u1 \
        libabsl20220623=20220623.1-1+deb12u2 \
        libc-ares2=1.18.1-3 \
        libre2-9=20220601+dfsg-1+b1 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf python3.11 /usr/bin/python3

# The user that the server runs as. A fixed uid gives the same files on each build.
RUN groupadd --gid 10001 chat \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin chat

COPY ./service /service
RUN chmod 0755 /service \
    && chmod 0644 /service/*.py

# The entrypoint (service.json `init.entry_path`). The guest init starts it as
# PID 1 and as root. It starts the server as the service user, on port 8080 (the
# slot of service.json). It is written here so that service/ stays the source of
# the server only. The server is a module of the package `service`, so it starts
# from `/`.
RUN printf '%s\n' \
        '#!/bin/sh' \
        'export PORT=8080' \
        'cd /' \
        'exec setpriv --reuid 10001 --regid 10001 --clear-groups /usr/bin/python3 -m service.server' \
        > /entrypoint.sh \
    && chmod 0755 /entrypoint.sh

# Build-time smoke test: the server starts as the service user, over h2c, and
# echoes one message. An image that cannot answer does not become a service id.
RUN cd / && setpriv --reuid 10001 --regid 10001 --clear-groups /usr/bin/python3 -c "\
import grpc, time; \
from service import chat_pb2 as pb, chat_pb2_grpc as rpc; \
from service.server import make_server; \
server, port, chat = make_server('127.0.0.1', 0); \
server.start(); \
stub = rpc.ChatServiceStub(grpc.insecure_channel('127.0.0.1:%d' % port)); \
assert list(stub.GetCapabilities(pb.GetCapabilitiesRequest()).accepted_mime_types), 'capabilities'; \
user = stub.SendMessage(pb.SendMessageRequest(content=[pb.ContentPart(text=pb.TextContent(text='ping'))], idempotency_key='smoke')); \
time.sleep(0.5); \
messages = list(stub.GetMessages(pb.GetMessagesRequest())); \
assert [m.sequence for m in messages] == [1, 2], messages; \
assert messages[1].reply_to_message_id == user.message_id and messages[1].content[0].text.text == 'ping', messages; \
print('smoke test: echo ok')"
