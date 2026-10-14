# linux/arm64 variant. nodo builds it with --opt platform=linux/arm64 (from
# service.json). amd64/.service/Dockerfile is the same file; only this comment
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

# The service uses only the standard library of Python. These four packages are
# the interpreter, at the same versions on amd64 and arm64. `python3.11` and not
# `python3`: the `python3` package is a pointer that moves. The link is relative,
# so the packer does not have to resolve it.
RUN apt-get -o Acquire::Retries=5 update \
    && apt-get -o Acquire::Retries=5 install --no-install-recommends -y \
        python3.11=3.11.2-6+deb12u8 \
        python3.11-minimal=3.11.2-6+deb12u8 \
        libpython3.11-stdlib=3.11.2-6+deb12u8 \
        libpython3.11-minimal=3.11.2-6+deb12u8 \
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
# the server only.
RUN printf '%s\n' \
        '#!/bin/sh' \
        'export PORT=8080' \
        'exec setpriv --reuid 10001 --regid 10001 --clear-groups /usr/bin/python3 /service/server.py' \
        > /entrypoint.sh \
    && chmod 0755 /entrypoint.sh

# Build-time smoke test: the server starts as the service user and echoes one
# message. An image that cannot answer does not become a service id.
RUN setpriv --reuid 10001 --regid 10001 --clear-groups /usr/bin/python3 -c "\
import http.client, json, sys, threading; \
sys.path.insert(0, '/'); \
from service.server import make_server; \
s = make_server('127.0.0.1', 0); \
threading.Thread(target=s.serve_forever, daemon=True).start(); \
c = http.client.HTTPConnection(*s.server_address); \
c.request('POST', '/chat/messages', json.dumps({'expect': 0, 'text': 'ping'})); \
r = json.loads(c.getresponse().read()); \
assert r['messages'][1] == {'seq': 1, 'role': 'assistant', 'text': 'ping'}, r; \
print('smoke test: echo ok')"
