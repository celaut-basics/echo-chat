#!/bin/sh
# Make the stubs of service/ from proto/chat.proto. Use the versions of
# requirements-dev.txt: they are the versions that the image installs (Debian
# bookworm: python3-grpcio 1.51.1, python3-protobuf 3.21.12 = PyPI protobuf 4.21.12).
#
#   python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
#   PATH=$PWD/.venv/bin:$PATH scripts/gen_proto.sh
set -eu
cd "$(dirname "$0")/.."
python3 -m grpc_tools.protoc -I proto --python_out=service --grpc_python_out=service proto/chat.proto
# chat.proto has no package, so the stub imports its module by a bare name.
sed -i 's/^import chat_pb2 as chat__pb2$/from . import chat_pb2 as chat__pb2/' service/chat_pb2_grpc.py
