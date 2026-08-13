#!/usr/bin/env bash
# Builds both coding worker targets and exercises their language-specific gRPC
# generators. This is a CPU-only development test; it starts no inference
# service and needs no GPU.
set -euo pipefail

cd "$(dirname "$0")/.."

./gradlew :protomolt-agent-host:installDist --no-daemon --console=plain

docker build \
  --target java \
  -f apps/agent-host/Dockerfile.workers \
  -t protomolt-worker-java:test \
  apps/agent-host

docker build \
  --target cpp \
  -f apps/agent-host/Dockerfile.workers \
  -t protomolt-worker-cpp:test \
  apps/agent-host

docker run --rm -i --entrypoint bash protomolt-worker-java:test -s <<'JAVA_SMOKE'
set -eo pipefail
source "$SDKMAN_DIR/bin/sdkman-init.sh"
set -u
protomolt-agent-host --help >/dev/null
node --version
bun --version
buf --version
grpcurl --version
docker --version
sdk use java 21.0.12-tem >/dev/null
java -version
sdk use java 25.0.4-tem >/dev/null
java -version
sdk use java 26.0.2-tem >/dev/null
java -version
gradle --version
mvn --version
native-image --version

work=$(mktemp -d)
cd "$work"
printf '%s\n' \
  'syntax = "proto3";' \
  'package smoke;' \
  'option java_package = "smoke";' \
  'message Ping { string value = 1; }' \
  'service Echo { rpc Send(Ping) returns (Ping); }' > smoke.proto
protoc --java_out=. --grpc-java_out=. smoke.proto
test -f smoke/EchoGrpc.java
printf 'java worker smoke: OK\n'
JAVA_SMOKE

docker run --rm -i --entrypoint bash protomolt-worker-cpp:test -s <<'CPP_SMOKE'
set -euo pipefail
protomolt-agent-host --help >/dev/null
java -version
node --version
bun --version
buf --version
grpcurl --version
docker --version
clang++ --version
cmake --version
ninja --version
conan --version

work=$(mktemp -d)
cd "$work"
printf '%s\n' \
  'syntax = "proto3";' \
  'package smoke;' \
  'message Ping { string value = 1; }' \
  'service Echo { rpc Send(Ping) returns (Ping); }' > smoke.proto
protoc \
  --cpp_out=. \
  --grpc_out=. \
  --plugin=protoc-gen-grpc="$(command -v grpc_cpp_plugin)" \
  smoke.proto
c++ -c smoke.pb.cc smoke.grpc.pb.cc $(pkg-config --cflags protobuf grpc++)
printf 'C++ worker smoke: OK\n'
CPP_SMOKE
