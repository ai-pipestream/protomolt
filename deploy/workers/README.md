# Coding worker images

ProtoMolt publishes two coding worker images with a common agent and gRPC
tooling layer:

- `protomolt-worker-java` carries JDK 21, 25, and 26, GraalVM, SDKMAN,
  Gradle, Maven, and the Java gRPC generator.
- `protomolt-worker-cpp` carries GCC, Clang, CMake, Ninja, Conan, the gRPC C++
  development libraries, and the C++ gRPC generator.

Both images carry `protomolt-agent-host` on a Java 21 runtime, Codex CLI,
Node, Bun, Python, uv, Git, `buf`, `protoc`, `grpcurl`, and a Docker client.
The Docker client has no authority by itself. Do not mount the host Docker
socket into an untrusted coding worker. Use a separately authenticated remote
or rootless builder when a task must build an image.

Provider credentials are mounted at runtime. They are not copied into either
image. A workspace and the agent state directory should also be durable
mounts.

Treat each coding container as single-tenant. Its model process can execute
arbitrary build commands and can switch the installed SDKMAN candidates. Do
not place workers from different trust domains in one container or share a
writable language cache between them.

## Build

Build the agent host before either image because the Docker context consumes
its Gradle application distribution:

```shell
./gradlew :protomolt-agent-host:installDist

docker build \
  --target java \
  -f apps/agent-host/Dockerfile.workers \
  -t protomolt-worker-java:local \
  apps/agent-host

docker build \
  --target cpp \
  -f apps/agent-host/Dockerfile.workers \
  -t protomolt-worker-cpp:local \
  apps/agent-host
```

Run both builds and their language-specific gRPC code-generation checks with:

```shell
scripts/worker-images-smoke.sh
```

Use an entrypoint override to inspect the toolchains:

```shell
docker run --rm --entrypoint bash protomolt-worker-java:local -lc \
  'java -version; sdk list java; native-image --version; grpcurl --version'

docker run --rm --entrypoint bash protomolt-worker-cpp:local -lc \
  'clang++ --version; cmake --version; conan --version; grpcurl --version'
```

The Java image defaults to JDK 21. Interactive shells can select another
installed runtime without downloading it:

```shell
source "$SDKMAN_DIR/bin/sdkman-init.sh"
sdk use java 25.0.4-tem
sdk use java 26.0.2-tem
sdk use java 25.2.4-graalce
```

## Run a worker

The image entrypoint is `protomolt-agent-host`. This example runs a Java Kimi
worker. The C++ image takes the same arguments.

```shell
docker run --rm \
  --name protomolt-worker-java \
  --user "$(id -u):$(id -g)" \
  --env PROTOMOLT_MCP_TOKEN \
  --mount type=bind,src="$PWD",dst=/workspace \
  --mount type=bind,src="$HOME/.kimi-code",dst=/home/protomolt/.kimi-code \
  --mount type=bind,src="$HOME/.local/state/protomolt-agents",dst=/state \
  ghcr.io/ai-pipestream/protomolt-worker-java:edge \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role worker \
  --identity java-kimi-worker \
  --provider kimi \
  --workspace /workspace \
  --state /state/java-kimi-worker.json \
  --token-env PROTOMOLT_MCP_TOKEN
```

The current agent host uses MCP streamable HTTP for the durable delegation
control plane. Application discovery, reflection, invocation, generated
clients, and the bundled protocol tools are gRPC. A local gRPC sidecar
transport for the delegation control plane is a separate runtime feature; the
images do not claim that boundary before it exists.

For Codex, mount `~/.codex` at `/home/protomolt/.codex` and select
`--provider codex`. The CLI continues the ChatGPT-backed login stored in that
directory. The provider login and the ProtoMolt control-plane token remain
separate credentials.

## Sidecar boundary

Keep credentials, coordinator TLS, reconnect, cursor persistence, and task
transcripts in the ProtoMolt process. Keep source trees and language build
caches in the coding worker. The intended deployment has a private local
boundary between them and no published worker port.

Until the delegation control plane has a gRPC sidecar transport, run the
agent host inside the selected coding image as shown above. Moving it into a
separate sidecar also requires a bounded workspace-execution protocol. Giving
the sidecar a Docker or Kubernetes administrative socket is not that
protocol.
