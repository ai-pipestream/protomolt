# Coding workers

ProtoMolt coding workers combine a persistent model session with a language
toolchain and the ProtoMolt agent host. A worker can receive a bounded task,
operate on its mounted workspace, publish progress and checkpoints, and submit
evidence for coordinator review. The durable task record stays on the
coordinator, so reconnecting does not depend on the model keeping an HTTP
connection alive.

The published images are intentionally language-specific:

| Image | Language toolchain |
| --- | --- |
| `ghcr.io/ai-pipestream/protomolt-worker-java:edge` | Temurin JDK 21, 25, and 26; GraalVM; SDKMAN; Gradle; Maven; Java gRPC generation |
| `ghcr.io/ai-pipestream/protomolt-worker-cpp:edge` | GCC; Clang; CMake; Ninja; Conan; C++ gRPC libraries and generation |

Both images support `linux/amd64` and `linux/arm64`. Their common layer
contains the Java 21 agent-host runtime, Codex CLI, Node, Bun, Python, uv, Git,
`buf`, `protoc`, `grpcurl`, and the Docker client. The Docker client does not
grant build authority unless a separately controlled builder is configured.

Use one container per trust boundary and language environment. This keeps the
large compiler stacks separate and avoids sharing writable build caches across
unrelated agents. More language images can use the same common layer when a
real workload needs them.

## Current connection model

The agent host runs inside the coding worker and connects to the coordinator's
MCP streamable HTTP endpoint over TLS. It keeps a pooled HTTP/2 connection,
persists its cursor and provider session locally, and reconnects from the
durable coordinator transcript.

```text
coding worker
  model CLI or local model
        |
  protomolt-agent-host
        |  MCP streamable HTTP over TLS
        v
NAS coordinator  ->  encrypted transcript  ->  repository service and S3

coding worker  ->  gRPC reflection, invocation, and generated clients
                ->  registered application services
```

The control link is currently MCP, not gRPC. gRPC is already the application
plane: workers can inspect endpoints through reflection, generate clients, and
call registered services. The planned local sidecar transport will move the
control link and workspace execution behind a typed, private gRPC boundary.
Until that transport exists, do not describe the worker images as gRPC
sidecars.

## State and credentials

Keep each kind of state at its narrowest boundary:

| Data | Owner | Persistence |
| --- | --- | --- |
| Task lifecycle, messages, checkpoints, and evidence | coordinator | encrypted repository-backed transcript |
| Event cursor and provider session id | agent host | mounted state directory |
| Source tree and language caches | coding worker | mounted workspace or named volumes |
| Codex login | Codex provider | mounted `.codex` directory |
| Kimi login and CLI | Kimi provider | mounted `.kimi-code` directory |
| Cursor login and CLI | Cursor provider | mounted `.cursor` directory and the `agent` binary |
| Antigravity login and CLI | Antigravity provider | mounted `.gemini` directory and the `agy` binary |
| Muse login, sessions and CLI | Muse provider | mounted `.config/muse` and `.local/share/muse` directories and the `muse` binary |
| ProtoMolt API token | agent host control link | runtime secret or environment variable |
| Local OpenAI-compatible model access | local Compose network | no bearer token in the verified loopback deployment |

Provider login and the ProtoMolt API token are separate credentials. Never
bake either into an image. Do not copy them into a task, transcript, workspace,
or image layer.

The workspace and state mounts must survive worker replacement. A replacement
host loads the saved Codex thread or Kimi ACP session and asks the coordinator
for events after its committed cursor. The coordinator remains authoritative
when local and remote state disagree.

## Container boundary

The images run as an unprivileged `protomolt` user by default. Match the
container UID and GID to the workspace owner when using bind mounts.

Do not mount the host Docker socket into a coding worker. A model process can
execute arbitrary build commands, so that mount would provide host-level
control. Tasks that need container builds should use an explicitly enabled
rootless or remote builder with its own credentials and policy.

The future sidecar should own coordinator TLS, workload identity, reconnect,
cursor storage, and transcript writes. The coding worker should expose a
bounded task-scoped workspace API on a private endpoint. It should not receive
cluster administration credentials.

## Selecting a Java runtime

JDK 21 is the default in the Java image. The other installed runtimes require
no download:

```shell
source "$SDKMAN_DIR/bin/sdkman-init.sh"
sdk use java 25.0.4-tem
sdk use java 26.0.2-tem
sdk use java 25.2.4-graalce
```

This lets a worker build the current JDK 21 application, exercise newer JDK
targets, and produce GraalVM native images without maintaining one oversized
all-language image.

## Build, run, and verify

[The worker deployment guide](../../deploy/workers/README.md) contains local
build commands, mount examples, published image names, and the smoke test. The
smoke test verifies the common agent and protobuf tools plus each image's gRPC
code generator.

The [agent host guide](agent-host.md) documents provider behavior, cursor
recovery, and coordinator versus worker roles. The [task console](task-console.md)
shows the shared durable task, messages, progress, and review decisions.

The krick-1 deployment uses a Kimi worker and a GPU-only Muse Glimmer worker.
Its current Compose file still builds the smaller agent-host image, so changing
that deployment to the language-specific images is a separate, explicit
migration. See [the krick-1 guide](../../deploy/krick-1/README.md) for the
verified Intel B70 inference settings.
