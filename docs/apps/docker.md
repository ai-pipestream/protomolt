# Running in Docker

The public demo is a one-liner on Docker Hub. The image is a Docker Hardened
Image runtime (`dhi.io/eclipse-temurin:25-debian13`, Debian 13 JRE, uid 65532,
no shell). Alpine DHI tags are not used.

```shell
docker run -p 8080:8080 -p 9090:9090 pipestreamai/protomolt-serve --demo
```

`--demo` seeds a throwaway git registry and a sample schema under `/tmp`
(`HOME` and `java.io.tmpdir` in this image). Do not add `--read-only`
unless you mount a writable volume or tmpfs on `/tmp`. That is the
opposite of diskless collectors such as grPOIc, which never write.

From a clone, smoke the same Dockerfile before trusting Hub:

```shell
./scripts/serve-dhi-smoke.sh
```

The NAS / Portainer coordinator is a different image:
`ghcr.io/ai-pipestream/protomolt-serve` (uid 10001, bash `HEALTHCHECK`),
published by `.github/workflows/docker-publish.yml`. Do not point that
stack at the Hub DHI tag: Portainer chowns `/data` to 10001.

ProtoMolt also ships two runtime images built from this repository for Compose: the server
(`protomolt-serve`), which exposes the whole API over the network, and the ACP agent
(`protomolt-acp-agent`), which an IDE drives over stdio. `docker-compose.yml` at the
repository root builds those from `apps/serve/Dockerfile` (the GHCR layout), not
`Dockerfile.dhi`. (The release pipeline additionally publishes a
third image, the native `protomolt-cli`: see [The published images](#the-published-images).)

## Build and run

The images are thin JRE layers over a Gradle distribution, so build the distributions first,
then the images:

```shell
./gradlew :protomolt-serve:installDist :protomolt-acp-agent:installDist
docker compose build
docker compose up
```

`up` starts the `serve` container and reports it healthy once `/health` answers. `acp` is a
stdio agent with no port, so it is in a Compose profile and is driven on demand (see
[The ACP agent](#the-acp-agent-stdio) below).

The server starts with `--demo`, which seeds a throwaway git registry, a sample schema, and a
sample workflow, so every surface has something to answer:

| Surface | URL | What it is |
|---|---|---|
| Console | `http://localhost:8080/console` | The web UI |
| Swagger | `http://localhost:8080/docs` | Interactive REST docs |
| OpenAPI | `http://localhost:8080/openapi.json` | The generated spec |
| REST | `http://localhost:8080/grpc-json/ProtoMoltService/{Method}` | JSON in, JSON out |
| MCP | `http://localhost:8080/mcp` | The catalog as MCP tools |
| gRPC | `localhost:9090` | `ProtoMoltService`, reflection enabled |
| Registry | `localhost:8081` | Git-backed, Confluent protocol |

If something local already holds 8080, override the host ports without editing the file:

```shell
PROTOMOLT_HTTP_PORT=38080 PROTOMOLT_GRPC_PORT=39090 PROTOMOLT_REGISTRY_PORT=38081 docker compose up
```

## MCP over HTTP

The MCP endpoint is streamable HTTP: JSON-RPC posted to `/mcp`, answered as JSON.
An MCP client connects with just the URL and negotiates the rest itself:

```shell
claude mcp add --transport http protomolt http://localhost:8080/mcp
```

Exercising it by hand takes the real handshake. Every request must accept both
`application/json` and `text/event-stream` (406 otherwise); `initialize` answers with
an `Mcp-Session-Id` response header; and every later call carries that session id plus
a matching `MCP-Protocol-Version` (404 and 400 respectively when they are missing):

```shell
SESSION=$(curl -sS -D - -o /dev/null \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}' \
  http://localhost:8080/mcp | grep -i '^mcp-session-id:' | cut -d' ' -f2 | tr -d '\r')

mcp() {
  curl -sS -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "mcp-session-id: $SESSION" \
    -H 'mcp-protocol-version: 2025-06-18' \
    -d "$1" http://localhost:8080/mcp
}

mcp '{"jsonrpc":"2.0","method":"notifications/initialized"}'
mcp '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

## The ACP agent (stdio)

The Agent Client Protocol is spoken over stdin/stdout: there is no port to open. An
ACP-capable IDE (Zed, JetBrains AI chat) is configured to launch the container as its agent:

```shell
docker run -i --rm protomolt-acp-agent:local
```

To drive it from this repository and read back a transcript: the same exchange an IDE runs,
build the images (above) and run the smoke driver, which launches the container, initializes,
opens a session, sends one `list` prompt, and prints the verb catalog:

```shell
./gradlew :protomolt-acp-agent:acpSmoke
```

Point the driver at something other than the container with `-Pagent`, e.g. to drive the agent
in-process without Docker:

```shell
./gradlew :protomolt-acp-agent:acpSmoke -Pagent="$(pwd)/surface/acp/build/install/protomolt-acp-agent/bin/protomolt-acp-agent"
```

## Live self-test: the agent calling our own gRPC

`scripts/acp-grpc-live.sh` is a self-hosting proof. It brings up the server, then drives the ACP
agent from a container on the server's Compose network. The agent uses `reflect` on ProtoMolt's
own gRPC service and `grpc-invoke` on one method. It
discovers `ProtoMoltService` by reflection and calls `ListTypes` with the reflected descriptor
set as its schema, so the toolkit describes and calls itself.

```shell
./scripts/acp-grpc-live.sh
```

The same driver runs against any gRPC target and method, and against an agent launched however
you like:

```shell
./gradlew :protomolt-acp-agent:acpGrpcLive \
  -Pagent="docker run -i --rm --network protomolt_default protomolt-acp-agent:local" \
  -Ptarget="serve:9090" \
  -Pmethod="ai.protomolt.proto.grpc.service.v1.ProtoMoltService/ListTypes"
```

## Prove both surfaces at once

`scripts/docker-smoke.sh` runs the whole thing end to end: it builds the distributions and
images, brings up the server, calls it over REST and over MCP (a real initialize plus
`tools/list`), drives the ACP container over stdio, and tears everything down. It uses high host
ports so it does not collide with a local 8080.

```shell
./scripts/docker-smoke.sh
```

## The image under test

The serve image is also exercised end to end from the test suite.
`ContainerSmokeIntegrationTest` in `:protomolt-serve` has Testcontainers build
`apps/serve/Dockerfile` from the same context CI ships: the Dockerfile plus the
`installDist` output: start it with `--demo`, and wait on the image's own
`HEALTHCHECK`. It then asserts every published surface answers over the mapped
ports: `/health` over REST, an MCP initialize over streamable HTTP, a dynamic
gRPC call driven purely by reflection, and the demo registry's subjects and
workflows. The suite runs with the module's ordinary `test` task (which builds
the distribution first) and skips when Docker is unavailable:

```shell
./gradlew :protomolt-serve:test --tests '*ContainerSmokeIntegrationTest'
```

That suite covers `apps/serve/Dockerfile` (the GHCR / Compose image). The
hardened Hub image is smoked separately: `./scripts/serve-dhi-smoke.sh`
builds `Dockerfile.dhi`, starts `--demo`, and waits on `GET /health` from
the host because the DHI runtime has no shell for an image HEALTHCHECK.

## Keeping schemas

`--demo` uses an ephemeral registry that is gone when the container stops. For a registry that
survives restarts, mount a directory and point the server at it. In `docker-compose.yml`:

```yaml
    command: ["--registry-git", "/data/schemas.git"]
    volumes:
      - ./schemas.git:/data/schemas.git
```

## Locking it down

By default every surface is open, which suits a laptop or a trusted network. Set a shared
secret to require it on every operational surface (gRPC, REST, MCP, and the registry); the
documentation surfaces `/health`, `/openapi.json`, and `/docs` stay open. The browser
console is disabled unless a separate task-console login is configured.

```yaml
    environment:
      PROTOMOLT_API_TOKEN: "change-me"
      PROTOMOLT_TASK_CONSOLE_TOKEN: "generate-a-different-32-character-or-longer-secret"
```

This enables `/console/tasks` and its bounded task API. The registry and
general action proxies remain disabled. See [Task console](task-console.md)
for the browser session boundary.

MCP clients then pass it as a header:

```shell
claude mcp add --transport http protomolt http://localhost:8080/mcp --header "api_token: change-me"
```

## The published images

The hardened public demo publishes to Docker Hub on every push to `main`
(`.github/workflows/docker-hub-publish.yml`), as `:latest`, and also as a
version tag when the workflow is dispatched with one. Secrets required:
`DOCKER_USER` and `DOCKER_TOKEN` (a Docker Hub account that can push
`pipestreamai/protomolt-serve` and pull `dhi.io/eclipse-temurin`).

```shell
docker run -p 8080:8080 -p 9090:9090 pipestreamai/protomolt-serve --demo
```

`FROM` lines in `apps/serve/Dockerfile.dhi` pin the Debian 13 multi-arch
index digests (`25-debian13` JRE, `25-jdk-debian13-dev` for the chmod
stage). To refresh after `docker login dhi.io`:

```shell
docker buildx imagetools inspect dhi.io/eclipse-temurin:25-debian13 --format '{{.Manifest.Digest}}'
docker buildx imagetools inspect dhi.io/eclipse-temurin:25-jdk-debian13-dev --format '{{.Manifest.Digest}}'
```

Replace `@sha256:<index-digest>` on each `FROM`. Use the multi-arch index
digest, not a single-platform blob. The Hub catalog specifications pages
for `debian-13/jre-25` and `debian-13/jdk-25-dev` also list the current
index digest. The Hub publish job prints the resolved digests on every run.

GHCR remains the NAS path, published by the existing Docker Publish
workflow (release semver + `latest`, or `edge` on dispatch):

```shell
docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
```

The document platform publishes as `protomolt-document-platform` (JRE 25 — the
search service links the Lucene 11 line, which ships Java 25 bytecode). It needs its
PostgreSQL databases and an S3-compatible store alongside;
[`deploy/document-platform/compose.yml`](../../deploy/document-platform/compose.yml)
is the worked example, and `repo-service` publishes standalone as
`protomolt-repo-service` for split topologies.

The CLI ships as a third image, `protomolt-cli`: the GraalVM native binary on a thin
Debian base, no JRE inside. It is multi-arch (linux/amd64 + linux/arm64): native-image
does not cross-compile, so each architecture builds on a matching runner and a manifest
joins them. Any verb works as the command:

```shell
docker run --rm ghcr.io/ai-pipestream/protomolt-cli list
echo '{"samples": [{"name": "x", "n": 1}]}' | docker run --rm -i ghcr.io/ai-pipestream/protomolt-cli infer-schema
```

Persistent software agents use the language-specific
[`protomolt-worker-java` and `protomolt-worker-cpp` images](coding-workers.md).
They share the agent host, gRPC tooling, Node, Bun, and uv, while keeping the
large Java and C++ compiler stacks in separate images.
