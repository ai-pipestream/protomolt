# Running in Docker

ProtoMolt ships two images built from this repository: the server (`protomolt-serve`),
which exposes the whole API over the network, and the ACP agent (`protomolt-acp`), which
an IDE drives over stdio. `docker-compose.yml` at the repository root builds and runs both.

## Build and run

The images are thin JRE layers over a Gradle distribution, so build the distributions first,
then the images:

```shell
./gradlew :protomolt-serve:installDist :protomolt-acp:installDist
docker compose build
docker compose up
```

`up` starts the `serve` container and reports it healthy once `/health` answers. `acp` is a
stdio agent with no port, so it is in a Compose profile and is driven on demand (see
[The ACP agent](#the-acp-agent-stdio) below).

The server starts with `--demo`, which seeds a throwaway git registry, a sample schema, and a
sample chain, so every surface has something to answer:

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

The MCP endpoint is stateless streamable HTTP: JSON-RPC posted to `/mcp`, answered as JSON.
Any MCP client connects with just the URL.

```shell
# Point Claude at it:
claude mcp add --transport http protomolt http://localhost:8080/mcp

# Or exercise it directly — initialize, then list the tools:
curl -s -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}' \
  http://localhost:8080/mcp

curl -s -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  http://localhost:8080/mcp
```

## The ACP agent (stdio)

The Agent Client Protocol is spoken over stdin/stdout — there is no port to open. An
ACP-capable IDE (Zed, JetBrains AI chat) is configured to launch the container as its agent:

```shell
docker run -i --rm protomolt-acp:local
```

To drive it from this repository and read back a transcript — the same exchange an IDE runs —
build the images (above) and run the smoke driver, which launches the container, initializes,
opens a session, sends one `list` prompt, and prints the verb catalog:

```shell
./gradlew :protomolt-acp:acpSmoke
```

Point the driver at something other than the container with `-Pagent`, e.g. to drive the agent
in-process without Docker:

```shell
./gradlew :protomolt-acp:acpSmoke -Pagent="$(pwd)/acp/build/install/protomolt-acp/bin/protomolt-acp"
```

## Live self-test: the agent calling our own gRPC

`scripts/acp-grpc-live.sh` is a self-hosting proof. It brings up the server, then drives the ACP
agent — as a container joined to the server's Compose network — to `reflect` ProtoMolt's own
gRPC service and `grpc-invoke` a method on it, over gRPC, container to container. The agent
discovers `ProtoMoltService` by reflection and calls `ListTypes` with the reflected descriptor
set as its schema, so the toolkit describes and calls itself.

```shell
./scripts/acp-grpc-live.sh
```

The same driver runs against any gRPC target and method, and against an agent launched however
you like:

```shell
./gradlew :protomolt-acp:acpGrpcLive \
  -Pagent="docker run -i --rm --network protomolt_default protomolt-acp:local" \
  -Ptarget="serve:9090" \
  -Pmethod="ai.pipestream.protomolt.v1.ProtoMoltService/ListTypes"
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
`apps/serve/Dockerfile` from the same context CI ships — the Dockerfile plus the
`installDist` output — start it with `--demo`, and wait on the image's own
`HEALTHCHECK`. It then asserts every published surface answers over the mapped
ports: `/health` over REST, an MCP initialize over streamable HTTP, a dynamic
gRPC call driven purely by reflection, and the demo registry's subjects and
chains. The suite runs with the module's ordinary `test` task (which builds
the distribution first) and skips when Docker is unavailable:

```shell
./gradlew :protomolt-serve:test --tests '*ContainerSmokeIntegrationTest'
```

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
documentation surfaces — `/health`, `/openapi.json`, `/docs` — stay open, and the browser
console is disabled because a browser cannot hold the secret.

```yaml
    environment:
      PROTOMOLT_API_TOKEN: "change-me"
```

MCP clients then pass it as a header:

```shell
claude mcp add --transport http protomolt http://localhost:8080/mcp --header "api_token: change-me"
```

## The published single image

A release also publishes the server image on its own, for a one-line run without a clone:

```shell
docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
```
