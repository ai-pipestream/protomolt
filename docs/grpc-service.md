# The gRPC service

`protomolt-grpc-service` is the action catalog as a gRPC service, and
`protomolt-serve` is the one-process server that mounts it everywhere at
once: gRPC with server reflection, JSON/REST with a generated OpenAPI
document, Swagger UI, and optionally the git-backed registry — the same
twenty-three verbs on every surface.

The service is defined in protobuf, of course:
`ai.pipestream.protomolt.v1.ProtoMoltService`, twenty-three typed RPCs in
[`protomolt_service.proto`](../grpc/service/src/main/resources/ai/pipestream/protomolt/v1/protomolt_service.proto).
Each request and response message is designed so its canonical proto3 JSON
form is exactly the action's JSON envelope. That one decision makes every
surface identical: a `CheckCompatRequest` over gRPC, the JSON body of
`POST /grpc-json/ProtoMoltService/CheckCompat`, and the `check-compat` MCP
tool input are the same document.

## Served descriptor-natively

The server does not compile the proto to stubs. At startup it compiles
`protomolt_service.proto` with ProtoMolt's own runtime compiler, binds one
dynamic-message handler per method, and attaches the compiled file
descriptor so server reflection lists the service exactly as it would a
stub-generated one. The tool that manages the format is defined in the
format, served through its own machinery, and discoverable by its own
`reflect` verb.

```java
var catalog = ProtoMoltCatalog.full(ActionContext.create());

// Into your own grpc-java server:
serverBuilder.addService(ProtoMoltGrpcService.definition(catalog));

// Or standalone, reflection included:
try (var server = ProtoMoltGrpcServer.start(9090, catalog)) {
    server.awaitTermination();
}
```

## Running everything: protomolt-serve

Operator flags cover every disk location the server may touch:
`--registry-git` for the registry's Git repository and `--gather-cache`
(or `PROTOMOLT_GATHER_CACHE`) for `gather-git`'s clone caches — cache
placement is server configuration, never request input. Nothing else
writes to disk.

When the console has been built (`cd apps/console && npm ci && npm run build`)
before `protomolt-serve`, its bundle rides inside the jar and is served at
`/console` — the schema-registry browser, type explorer, and
connect-a-service wizard, same-origin with the verbs. The server bridges
`/api/protomolt/*` to the in-process registry and `/api/serve/*` back onto
its own REST mount, so the app needs no configuration. Docker images and
release zips ship with the console bundled.

```shell
docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
# or, from a release zip or a clone:
./gradlew :protomolt-serve:installDist
serve/build/install/protomolt-serve/bin/protomolt-serve \
    [--host <addr>] [--grpc-port <n>] [--http-port <n>] \
    [--registry-git <path> [--registry-port <n>]] \
    [--api-token <secret>] [--gather-cache <dir>] [--demo]
```

That is the whole flag set; `--help` (or `-h`) prints it and exits, and any
other argument exits 2.

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--host` | — | `0.0.0.0` | Bind address for every listener — HTTP, gRPC, and the registry |
| `--grpc-port` | — | `9090` | Port for `ProtoMoltService` with server reflection |
| `--http-port` | — | `8080` | Port for the REST mount, OpenAPI, Swagger UI, MCP, and the console |
| `--registry-git` | — | — | Git repository for the registry; mounts the registry server when set |
| `--registry-port` | — | `8081` | Port for the registry server; used only when the registry is mounted (`--registry-git` or `--demo`) |
| `--api-token` | `PROTOMOLT_API_TOKEN` | — | Shared secret guarding every operational surface |
| `--gather-cache` | `PROTOMOLT_GATHER_CACHE` | the library default under the process owner's home | Directory for `gather-git`'s per-repo clone caches |
| `--demo` | — | off | Seed the sample schema described below |

`--demo` seeds a sample order-management schema (validation rules, indexing
hints, metadata, a service) into a temp-directory registry and registers its
types, so `{"type": "demo.shop.v1.Order"}` resolves on every verb the moment
the process is up — Swagger's try-it, grpcurl, and MCP all have material
with zero setup.

```
ProtoMolt serving:
  gRPC  0.0.0.0:9090   ai.pipestream.protomolt.v1.ProtoMoltService (reflection on)
  REST  http://0.0.0.0:8080/grpc-json/ProtoMoltService/{Method}
  API   http://0.0.0.0:8080/openapi.json
  Docs  http://0.0.0.0:8080/docs
  MCP   http://0.0.0.0:8080/mcp   (streamable HTTP)
```

With `--registry-git`, the Confluent-protocol registry server joins the
same process on its own port, so one binary serves schemas, verbs, and
documentation together. The path does not need to exist: the store creates
and initializes the repository on first use, so first startup requires no
`git init`.

The `/mcp` endpoint is the [MCP server](mcp.md) on the streamable HTTP
transport: any MCP client on the network becomes gRPC-aware with
`claude mcp add --transport http protomolt http://host:8080/mcp` — no
local install, and the registry resources ride along when mounted.

### Authentication

`--api-token <secret>` (or the `PROTOMOLT_API_TOKEN` environment variable)
guards every operational surface with one shared secret, compared in
constant time:

- **gRPC** — every call, reflection included, must carry `api_token`
  metadata or an `authorization: Bearer` credential
  (`grpcurl -H 'api_token: ...'`).
- **REST** — every `/grpc-json` method requires the `api_token` header and
  answers 401 without it. The OpenAPI document declares the security
  scheme, so Swagger UI's Authorize button works.
- **MCP** — `/mcp` requires the same header:
  `claude mcp add --transport http protomolt http://host:8080/mcp --header "api_token: ..."`.
- **Registry** — when mounted, every registry route requires the same
  header or bearer credential; only its `/health` stays open.
- **Console** — disabled in token mode (503 with the reason): a browser
  cannot hold the process's shared secret, and a half-secured console
  would be worse than none. Run without a token on a trusted network to
  use it.

`--host` constrains every listener — HTTP, gRPC, and the registry alike.
Documentation surfaces (`/health`, `/openapi.json`, `/docs`) stay open.
Without the flag every surface is open — fine on a trusted network, not
beyond it.

## The gRPC surface

Reflection is on, so any gRPC client works with no schema in hand:

```shell
$ grpcurl -plaintext localhost:9090 list
ai.pipestream.protomolt.v1.ProtoMoltService
grpc.reflection.v1.ServerReflection

$ grpcurl -plaintext -d '{"sources": {"shop/v1/order.proto":
    "syntax = \"proto3\";\npackage shop.v1;\nmessage Order { string id = 1; }"}}' \
    localhost:9090 ai.pipestream.protomolt.v1.ProtoMoltService/Compile
{
  "ok": true,
  "files": ["shop/v1/order.proto"],
  "descriptor_set_base64": "CjsKE3Nob3AvdjEvb3JkZXIucHJvdG8SB3Nob3Au..."
}
```

Action failures map to gRPC statuses: client-repairable codes
(`invalid-input`, `unknown-type`, …) become `INVALID_ARGUMENT`,
`unknown-action` becomes `UNIMPLEMENTED`, and internal faults become
`INTERNAL`. The status description carries `code: message`; trailers carry
the stable code (`protomolt-error`) and, when present, the details document
(`protomolt-error-details-bin`).

And because the service is discoverable and stub-free, ProtoMolt operates
itself: the `GrpcInvoke` RPC of one server can call the `ListTypes` RPC of
another (or the `reflect` verb can discover it), using the service's own
`.proto` as the schema — a case the test suite pins.

## The REST surface

Every RPC is `POST /grpc-json/ProtoMoltService/{Method}` with the same
envelope as the JSON body:

```shell
$ curl -s -H 'content-type: application/json' \
    -d '{"schema": {"sources": {"t.proto": "syntax = \"proto3\"; message T { int32 n = 1; }"}},
         "message": {"n": 6}, "expression": "input.n * 7"}' \
    http://localhost:8080/grpc-json/ProtoMoltService/EvalCel
{
  "result": 42.0,
  "resultType": "int"
}
```

`GET /openapi.json` documents all twenty-three operations with schemas derived
from the same descriptors, and `GET /docs` serves Swagger UI over that
document — a browsable, try-it console with no frontend build. Action
failures with client-repairable codes return 400 with the code in the body;
internal faults return 500 with details kept server-side.

Two proto3 JSON semantics to know:

- Absent means default: a `false`/`0`/empty field may be omitted from
  responses (standard proto3 JSON). `valid` missing means `false`.
- `EvalCel` results ride a `google.protobuf.Value`, whose numbers are JSON
  numbers (doubles) — `42.0` above. The `resultType` label preserves what
  CEL actually produced.

## One catalog, four surfaces

| Surface | Module | Transport |
|---|---|---|
| Java | `protomolt-actions` | `catalog.execute(name, json)` |
| MCP | `protomolt-mcp` | JSON-RPC over stdio or streamable HTTP (`/mcp`), tools + resources |
| gRPC | `protomolt-grpc-service` | `ProtoMoltService`, reflection on |
| REST | `protomolt-serve` | `/grpc-json`, OpenAPI, Swagger UI |

The registry server's native mount (`POST /protomolt/actions/{name}`)
remains available where the registry is the primary surface.

## Related

- [Actions](actions.md) — the verbs and their envelopes
- [MCP server](mcp.md) — the same catalog as agent tools
- [REST gateway and servers](rest-gateway.md) — the gateway underneath the REST surface
- [The registry](registry.md) — the optional third port
