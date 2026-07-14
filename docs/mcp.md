# MCP server

`protomolt-mcp` exposes the toolkit to AI agents over the Model Context
Protocol. Every [action](actions.md) becomes an MCP tool with no translation
layer — the catalog manifest's `{name, description, inputSchema}` entries are
already the shape MCP requires, and the input schemas are JSON Schema in both
worlds — and a schema registry is optionally served as MCP resources, so an
agent browses subjects and reads schema versions without spending tool calls.

Together with the gRPC verbs (`reflect`, `grpc-invoke`) this makes any gRPC
service an agent-operable service: given a schema — registered, pasted, or
reflected off the wire — an agent can introspect its types, call its methods,
process the responses, and generate native clients for it in eight languages,
every step machine-verified. See [The gRPC agent workflow](#the-grpc-agent-workflow).

The implementation is deliberately plain Java: JSON-RPC 2.0 over stdio,
newline-delimited, on Jackson and the JDK. No framework, no reactive runtime.
`McpServer.handle(JsonNode)` is the pure message-in/message-out core; the
stdio loop, the tests, and any future transport drive it the same way.

## Running

```shell
./gradlew :protomolt-mcp:installDist
mcp/core/build/install/protomolt-mcp/bin/protomolt-mcp [--registry-git <path>]
```

Register it with an MCP client, for example Claude Code:

```shell
claude mcp add protomolt -- \
  /path/to/protomolt-mcp/bin/protomolt-mcp --registry-git /srv/schemas.git
```

Without `--registry-git` the server exposes the tools only. With it, the
git-backed registry at the path is additionally served as resources.

### Streamable HTTP

The same server is also reachable over MCP's streamable HTTP transport, with
no local install: [`protomolt-serve`](grpc-service.md) mounts it at
`/mcp` next to the gRPC and REST surfaces, so one running process makes
every agent on the network gRPC-aware:

```shell
claude mcp add --transport http protomolt http://host:8080/mcp
```

The server core is stateless, so there is no session handshake to manage;
POST one JSON-RPC message, get one response (`202` for notifications).
Server-initiated streams are not used, and browser requests from non-local
origins are refused (the specification's DNS-rebinding guard). Registry
resources ride along when the launcher mounts a registry.

## Tools

| Tool | Does |
|---|---|
| `compile` | Compile inline `.proto` sources; returns file names and a base64 descriptor set |
| `list-types` | Enumerate messages, enums, and services with fields — the grounding verb |
| `validate-message` | Validate a JSON message against the rules on its schema |
| `diff-schemas` | Typed change list between two schemas (rule, path, impacts) |
| `check-compat` | Compatibility verdict under a mode, with violations and change list |
| `render-json-schema` | JSON Schema (2020-12) for a message type |
| `render-index-mappings` | OpenSearch / Solr / Lucene field specs from indexing hints |
| `eval-cel` | Evaluate a CEL expression against a message |
| `map-message` | Apply text and CEL mapping rules to a message |
| `extract-metadata` | The declared metadata bag for a type |
| `reflect` | Discover a live gRPC server's schema from its address (server reflection) |
| `grpc-invoke` | Call a unary or server-streaming gRPC method, no generated stubs |
| `generate-stubs` | Generate client/message code in eight languages (protoc as WebAssembly) |

Wherever a tool takes a schema it accepts exactly one of `{"type": "fully.qualified.Name"}`
(resolved from the registry), `{"sources": {...}}` (inline `.proto`, compiled
per call), or `{"descriptorSetBase64": ...}` (a serialized `FileDescriptorSet`).
The `reflect` verb returns the third form, so its output is a schema input to
every other verb.

## Resources

With `--registry-git`, the registry is browsable as MCP resources — reads that
do not spend tool calls:

| URI | Contents |
|---|---|
| `protomolt://registry/subjects` | All subjects plus the global compatibility mode |
| `protomolt://registry/subjects/{subject}` | Version index, per-subject mode, latest schema |
| `protomolt://registry/subjects/{subject}/versions/{n}` | One exact version with references |

Subject names are URL-encoded in URIs. All resource contents are JSON.

## The gRPC agent workflow

The three gRPC verbs compose into a single capability: **point an agent at a
running gRPC service and let it operate the service.** The path an agent takes:

1. **`reflect` the address.** If the server enables gRPC server reflection,
   this returns its service names and a descriptor set — no schema needed in
   advance. Feed that descriptor set straight to the next steps.

2. **Fall back to a schema when reflection is off.** Many production servers
   (OpenVINO Model Server, NVIDIA Triton, and others) do not enable
   reflection; `reflect` returns `ok: false` so the agent knows to get the
   schema elsewhere — read it from the registry, or gather the service's
   `.proto` from its Git repository and register it. Either way the agent now
   holds a schema.

3. **`list-types` to ground.** Enumerate the services and messages so the
   agent knows the exact method and message names before calling.

4. **`grpc-invoke` to call.** Unary and server-streaming methods, request and
   responses as proto3 JSON, no generated stubs on either side. gRPC status
   failures come back as `ok: false` with the status name — an outcome to
   reason about, not an input to repair.

5. **`generate-stubs` for a native client.** When the agent (or the human)
   wants a real client rather than JSON-over-MCP calls, generate compilable
   source in java, kotlin, python, cpp, csharp, ruby, php, or objc, plus
   grpc-java service stubs. This is the right move for tensor-heavy or
   high-throughput services, where hand-authoring message JSON is impractical.

A worked end-to-end example against a real OpenVINO Model Server — reflect,
fall back to the KServe schema, introspect the models, and run a text →
embedding inference — is in [Operating an OpenVINO server](tutorials/openvino.md).

## Semantics

- Tool execution failures are MCP tool errors (`isError: true`) carrying the
  action error envelope (`{error, message, details?}`) as structured content,
  so a calling model sees the stable error code and can repair its input.
  Protocol-level problems (unknown method, malformed params) are JSON-RPC
  errors.
- Results carry both `structuredContent` (the action's JSON document) and a
  `text` block with the same document serialized, for clients without
  structured-output support.
- Protocol revisions `2025-06-18`, `2025-03-26`, and `2024-11-05` are accepted
  during initialization; unknown requested versions negotiate down to the
  latest supported.
- Stdout carries protocol traffic only; diagnostics go to stderr, as the stdio
  transport requires.

## Framework hosts

Spring AI and the Quarkus MCP server extension both accept programmatic tool
registration, so the same catalog can be mounted in an existing framework MCP
host without this module's transport. The adapter logic is the thin part;
nothing needs rewriting to move between hosts.

## Related

- [Actions](actions.md) — the verb catalog this server exposes
- [The gRPC service](grpc-service.md) — the same thirteen verbs as typed RPCs and JSON/REST
- [The registry](registry.md) — the store behind the resource URIs
- [Operating an OpenVINO server](tutorials/openvino.md) — a full gRPC-agent walkthrough
- [Roadmap](roadmap.md) — per-method dynamic tools and registry-backed type
  resolution extend this surface next
