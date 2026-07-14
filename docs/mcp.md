# MCP server

`protomolt-mcp` exposes the [action catalog](actions.md) to AI agents over the
Model Context Protocol. Every catalog action becomes an MCP tool with no
translation layer: the manifest's `{name, description, inputSchema}` entries
are already the shape MCP requires, and the input schemas are JSON Schema in
both worlds. A schema registry can additionally be exposed as MCP resources,
so an agent browses subjects and reads schema versions without spending tool
calls.

The implementation is deliberately plain Java: JSON-RPC 2.0 over stdio,
newline-delimited, on Jackson and the JDK. No framework, no reactive runtime.
`McpServer.handle(JsonNode)` is the pure message-in/message-out core; the
stdio loop, tests, and future transports all drive it the same way.

## Running

```shell
./gradlew :protomolt-mcp:installDist
mcp/core/build/install/protomolt-mcp/bin/protomolt-mcp [--registry-git <path>]
```

Without arguments the server exposes the ten catalog verbs as tools, plus
`grpc-invoke` from `protomolt-grpc-invoke`: call any unary or server-streaming
method on a live gRPC server, driven entirely by descriptors, with the request
and responses as proto3 JSON. The service comes from the same schema-source
convention every action uses, so reading a subject's resource and passing its
text as `sources` makes any registered service callable. gRPC status failures
return `ok: false` with the status name; only malformed input is an error. And
`generate-stubs` from `protomolt-codegen`: protoc's Java and Kotlin generators
plus the grpc-java plugin, compiled to WebAssembly and run inside the JVM, so
an agent can produce a complete, compilable gRPC client for any schema with no
protoc installation on either side. What quarkus-grpc-zero does for a build,
this does as a live call. With
`--registry-git`, the git-backed registry at the path is served as resources:

| URI | Contents |
|---|---|
| `protomolt://registry/subjects` | All subjects plus the global compatibility mode |
| `protomolt://registry/subjects/{subject}` | Version index, per-subject mode, latest schema |
| `protomolt://registry/subjects/{subject}/versions/{n}` | One exact version with references |

Subject names are URL-encoded in URIs. All resource contents are JSON.

Register it with an MCP client, for example Claude Code:

```shell
claude mcp add protomolt -- \
  /path/to/protomolt-mcp/bin/protomolt-mcp --registry-git /srv/schemas.git
```

## Semantics

- Tool execution failures are MCP tool errors (`isError: true`) carrying the
  action error envelope (`{error, message, details?}`) as structured content,
  so a calling model sees the stable error code and can repair its input.
  Protocol-level problems (unknown method, malformed params) are JSON-RPC
  errors.
- Results carry both `structuredContent` (the action's JSON document) and a
  `text` content block with the same document serialized, for clients
  without structured-output support.
- Protocol revisions `2025-06-18`, `2025-03-26`, and `2024-11-05` are
  accepted during initialization; unknown requested versions negotiate down
  to the latest supported.
- Stdout carries protocol traffic only; diagnostics go to stderr, as the
  stdio transport requires.

## Framework hosts

Spring AI and the Quarkus MCP server extension both accept programmatic tool
registration, so the same catalog can be mounted in an existing framework
MCP host without this module's transport. The adapter logic is the thin
part; nothing needs rewriting to move between hosts.

## Related

- [Actions](actions.md) — the verb catalog this server exposes
- [The registry](registry.md) — the store behind the resource URIs
- [Roadmap](roadmap.md) — registry-backed type resolution and more
  generator targets extend this surface next
