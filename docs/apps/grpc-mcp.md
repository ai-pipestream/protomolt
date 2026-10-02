# gRPC tools over MCP

`protomolt-grpc-mcp` is a small stdio application for agents that need to inspect
and call one gRPC endpoint. It exposes two existing tools: `reflect` fetches the
server's descriptors, and `grpc-invoke` uses descriptors to make unary or
server-streaming calls. It does not start a registry, workflow engine, delegation
coordinator, search engine, or browser console.

## Run

Build the application distribution:

```shell
./gradlew :protomolt-grpc-mcp:installDist
```

Configure the endpoint and explicitly choose its transport. For a local plaintext
server on port 9090:

```shell
PROTOMOLT_GRPC_TARGET=127.0.0.1:9090 \
PROTOMOLT_GRPC_TRANSPORT=plaintext \
apps/grpc-mcp/build/install/protomolt-grpc-mcp/bin/protomolt-grpc-mcp
```

Use that executable as the command in a stdio MCP client's configuration, with
those two environment variables. For a TLS endpoint, set the actual host and port
and choose `tls`; it uses the JVM's system trust roots. Protocol messages use
stdout and logs use stderr. There is no HTTP listener or browser login.

Both variables are required. Unknown transport values and invalid target syntax
fail startup. A missing service provider fails startup rather than serving an
empty tool inventory. Configuration selects a host, port, target scheme and
transport; calls to others are refused. The existing outbound policy imposes a
60-second maximum call deadline and 64 active channels shared by both tools.
The action contracts retain their documented per-call bounds and defaults.

The process acts with the authority of its local MCP client. Use a separate
process for a different trust boundary. This launcher does not add credential
resolution, custom trust stores or a multi-user authentication layer.

## Use the tools

Read `protomolt://workspace` for the actual inventory and tool schemas. Call
`reflect` with `target` matching the configured endpoint and `tls` matching the
configured transport. Check `ok` in the result. Reflection must be enabled on
the server for this step; an endpoint without it returns an explicit failure.

Copy the returned `descriptorSetBase64` into `schema.descriptorSetBase64` when
calling `grpc-invoke`. Supply the same target and TLS setting, the method as
`package.Service/Method`, and the method's protobuf JSON request. For example,
a reflected gRPC health service accepts `grpc.health.v1.Health/Check` with an
empty request object. Inspect `ok`, `status` and `responses` before proceeding.
A known complete descriptor set can also be supplied without reflection.

The same request parsing, runtime validation, channel policy and invocation
behavior used by the existing gRPC actions apply here. This assembly does not
add stronger validation semantics to those actions. Client-streaming and
bidirectional-streaming methods are not supported by this MCP tool pair.

## Module boundaries

The launcher depends on `protomolt-mcp-transport` and `protomolt-composer`.
`protomolt-grpc-invoke-service` is a runtime dependency registered through
`META-INF/services/ai.protomolt.proto.composer.ServiceModule`. Its `grpc-invoke`
role contributes the two actions during wiring. Discovery opens no connections;
individual calls acquire and close their channels under a shared policy.

The launcher selects this role explicitly, builds an action catalog from its
contributions, then starts stdio. Other composed applications can reuse the role
with the same environment settings. The underlying `protomolt-grpc-invoke`
library still supports hosts supplying their own `ChannelFactory`.

A runtime dependency gate rejects the full MCP distribution and the optional
registry, workflow, delegation, code-generation, Git and concrete search modules.
The schema resolver remains installed because these tools accept schema sources
and use the existing descriptor-defined request and response contracts.
