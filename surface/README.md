# surface/

Protocol endpoints over the toolkit: the action catalog and its fronts
(typed gRPC, JSON/REST, OpenAPI, MCP, ACP), dynamic gRPC invocation,
service workspaces, validating interceptors and authorization.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-actions` | `surface/actions` | Action contracts, dispatch, authorization and validation; the catalog core |
| `:protomolt-actions-index` | `surface/actions/index` | Optional index-rendering actions for the catalog |
| `:protomolt-actions-schema` | `surface/actions/schema` | Schema-source resolution provider for action callers |
| `:protomolt-actions-toolkit` | `surface/actions/toolkit` | Optional protobuf toolkit actions |
| `:protomolt-grpc-service-contract` | `surface/grpc/service-contract` | The `ProtoMoltService` definition and its compiled schema |
| `:protomolt-grpc-service` | `surface/grpc/service` | The action catalog as a typed gRPC service, served descriptor-natively |
| `:protomolt-grpc-invoke` | `surface/grpc/invoke` | Dynamic gRPC invocation from descriptors, no generated stubs |
| `:protomolt-grpc-invoke-service` | `surface/grpc/invoke/service` | Configured reflection and invocation contributions for composed nodes |
| `:protomolt-grpc-adapter` | `surface/grpc/adapter` | Descriptor-driven gRPC adapter for a supplied action catalog |
| `:protomolt-grpc-channel-policy` | `surface/grpc/channel-policy` | Validated, host-configurable outbound channel policy |
| `:protomolt-grpc-service-profile` | `surface/grpc/service-profile` | Durable service profiles and descriptor artifacts |
| `:protomolt-grpc-service-workspace` | `surface/grpc/service-workspace` | Agent-facing service registration, inspection and invocation actions |
| `:protomolt-grpc-workflow` | `surface/grpc/workflow` | Descriptor-grounded workflow, artifact and run-evidence contracts |
| `:protomolt-grpc-validation` | `surface/grpc/validation` | Validating gRPC interceptors, server and client side |
| `:protomolt-grpc-validation-micrometer` | `surface/grpc/validation-micrometer` | Micrometer binding for the gRPC validation metrics SPI |
| `:protomolt-http-json` | `surface/http/json` | Protobuf and JSON transcoding, typed and dynamic |
| `:protomolt-http-rest` | `surface/http/rest` | The framework-agnostic REST gateway |
| `:protomolt-http-connect` | `surface/http/connect` | The Connect protocol, binary protobuf only, over in-process gRPC services on the JDK `HttpServer` |
| `:protomolt-http-openapi` | `surface/http/openapi` | OpenAPI 3 generation from service descriptors |
| `:protomolt-http-jsonschema` | `surface/http/jsonschema` | JSON Schema draft 2020-12 from descriptors and validation constraints |
| `:protomolt-mcp` | `surface/mcp` | The MCP server over the action catalog and registry |
| `:protomolt-mcp-transport` | `surface/mcp/transport` | MCP protocol, sessions and stdio transport over a supplied catalog |
| `:protomolt-acp-agent` | `surface/acp` | The action catalog as an Agent Client Protocol agent |
| `:protomolt-authz` | `surface/authz` | Access-policy validation, loading and credential-digest caller resolution |
| `:protomolt-authz-grpc` | `surface/authz-grpc` | Credential interceptor, caller context and the per-method scope table |
| `:protomolt-authz-jdbc` | `surface/authz-jdbc` | Authorization principals in the operator's own PostgreSQL |

Docs: [actions](../docs/surface/actions.md),
[gRPC service](../docs/surface/grpc-service.md),
[MCP](../docs/surface/mcp.md), [ACP](../docs/surface/acp.md),
[REST gateway](../docs/surface/rest-gateway.md),
[service workspaces](../docs/surface/service-workspace.md),
[framework integrations](../docs/surface/framework-integrations.md).
