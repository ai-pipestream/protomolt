# apps/

The shipped end-user artifacts: launchers and binaries composed from the
libraries and services, plus the browser console they bundle.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-serve` | `apps/serve` | One-process launcher: gRPC with reflection, JSON/REST with OpenAPI and Swagger UI, MCP over HTTP, optional git-backed registry |
| `:protomolt-document-platform` | `apps/document-platform` | The document platform in one container, or split across nodes by `PROTOMOLT_ROLES` |
| `:protomolt-cli` | `apps/cli` | Run any catalog verb from the terminal or an interactive console; also builds as a GraalVM native image |
| `:protomolt-agent-host` | `apps/agent-host` | Persistent coding-agent process host attached to delegation over MCP |
| `:protomolt-grpc-mcp` | `apps/grpc-mcp` | Minimal stdio MCP application for one configured gRPC endpoint |
| `:protomolt-correction` | `apps/correction` | Fixed recorded contact correction candidate service (tutorial fixture) |
| `:protomolt-record-verifier` | `apps/record-verifier` | External verifier for signed work records: pure JDK, zero dependencies |
| (not a Gradle module) | `apps/console` | The browser console, a standalone Vite app (Node 22+); `apps/serve` bundles `apps/console/dist` when it exists |

Docs: [command line](../docs/apps/cli.md), [console](../docs/apps/console.md),
[agent host](../docs/apps/agent-host.md), [Docker](../docs/apps/docker.md),
[document platform](../docs/apps/document-platform.md),
[role nodes](../docs/apps/role-nodes.md),
[record verifier](../docs/apps/record-verifier.md),
[gRPC tools over MCP](../docs/apps/grpc-mcp.md).
