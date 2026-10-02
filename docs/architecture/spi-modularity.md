# SPI composition and dependency boundaries

This work separates reusable protocol and dispatch code from optional capabilities.
Java SPI discovers trusted implementations installed in the application. gRPC remains
the boundary for services implemented in other languages or running on other nodes.

## Required outcomes

1. **Actions:** keep public action types, dispatch, authorization, budgets and contract
   validation usable without toolkit implementations. Load action families through
   `ActionProvider`; make schema compilation and named toolkit contracts optional.
2. **MCP:** separate protocol handling from registry, workspace, workflow, delegation,
   code generation and Git capabilities. Preserve a full assembly and provide a
   minimal, usable gRPC integrator assembly.
3. **Service modules:** reuse `ServiceModule`, `NodeContext`, `ServiceMount` and
   `Composer` for configured capability discovery and lifecycle. Remove concrete
   capability construction from general launchers where it prevents independent use.
4. **Registry:** separate store interfaces from Git and workflow implementations so
   applications select their storage without taking unrelated implementation dependencies.
5. **CEL:** extract shared CEL support used by mapping and validation, preserving
   validation semantics and conformance.

## Rules for implementation

- An SPI/core module must not depend on its implementation modules. Application
  assemblies select implementations with runtime dependencies.
- Do not split a production Java package across modules. Preserve existing public
  types where feasible; record required dependency or source migration explicitly.
- Discovery must not start workers, open network connections or load models.
  Configure selected capabilities before starting them. Unselected capabilities must
  not require valid operational configuration.
- Missing selected providers, duplicate identities, invalid configuration and provider
  construction failures must be explicit failures. Do not silently select a different
  provider or replace invalid values with guessed configuration.
- Preserve causes when translating failures. Cleanup failures must remain observable;
  preserve the original failure and attach cleanup failures where appropriate.
- Keep validation and authorization in the dispatch boundary. Providers must not
  advertise capabilities that are not installed or ready to be served.
- Preserve shared budget accounting, cancellation and resource ownership. Shutdown
  and failed startup must release acquired resources in reverse order.
- A module extraction must remove dependencies for a supported consumer or establish
  an independently useful capability. Class splitting alone is not a packaging result.

## Verification

For each boundary, test a minimal consumer without optional implementation JARs and
a full assembly with its existing capabilities. Exercise missing, duplicate and broken
providers. Check generated Maven and Gradle metadata, classpath dependency gates,
existing affected tests and startup/shutdown behavior. Record before/after dependency
counts separately from runtime or performance claims.

The implementation is in progress. Local checks do not establish publication or
deployment; the remaining service, registry and CEL extractions are unfinished.

## MCP transport boundary

`protomolt-mcp-transport` contains JSON-RPC handling, MCP sessions, stdio,
resource composition and the catalog-backed workspace resource. Its production
dependencies are the action core and SLF4J. It does not discover or construct
registry, workflow, delegation, Git, indexing or code-generation implementations.
An embedding application supplies its `ActionCatalog` and optional `McpResources`.
The runtime dependency gate rejects those optional implementation modules.

The existing `protomolt-mcp` remains the full distribution. Its public server,
session and resource types delegate to the transport module, retaining the
original resource page type and registry constructor. Existing callers need no
import changes. New minimal embeddings use `ai.protomolt.proto.mcp.transport`.
The `protomolt-grpc-mcp` assembly loads the `grpc-invoke` service module at runtime,
requiring an explicit target and transport. See [gRPC tools over MCP](../apps/grpc-mcp.md)
for its scope and local build instructions.

Unexpected asynchronous response failures are retained by the session even after
the failed task leaves the in-flight map. Subsequent handling, submission,
dispatch, draining and close report the original cause. Additional concurrent
failures are logged. A drain timeout or interruption is explicit, and interruption
preserves the thread flag. Normal tool error responses and explicit request
cancellation do not poison a session. Cancellation cleanup removes only its own
request, preserving a later request that reused the same ID.

Composer shutdown attempts all resources and channels before reporting cleanup
failures. A startup failure retains its original cause and carries cleanup
failures as suppressed exceptions. Channel termination timeout and interruption
are failures; interruption preserves the thread flag.
