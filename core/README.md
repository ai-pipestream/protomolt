# core/

The schema model and its zero-IO foundations: descriptor lookup, protobuf
helpers, `.proto` source compilation without a protoc binary, string format
validators, code generation through WebAssembly protoc, compatibility
checking, shared CEL, and the Agent Client Protocol core. Everything here is
an in-process library with no server and no port.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-descriptors` | `core/descriptors` | Descriptor models and lookup for protobuf schemas, with a loader SPI |
| `:protomolt-helpers` | `core/helpers` | Shared protobuf and JSON helper APIs (`Any`, `Struct`, conversion, message diff) |
| `:protomolt-sources` | `core/sources` | Proto source-set model, import graph, Wire-based compilation and the schema publisher SPI |
| `:protomolt-formats` | `core/formats` | RFC-accurate string format validators (hostname, email, IP, URI), zero runtime dependencies |
| `:protomolt-codegen` | `core/codegen` | Code generation from descriptors: protoc and its generators as WebAssembly, no native toolchain |
| `:protomolt-compat` | `core/compat` | Schema compatibility checking: typed diffs and policy evaluation |
| `:protomolt-cel` | `core/cel` | Shared CEL environments, compilation and evaluation independent of mapping |
| `:protomolt-acp` | `core/acp` | Agent Client Protocol core: newline-delimited JSON-RPC 2.0 on virtual threads |

Docs: [descriptor sources](../docs/core/descriptor-sources.md),
[core utilities](../docs/core/helpers.md),
[compatibility checking](../docs/schema/compatibility.md),
[ACP agent](../docs/surface/acp.md).
