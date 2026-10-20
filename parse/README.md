# parse/

The parsing coordinator and its parser fleet contract: CEL routing over
content type and account, scatter-gather fan-out over `ParserPluginService`,
the shared fleet document model, the reference text parser, the gRParse
adapter, and the browser playground.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-parse-proto` | `parse/proto` | Two contracts: `ParseCoordinatorService` and `ParserPluginService`, which is how any parser joins |
| `:protomolt-parse-document` | `parse/document` | The parser-fleet document model: the canonical docling-core v2 parity `document.proto`, plus projections to the repo document |
| `:protomolt-parse-service` | `parse/service` | The coordinator runtime: content sniffing, CEL routing, scatter-gather fan-out, the `SearchMetadata` fold |
| `:protomolt-parse-text` | `parse/text` | The reference parser: pure JDK, plain text and markdown |
| `:protomolt-parse-grparse` | `parse/grparse` | The gRParse adapter: bridges the plugin contract to gRParse's `StreamProcessDocument` |
| `:protomolt-parse-playground` | `parse/playground` | The streaming parser playground, a pure-JDK web front end |

There is no `docs/parse` chapter; the architecture is in
[intake and parsing](../docs/design/intake-and-parsing.md) and the role
wiring in [role nodes](../docs/apps/role-nodes.md).
