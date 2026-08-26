# asset/ — typed formats, classification, and characterization

What an asset's bytes *are*, as contract: typed format messages whose
validate rules are the claim's own definition, a classification state
machine that makes "unknown" and "the evidence disagrees" stored facts
instead of silent defaults, and the one detection seam every consumer of
"what is this file" calls. The design of record is
[docs/design/asset-formats.md](../docs/design/asset-formats.md).

| Module | Gradle project | Role |
|--------|----------------|------|
| `asset/proto` | `:protomolt-asset-proto` | The contract: `FormatFact` (the closed format registry, each format's rules annotated), `Classification` (the five-state machine with per-state shape rules), `ContentProfile` (content classes with measured quality) |
| `asset/characterize` | `:protomolt-asset-characterize` | The engine: the shared media-type sniffer, format grammars compiled from the contract's own expressions (descriptor-parity tested), the identifier, the declared-versus-identified compatibility relation, and the state machine's one resolution point |

Consumers: the archive (`repo/`) stores classifications and validates
declarations at its doors; the parse coordinator routes on the shared
sniffer. Pure JDK throughout; internal code passes the generated `v1`
messages — there is no parallel model.
