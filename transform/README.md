# transform/

Mapping, shaping and composition between messages: field mapping with text
rules and CEL, projections, derived shapes, checked pipelines and workflows,
agent delegation, screening and signed work records.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-mapper` | `transform/mapper/core` | Core protobuf mapping APIs: path rules (`target = source`, `+=`, `-target`) |
| `:protomolt-mapper-cel` | `transform/mapper/cel` | CEL filters and selectors applied through the field mapper |
| `:protomolt-mapper-metadata` | `transform/mapper/metadata` | Named metadata extraction from message contents with CEL selectors |
| `:protomolt-shapes` | `transform/shapes` | Joins, unions and derived shapes; runtime message-type synthesis |
| `:protomolt-projection` | `transform/projection` | Self-describing message-to-message projections via descriptor options |
| `:protomolt-pipeline` | `transform/pipeline` | The compiled streaming-aware pipeline contract, checker and in-process executor |
| `:protomolt-workflow` | `transform/workflow` | Checked serial gRPC compositions with gates, run recording and replay |
| `:protomolt-workflow-authoring` | `transform/workflow/authoring` | Validated authoring review and recoverable launch of accepted workflows |
| `:protomolt-delegation` | `transform/delegation` | Delegation coordinator, worker runtime and catalog actions |
| `:protomolt-delegation-proto` | `transform/delegation/proto` | Delegation wire contracts with stable protobuf and generated Java identities |
| `:protomolt-delegation-contract` | `transform/delegation/contract` | Delegation message and deliverable validation, no coordinator or persistence |
| `:protomolt-delegation-lifecycle` | `transform/delegation/lifecycle` | Offline delegation lifecycle replay and the transcript storage interface |
| `:protomolt-delegation-repository` | `transform/delegation/repository` | Encrypted delegation transcript persistence through the repository service |
| `:protomolt-delegation-receipt` | `transform/delegation/receipt` | Projects delegation transcripts into signed-work-record manifests |
| `:protomolt-screening` | `transform/screening` | Model-driven detection over `meta.v1` sensitivity classes (the only OpenNLP consumer) |
| `:protomolt-receipt` | `transform/receipt` | Canonical signed work records, the trust snapshot and the strict offline verifier |

Docs: [field mapping](../docs/transform/mapping.md),
[projections](../docs/transform/projection.md),
[joins and derived shapes](../docs/transform/join-shapes.md),
[pipelines](../docs/transform/pipeline.md),
[workflows](../docs/transform/workflows.md),
[workflow manager](../docs/transform/workflow-manager.md),
[delegation](../docs/transform/delegation.md),
[masking](../docs/transform/masking.md),
[screening](../docs/design/screening.md),
[signed work records](../docs/design/receipts.md).
