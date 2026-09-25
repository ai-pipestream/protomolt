# Goal 1 inventory coverage and decisions

This reconciliation uses main `dc49800cd66950356a883d99141e00427df6ad65` plus
the local starter contracts. It is a source-backed design inventory, not a claim
that all deployment modes, providers, or modules passed a new integration run.

## Coverage basis

The build declares **150 Gradle projects**, including build-only projects,
adapters, test harnesses, and schema modules. That is not a count of public
libraries or user-facing features. The [module roster](inventory-modules.json)
records every project and directory from [settings.gradle](../../settings.gradle),
with its owning review. The browser console is explicitly included outside that
roster because it is not a Gradle project.

- [Application and execution review](inventory-apps-execution.md): apps, host,
  surface, core, transform, mesh, jobs, inference, and the browser console.
- [Data foundations review](inventory-data-foundations.md): acquire, protobuf,
  schema, repo, account, intake, parse, search, metric, and sink.
- The asset, build, sample, and system-test families are reviewed below.
- [Operation inventory](starter-contract-inventory.md): all 44 methods of the
  descriptor-native ProtoMoltService, the additive evaluator, and their existing,
  extended, or new status. This service is not the entire ecosystem's RPC set.

Cross-check indexes are the [documentation index](../README.md),
[generated action inventory](../generated/action-inventory.json),
[console router](../../apps/console/src/router.ts), court examples, tutorials,
deployment files, and the earlier website research. Catalog counts depend on
mounts: the generated base/full catalog does not include every optional serve
contribution. Neither a documentation label nor a proto declaration establishes
that a handler is mounted.

## Asset families

**Asset contracts and characterization (`asset/proto`, `asset/characterize`):
defer from the minimal correction; reuse for optional document intake in Goals
2/5.** [Format contracts](../../asset/proto/src/main/proto/ai/protomolt/proto/asset/v1/format.proto),
[Characterizer](../../asset/characterize/src/main/java/ai/protomolt/proto/asset/characterize/Characterizer.java),
and [Classifications](../../asset/characterize/src/main/java/ai/protomolt/proto/asset/characterize/Classifications.java)
distinguish format declarations, byte evidence, and classification state.
[CharacterizerTest](../../asset/characterize/src/test/java/ai/protomolt/proto/asset/characterize/CharacterizerTest.java)
and [ClassificationsTest](../../asset/characterize/src/test/java/ai/protomolt/proto/asset/characterize/ClassificationsTest.java)
cover identification and state transitions. The repository's
[ArchiveClassifications](../../repo/service/src/main/java/ai/protomolt/proto/repo/service/ArchiveClassifications.java)
uses this engine. Source/test inspection only; no new file-format conformance run.
The starter's text input does not require archive classification.

**Asset bridges (`asset/bridge`, `asset/bridge-parse`): defer from the minimal
starter; reuse when Goals 2/5 take source documents.**
[BridgeEngine.standard](../../asset/bridge/src/main/java/ai/protomolt/proto/asset/bridge/BridgeEngine.java)
provides container-member and dataset-schema bridges; applicability and executable
capability are distinct. [ParserTextBridge](../../asset/bridge-parse/src/main/java/ai/protomolt/proto/asset/bridge/parse/ParserTextBridge.java)
needs an extraction implementation and distinguishes prose extraction from OCR
with quality evidence. [BridgesTest](../../asset/bridge/src/test/java/ai/protomolt/proto/asset/bridge/BridgesTest.java)
and [ParserTextBridgeTest](../../asset/bridge-parse/src/test/java/ai/protomolt/proto/asset/bridge/parse/ParserTextBridgeTest.java)
provide test-source evidence. [RepoServices](../../repo/service/src/main/java/ai/protomolt/proto/repo/service/RepoServices.java)
defaults to the standard engine and accepts an injected engine. Do not infer
that every parser-backed bridge is mounted by that default.

**Asset catalog (`asset/catalog`): defer from the correction acceptance path.**
[AssetCatalogRows](../../asset/catalog/src/main/java/ai/protomolt/proto/asset/catalog/AssetCatalogRows.java)
projects archive metadata/manifests into typed catalog rows, without inventing
new source facts. [AssetCatalogRowsTest](../../asset/catalog/src/test/java/ai/protomolt/proto/asset/catalog/AssetCatalogRowsTest.java)
and [ArchiveCatalogIT](../../repo/service/src/test/java/ai/protomolt/proto/repo/service/ArchiveCatalogIT.java)
cover projection and repository integration. Source/test inspection only. This
can support later browsing; it is neither a judge nor a replacement receipt store.

## Build and non-service families

- **BOM: reuse for Goal 3 client/version packaging.**
  [bom/build.gradle](../../bom/build.gradle) maintains the supported publication
  list and generated consumer catalog. It is a dependency alignment artifact,
  not a callable API. No release/publication was performed by this review.
- **Samples: reuse and extend for Goals 1/2.**
  [StarterContractTest](../../samples/src/test/java/ai/protomolt/proto/samples/StarterContractTest.java)
  exercises the new small fixture through existing components.
  [CourtDocumentsTest](../../samples/src/test/java/ai/protomolt/proto/samples/CourtDocumentsTest.java)
  covers corpus loading; historical court enrichment is separately recorded in
  the operation inventory. A sample asset is not a seeded production workflow.
- **System tests: reuse for Goals 3/6 integration evidence.**
  [GoldenPathSystemTest](../../tests/system/src/test/java/ai/protomolt/proto/systemtests/GoldenPathSystemTest.java)
  composes the document pipeline;
  [CatalogInventorySystemTest](../../tests/system/src/test/java/ai/protomolt/proto/mcp/CatalogInventorySystemTest.java)
  checks the generated catalog. [Build configuration](../../tests/system/build.gradle)
  declares container-backed dependencies and Java 25. Inspected, not run by
  this reconciliation; these are not substitutes for starter acceptance tests.

## Explicit non-Gradle coverage

- **Browser:** existing routes, editors and task/review/receipt services are
  inventoried in the application review. Reuse components; extend interaction
  and authenticated setup in Goal 3. Current UI tests are source evidence here.
- **Court example and external harness:** reuse as Goal 2 regression inputs,
  with the old imports/jobs commands ported before execution. Historical records
  remain distinct from fresh tests and corpus-scale claims.
- **Deployments:** root demo Compose, document-platform and role Compose,
  NAS/Portainer coordinator, coding workers, and workstation/ARM64 configurations
  provide packaging precedents. Reuse applicable pieces for Goal 3; defer
  host-specific GPU setups from the minimal starter. These files describe desired
  deployment; this review does not prove current containers or credentials.
- **Tutorials and scripts:** reuse existing Python-client, streaming, receipt,
  and OpenVINO tutorials as optional examples. Build/release scripts remain
  operational tooling, not product APIs. Defer provider-specific prerequisites
  from the first no-provider walkthrough.
- **Design/archive docs, generated outputs, licenses and build caches:** use
  design docs as indexes and generated files as build evidence only. They do
  not independently add implemented capabilities. No capability is inferred
  from a cache or an old research label.

## Consequence for Goal 1

### Starter dependency decisions

Every dependency here has an existing implementation owner or an explicitly new
handler owner; the named acceptance cases are specified in the
[implementation backlog](starter-contract-inventory.md#small-implementation-backlog-after-contract-review).

- **Descriptor/schema authority:** reuse core descriptors/sources and the registry
  identified in the two family reviews. `serve --registry-git` supplies named
  schema/workflow storage; startup must load the import closure and pin its
  resolved version. CORE-1 `named_workflow_resolves_pinned_version` and EVAL-1
  `changed_task_contract_refused` verify the runtime binding. Goal 1's sample
  tests already exercise complete closure linking and invalid closure refusal.
- **Mapping and grounding:** reuse `MessageProjection` and checked workflow
  edges. The sample supplies the projection in its descriptor; CORE-1 checks
  excluded fields and invalid grounding before invoking a provider. Goal 1's
  projection fixture already exercises field exclusion.
- **Generation:** reuse the inference model catalog, structured generator and
  workflow runner. `serve` owns model/profile configuration and credentials;
  the no-provider acceptance fixture supplies a deterministic implementation.
  CORE-1 covers repair/exhaustion and provider failure; no new public generation
  method is required.
- **Evidence and signing:** reuse workflow artifact/run repositories and record
  operations. `PROTOMOLT_WORKFLOW_WORKSPACE` owns persisted run artifacts; signing
  and verification require separately configured keys/trust. CORE-1 covers
  recorded outcomes, offline replay, signature verification, changed source and
  insufficient redacted evidence. No successful plain run implies a receipt.
- **Semantic evaluation:** new inference evaluator handler, not currently mounted.
  EVAL-1 owns policy/profile resolution, stored artifact/offer matching, typed
  response validation and source support; REVIEW-1 owns atomic freshness and
  idempotency/cancellation. The new RPC contract is the seam, not a delivered
  provider integration.
- **Durable async execution:** reuse jobs service/JDBC when selected. Kafka is an
  optional request/event transport, not a prerequisite for the synchronous first
  run. AUTHOR-1 covers checkpoint/restart and reused-ID conflicts; ENTRY-2 proves
  persistence for the selected starter configuration. Do not promise new job
  cancellation or exactly-once external effects.
- **Browser and transports:** reuse console editors, catalog, existing
  gRPC/MCP/ACP adapters and workspace discovery. ENTRY-1 covers transport/schema
  equivalence; ENTRY-2 covers bundled assets, authenticated browser access and
  clean startup. Choose between serve and role composition only after comparing
  their actual mounted surfaces, not their names.
- **Coordination extension:** reuse delegation, agent host, configured transcript
  repository and mesh directory. Distributed mode additionally needs repository
  storage/keys and provider/session configuration. REVIEW-1 precedes automated
  review; COORD-1 covers waiting/recovery and changed scope. These dependencies are
  for the coordination template, not mandatory for inline correction.

All other families have explicit defer/reuse dispositions in the reviews. A later
document-backed court scenario adds intake, parsing and optional retrieval only
when its source format requires them, with COURT-1's source support cases.

The audit found reusable implementations and packaging constraints, not a need
for a second workflow language, storage abstraction, or structured-generation RPC.
Keep the reviewed wire definitions. Tighten source/offer identity obligations,
map setup/recovery/court evidence gaps to acceptance cases, and select a host
through the documented Goal 3 comparison. Goal 2 remains the bounded recorded
correction path, with a deterministic fixture before optional external services.
