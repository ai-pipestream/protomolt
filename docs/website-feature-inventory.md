# Website feature inventory

An investigation deliverable for the website team, grounded in the repository
as of September 2026 (`main`, pre-1.0, publishing `0.1.0-SNAPSHOT`). Every
claim below is verifiable in this tree; where a capability is a prototype,
unwired, or planned, that is stated. The
[README](../README.md) is the module-level source of truth; this document is
the marketing-facing distillation plus a fact-check of the founder's summary.

## 1. Architecture overview

ProtoMolt is a Java toolkit and platform whose organizing idea is:
**the protobuf descriptor is the semantic layer.** A message declares its
validation rules, index mapping, vector configuration, projection provenance,
metric membership, LLM prompt shape, metadata, and quality dimensions once, as
descriptor options, and every subsystem reads those declarations. All code
operates on descriptors (`Descriptor`/`FileDescriptor`), never generated
classes, so the same paths serve runtime-resolved `DynamicMessage`s and
compiled-in types.

The tree splits into three tiers (140 Gradle modules):

1. **Libraries (98 modules)** — in-process, no server, no port. Descriptor
   registry and helpers, an in-memory `.proto` compiler (Square Wire, no
   protoc binary), the validation engine and dialects, the CEL engine, field
   mapping, projections, shapes/joins, chunking, embeddings, index mappers
   (Lucene, OpenSearch, Solr, Qdrant), sinks (Parquet, Iceberg, Kafka serde
   and Connect), schema-registry clients, and protoc-as-WebAssembly codegen.
2. **Services (24 modules)** — each defines a gRPC or HTTP contract:
   the 44-RPC `ProtoMoltService` verb surface, the Confluent-protocol schema
   registry, and the document platform lane (intake, repo/document store,
   parse coordinator, search, metrics, jobs, inference, accounts).
3. **Servers and apps (18 modules)** — `apps/serve` (the one-process demo),
   `apps/document-platform` (a role-composable node binary), a CLI that also
   builds as a GraalVM native image, six REST server hosts (JDK, Vert.x,
   Netty, plus Spring/Micronaut/Quarkus facades), Spring Boot and Quarkus
   integrations, browser consoles, and a multi-agent host.

Two cross-cutting structures matter for explaining the product:

- **One integration primitive.** `ProtoAction` is a JSON-in/JSON-out verb
  with a JSON Schema and a required authorization scope. A catalog of 72
  verbs is dispatched by eight protocol fronts: typed gRPC with server
  reflection, JSON/REST, OpenAPI 3 + Swagger UI, MCP (stdio and streamable
  HTTP), ACP over stdio, and a CLI with an interactive console. 44 verbs are
  typed RPCs and appear on every front; 28 are contributed at wire time and
  reach HTTP only via the registry's actions route.
- **Nine annotation families.** `validate.v1`, `meta.v1`, `quality.v1`,
  `llm.v1`, `index.hints.v1`, `projection.v1`, `metric.v1`, `mesh.v1`, plus
  the vendored `buf.validate` dialect. Six have shipped runtime readers;
  `metric.v1` is read but no `.proto` in the tree uses it yet; `mesh.v1` has
  no main-source reader (contract pinned by test only).

**Deployment shapes:** one container (`protomolt-serve` or
`protomolt-document-platform` with all roles), or split across nodes by
`PROTOMOLT_ROLES` with remote roles reached over `PROTOMOLT_<ROLE>_TARGET`.
Twelve roles: repo, intake, parse, parse-text, search, metric, registry, jobs,
playground, search-console, acquire-s3, acquire-jdbc.

## 2. Feature catalog

Status vocabulary: **shipped** (exercised by tests/CI), **working, pre-release**,
**prototype/unwired**, **proto only** (contract exists, no runtime),
**planned** (documented in `docs/design/planned-work.md`, not in the tree).

### Schema toolkit (the original library layer)

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Descriptor registry & helpers | Lookup by name with a loader SPI; `Any` packing, type conversion, message diff, schema hygiene checks | Java teams working with dynamic protobuf | Shipped |
| In-memory proto compiler | Compiles `.proto` source sets with Square Wire on an in-memory filesystem — no protoc binary | Anyone compiling schemas at runtime | Shipped |
| CEL mapping API | Text mapping rules (`target = source`, `+=`, `-target`) plus one shared CEL engine used by validation, quality, projection, metrics, shapes, and Connect transforms | Data/integration engineers | Shipped |
| Validation engine | `validate.v1` dialect (36 string rule tags, taxonomy/postal gates) behind a neutral rule-source SPI | Schema owners | Shipped |
| protovalidate compatibility | Schemas annotated with `(buf.validate.field)` / `(buf.validate.message)` options in `.proto` files — including predefined-rule extensions — validate end-to-end through the same `ProtoValidator`; the module vendors `buf/validate/validate.proto` (pinned v1.2.2) and re-parses descriptors built without the extension registry so the options are never silently dropped. **Passes buf's full protovalidate conformance suite, 2872 of 2872 cases, no skip list, enforced in CI on every push.** The same options also feed JSON Schema generation through the neutral constraint model | Teams already on buf validate | Shipped |
| Breaking-change detection | Typed schema diff (33 stable rule ids) plus the seven Confluent compatibility policies | Registry operators, CI | Shipped |
| Code generation, no protoc | protoc's own generators bundled as one WebAssembly module: java, kotlin, grpc-java, python, cpp, csharp, ruby, php, objc; works inside the native-image CLI | Polyglot consumers | Shipped |
| JSON Schema / OpenAPI generation | JSON Schema 2020-12 folding in validation rules; OpenAPI 3.0.3 from service descriptors | API publishers | Shipped (OpenAPI does not read validation rules) |
| String format parsers | RFC-accurate email/hostname/IP/URI/date/GTIN parsers, zero dependencies, no `java.util.regex` | Validation internals, reusable standalone | Shipped |
| Schema inference | Reverse-engineer a proto definition from JSON samples (`infer-schema`) | Migration/onboarding | Shipped |
| Joins & derived shapes | Synthesized envelope/projection/tagged-union types emitted as real `.proto` source, so a join's output is a governed schema | Data modeling teams | Shipped |
| Projections | Per-field provenance declared as options on the target type (candidate paths, CEL, literals) | Message-to-message transformation | Shipped (map fields refused) |
| Quality scoring | CEL-scored dimensions declared as message options, weighted mean | Data quality owners | Shipped |
| Sensitivity masking & metadata | `meta.v1` descriptions, owners, sensitivity classes; mask/redact/encrypt by class | Governance | Shipped |
| LLM prompt rendering | Renders a descriptor into a complete form-filling briefing: descriptions + constraints + instructions + the JSON Schema decoder constraint + rejection feedback for retries | Teams doing structured LLM extraction | Shipped |

### Serving any gRPC service to agents and HTTP

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Dynamic gRPC invocation | Invoke any method on any server from descriptors — every streaming shape, no generated stubs; client-side server reflection | Agents, tooling, integrators | Shipped |
| MCP server | The verb catalog over MCP (stdio and streamable HTTP with sessions/cancellation), plus `protomolt://` resources. Combined with `reflect`, `grpc-invoke`, and the service-workspace verbs, **an AI agent can discover, register, inspect, and invoke any reflectable gRPC service through MCP** — this claim is real and is the flagship demo (`claude mcp add --transport http protomolt …`) | AI/agent teams | Shipped |
| ACP agent | The same catalog as an Agent Client Protocol agent over stdio (JetBrains AI chat, Zed) | IDE users | Shipped |
| JSON/REST gateway + OpenAPI + Swagger UI | `POST /grpc-json/{Service}/{Method}` over any descriptor source; six host options (JDK, Vert.x 5, Netty 4.2 native; Spring/Micronaut/Quarkus facades); Spring Boot auto-configuration and a Quarkus extension | Java platform teams | Shipped |
| Service workspace | Durable gRPC service profiles: register by endpoint, inspect methods/shapes, refresh fingerprints, invoke by profile name | Agents and operators | Shipped (credentials on profiles accepted but refused at call time — authenticated reflection is planned) |
| Validating gRPC interceptors | Declared rules enforced at the call boundary, server and client side, with Micrometer metrics | Service owners | Shipped |
| CLI + native image | Any verb from the terminal, interactive console; GraalVM native build (~10 ms startup), published as a multi-arch container | Ops, scripting | Shipped |

### Schema registry

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Git-backed registry | The git repository *is* the storage: one commit per registration, monotonic global id, compatibility write gate, transitive reference resolution | Platform teams wanting auditable schema history | Shipped |
| Confluent-protocol surface | Speaks the Confluent subjects REST protocol, so Confluent-wire clients (including ProtoMolt's own serde) work against it; plus native routes for descriptor sets and Parquet schema derivation | Kafka shops | Shipped (server side; the API is HTTP, no registry gRPC) |
| Registry federation | Pull-only git-to-git sync of whole remotes (`registry-remotes`, `registry-sync`) | Multi-registry setups | Shipped, narrow (no subject selection, no push, no Confluent/Apicurio mirroring — that's planned) |
| Confluent & Apicurio clients | Read client for Confluent's REST API and a publisher (reverse-topological order); Apicurio Registry v3 native loader/publisher as a Quarkus extension | Existing registry users | Shipped, client-side only |
| Config lane | Typed protobuf config documents (access policy, trust snapshots, taxonomies, postal codes) served from the registry or a compacted Kafka topic, applied verify-then-swap | Operators | Shipped |

### Kafka

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Confluent-wire protobuf serde | Exact Confluent framing (magic byte, schema id, message-index array), declared rules enforced on write, Confluent-identical subject strategies, loud refusal over silent id fallback; Kafka Streams `Serde` included; Micrometer metrics | Kafka producers/consumers | Shipped |
| Kafka Connect: gRPC sink & source | A sink that drives any gRPC method from a topic; a source feeding a topic from a server stream with CEL resume-token offsets (the only acquisition path with durable resumption) | Connect operators | Shipped |
| Kafka Connect transforms | Four protobuf-aware SMTs: validate, map, redact, CEL filter | Connect operators | Shipped |
| Kafka Connect: Iceberg & OpenSearch sinks | Topic records to Iceberg snapshots (at-least-once); topic records to OpenSearch through the index-hint SPI with `Any` expansion and deterministic ids | Lakehouse & search pipelines | Shipped |

### Search, vectors, and metrics

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Indexing hints dialect | `index.hints.v1` descriptor options: field types, analyzers, **vector dims/similarity and HNSW parameters**, sub-fields, chunk-and-embed policy, `Any` payload gating | Search platform teams | Shipped |
| Index mappers | One mapping model rendered per engine: **Lucene** (mapper + field specs), **OpenSearch** (`knn_vector` mappings, field-level security, thin HTTP sink, reranked semantic search), **Solr** (managed schema incl. `DenseVectorField`), **Qdrant** (points + schema + gRPC sink), NDJSON export | Search platform teams | Shipped; OpenSearch is the only backend with a read side outside the Lucene service; Solr and Qdrant are write-shape only |
| Search service | Lucene-backed gRPC query + indexing over mapping subjects: lexical, vector, and hybrid lanes (reciprocal-rank fusion), chunk-and-embed at index time; read-only replica nodes restore from S3 index snapshots | Application search | Shipped (rerank providers exist but are not called by the service; snapshot store is prototype-grade) |
| Deterministic chunking | One sentence-packed strategy over versioned boundary rules — equal policy digests guarantee equal chunk boundaries | RAG pipelines | Shipped |
| Embeddings | Provider SPI with three implementations: Model2Vec (in-process, OpenNLP-backed), Hugging Face TEI (gRPC), OpenVINO Model Server (KServe v2); pairwise cosine-equivalence certification before mixing providers | RAG pipelines | Shipped |
| Reranking | Provider SPI with TEI and OVMS implementations plus rank-equivalence certification | RAG pipelines | Shipped as libraries, **unwired in the mounted search service** |
| Metrics over the same index | `metric.v1` declarations compiled into bounded aggregate queries; Lucene executor over the search service's doc values (no ETL) and an Iceberg/DuckDB lake executor; per-principal row and member security | Analytics on operational data | Shipped, but **no `.proto` in the tree uses the metric options yet** — exercised programmatically in tests |

### Document platform

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Intake service | The only authenticated way in: API keys resolve to scopes (account, datasources, mime types, payload cap); gRPC unary + client-streaming plus one HTTP upload route; in-memory, JDBC, and OIDC key stores | Ingest operators | Shipped |
| Claim-check document store (repo) | Documents split into parts (`core.pb`, blobs, parsed, chunks) reassembled by field-level merge; S3 blob store, Postgres ledger, Merkle-root dedupe, two-phase purge, transactional outbox for document events | Document platform operators | Shipped; **no authentication by design — must not be network-reachable by untrusted callers** |
| Parse coordinator | Magic-byte sniffing, CEL routing rules, scatter-gather fan-out over a `ParserPluginService` contract any parser can implement; reference plain-text/markdown parser included; adapter for the external gRParse C++ fleet (17 parser repos, docling-v2-parity document model) | Document processing | Shipped; gRParse fleet is a separate project |
| Jobs (durable workflows) | Asynchronous workflow execution: Postgres as truth, Kafka request topic in, lifecycle events out through a transactional outbox; parked external steps resumable via `complete-step` | Long-running orchestration | Shipped |
| Accounts | Account CRUD + drive provisioning (two drives per account) with an event outbox | Multi-tenant setups | Shipped, not in default roles |
| Pull connectors | S3 (list past a `(lastModified, key)` watermark) and JDBC (caller's watermark query) feeding intake; bounded single passes, resumption returned to the caller — deliberately no crawler and no scheduler | Content ingestion | Shipped as verbs; the role mounts are inert (call-triggered only) |
| Confluence connector | 19-proto domain model of Confluence Cloud, REST crawler with resume cursors, `ChangeSink` SPI (Kafka, repo, Parquet…), own gRPC proxy service | Confluence content ingestion | Working, pre-release (8 of 27 entity arms crawled, no deployment artifact) |
| Microsoft Graph | Hand-rolled OAuth2 + OneDrive/SharePoint/Copilot connector client, no Microsoft SDK | M365 content | Library exists, **unwired** (no verb, no role, no incremental checkpoints) |

### Storage and lake sinks

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Parquet, Hadoop-free | Descriptor-driven Parquet from `Descriptor` + `Message`, no generated classes, no Hadoop (a test fails if a Hadoop class loads); S3 upload variant | Lake pipelines | Shipped |
| Apache Iceberg | Descriptor-driven table schemas and append sink through any catalog; `S3FileIO` wiring for any S3-compatible store | Lakehouse | Shipped (equality deletes and v3 variant columns are later phases) |
| Pluggable object storage | `BlobStore` (repo blobs) and `SnapshotStore` (search index snapshots) are provider-neutral interfaces — callers never see provider SDK types, so Azure Blob or GCS is one new adapter, not a sweep. Shipped adapters target **any S3-compatible endpoint** (AWS SDK with path-style endpoint override: AWS, MinIO, SeaweedFS, RustFS), with Redis and caching blob-store decorators; Parquet and Iceberg sinks are S3-wired today | Self-hosters and AWS users | Shipped (S3-compatible adapters only; no Azure/GCS adapter in-tree yet) |
| Bundle sinks | Rendered file bundles to directory, git (JGit), zip; OKF markdown knowledge bundles from schemas | Docs/knowledge pipelines | Shipped (S3 bundle sink planned, does not exist) |

### Inference and LLM integration

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Inference provider SPI | Three-method provider contract, in-memory model catalog, credential resolver that never leaks material; OpenVINO Model Server and OpenAI-compatible (`/v1`: Ollama, vLLM, llama.cpp) providers | Self-hosted model users | Shipped (catalog rebuilt from flags at start; not persistent) |
| Inference gRPC service | `Generate`, `GenerateStream`, `ListModels`, `DescribeModel`, requests validated against declared rules before any provider is touched | Remote inference consumers | Shipped, but **has no production client** — every in-tree consumer uses the in-process facade |
| Structured generation | Fill a protobuf message with a model: prompt packet + JSON Schema decoder constraint + strict parse + validation + retry from rendered rejection feedback (max 3), every attempt carried as provenance | Structured extraction | Shipped (unary only, by design) |
| Court-document metadata PoC | A worked, in-tree case study (`protobuf/llm/examples/court-decoration/`): the CourtListener corpus (9.7M opinions, cleaned via a sibling gRPC HTML-rewriter service, chunked to 86.6M), metadata extracted by LLM against an `llm.v1`/`validate.v1`-annotated proto form, output stored as citation-backed claims that can be challenged and re-derived; four unedited QA rounds with findings | Evidence for the structured-extraction story | Proven PoC / case study (harness lives outside this repo; outputs and schema are in-tree) |
| Screening | OpenNLP model-driven detection over declared sensitivity classes with mask/tag/refuse policy; **the model manifest version is carried as evidence** with every decision | PII/compliance | Shipped as a library (no gRPC service of its own; masking is reachable via the `mask-message` verb) |

### Workflows, pipelines, and evidence

| Feature | What it is | For whom | Status |
|---|---|---|---|
| Workflows | Checked serial gRPC compositions with gates, deadlines, joins; record real runs as redacted content-addressed fixtures, replay offline, report drift, promote to immutable registry versions | Integration teams | Shipped (11 verbs) |
| Pipelines | The compiled streaming form: typed edges, offline checker, in-process executor covering every gRPC streaming shape plus structured generation, unnest, collect, bounded fan-out | Streaming compositions | Shipped in-process; external-completion steps refused (no coordinator wired) |
| Signed work records | Ed25519-signed canonical work records with detached signatures, digests, trust snapshots, a strict offline verifier, and a zero-dependency external verifier app | Anyone needing portable proof of work done | Shipped |
| Agent delegation | Coordinator/worker bidirectional contract: bounded task offers, leases, checkpoints, candidate review, encrypted transcripts; 12 verbs; a persistent agent host with six provider adapters | Multi-agent deployments | Shipped in-process via `apps/serve`; live-proven Codex-to-Kimi path; symmetric collaboration not yet proven |
| Mesh cluster directory | Memory-resident, deterministic cluster directory with fenced presence/capacity and encrypted restart persistence; 6 verbs | Distributed deployments | Shipped in-process (no port of its own); the `mesh.v1` routing options have no runtime reader yet |
| Authorization scopes | Closed 10-scope vocabulary enforced identically at catalog dispatch, gRPC, REST, and the registry route; access-policy documents, OIDC introspection, JDBC principals, per-scope budgets | Operators | Shipped (MCP resource reads remain authenticated-only, not scope-filtered) |

## 3. Gaps versus the founder's list

Verdicts: **confirmed**, **needs rewording**, **understated**, or **not in this repo**.

| Founder claim | Verdict | Detail |
|---|---|---|
| Started as protobuf library helpers (CEL mapping API, descriptor helpers) | **Confirmed** | `core/` + `transform/mapper*` are exactly this and remain the foundation. |
| gRPC services exposable as MCP + OpenAPI | **Confirmed, understated** | It's eight fronts, not two: typed gRPC + reflection, JSON/REST, OpenAPI 3, Swagger UI, MCP (stdio + streamable HTTP), ACP, CLI. And the inverse also holds: ProtoMolt can *consume* any reflectable gRPC service dynamically. |
| Validations compatible with buf validation (`buf.validate` annotations in `.proto` files) | **Confirmed** | The founder means the protobuf annotation syntax, and that is supported end-to-end: `.proto` files carrying `(buf.validate.field)` and `(buf.validate.message)` options — including predefined-rule extensions — compile and validate through `ProtoValidator` with the vendored `buf/validate/validate.proto` (pinned v1.2.2), and options on descriptors built without the extension registry are recovered by re-parse rather than dropped. Conformance-proven: **2872 of 2872 cases of buf's own protovalidate conformance suite, no skip list, enforced in CI on every push.** Website wording: "supports buf's protovalidate annotations, proven by buf's full conformance suite." ProtoMolt additionally has its own `validate.v1` dialect, richer in places (taxonomy, postal, `ignore_if_zero`, `skip_when`). |
| Proto enhancements for vectors; Lucene, Solr, OpenSearch | **Confirmed, incomplete** | `index.hints.v1` carries vector dims, similarity, and HNSW parameters as descriptor options. Four engine mappers, not three: **Qdrant** is also shipped. Caveat: Solr and Qdrant are write-shape only; only the built-in Lucene service and OpenSearch have a read side. |
| Rust TurboQuant search engine coming | **Imminent, sibling repo** | turbovec (TurboQuant scalar quantization + SIMD exact scan, exact by construction) is a sibling ai-pipestream repository; the founder reports integration is about a week out. For website planning treat it as a present/near-term search engine option and feature it accordingly — but as of this commit there is no integration code in this tree, so keep the site honest that the engine ships from its own repo and lands as a protomolt search provider. distributed-search/knn-node (streamed cross-shard vector search on a forked Lucene) already consumes protomolt's mapping SPI. |
| Kafka Connect compatibility | **Confirmed, understated** | Not just compatibility: a gRPC sink, a resumable gRPC source, four protobuf-aware transforms, plus Iceberg and OpenSearch sink connectors, and a Confluent-wire serde with schema enforcement. |
| Pipeline architecture | **Confirmed with caveat** | Workflows (serial, durable via jobs) are complete including record/replay/promote. Streaming pipelines are checked and executed in-process, but external-completion steps are refused — durable pipeline execution is planned, not shipped. |
| Inference + evidence-based document cleanup (proven PoC: court documents, metadata filled on the fly) | **Confirmed — the PoC is in-tree** | `protobuf/llm/examples/court-decoration/` is the saved, unedited record of a real metadata pipeline over the CourtListener corpus (9.7M court opinions → 86.6M chunks). The document cleanup pass ran through `grpc-lol-html`, a sibling gRPC service wrapping Cloudflare's streaming HTML rewriter. The metadata shape is a protobuf message annotated with `llm.v1` instructions and `validate.v1` rules; `render-prompt` compiles it into the model's instruction packet and `validate-message` deterministically checks the output. Model output is stored as a **claim with citations into the source text and the index generation**, challengeable and re-derivable — that is the "evidence-based" part, and it's genuine. Four QA rounds (3 personas × Qwen3-14B) are saved with findings; round 1 was 45/45 schema-valid, and the `skip_when` validator feature was built for this. Website framing: "schema-as-contract metadata extraction, proven on 9.7M court opinions" — a PoC/case study, not a packaged product feature. |
| Confluent-compatible schema registry backed by git | **Confirmed** | The registry speaks the Confluent subjects protocol and git commits are the storage. Also worth saying: client-side publishers for real Confluent and Apicurio registries. Don't claim mirroring/federation between the three — only pull-only git-to-git sync exists. |
| Storage: generic interface, S3 and other providers can plug in | **Half confirmed** | The seam is real: `BlobStore` (repo blobs) is an explicitly provider-neutral port — its own contract states callers never see provider SDK types so "a future backend (Azure Blob, GCS, …) is one new implementation" — and `SnapshotStore` (search index snapshots) is likewise an interface. But the only provider adapters shipped today are S3-compatible (`S3BlobStore`, used against AWS S3, SeaweedFS, MinIO, RustFS via endpoint override) plus Redis/caching decorators; Parquet and Iceberg sinks are also wired to S3 only (Iceberg's own Azure/GCS FileIO bundles would be the path there). Accurate website claim: "pluggable object storage behind a provider-neutral interface; S3-compatible stores supported today, Azure/GCS are one adapter away" — do not claim Azure works today. |
| MCP can expose ANY gRPC service | **Confirmed** | Via `reflect` + `grpc-invoke` + the service-workspace verbs over MCP, an agent registers, inspects, and invokes any reflectable gRPC service with no stubs. This is the strongest single demo the project has. |
| Platform + library; protobuf/gRPC-focused application framework | **Confirmed** | 98 library modules usable independently; the platform composes from the same modules via roles. |

Things the founder's list omits that are website-worthy:

- **The action catalog** — 72 verbs, one scope model, eight fronts. This is the
  cleanest way to explain the whole surface story.
- **Code generation without protoc** — protoc's generators as a WebAssembly
  module, nine targets, works in the native CLI (e.g. "generate a Python
  client without installing protoc").
- **Native CLI** — GraalVM image, ~10 ms startup, multi-arch container.
- **The document platform** — intake → claim-check store → parse → search →
  metrics as one container or role-split nodes, one env var to choose.
- **Signed work records** — offline-verifiable Ed25519 receipts with a
  zero-dependency external verifier. Distinctive and fully shipped.
- **Agent delegation** — durable coordinator/worker task protocol with
  encrypted transcripts and live proof (single direction).
- **Deterministic chunking + embedding provider certification** — quiet but
  differentiating rigor for RAG (equal policy digests ⇒ equal chunks; two
  providers must prove cosine equivalence before mixing).
- **Hadoop-free Parquet and Iceberg** — a genuine operational selling point.
- **The nine annotation families** — the "declare once on the schema"
  story that ties everything together.

## 4. Suggested website sections and visuals

Plain-language, evidence-backed sections; each maps to shipped code.

1. **"Your schema is the contract" (hero concept).** Visual: one `.proto`
   message with option annotations fanning out to validation, an OpenSearch
   mapping, a JSON Schema, an LLM prompt, and a metric query. This is the
   product's actual thesis and needs no embellishment.
2. **"One catalog, eight fronts."** Visual: the 72-verb catalog in the center;
   gRPC, REST, OpenAPI/Swagger, MCP, ACP, CLI radiating out. Copy can be a
   literal terminal capture: `claude mcp add --transport http protomolt …`
   then an agent invoking a gRPC service it has never seen.
3. **"Point an agent at any gRPC service."** A 30-second sequence:
   `service-register` → `service-inspect` → `service-invoke` over MCP.
   Strongest demo; entirely shipped.
4. **"Validation you can audit."** State the conformance fact plainly: passes
   buf's protovalidate conformance suite, no skip list, enforced in CI on
   every push. Link the CI workflow. No superlatives needed.
5. **"A schema registry that is a git repo."** Visual: `git log` of the
   registry beside a Confluent-protocol `curl`. One commit per registration.
6. **"From topic to lake to index."** Diagram: Kafka topic → Connect →
   Iceberg / OpenSearch, with the serde enforcing declared rules on write.
7. **"The document platform."** Diagram of the twelve roles and the
   one-container vs. split-node story (`PROTOMOLT_ROLES`).
8. **"Search that explains itself."** Descriptor-declared mappings, vector +
   lexical + hybrid lanes, deterministic chunking, metrics over the same
   index with no ETL.
9. **"Proof of work done."** Signed work records and the zero-dependency
   verifier — show a record verifying offline.
9b. **Case study: 9.7 million court opinions.** The court-decoration example
   is a ready-made story: dirty scraped HTML → gRPC cleaning service → an
   annotated proto form as both prompt and contract → deterministic
   validation → citation-backed claims instead of unverifiable model output.
   The unedited QA rounds are publishable evidence of the method working and
   failing honestly.
10. **Getting started.** The existing one-liner:
    `docker run -p 8080:8080 -p 9090:9090 pipestreamai/protomolt-serve --demo`.

Tone guidance: the codebase's own documentation style — stating limits by
name ("write shape only, no read side", "the role mount is inert") — is
unusual and credible. Carrying that honesty onto the website (e.g. a visible
"pre-1.0" status and a linked planned-work page) differentiates more than
superlatives would.

## 5. Terms to use carefully

- **ProtoMolt** — project and artifact name (`protomolt-*`). Maven group id
  is `ai.pipestream`. Java/proto namespace is `ai.protomolt.proto.*`. Config
  prefix `protomolt.*`, env prefix `PROTOMOLT_*`.
- **Pre-1.0** — the project publishes `0.1.0-SNAPSHOT`. Do not imply GA.
- **"Supports buf's protovalidate annotations (`buf.validate` options in
  `.proto` files), proven by buf's full conformance suite — 2872/2872 cases,
  no skip list, enforced in CI"** — use this shape. It is about the protobuf
  annotation syntax, not the buf CLI; don't imply buf-CLI feature parity.
- **"Confluent subjects protocol"** — the registry *speaks the protocol*;
  don't say "drop-in Confluent replacement" (no mirroring, HTTP-only API,
  server-side compatibility gate semantics are ProtoMolt's own).
- **"Confluent wire format"** — the serde implements the published framing
  spec exactly; safe to claim.
- **Storage wording** — "pluggable object storage behind a provider-neutral
  interface; any S3-compatible store today" is accurate. Azure/GCS are
  designed-for but have no shipped adapter; don't list them as supported.
- **Vocabulary (ADR-001), enforced in this repo:** *workflow* (authored
  definition), *run* (durable execution), *pipeline/processor* (in-process
  streaming), *service*, *role*, *gate*, *mapping* (index definitions),
  *instruction* (LLM guidance), *chunking policy*. Retired words that must
  not appear: recipe, chain, directive, index plan, door.
- **MCP / ACP** — Model Context Protocol and Agent Client Protocol; both
  shipped. MCP is stdio + streamable HTTP; ACP is stdio.
- **turbovec / TurboQuant** — a sibling ai-pipestream repository (Rust,
  exact-by-construction quantized search); integration into ProtoMolt is
  imminent per the founder, so it can be featured — but attribute it to its
  own repo and, until the integration lands here, use "landing now" rather
  than "shipped in ProtoMolt".
- **gRParse** — a separate C++ parsing fleet ProtoMolt adapts to; the shared
  document model is docling-core v2 field parity, canonical in this repo.
- **OpenNLP** — Apache OpenNLP preview builds are used in exactly two places
  (screening; chunking/Model2Vec). The project owner is an OpenNLP committer —
  fine to state, keep it factual.
- **"No authentication" caveats** — repo-service is unauthenticated by
  design; never market the document store as independently exposable.
- **Numbers safe to cite** (pinned by tests/CI): 72 verbs, 44 typed RPCs,
  eight surface fronts, nine annotation families, twelve platform roles, nine
  codegen targets, 140 Gradle modules (98 libraries / 24 services / 18 apps).
