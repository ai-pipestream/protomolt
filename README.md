# ProtoMolt

ProtoMolt is a Java toolkit that treats a protobuf descriptor as the semantic
layer. A message declares its validation rules, index mapping, vector lane,
projection provenance, metric membership, prompt shape, descriptive metadata,
quality dimensions and processing roles once, as descriptor options, and every
subsystem reads those declarations instead of restating them in a parallel
config model.

Everything operates on descriptors (`Descriptor` / `FileDescriptor`), never on
generated classes. The same code paths serve a `DynamicMessage` resolved from a
registry at runtime and a compiled-in type, and no module is coupled to any
particular message type.

On top of that layer sits one integration primitive. `ProtoAction`
(`surface/actions/src/main/java/ai/protomolt/proto/actions/ProtoAction.java`)
is a JSON-in, JSON-out verb with a JSON Schema and a required authorization
scope. A catalog of 72 such verbs is dispatched by eight protocol fronts:
typed gRPC with server reflection, JSON/REST, OpenAPI 3, Swagger UI, MCP over
stdio and streamable HTTP, ACP over stdio, and a CLI with an interactive
console. Around that sit document ingest, a claim-check document store, a
parsing coordinator, a Lucene search service, a metric service over the same
index, a git-backed schema registry, and Kafka connectors.

The project is pre-1.0 and publishes `0.1.0-SNAPSHOT`. Where a capability is a
prototype, unwired, or designed only, the tables below say so.

## How this README is organized

Modules are split three ways.

1. **[Libraries](#1-libraries)**, 98 modules. You depend on them and call them
   in-process. No server, no port.
2. **[Services](#2-services)**, 24 modules. Each defines a gRPC or HTTP
   contract and runs as a process or a composable role node.
3. **[Servers and apps](#3-servers-and-apps)**, 18 modules. Launchers, HTTP
   hosts, framework embeddings, browser consoles and end-user binaries.

Three more modules are unpublished or build-only: `bom` (version alignment for
every published artifact), `samples` (runnable examples under `samples/src`),
and `protomolt-system-tests` at `tests/system` (cross-module end-to-end tests
plus the generator behind `docs/generated/action-inventory.json`).

Then: the [complete 72-verb action catalog](#the-action-catalog), the
[surface inventory](#surfaces), the
[nine annotation families](#the-nine-annotation-families),
[role nodes](#role-nodes), and [adjacent projects](#adjacent-projects).

Maven artifact ids carry the `protomolt-` prefix; Java packages use the
`ai.protomolt.proto.*` namespace (see [Naming](#naming)).

## Getting started

Run one process that gives you gRPC with reflection, the same verbs over
JSON/REST, OpenAPI, Swagger UI, MCP, a task console and a git-backed registry:

```shell
docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
```

What you get, all from `apps/serve`
(`apps/serve/src/main/java/ai/protomolt/proto/serve/ProtoMoltServe.java`):

| Endpoint | Default | What it serves |
|---|---|---|
| gRPC | `localhost:9090` | `ProtoMoltService`, 44 typed RPCs, with server reflection |
| `POST /grpc-json/{Service}/{Method}` | `localhost:8080` | The same 44 RPCs as JSON over HTTP |
| `GET /openapi.json` | `localhost:8080` | OpenAPI 3.0.3 for the REST mount |
| `GET /docs` | `localhost:8080` | Swagger UI over that document |
| `GET /health` | `localhost:8080` | Liveness |
| `/mcp` | `localhost:8080` | MCP over streamable HTTP, with sessions and cancellation |
| `/console` | `localhost:8080` | Task console, plus `/api/tasks` |
| Registry HTTP | `localhost:8081` | Confluent subjects protocol, plus `POST /protomolt/actions/{verb}` |

Exercise it:

```shell
# Validate a message against the demo schema's declared rules:
curl -s -H 'content-type: application/json' \
  -d '{"schema": {"type": "demo.shop.v1.Order"}, "message": {"id": "not-a-uuid"}}' \
  http://localhost:8080/grpc-json/ProtoMoltService/ValidateMessage

# Any gRPC client sees a real, reflectable service:
grpcurl -plaintext localhost:9090 list

# Make an AI agent gRPC-aware with one command:
claude mcp add --transport http protomolt http://localhost:8080/mcp
```

Setting `--api-token` guards every operational surface (gRPC, the REST verbs,
MCP) while leaving the documentation surfaces (health, OpenAPI, Swagger UI)
open. With a token set and no task-console options, the browser surfaces are
replaced by a handler that says they are disabled.

From a clone, `docker compose up` builds and runs the same server, and
`docker compose run --rm acp` is the ACP agent an IDE drives over stdio;
`./scripts/docker-smoke.sh` brings the stack up and proves both the MCP and ACP
surfaces answer. See [Running in Docker](docs/apps/docker.md).

Prefer a process over a container? Every release attaches runnable
`protomolt-serve` and `protomolt-mcp` zips (JRE 25+ is the only prerequisite),
or build from a clone:

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew :protomolt-serve:installDist
apps/serve/build/install/protomolt-serve/bin/protomolt-serve --demo
```

To use the toolkit as a library, depend on the artifacts (or
`./gradlew publishToMavenLocal` from a clone):

```groovy
dependencies {
    implementation platform('ai.pipestream:protomolt-bom:0.1.0-SNAPSHOT')
    implementation 'ai.pipestream:protomolt-mapper-cel'
}
```

Map fields with text rules, or gate and select values with CEL:

```java
var registry = DescriptorRegistry.create();
var mapper = new ProtoFieldMapperImpl(registry);

mapper.mapInPlace(builder, List.of(
    "title = body",        // assign
    "tags += \"proto\"",   // append
    "-scratch"             // clear
));
```

Validate a message against rules declared on its descriptor:

```java
var result = ProtoValidator.forMessageType(Person.getDescriptor()).validate(person);
result.throwIfInvalid();
```

Serve JSON/REST over any descriptor source:

```java
var gateway = new ProtoRestGateway(methods, transcoder, tokenValidator);
var server = new JdkProtoRestServer(config, gateway);
server.start();
// POST /grpc-json/{service}/{method}, GET /openapi.json, GET /health
```

For the full document platform (ingest, store, parse, search, metrics) run
`apps/document-platform` instead and select roles with `PROTOMOLT_ROLES`; see
[Role nodes](#role-nodes).

---

# 1. Libraries

In-process only. Every entry point path is relative to the repository root.
"Status" is blank where the module is shipped and exercised by tests; it names
the exception otherwise.

## core/ : the schema model and its zero-IO foundations

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-descriptors` | `core/descriptors` | Descriptor lookup by full and simple name, with a loader SPI, negative caching, and the Google well-known types registered at construction. `DescriptorCatalog` is the preferred alias for the same type. | `core/descriptors/src/main/java/ai/protomolt/proto/descriptors/DescriptorRegistry.java` | |
| `protomolt-helpers` | `core/helpers` | `Any` packing and unpacking, type conversion between messages, primitives and `Struct`, field-level message diff, dotted-path field enumeration, and schema hygiene checks that reject cross-file FQN redefinitions and illegal identifiers inside binary descriptors. | `core/helpers/src/main/java/ai/protomolt/proto/helpers/PayloadCodec.java` | |
| `protomolt-sources` | `core/sources` | The `.proto` source-set model and its compiler: Square Wire's schema library on an in-memory filesystem, no protoc binary. Also the publisher SPI and subject naming strategies. | `core/sources/src/main/java/ai/protomolt/proto/sources/ProtoSourceCompiler.java` | |
| `protomolt-formats` | `core/formats` | RFC-accurate string format parsers behind the validation string rules: email, hostname, IP and CIDR, RFC 3986 URI, dates, media types, hex and base64, slug, GTIN check digit, postal masks. Zero runtime dependencies and no `java.util.regex` anywhere. | `core/formats/src/main/java/ai/protomolt/proto/formats/Formats.java` | |
| `protomolt-codegen` | `core/codegen` | Runs protoc's own generators as one bundled WebAssembly module: java, kotlin, grpc-java, python, cpp, csharp, ruby, php, objc. No native toolchain. Ships the `generate-stubs` verb. | `core/codegen/src/main/java/ai/protomolt/proto/codegen/WasmProtoc.java` | |
| `protomolt-compat` | `core/compat` | Breaking-change detection: `SchemaDiff` observes typed differences with 33 stable rule ids and five impact classes; `CompatibilityChecker` applies the seven Confluent policies over them. | `core/compat/src/main/java/ai/protomolt/proto/compat/CompatibilityChecker.java` | |
| `protomolt-acp` | `core/acp` | Agent Client Protocol core: newline-delimited JSON-RPC 2.0 on virtual threads, a blocking client, and an agent runtime, with no reactive runtime. | `core/acp/src/main/java/ai/protomolt/proto/acp/AcpAgent.java` | |

## protobuf/ : the annotation dialects and their readers

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-protobuf-validation` | `protobuf/validation` | The `validate.v1` dialect and the engine behind it: eager per-type rule compilation, 36 string rule tags, taxonomy and postal gates, and a neutral rule-source SPI so other dialects plug in. | `protobuf/validation/src/main/java/ai/protomolt/proto/validate/ProtoValidator.java` | |
| `protomolt-protobuf-validation-protovalidate` | `protobuf/validation-protovalidate` | The vendored `buf.validate` dialect as a ServiceLoader-registered rule source. Putting the jar on the classpath makes protovalidate-annotated schemas validate through the same `ProtoValidator`. | `protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/ProtovalidateRuleSource.java` | |
| `protomolt-protobuf-validation-conformance` | `protobuf/validation-conformance` | Drives `ProtoValidator` against buf's protovalidate conformance suite, pinned at v1.2.2, with no skip list. CI runs it bare on every push. | `protobuf/validation-conformance/src/main/java/ai/protomolt/proto/validate/conformance/ConformanceRunner.java` | Also ships a stdin/stdout executor `ConformanceMain` |
| `protomolt-protobuf-metadata` | `protobuf/metadata` | The `meta.v1` dialect: description, display name, owner, sensitivity class, labels, and a `json_name` that survives text round-trips. `SensitivityMasker` acts on the sensitivity class. | `protobuf/metadata/src/main/java/ai/protomolt/proto/meta/DescriptorMetadata.java` | |
| `protomolt-protobuf-quality` | `protobuf/quality` | The `quality.v1` dialect: CEL-scored dimensions declared as a message option, composed as a weighted mean, with `exp` and `clamp` helpers. Compile failures are schema errors; evaluation failures mark a dimension failed rather than taking the data path down. | `protobuf/quality/src/main/java/ai/protomolt/proto/quality/QualityScorer.java` | |
| `protomolt-protobuf-llm` | `protobuf/llm` | The `llm.v1` dialect: per-field and per-message fill instructions, safeguards, and a volatility flag. Annotations plus a 40-line descriptor reader; the renderer lives in `protobuf/prompt`. | `protobuf/llm/src/main/java/ai/protomolt/proto/llm/DescriptorLlm.java` | |
| `protomolt-protobuf-metric` | `protobuf/metric` | The `metric.v1` dialect: measures, dimensions, aggregates, row filters, calculated members and synthetic members declared as field and message options. | `protobuf/metric/src/main/proto/ai/protomolt/proto/metric/v1/metric.proto` | Proto only. No `.proto` in the tree uses these options yet |
| `protomolt-protobuf-prompt` | `protobuf/prompt` | Renders a descriptor into a complete LLM form-filling briefing: `meta.v1` descriptions, `validate.v1` constraints, `quality.v1` dimensions and `llm.v1` instructions in reading order, plus the JSON Schema the answer must satisfy and the rejection feedback for a retry. | `protobuf/prompt/src/main/java/ai/protomolt/proto/prompt/PromptRenderer.java` | |
| `protomolt-protobuf-seo` | `protobuf/seo` | The search-metadata standard: Dublin Core plus one Google structured-data entity, carrying validation, metadata and indexing-hint options on every field. | `protobuf/seo/src/main/java/ai/protomolt/proto/seo/SeoIndexing.java` | |
| `protomolt-protobuf-types` | `protobuf/types` | Well-known structural types whose invariants ride the schema: `DateRange`, `LongRange`, `DoubleRange`, `TreePath`, `Taxonomy`, `PostalCodePack`, `ScreeningConfig`, and the `authz.v1` `AccessPolicy` family. | `protobuf/types/src/main/proto/ai/protomolt/proto/types/v1/ranges.proto` | Proto only. No `IntRange` or `FloatRange` message exists; those index kinds are duck-typed |

## transform/ : mapping, shaping and composition between messages

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-mapper` | `transform/mapper/core` | Path-based field access on one message plus a three-form text rule language (`target = source`, `target += source`, `-target`), with `Any` auto-unpacking and a machine-readable absent-versus-broken error category. | `transform/mapper/core/src/main/java/ai/protomolt/proto/mapper/ProtoFieldMapper.java` | |
| `protomolt-mapper-cel` | `transform/mapper/cel` | The one CEL engine in the tree. Validation, quality, projection, metric filters, shapes and mapping rules all compile through this environment factory. | `transform/mapper/cel/src/main/java/ai/protomolt/proto/cel/CelEvaluator.java` | |
| `protomolt-mapper-metadata` | `transform/mapper/metadata` | Extracts named metadata from message contents at runtime with CEL selectors, eagerly type-checked in two environments before anything evaluates. Distinct from the `meta.v1` annotations. | `transform/mapper/metadata/src/main/java/ai/protomolt/proto/mapper/metadata/MetadataExtractor.java` | |
| `protomolt-shapes` | `transform/shapes` | Joins, unions and derived shapes. A synthesized shape is a real linked protobuf type (envelope, projection, or tagged union) emitted as `.proto` source with true import paths, so a join's output contract becomes a governed schema. Also schema merging, struct-to-proto inference, rule checking and mapping suggestion. | `transform/shapes/src/main/java/ai/protomolt/proto/shapes/ShapeSynthesizer.java` | |
| `protomolt-projection` | `transform/projection` | Self-describing message-to-message projections: per-field provenance (candidate paths, CEL, literals, plus a `default_from` fallback) declared as options on the target, so one target joins differently-shaped sources. | `transform/projection/src/main/java/ai/protomolt/proto/projection/MessageProjection.java` | Map fields are refused by name |
| `protomolt-pipeline` | `transform/pipeline` | The compiled, streaming-aware form of a workflow: typed edges, an offline checker with a cardinality discipline, and an in-process executor covering every gRPC streaming shape plus structured generation, unnest, collect and bounded fan-out. | `transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/PipelineExecutor.java` | External-completion steps are refused at runtime; no coordinator is wired |
| `protomolt-workflow` | `transform/workflow` | Checked serial gRPC compositions with gates, deadlines, named registry storage and keyed or zip joins over two live streams, plus run recording, offline replay, drift reporting and the bridge into the receipt layer. Carries 11 of the 72 verbs. | `transform/workflow/src/main/java/ai/protomolt/proto/workflow/WorkflowRunner.java` | |
| `protomolt-delegation` | `transform/delegation` | The coordinator and worker bidirectional contract (`AgentDelegationService.Delegate`), an offline lifecycle reducer, an admission policy, a candidate reviewer, encrypted transcript persistence, and the 12 `delegation-*` verbs. | `transform/delegation/src/main/java/ai/protomolt/proto/delegation/DelegationBridge.java` | Mounted in-process by `apps/serve`; no standalone process |
| `protomolt-screening` | `transform/screening` | Model-driven detection over `meta.v1` sensitivity classes with mask, tag or refuse policy, the model manifest version as evidence, and unresolvable `Any` payloads reported rather than passed. The only module that pulls OpenNLP. | `transform/screening/src/main/java/ai/protomolt/proto/screening/Screener.java` | |
| `protomolt-receipt` | `transform/receipt` | Canonical signed work records: Ed25519 keys in portable raw encodings, deterministic manifest bytes with a detached issuer signature, artifacts carried by digest, a trust snapshot, and a strict offline verifier. | `transform/receipt/src/main/java/ai/protomolt/proto/receipt/RecordSigner.java` | |

## mesh/ : the protobuf mesh profile

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-mesh-proto` | `mesh/proto` | The mesh core contract: `EntityEnvelope`, `EntityHeader`, `SchemaReference`, claim checks, and the `mesh.v1` processing options. | `mesh/proto/src/main/proto/ai/protomolt/proto/mesh/v1/entity.proto` | Proto only. The `mesh.v1` options have no main-source reader |
| `protomolt-mesh-contracts` | `mesh/contracts` | The contract gate: fail-fast envelope validation, canonical descriptor-set fingerprinting, and the schema-identity resolver. | `mesh/contracts/src/main/java/ai/protomolt/proto/mesh/MeshGate.java` | |
| `protomolt-mesh-cluster` | `mesh/cluster` | The memory-resident cluster directory: a deterministic reducer emitting a gap-free event log, fenced presence and capacity, replay-validate-then-install persistence through repo-service with an AES-256-GCM encrypted log, and the six `mesh-*` verbs. | `mesh/cluster/src/main/java/ai/protomolt/proto/mesh/cluster/ClusterDirectory.java` | No gRPC service and no port of its own; mounted in-process by `apps/serve` |

## search/ : index mappings, chunking, embeddings and rerank

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-search-index-spi` | `search/index/spi` | The `index.hints.v1` dialect and the mapping factory: hint sources chained catalog, then options, then inference; `IndexMapping` with two naming vocabularies; the `Any` payload gate; `TreePath` ancestor chains and range bound resolution. | `search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/IndexMappingFactory.java` | This is a mapping SPI. `SearchEngineIndexer` has no query method; there is no query-side index SPI |
| `protomolt-search-chunk` | `search/chunk` | The deterministic chunker: one strategy (`sentence-packed`) over pinned, versioned boundary rules, so two corpora agree on chunk boundaries exactly when their policy digests agree. | `search/chunk/src/main/java/ai/protomolt/proto/search/chunk/SentencePackedChunker.java` | |
| `protomolt-search-index-lucene` | `search/index/lucene` | Lucene document mapper plugin, ServiceLoader-registered as engine `lucene`, plus the field-spec rendering behind `render-index-mappings`. | `search/index/lucene/src/main/java/ai/protomolt/proto/search/index/lucene/ProtoLuceneMapper.java` | |
| `protomolt-search-index-opensearch` | `search/index/opensearch` | OpenSearch document mapper and mapping generator (`knn_vector` with HNSW parameters, sub-fields, sensitivity-driven field-level security), plus a thin HTTP sink and a reranked semantic search composed over it. | `search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/OpenSearchDocumentMapper.java` | The only backend with a read side outside the Lucene service |
| `protomolt-search-index-solr` | `search/index/solr` | Solr document mapper and managed-schema generator: field types, fields and copy fields, with a synthesized `solr.DenseVectorField` for vectors. | `search/index/solr/src/main/java/ai/protomolt/proto/search/index/solr/SolrDocumentMapper.java` | Write shape only, no read side |
| `protomolt-search-index-qdrant` | `search/index/qdrant` | Qdrant point mapper, vector and payload-index schema generator, and a thin gRPC sink. | `search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapper.java` | Write shape only, no read side |
| `protomolt-search-index-ndjson` | `search/index/ndjson` | Engine-agnostic protobuf to newline-delimited JSON encoding. | `search/index/ndjson/src/main/java/ai/protomolt/proto/search/index/ndjson/ProtoNdjsonWriter.java` | An export utility, not a mapping target; it consults no hints |
| `protomolt-search-index-protobuf` | `search/index/protobuf` | The descriptor-driven indexer and its `google.protobuf.Any` payload gate, which binds a packed payload to the declared rules of its own type. | `search/index/protobuf/src/main/java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexer.java` | Registers an `AnyPayloadValidator`, not an engine indexer |
| `protomolt-search-embedding` | `search/embedding/core` | The embedding provider SPI (ServiceLoader) and the mapping-driven embedder that fills a document's VECTOR field from its TEXT field, refusing restricted content by sensitivity class. | `search/embedding/core/src/main/java/ai/protomolt/proto/search/embedding/MappingEmbedder.java` | |
| `protomolt-search-embedding-model2vec` | `search/embedding/model2vec` | In-process Model2Vec static embeddings backed by the OpenNLP embeddings module. | `search/embedding/model2vec/src/main/java/ai/protomolt/proto/search/embedding/model2vec/Model2VecEmbeddingProvider.java` | |
| `protomolt-search-embedding-tei` | `search/embedding/tei` | Remote embeddings from Hugging Face Text Embeddings Inference over gRPC. | `search/embedding/tei/src/main/java/ai/protomolt/proto/search/embedding/tei/TeiEmbeddingProvider.java` | |
| `protomolt-search-embedding-ovms` | `search/embedding/ovms` | Remote embeddings from OpenVINO Model Server over the KServe v2 gRPC prediction protocol. | `search/embedding/ovms/src/main/java/ai/protomolt/proto/search/embedding/ovms/OvmsEmbeddingProvider.java` | |
| `protomolt-search-embedding-harness` | `search/embedding/harness` | Pairwise cosine-equivalence certification: two providers serving one model must agree before they may be mixed. | `search/embedding/harness/src/main/java/ai/protomolt/proto/search/embedding/harness/EmbeddingEquivalence.java` | |
| `protomolt-search-rerank` | `search/rerank/core` | The rerank provider SPI: score a query's candidate texts so a pipeline can re-order hits. | `search/rerank/core/src/main/java/ai/protomolt/proto/search/rerank/RerankProvider.java` | Not called by the mounted Lucene search service |
| `protomolt-search-rerank-tei` | `search/rerank/tei` | Rerank over Hugging Face Text Embeddings Inference. | `search/rerank/tei/src/main/java/ai/protomolt/proto/search/rerank/tei/TeiRerankProvider.java` | Unwired in the mounted search service |
| `protomolt-search-rerank-ovms` | `search/rerank/ovms` | Rerank over the OpenVINO Model Server REST rerank endpoint. | `search/rerank/ovms/src/main/java/ai/protomolt/proto/search/rerank/ovms/OvmsRerankProvider.java` | Unwired in the mounted search service |
| `protomolt-search-rerank-harness` | `search/rerank/harness` | Ranked-list equivalence certification (Kendall tau-b plus top-1 agreement) for two providers serving one model. | `search/rerank/harness/src/main/java/ai/protomolt/proto/search/rerank/harness/RerankEquivalence.java` | |
| `protomolt-search-snapshot-s3` | `search/snapshot-s3` | An S3 implementation of the search service's `SnapshotStore` seam: commit-point index snapshots in a bucket, which is what a read-only remote search node restores from. | `search/snapshot-s3/src/main/java/ai/protomolt/proto/search/snapshot/s3/S3SnapshotStore.java` | Prototype by test evidence: one integration test, no unit tests |

## metric/ : descriptor-declared measures over the same index

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-metric-spi` | `metric/spi` | Builds a metric mapping from `metric.v1` declarations, compiles a bounded aggregate query, refuses what a backend cannot run by name, and declares the executor seam engines implement. | `metric/spi/src/main/java/ai/protomolt/proto/metric/spi/MetricMappings.java` | |
| `protomolt-metric-iceberg` | `metric/iceberg` | The lake-native executor: one `SELECT ... GROUP BY` rendered from the compiled query and run by DuckDB over the Parquet files the Iceberg sink wrote. The rendered SQL is returned as the physical plan. Also the rollup sink and the `rollup:<table>` subject naming. | `metric/iceberg/src/main/java/ai/protomolt/proto/metric/iceberg/IcebergMetricExecutor.java` | Refuses tables carrying delete files |

## sink/ : where rendered output goes

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-emit` | `sink/emit/core` | Bundles of rendered files (ordered path to bytes, path-safety validated) and the sinks that deliver them: directory, git (JGit, honest about a no-op commit), and an in-memory deterministic zip helper. | `sink/emit/core/src/main/java/ai/protomolt/proto/emit/BundleSink.java` | An S3 bundle sink is planned and does not exist |
| `protomolt-emit-okf` | `sink/emit/okf` | Renders a descriptor set or a whole registry as an Open Knowledge Format v0.1 markdown bundle, one concept per message, enum and service, with YAML frontmatter from `meta.v1`. Ships the `emit-okf` verb. | `sink/emit/okf/src/main/java/ai/protomolt/proto/emit/okf/OkfRenderer.java` | |
| `protomolt-emit-parquet` | `sink/emit/parquet` | Descriptor-driven Parquet from `Descriptor` plus `Message`, with no generated classes and no Hadoop: a `PlainParquetConfiguration`, pure snappy-java or uncompressed, and a test that fails if a Hadoop class loads. | `sink/emit/parquet/src/main/java/ai/protomolt/proto/emit/parquet/ParquetEmitter.java` | |
| `protomolt-emit-parquet-s3` | `sink/emit/parquet-s3` | Uploads those Parquet files to any S3-compatible store through the AWS SDK with the JDK URL-connection HTTP client and path-style endpoint override. | `sink/emit/parquet-s3/src/main/java/ai/protomolt/proto/emit/parquet/s3/S3ParquetSink.java` | |
| `protomolt-iceberg` | `sink/iceberg` | Descriptor-driven Iceberg table schemas and an append sink writing ProtoMolt Parquet through any catalog, with a name mapping installed after creation, column metrics from a Hadoop-free footer read, and a `LocalFileIO` that replaces Iceberg's Hadoop file IO on JDK 24+. | `sink/iceberg/src/main/java/ai/protomolt/proto/iceberg/IcebergSink.java` | Equality deletes and v3 variant columns are later phases |
| `protomolt-iceberg-s3` | `sink/iceberg-s3` | Wires Iceberg `S3FileIO` so tables live on any S3-compatible store, forcing the JDK URL-connection HTTP client. | `sink/iceberg-s3/src/main/java/ai/protomolt/proto/iceberg/s3/S3Catalogs.java` | |
| `protomolt-kafka-wire` | `sink/kafka/wire` | The Confluent protobuf frame, written to its published spec: magic byte, big-endian schema id, zigzag varint message-index array, payload. Pure framing, no config. | `sink/kafka/wire/src/main/java/ai/protomolt/proto/kafka/wire/ConfluentWireFormat.java` | |
| `protomolt-kafka-serde` | `sink/kafka/serde` | A protobuf serde speaking the Confluent wire format, enforcing the schema's declared rules on write, with Confluent-identical subject strategies, cached schema ids with independent backoff windows, and a loud refusal rather than a silent id fallback when the registry cannot answer. Includes the Kafka Streams `Serde`. | `sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/ProtoMoltSerde.java` | |
| `protomolt-kafka-serde-micrometer` | `sink/kafka/serde-micrometer` | Binds the serde metrics SPI to Micrometer: records, rejections, violations, refusals, registry fallbacks and quality scores. | `sink/kafka/serde-micrometer/src/main/java/ai/protomolt/proto/kafka/serde/micrometer/MicrometerSerdeMetrics.java` | |
| `protomolt-kafka-connect` | `sink/kafka/connect` | A Kafka Connect sink that drives any gRPC method from a topic, a source that feeds a topic from a server stream with CEL resume-token offsets, and four protobuf-aware transforms: validate, map, redact and CEL filter. | `sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSinkConnector.java` | The source is the only acquisition path in the tree with durable resumption |
| `protomolt-kafka-connect-iceberg` | `sink/kafka/connect-iceberg` | A Connect sink landing topic records as Iceberg snapshots, one snapshot per batch, at-least-once. | `sink/kafka/connect-iceberg/src/main/java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkConnector.java` | |
| `protomolt-kafka-connect-opensearch` | `sink/kafka/connect-opensearch` | A Connect sink driving topic records into OpenSearch through the search-index SPI, with `Any` expansion, declared-rule payload validation and deterministic document ids. | `sink/kafka/connect-opensearch/src/main/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConnector.java` | |

## surface/ : the integration primitive and its HTTP generators

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-actions` | `surface/actions` | The action catalog: registration, forkable layering, a machine-readable manifest, and one dispatch point that gates on scope and budget. Also 17 of the built-in verbs, the `Caller` record, the closed 10-scope vocabulary and `ScopeBudgets`. | `surface/actions/src/main/java/ai/protomolt/proto/actions/ActionCatalog.java` | |
| `protomolt-grpc-invoke` | `surface/grpc/invoke` | Dynamic gRPC invocation from descriptors, every streaming shape, no generated stubs, plus client-side server reflection. Ships the `reflect` and `grpc-invoke` verbs. | `surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/DynamicGrpcCalls.java` | |
| `protomolt-grpc-channel-policy` | `surface/grpc/channel-policy` | Validated, host-configurable outbound channel policy: scheme, host and port allowlists, TLS toggles, a deadline ceiling and a leased concurrency semaphore. | `surface/grpc/channel-policy/src/main/java/ai/protomolt/proto/grpc/policy/OutboundChannelPolicy.java` | Three of five outbound call sites route through it; no payload limits or resolved-address recheck |
| `protomolt-grpc-service-profile` | `surface/grpc/service-profile` | Durable gRPC service profiles and content-addressed descriptor artifacts for the workflow workbench. | `surface/grpc/service-profile/src/main/java/ai/protomolt/proto/grpc/profile/ServiceProfileRepository.java` | `credential_ref`, `trust_ref` and `client_certificate_ref` are accepted and refused by name with `unsupported-transport` |
| `protomolt-grpc-service-workspace` | `surface/grpc/service-workspace` | The agent-facing workspace verbs: register a service by endpoint, list profiles, inspect methods and shapes, re-reflect and refresh a fingerprint, invoke a registered method by profile name. | `surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceInvokeAction.java` | |
| `protomolt-grpc-workflow` | `surface/grpc/workflow` | Descriptor-grounded workflow, artifact and run-evidence contracts, with filesystem repositories and workflow validation. | `surface/grpc/workflow/src/main/java/ai/protomolt/proto/grpc/workflow/WorkflowVersionRepository.java` | |
| `protomolt-grpc-validation` | `surface/grpc/validation` | Validating gRPC interceptors, server and client side: the schema's declared rules enforced at the call boundary. | `surface/grpc/validation/src/main/java/ai/protomolt/proto/grpc/validate/ValidatingServerInterceptor.java` | |
| `protomolt-grpc-validation-micrometer` | `surface/grpc/validation-micrometer` | Micrometer binding for the gRPC validation metrics SPI. | `surface/grpc/validation-micrometer/src/main/java/ai/protomolt/proto/grpc/validate/micrometer/MicrometerGrpcValidationMetrics.java` | |
| `protomolt-http-json` | `surface/http/json` | Protobuf and JSON transcoding, typed and dynamic. | `surface/http/json/src/main/java/ai/protomolt/proto/http/json/ProtobufJsonTranscoder.java` | |
| `protomolt-http-rest` | `surface/http/rest` | The framework-agnostic REST gateway: a method registry keyed `{service}/{method}` that rejects a duplicate registration at startup, `@ProtoRestExposed` opt-in, API-token requirements, and the shared status and error mapping. | `surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestGateway.java` | REST exposure is a Java annotation, not a descriptor option |
| `protomolt-http-openapi` | `surface/http/openapi` | OpenAPI 3.0.3 generation from service descriptors. Paths are canonical and mechanical (`{prefix}/{Service}/{Method}`); there are no `google.api.http` annotations anywhere in the tree. | `surface/http/openapi/src/main/java/ai/protomolt/proto/http/openapi/ProtoOpenApiGenerator.java` | An independent reimplementation: it reads no validation rules and derives `required` from the proto2 label |
| `protomolt-http-jsonschema` | `surface/http/jsonschema` | JSON Schema draft 2020-12 from descriptors plus the neutral constraint model, so `buf.validate` and `validate.v1` feed it identically. Collisions conjoin into `allOf` rather than overwrite. | `surface/http/jsonschema/src/main/java/ai/protomolt/proto/http/jsonschema/ProtoJsonSchemaGenerator.java` | Does not emit `meta.v1` descriptions and does not model oneofs |
| `protomolt-authz` | `surface/authz` | The access-policy document validator and loader, the credential-digest caller store, an RFC 7662 OIDC introspection resolver, the first-match-wins resolver chain, and `ConsoleSessions` for browser surfaces. | `surface/authz/src/main/java/ai/protomolt/proto/authz/CallerResolver.java` | |
| `protomolt-authz-grpc` | `surface/authz-grpc` | Authorization for gRPC surfaces: the credential interceptor (`api_token` metadata or bearer), the caller `Context` glue, and the per-service and per-method scope table with budget spend. | `surface/authz-grpc/src/main/java/ai/protomolt/proto/authz/grpc/ScopeServerInterceptor.java` | |
| `protomolt-authz-jdbc` | `surface/authz-jdbc` | Authorization principals in the operator's own PostgreSQL, for air-gapped deployments with no identity provider. HikariCP plus Flyway, fail-fast, no localhost fallback. | `surface/authz-jdbc/src/main/java/ai/protomolt/proto/authz/jdbc/JdbcCallerResolver.java` | |

## acquire/ : how protos and documents get in

Three unrelated families share this directory. `gather/*` acquires schema;
`connector` acquires live messages; `pull`, `s3`, `jdbc` and `msgraph` acquire
content. There is no crawl engine and no scheduler anywhere in protomolt: every
content connector is a single bounded pass triggered by a call, and every one
returns its resumption position to the caller rather than persisting it.

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-acquire-gather` | `acquire/gather/core` | The proto gatherer SPI plus filesystem and jar sources, an ordered composite, and the adapter that turns any gatherer into a `DescriptorLoader`. An import path may be produced twice only with identical content. | `acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/ProtoGatherer.java` | |
| `protomolt-acquire-gather-git` | `acquire/gather/git` | Clones or fetches a git repository over JGit and stages its proto trees in three layout modes, with a lock-guarded persistent clone cache and an offline mode. Ships the `gather-git` verb. | `acquire/gather/git/src/main/java/ai/protomolt/proto/acquire/gather/git/GitProtoGatherer.java` | |
| `protomolt-acquire-gather-maven` | `acquire/gather/maven` | Resolves artifacts by coordinate with the standalone Maven Resolver (no Maven installation) and stages protos from their jars, optionally walking the runtime graph. | `acquire/gather/maven/src/main/java/ai/protomolt/proto/acquire/gather/maven/MavenProtoGatherer.java` | Library only; no verb |
| `protomolt-acquire-connector` | `acquire/connector` | Push-style streaming inputs behind one bounded, pausable SPI: gRPC server streams and Kafka topics, bridged to a synchronous consumer by a bounded-queue pump. Offset and resume-token ownership stays in the deployment layer. | `acquire/connector/src/main/java/ai/protomolt/proto/acquire/connector/StreamSource.java` | |
| `protomolt-acquire-pull` | `acquire/pull` | The shared pull-connector core: the intake feed seam (gRPC submission with the API key), a stable-identity document wrap whose doc id is a name-based UUID over connector, datasource and source key, and a watermark that only advances through an unbroken prefix of successes. | `acquire/pull/src/main/java/ai/protomolt/proto/acquire/pull/IntakeFeed.java` | `GrpcIntakeFeed` is plaintext only; there is no TLS option |
| `protomolt-acquire-s3` | `acquire/s3` | Lists a bucket, pulls objects strictly past a `(lastModified, key)` watermark, and feeds them through intake. Ships the `pull-s3` verb and mounts as the `acquire-s3` role. | `acquire/s3/src/main/java/ai/protomolt/proto/acquire/s3/S3Pull.java` | The role mount is inert: nothing listens and nothing is scheduled |
| `protomolt-acquire-jdbc` | `acquire/jdbc` | Runs the caller's watermark query against a source database, wraps each row as a stable-identity JSON document, and feeds it through intake. Refuses a descending or unordered result set by name. Ships the `pull-jdbc` verb and the `acquire-jdbc` role. | `acquire/jdbc/src/main/java/ai/protomolt/proto/acquire/jdbc/JdbcPull.java` | The role mount is inert |
| `protomolt-acquire-msgraph` | `acquire/msgraph` | Microsoft Graph over the JDK HTTP client with no Microsoft SDK: hand-rolled OAuth2 (client credentials and device code), OneDrive and SharePoint files and list-item metadata, Copilot connector ingestion, and `GraphSchemas`, a fourth backend of the indexing-hints standard. | `acquire/msgraph/src/main/java/ai/protomolt/proto/acquire/msgraph/GraphClient.java` | Unwired: no verb, no role, no platform mount. It follows no `@odata.nextLink` and has no incremental checkpoints |

## schema/ : registry storage and third-party registry clients

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-registry` | `schema/registry/core` | The subject and version store SPI and its git-backed implementation: the repository is the storage, one commit per registration, with a monotonic global id, a compatibility write gate, path-traversal guards and transitive reference resolution. | `schema/registry/core/src/main/java/ai/protomolt/proto/registry/GitSchemaRegistryStore.java` | |
| `protomolt-schema-confluent` | `schema/confluent` | A read client over the Confluent subjects REST API, a pre-compiled descriptor-set source, and a publisher that registers in reverse-topological import order. | `schema/confluent/src/main/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryLoader.java` | Client side only |
| `protomolt-schema-apicurio` | `schema/apicurio/runtime` | A descriptor loader and publisher for Apicurio Registry v3's native API, with references first-class. Ships as a Quarkus extension runtime. | `schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoader.java` | Client side only |
| `protomolt-schema-apicurio-deployment` | `schema/apicurio/deployment` | The Quarkus build-time half of that extension. | `schema/apicurio/deployment/src/main/java/ai/protomolt/proto/schema/apicurio/deployment/ProtoToolsApicurioProcessor.java` | |

## inference/ : one contract in front of every model backend

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-inference-spi` | `inference/spi` | Three-method `InferenceProvider` SPI, a thread-safe in-memory model catalog with a mutation generation counter, a credential resolver that never leaks credential material into messages, and the resolution facade every surface shares. Providers never fall back. | `inference/spi/src/main/java/ai/protomolt/proto/inference/spi/InferenceEngines.java` | The catalog is rebuilt from launcher flags at every start; it is neither persistent nor distributed |
| `protomolt-inference-openvino` | `inference/openvino` | The OpenVINO provider plus the shared OpenAI-compatible chat transport (`/v3/chat/completions`, unary and SSE), JDK HTTP client only, unset sampling knobs omitted from the wire body. | `inference/openvino/src/main/java/ai/protomolt/proto/inference/openvino/OpenVinoProvider.java` | Never sets `model_version`, so that provenance field is always empty |
| `protomolt-inference-openai` | `inference/openai` | The `openai` profile of the same transport for `/v1` backends: Ollama, vLLM, llama.cpp, the edge-box lane. | `inference/openai/src/main/java/ai/protomolt/proto/inference/openai/OpenAiCompatProvider.java` | Depends on `inference/openvino` for the shared transport |
| `protomolt-inference-structured` | `inference/structured` | Fill a protobuf message with a model: render the prompt packet, send the rendered JSON Schema as the decoder constraint, parse strict protobuf JSON, validate against `validate.v1`, and retry only from rendered rejection feedback, capped at 3 attempts. Every attempt is carried back as provenance. | `inference/structured/src/main/java/ai/protomolt/proto/inference/structured/StructuredGenerator.java` | Provider errors abort immediately and are never retried |

## repo/ and parse/ libraries

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-repo-container` | `repo/container` | The claim-check storage engine: a descriptor-driven part codec splitting a document into `core.pb`, `blobs.pb`, `parsed.pb` and chunk objects that reassemble by field-level merge, an S3 blob store with virtual-thread part fan-out (Redis and caching variants included), and the Postgres ledger. | `repo/container/src/main/java/ai/protomolt/proto/repo/container/codec/DocumentPartCodec.java` | |
| `protomolt-parse-document` | `parse/document` | The parser-fleet document model: the canonical docling-core v2 parity `document.proto` that fleet repos re-vendor byte-identical, plus projections between it and the repo document. | `parse/document/src/main/java/ai/protomolt/proto/parse/document/DoclingProjection.java` | |

## host/config : distributed configuration

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-config` | `host/config` | Typed protobuf config documents behind a pluggable `ConfigSource`, applied verify-then-swap: parse, validate against the document's own declared rules, then swap atomically. A refused or malformed document never disturbs what is being served. Four typed mounts: access policy, trust snapshot, taxonomies and postal codes. | `host/config/src/main/java/ai/protomolt/proto/config/DistributedConfig.java` | |
| `protomolt-config-registry` | `host/config-registry` | Reads config documents from the git-backed registry over its native HTTP surface; the commit is the version. | `host/config-registry/src/main/java/ai/protomolt/proto/config/registry/RegistryConfigSource.java` | |
| `protomolt-config-kafka` | `host/config-kafka` | Reads a compacted topic through the house serde against the registry, with a deterministic name-UUID record key verified on read, no consumer group, tombstone removal, and a poisoned-subject slot rather than a crash. | `host/config-kafka/src/main/java/ai/protomolt/proto/config/kafka/KafkaConfigSource.java` | |

---

# 2. Services

Each of these defines a gRPC or HTTP contract. Most also ship a `ServiceModule`
so `host/composer` can mount them as a role; see [Role nodes](#role-nodes).

## The verb surface

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-grpc-service` | `surface/grpc/service` | The action catalog as a typed gRPC service: `ProtoMoltService`, 44 RPCs, served descriptor-natively with no generated stubs, with server reflection on. `ProtoMoltCatalog` is the assembled 44-verb catalog `apps/serve` and the CLI use. | `surface/grpc/service/src/main/java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcServer.java`, proto at `surface/grpc/service/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto` | Typed RPCs cover 44 of the 72 verbs |
| `protomolt-mcp` | `surface/mcp` | An MCP server over the action catalog and registry: JSON-RPC 2.0 on stdio, no framework, plus `protomolt://` resources for the workspace, registry, service profiles and delegation. `McpMain` is a standalone launcher registering 34 verbs. | `surface/mcp/src/main/java/ai/protomolt/proto/mcp/McpServer.java` | |
| `protomolt-acp-agent` | `surface/acp` | The action catalog as an Agent Client Protocol agent, so an ACP-capable IDE (JetBrains AI chat, Zed) runs any verb over stdio. | `surface/acp/src/main/java/ai/protomolt/proto/acp/agent/ProtoMoltAcpAgent.java` | |

## Schema registry

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-registry-service` | `schema/registry/service` | The registry's HTTP surface: a JDK `HttpServer` on virtual threads speaking the Confluent subjects protocol, plus native routes for descriptor-set and Parquet-schema derivation and `GET/POST {prefix}/actions`, which is the only HTTP home for contributed verbs. Mounts as role `registry` on port 8081 and ships `registry-remotes`, `registry-sync` and `publish-config`. | `schema/registry/service/src/main/java/ai/protomolt/proto/registry/service/SchemaRegistryServer.java` | The registry API is HTTP; there is no registry proto and no registry gRPC service |

## The document lane: intake, repo, account

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-intake-proto` | `intake/proto` | The intake contract: `IngestDocument` unary and `IngestStream` client-streaming, with a normative frame discipline, plus the HTTP upload lane that is deliberately not an RPC. Authentication is gRPC metadata, never a proto field. | `intake/proto/src/main/proto/ai/protomolt/proto/intake/v1/intake_service.proto` | |
| `protomolt-intake-service` | `intake/service` | The only authenticated way in. An API key resolves to an `IntakeScope` (account, datasources, drives, mime types, payload cap); the request may narrow within it and never widen it, and ownership is overwritten from the scope. Three key stores: in-memory, JDBC (SHA-256 digests, revocation by timestamp), and OIDC introspection. One HTTP route, `POST /v1/intake:upload`. Role `intake`, gRPC port 9092. | `intake/service/src/main/java/ai/protomolt/proto/intake/service/IntakeGrpcService.java` | No pull connector uses the streaming lane. `IntakeModule` and `IntakeServiceMain` disagree on the HTTP-port gate (`>= 0` versus `> 0`) |
| `protomolt-repo-proto` | `repo/proto` | The claim-check document store contract: `NodeAddress` (doc, graph address, account, graph), the `Document` model with blobs by reference or inline, the part manifest, `DocumentService` with 9 RPCs and `DriveService` with 3, plus the document event envelopes. | `repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document_service.proto` | Marked pre-release in `buf.yaml` |
| `protomolt-repo-service` | `repo/service` | The store as a gRPC service set, embeddable in-JVM over the in-process transport or standalone over Netty, plus an HTTP upload route that streams through a digest straight into the blob store. Dedupe is by Merkle root of the part split under a row lock; purge is two-phase with a staleness guard; a transactional outbox relays `document-events`. Role `repo`, gRPC port 9090. | `repo/service/src/main/java/ai/protomolt/proto/repo/service/DocumentGrpcService.java` | No authentication at all. `account_id` is a plain field, so repo-service must never be network-reachable by an untrusted caller. That constraint is prose, enforced by nothing in code |
| `protomolt-account-proto` | `account/proto` | The account contract: `Account`, `AccountService`, and the account-event outbox payloads. | `account/proto/src/main/proto/ai/protomolt/proto/account/v1/account_service.proto` | |
| `protomolt-account-service` | `account/service` | Account CRUD and activation, drive provisioning through repo-service RPC (exactly two drives per account, `intake` and `pipeline`), and the account-events outbox. Embeddable in-JVM or standalone over Netty. | `account/service/src/main/java/ai/protomolt/proto/account/service/AccountGrpcService.java` | Not in `DEFAULT_ROLES`; no `ServiceModule` |

## Parsing

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-parse-proto` | `parse/proto` | Two contracts: `ParseCoordinatorService` (dry-run routing and execute) and `ParserPluginService` (`GetParserInfo` plus a bidi `Parse`), which is how any parser joins the platform. | `parse/proto/src/main/proto/ai/protomolt/proto/parse/v1/parse_coordinator.proto` | |
| `protomolt-parse-service` | `parse/service` | The parsing coordinator runtime: magic-byte content sniffing, CEL routing rules over mime type, filename, extension, size and account, scatter-gather fan-out over the plugin contract, and the `SearchMetadata` fold persisted back through repo-service. Role `parse`, gRPC port 9093. | `parse/service/src/main/java/ai/protomolt/proto/parse/service/ParseCoordinatorGrpcService.java` | |
| `protomolt-parse-text` | `parse/text` | The reference parser: a pure-JDK `ParserPluginService` for plain text and markdown producing the fleet document model. The in-JVM parser for tests, demos and the all-in-one container. Role `parse-text`. | `parse/text/src/main/java/ai/protomolt/proto/parse/text/TextParserService.java` | |
| `protomolt-parse-grparse` | `parse/grparse` | The gRParse adapter: a `ParserPluginService` that buffers the plugin's chunked request stream and replays it to gRParse's `StreamProcessDocument`, assembling pages, collector documents and collector failures into one fleet-model document. Ships `GrparseAdapterMain`, default port 9096. | `parse/grparse/src/main/java/ai/protomolt/proto/parse/grparse/GrparseParserAdapter.java` | The vendored `parse_stream.proto` is one additive revision behind gRParse's, so the adapter cannot request a recognition mode |

## Search and metrics

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-search-proto` | `search/proto` | The search contract: `SearchIndexService` (index, delete) and `SearchService` (search, list subjects). Three lanes the caller must choose between (lexical, vector, hybrid), `k` capped at 10000 and refused rather than clamped, and typed `StoredValue` hits so a caller never re-parses a rendered string. | `search/proto/src/main/proto/ai/protomolt/proto/search/v1/search_service.proto` | |
| `protomolt-search-service` | `search/service` | A Lucene-backed query and indexing gRPC service over mapping subjects, gated by mapping membership, running the chunk-and-embed lane at index time, with hybrid fusion by reciprocal rank at a fixed k of 60. Contributes its live store to co-mounted roles so the metric executor borrows its searchers. Ships the `search` and `replay-documents` verbs. Role `search`, gRPC port 9094. | `search/service/src/main/java/ai/protomolt/proto/search/service/SearchGrpcServices.java` | Retrieve-only: rerank is never called. A read-only variant answers `UNIMPLEMENTED` on `SearchIndexService` |
| `protomolt-metric-proto` | `metric/proto` | The metric contract: `MetricService` with `DescribeMapping`, `QueryMetrics` and `RebuildRollup` over a mapping subject, with an explicit backend selector that is never a silent pick. | `metric/proto/src/main/proto/ai/protomolt/proto/metric/v1/metric_service.proto` | |
| `protomolt-metric-service` | `metric/service` | `MetricService` over served subjects with the validating interceptor mounted from day one, the per-principal member deny and row-filter rewrite, rollup rebuild under an exact-or-refuse group budget, and the `describe-mapping`, `query-metrics` and `rebuild-rollup` verbs. | `metric/service/src/main/java/ai/protomolt/proto/metric/service/MetricGrpcService.java` | `rebuildRollup` is refused outright for any principal carrying metric access rules |
| `protomolt-metric-lucene` | `metric/lucene` | The interactive executor: aggregate collectors over the doc values the search service already writes, no copy and no ETL. Also `MetricServiceModule`, the mount that binds the `metric` role's port 9095. | `metric/lucene/src/main/java/ai/protomolt/proto/metric/lucene/MetricServiceModule.java` | Refuses `COUNT_DISTINCT`, which the Iceberg backend supports |

## Jobs and inference

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-jobs-proto` | `jobs/proto` | The workflow-runs contract: the `WorkflowRunEvent` lifecycle envelope and the `WorkflowRunRequest` request-topic payload. Messages only, because the verbs are catalog actions. | `jobs/proto/src/main/proto/ai/protomolt/proto/jobs/v1/workflow_run_event.proto` | No gRPC service by design |
| `protomolt-jobs-service` | `jobs/service` | Durable asynchronous workflow execution running the same definition with the same serial semantics as `run-workflow`, detached: Postgres is the truth, a Kafka request topic in, lifecycle events out through a transactional outbox written in the same transaction as the state change. Plain JDBC, plain kafka-clients, virtual threads. Ships `submit-workflow`, `get-job`, `list-jobs` and `complete-step`. Role `jobs`. | `jobs/service/src/main/java/ai/protomolt/proto/jobs/service/worker/WorkflowRunWorker.java` | Neither repo nor intake enqueues a job; downstream work rides the `document-events` topic |
| `protomolt-inference-proto` | `inference/proto` | The inference contract: `InferenceService` with `Generate`, `GenerateStream`, `ListModels` and `DescribeModel`; the catalog `ModelEntry` carrying a credential reference, never credential material; and provenance-carrying generation envelopes. | `inference/proto/src/main/proto/ai/protomolt/proto/inference/v1/inference.proto` | `GenerateStreamRequest` deliberately omits structured output, so structured generation is unary only |
| `protomolt-inference-service` | `inference/service` | The generic gRPC surface wrapped around the SPI facade: requests validated against their declared rules before any provider is touched, blocking provider calls on virtual threads. Also the `inference-generate`, `inference-list-models` and `inference-describe-model` verbs. | `inference/service/src/main/java/ai/protomolt/proto/inference/service/InferenceServiceImpl.java` | The gRPC service has no production client. Every consumer uses the in-process `InferenceEngines` facade |

## Content connectors with their own contract

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-acquire-confluence` | `acquire/confluence` | A protobuf domain model of Confluence Cloud content (19 protos, a 27-arm entity oneof), a REST crawler with a bounded resume-cursor `Sync` stream, a `ChangeSink` SPI with Kafka, repo, Parquet, projected-Parquet, logging and in-memory implementations, and `ConfluenceService` behind `ConfluenceProxyServer`, a reflection-on Netty launcher on port 9095. | `acquire/confluence/src/main/java/ai/protomolt/proto/acquire/confluence/ConfluenceProxyServer.java`, proto at `acquire/confluence/src/main/proto/ai/protomolt/proto/acquire/confluence/v1/confluence_service.proto` | Working, pre-release. The crawler reaches 8 of the 27 entity arms; there is no deployment artifact; `RepoChangeSink` writes to repo directly rather than through intake |

---

# 3. Servers and apps

## host/ : composition, HTTP hosts and framework embeddings

All six HTTP hosts serve the same `ProtoRestGateway` over the same
`ProtoToolsServerConfig` defaults: path prefix `/grpc-json`, OpenAPI at
`/openapi.json`, health at `/health`, 16 MiB request cap. Only jdk, vertx and
netty implement `ProtoRestServerHost`; spring, micronaut and quarkus are facades
their container binds.

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-composer` | `host/composer` | The role engine. A `ServiceModule` declares its role and requirements and has a two-phase lifecycle: `wire` constructs, publishes in-process endpoints and registers contributions without serving; `start` binds ports and starts loops. Two phases exist because contribution hosts must observe every contribution before they serve. Also the channel pivot between co-mounted and remote roles over `PROTOMOLT_<ROLE>_TARGET`. | `host/composer/src/main/java/ai/protomolt/proto/composer/Composer.java` | Its javadoc cites a `DESIGN-service-modules.md` that does not exist; `docs/apps/role-nodes.md` is the live description |
| `protomolt-server-common` | `host/server/common` | The shared host abstraction, config record and HTTP support: status mapping, header and query parsing, error JSON, `Allow` headers and the body-size check. | `host/server/common/src/main/java/ai/protomolt/proto/server/ProtoRestServerHost.java` | |
| `protomolt-server-jdk` | `host/server/jdk` | `com.sun.net.httpserver` on virtual threads. Engine id `jdk`. | `host/server/jdk/src/main/java/ai/protomolt/proto/server/jdk/JdkProtoRestServer.java` | |
| `protomolt-server-vertx` | `host/server/vertx` | Vert.x 5, exposing `createRouter()` for mounting into a larger app. Engine id `vertx`. | `host/server/vertx/src/main/java/ai/protomolt/proto/server/vertx/VertxProtoRestServer.java` | |
| `protomolt-server-netty` | `host/server/netty` | Netty 4.2 with a virtual-thread invoker pool. Engine id `netty`. | `host/server/netty/src/main/java/ai/protomolt/proto/server/netty/NettyProtoRestServer.java` | |
| `protomolt-server-spring` | `host/server/spring` | A Spring MVC `@RestController` bean over the gateway, reading `pipestream.proto.rest.*` properties. | `host/server/spring/src/main/java/ai/protomolt/proto/server/spring/SpringProtoRestController.java` | A facade, not a host; Spring binds it |
| `protomolt-server-micronaut` | `host/server/micronaut` | A plain facade with no Micronaut annotations, for you to wire yourself. | `host/server/micronaut/src/main/java/ai/protomolt/proto/server/micronaut/MicronautProtoRestFacade.java` | A facade. There is no Micronaut DI module, by design |
| `protomolt-server-quarkus` | `host/server/quarkus` | An `@ApplicationScoped` CDI bean fronted by JAX-RS resources. Quarkus is still on Vert.x 4, so it cannot reuse the Vert.x 5 host. | `host/server/quarkus/src/main/java/ai/protomolt/proto/server/quarkus/QuarkusProtoRestFacade.java` | A facade |
| `protomolt-integration-spring` | `host/integration/spring` | A Spring Boot 3 `@AutoConfiguration` supplying `@ConditionalOnMissingBean` producers for the descriptor registry, field mapper, CEL evaluator, transcoder, method registry, a fail-closed API-token validator and the gateway. | `host/integration/spring/src/main/java/ai/protomolt/proto/integration/spring/ProtoToolsAutoConfiguration.java` | |
| `protomolt-integration-quarkus` | `host/integration/quarkus/runtime` | The Quarkus extension runtime: a `@Singleton` with `@Produces @DefaultBean` methods mirroring the Spring bean list plus the server config. | `host/integration/quarkus/runtime/src/main/java/ai/protomolt/proto/integration/quarkus/ProtoToolsProducer.java` | Extension status is `experimental`; it ships no `@ConfigMapping` of its own |
| `protomolt-integration-quarkus-deployment` | `host/integration/quarkus/deployment` | The build-time half: a feature item, an additional-bean item making the producer unremovable, and a reflective-class item. | `host/integration/quarkus/deployment/src/main/java/ai/protomolt/proto/integration/quarkus/deployment/ProtoToolsProcessor.java` | |

## apps/ and the browser consoles

| Gradle module | Directory | What it does | Entry point | Status |
|---|---|---|---|---|
| `protomolt-serve` | `apps/serve` | The one-process story: demo schemas, the delegation runtime and its 12 verbs, the mesh cluster runtime and its 6 verbs, the caller-resolver chain (access policy, then OIDC, then JDBC), gRPC with reflection and interceptors, the jobs worker and outbox relay, the registry with the catalog on its actions route, the REST gateway with Swagger UI and MCP, and the task console with same-origin proxies. | `apps/serve/src/main/java/ai/protomolt/proto/serve/ProtoMoltServe.java` | Deliberately JVM only; not a native-image target |
| `protomolt-document-platform` | `apps/document-platform` | The document platform in one container, or split across nodes by `PROTOMOLT_ROLES`: repo, intake, the parsing coordinator with the embedded reference parser, the jobs worker, the git-backed registry, the playground, search, metrics and the search console, all wired over the in-process transport. | `apps/document-platform/src/main/java/ai/protomolt/proto/platform/DocumentPlatformMain.java` | |
| `protomolt-cli` | `apps/cli` | Run any catalog verb from the terminal (JSON in, JSON out), list the verbs, or open an interactive console at the `protomolt>` prompt over the same catalog the servers expose. Also builds as a GraalVM native image. | `apps/cli/src/main/java/ai/protomolt/proto/cli/ProtoMoltCli.java` | |
| `protomolt-agent-host` | `apps/agent-host` | Persistent Codex and Kimi processes attached to delegation over MCP, with structured command gates, cursor recovery and provider session resume. Three providers: `codex`, `kimi` and `openai`. | `apps/agent-host/src/main/java/ai/protomolt/proto/agenthost/AgentHostMain.java` | A client of protomolt's MCP surface, not a consumer of protomolt inference; its `openai` provider re-implements the chat transport |
| `protomolt-record-verifier` | `apps/record-verifier` | The external verifier for signed work records. Its `build.gradle` declares no main-source dependencies at all, deliberately, because the module's whole value is verifying records without the platform's runtime. It runs the same named checks as the runtime verifier and claims conformance by producing the same verdict on every corpus fixture. | `apps/record-verifier/src/main/java/ai/protomolt/receipt/verify/ExternalVerifier.java` | |
| `protomolt-search-console` | `search/console` | A framework-free product page over the search service: subjects, lanes and hits, with an operations panel riding the actions route and sessions shared with the other guarded consoles. Role `search-console`, port 8096. | `search/console/src/main/java/ai/protomolt/proto/search/console/SearchConsoleServer.java` | Working, thin |
| `protomolt-parse-playground` | `parse/playground` | A pure-JDK web front end that streams typed parse events into the page as they happen, rendering the document progressively instead of waiting behind a spinner. Role `playground`, port 8095. | `parse/playground/src/main/java/ai/protomolt/proto/parse/playground/ParsePlaygroundServer.java` | |

### Native CLI (GraalVM)

`protomolt-cli` builds as a GraalVM native image: sub-10 ms startup and roughly
35 MB RSS for catalog verbs, no JRE needed on the target machine. Building the
native binary is the only task that requires GraalVM (a GraalVM JDK 25, for
example `sdk install java 25.1.3-graalce`); every other build and test task runs
on any JDK via Gradle toolchains.

```shell
JAVA_HOME=<graalvm-home> ./gradlew :protomolt-cli:nativeCompile
apps/cli/build/native/nativeCompile/protomolt-cli list
```

Releases also publish it as a multi-arch container (linux/amd64 and
linux/arm64; native-image does not cross-compile, so each architecture builds on
a matching runner and a manifest joins them):

```shell
docker run --rm ghcr.io/ai-pipestream/protomolt-cli list
```

All `generate-stubs` generators work natively. The generators are one bundled
WebAssembly module; inside a native image it runs on Chicory's interpreter
rather than being compiled to JVM bytecode at first use: same module, same
generators, slower per-invocation execution. On the JVM nothing changes.

Only the CLI is a native-image target today. `apps/serve` is deliberately
JVM-only: it is a runnable example of embedding the toolkit into a server build,
not a supported native artifact. Native servers are planned through the Spring,
Quarkus and Micronaut integrations instead.

---

# The action catalog

72 registered verbs across 15 owning modules. Every verb declares a required
scope from the closed 10-scope vocabulary
(`surface/actions/src/main/java/ai/protomolt/proto/actions/Scopes.java`), and
the same scope check runs at every front: catalog dispatch, the gRPC
interceptor, the REST mount and the registry HTTP route. A verb declaring no
scope is refused for a scoped caller rather than granted; no verb in the tree is
blank-scoped today, so that branch guards future plugins.

**The Typed column is the important asymmetry.** 44 verbs have a typed
`ProtoMoltService` RPC and therefore appear on gRPC, JSON/REST, OpenAPI and
Swagger UI. The other 28 are contributed at wire time by the delegation, mesh,
metric, search, registry and acquire modules; they reach HTTP only through the
registry's `{prefix}/actions` route, and are otherwise reachable through MCP,
ACP and the CLI. `docs/surface/actions.md` documents the 44; the 28 are
documented in their owning subsystem chapters, except the six `mesh-*` verbs,
which appear in no document.

The 44 are pinned by `docs/generated/action-inventory.json`, generated by
`CatalogInventoryGenerator` in `tests/system` and asserted by
`CatalogInventorySystemTest`.

| # | Action | Scope | Owning module | Typed | Purpose |
|---|---|---|---|---|---|
| 1 | `compile` | `SCHEMA_READ` | `protomolt-actions` | yes | Compile inline `.proto` sources into a base64 `FileDescriptorSet` |
| 2 | `list-types` | `SCHEMA_READ` | `protomolt-actions` | yes | Enumerate messages, enums and services with field shapes |
| 3 | `validate-message` | `SCHEMA_READ` | `protomolt-actions` | yes | Validate a JSON message against rules declared on its descriptor |
| 4 | `diff-schemas` | `SCHEMA_READ` | `protomolt-actions` | yes | Typed change list between two schema versions with stable rule ids |
| 5 | `check-compat` | `SCHEMA_READ` | `protomolt-actions` | yes | Compatibility verdict under a named mode |
| 6 | `render-json-schema` | `SCHEMA_READ` | `protomolt-actions` | yes | JSON Schema for a message type, folding in validation rules |
| 7 | `render-prompt` | `SCHEMA_READ` | `protomolt-actions` | yes | Descriptor-grounded LLM prompt packet plus the decoder constraint |
| 8 | `render-index-mappings` | `SCHEMA_READ` | `protomolt-actions` | yes | OpenSearch, Solr, Lucene or Qdrant index artifact from declared hints |
| 9 | `eval-cel` | `SCHEMA_READ` | `protomolt-actions` | yes | Evaluate a CEL expression against a typed JSON message |
| 10 | `map-message` | `SCHEMA_READ` | `protomolt-actions` | yes | Apply text and CEL mapping rules to a message |
| 11 | `synthesize-shape` | `SCHEMA_READ` | `protomolt-actions` | yes | Derive envelope, projection or tagged-union types from named sources |
| 12 | `join-messages` | `SCHEMA_READ` | `protomolt-actions` | yes | Join named source messages into one output with scoped rules |
| 13 | `merge-schemas` | `SCHEMA_READ` | `protomolt-actions` | yes | Merge top-level fields of two or more types with clash analysis |
| 14 | `check-rules` | `SCHEMA_READ` | `protomolt-actions` | yes | Statically type-check mapping rules and CEL against descriptors |
| 15 | `infer-schema` | `SCHEMA_READ` | `protomolt-actions` | yes | Reverse-engineer a proto definition from JSON samples |
| 16 | `mask-message` | `SCHEMA_READ` | `protomolt-actions` | yes | Mask fields by declared sensitivity class (remove, redact, encrypt, decrypt) |
| 17 | `extract-metadata` | `SCHEMA_READ` | `protomolt-actions` | yes | Flat bag of declared `meta.v1` metadata for a type |
| 18 | `reflect` | `SERVICE_INVOKE` | `protomolt-grpc-invoke` | yes | Fetch a live server's schema over server reflection |
| 19 | `grpc-invoke` | `SERVICE_INVOKE` | `protomolt-grpc-invoke` | yes | Invoke a unary or server-streaming method with no generated stubs (streaming) |
| 20 | `generate-stubs` | `SCHEMA_READ` | `protomolt-codegen` | yes | Generate code in 8 languages plus grpc-java through protoc as WebAssembly |
| 21 | `gather-git` | `SCHEMA_READ` | `protomolt-acquire-gather-git` | yes | Gather and compile `.proto` sources from a git ref |
| 22 | `run-workflow` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Execute a workflow: serial mapped unary gRPC calls with gates |
| 23 | `check-workflow` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Statically verify a workflow definition without running it |
| 24 | `compile-workflow` | `SCHEMA_READ` | `protomolt-workflow` | yes | Compile a checked workflow into the deterministic contract |
| 25 | `suggest-mappings` | `SCHEMA_READ` | `protomolt-workflow` | yes | Propose verifiable `target=source.path` mappings, never guessing |
| 26 | `record-workflow-run` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Run as a probe and persist redacted content-addressed fixtures |
| 27 | `replay-workflow` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Replay recorded fixtures offline and report drift |
| 28 | `promote-workflow` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Store validated workflow content as an immutable registry version |
| 29 | `export-work-record` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Project run evidence into a canonical signed work record |
| 30 | `verify-work-record` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Verify a signed record against a caller trust snapshot, zero network |
| 31 | `evaluate-work-record` | `WORKFLOW_RUN` | `protomolt-workflow` | yes | Evaluate a record beside its evidence under a predeclared policy |
| 32 | `emit-okf` | `ARTIFACT_ACCESS` | `protomolt-emit-okf` | yes | Render a schema as an OKF v0.1 markdown knowledge bundle |
| 33 | `submit-workflow` | `WORKFLOW_RUN` | `protomolt-jobs-service` | yes | Submit a workflow as a durable asynchronous job |
| 34 | `get-job` | `SERVICE_INVOKE` | `protomolt-jobs-service` | yes | Read one workflow run by id with checkpoints and verdict |
| 35 | `list-jobs` | `SERVICE_INVOKE` | `protomolt-jobs-service` | yes | List workflow runs newest first, filtered by status or name |
| 36 | `complete-step` | `SERVICE_INVOKE` | `protomolt-jobs-service` | yes | Complete a run's parked external step with a validated response |
| 37 | `inference-generate` | `SERVICE_INVOKE` | `protomolt-inference-service` | yes | Run one chat generation against a catalog model |
| 38 | `inference-list-models` | `SERVICE_INVOKE` | `protomolt-inference-service` | yes | List the inference catalog with capabilities and generation counter |
| 39 | `inference-describe-model` | `SERVICE_INVOKE` | `protomolt-inference-service` | yes | Describe one catalog model by id |
| 40 | `service-register` | `SERVICE_INVOKE` | `protomolt-grpc-service-workspace` | yes | Register a durable gRPC service profile and store its descriptor |
| 41 | `service-list` | `SERVICE_INVOKE` | `protomolt-grpc-service-workspace` | yes | List registered profiles with endpoints and fingerprints |
| 42 | `service-inspect` | `SERVICE_INVOKE` | `protomolt-grpc-service-workspace` | yes | Inspect one profile's methods, shapes and top-level fields |
| 43 | `service-refresh` | `SERVICE_INVOKE` | `protomolt-grpc-service-workspace` | yes | Re-reflect an endpoint and update the profile fingerprint |
| 44 | `service-invoke` | `SERVICE_INVOKE` | `protomolt-grpc-service-workspace` | yes | Invoke a registered method by profile name (streaming) |
| 45 | `delegation-worker-register` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Open the worker stream, send hello, return admission |
| 46 | `delegation-worker-list` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | List registered workers with state and capabilities |
| 47 | `delegation-offer` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Offer a bounded task with lease seconds and acceptance checks |
| 48 | `delegation-accept` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Accept the open offer; the worker takes the attempt lease |
| 49 | `delegation-progress` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Report one bounded, sequenced progress note |
| 50 | `delegation-checkpoint` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Record a resumable checkpoint with a resume token |
| 51 | `delegation-candidate` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Submit a completion candidate with evidence for review |
| 52 | `delegation-review` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Apply an accept or revise verdict on the open candidate |
| 53 | `delegation-cancel` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Cancel a task's open attempt with a bounded reason |
| 54 | `delegation-message` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Send a non-transitioning task message in either direction |
| 55 | `delegation-watch` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Long-poll the event feed from a cursor, bounded batch |
| 56 | `delegation-transcript` | `WORKER_COORDINATE` | `protomolt-delegation` | contributed | Read the recorded transcript from a cursor |
| 57 | `mesh-node-register` | `WORKER_COORDINATE` | `protomolt-mesh-cluster` | contributed | Register or refresh a fenced node advertisement |
| 58 | `mesh-node-heartbeat` | `WORKER_COORDINATE` | `protomolt-mesh-cluster` | contributed | Extend a node's liveness window with a fenced heartbeat |
| 59 | `mesh-processor-register` | `WORKER_COORDINATE` | `protomolt-mesh-cluster` | contributed | Register or renew a health-gated processor lease |
| 60 | `mesh-capacity-update` | `WORKER_COORDINATE` | `protomolt-mesh-cluster` | contributed | Publish a fenced point-in-time capacity snapshot |
| 61 | `mesh-snapshot` | `WORKER_COORDINATE` | `protomolt-mesh-cluster` | contributed | Return the deterministic directory snapshot and eligibility |
| 62 | `mesh-sweep` | `WORKER_COORDINATE` | `protomolt-mesh-cluster` | contributed | Expire elapsed leases and presence windows, cascading node loss |
| 63 | `describe-mapping` | `METRICS_QUERY` | `protomolt-metric-service` | contributed | Describe a metric mapping subject's members and backends |
| 64 | `query-metrics` | `METRICS_QUERY` | `protomolt-metric-service` | contributed | Run one bounded aggregate query on a mapping subject |
| 65 | `rebuild-rollup` | `METRICS_REBUILD` | `protomolt-metric-service` | contributed | Rebuild a declared rollup, atomically replacing the lake table |
| 66 | `search` | `SEARCH_QUERY` | `protomolt-search-service` | contributed | Search a mapping subject: lexical, vector or hybrid lane |
| 67 | `replay-documents` | `SEARCH_INDEX` | `protomolt-search-service` | contributed | Re-run a stored workflow over every matching repository document |
| 68 | `registry-remotes` | `SCHEMA_WRITE` | `protomolt-registry-service` | contributed | Manage the git remotes this registry federates from |
| 69 | `registry-sync` | `SCHEMA_WRITE` | `protomolt-registry-service` | contributed | Fetch a configured remote registry and import its subjects |
| 70 | `publish-config` | `SCHEMA_WRITE` | `protomolt-registry-service` | contributed | Publish one typed config document to the registry config gate |
| 71 | `pull-s3` | `SERVICE_INVOKE` | `protomolt-acquire-s3` | contributed | Pull changed S3 objects past a watermark through intake |
| 72 | `pull-jdbc` | `SERVICE_INVOKE` | `protomolt-acquire-jdbc` | contributed | Run a watermark query and feed rows through intake |

Scope distribution: `SCHEMA_READ` 21, `WORKER_COORDINATE` 18, `SERVICE_INVOKE`
15, `WORKFLOW_RUN` 9, `SCHEMA_WRITE` 3, `METRICS_QUERY` 2, `ARTIFACT_ACCESS` 1,
`SEARCH_QUERY` 1, `SEARCH_INDEX` 1, `METRICS_REBUILD` 1.

Three catalog layers assemble subsets of these: `ActionCatalog.defaults(...)`
registers the 17 schema built-ins, `McpMain` registers 34 for the standalone
stdio MCP server, and `ProtoMoltCatalog` assembles the 44 that `apps/serve` and
the CLI use. The remaining 28 are added imperatively at wire time by whichever
module owns them.

Naming trap: `service-invoke` is both an action name and the string value of
`Scopes.SERVICE_INVOKE`. They are unrelated identifiers that collide.

Not actions, despite similar names: `list-subjects` (a `SearchService` RPC),
`grpc` and `kafka` (`SourcePump` connector names), and `codex`, `kimi` and
`openai` (agent-host provider names).

---

# Surfaces

Every front a verb or a service can be reached through.

| Surface | Module | Entry point | Status |
|---|---|---|---|
| Typed gRPC service (`ProtoMoltService`, 44 RPCs) | `surface/grpc/service` | `ProtoMoltGrpcServer`; proto at `surface/grpc/service/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto` | Shipped |
| gRPC server reflection, server side | `surface/grpc/service` | Enabled on `ProtoMoltGrpcServer` | Shipped |
| gRPC server reflection, client side | `surface/grpc/invoke` | `ReflectAction` | Shipped |
| Dynamic gRPC invocation, no stubs | `surface/grpc/invoke` | `GrpcInvokeAction`, `DynamicGrpcCalls` | Shipped |
| JSON/REST gateway | `surface/http/rest` | `ProtoRestGateway`, `ProtoRestMethodRegistry`, `@ProtoRestExposed` | Shipped |
| Protobuf and JSON transcoding | `surface/http/json` | `ProtobufJsonTranscoder` | Shipped |
| OpenAPI 3 generation, mounted at `/openapi.json` | `surface/http/openapi` | `ProtoOpenApiGenerator` | Shipped |
| Swagger UI at `/docs` | `apps/serve` | `SwaggerUiHandler` | Shipped |
| JSON Schema 2020-12 generation | `surface/http/jsonschema` | `ProtoJsonSchemaGenerator` | Shipped |
| MCP over stdio | `surface/mcp` | `McpServer`, `McpMain` | Shipped |
| MCP over streamable HTTP at `/mcp` | `apps/serve` | `McpHttpHandler`, bounded sessions and in-flight cancellation | Shipped |
| MCP resources (`protomolt://…`) | `surface/mcp` | `WorkspaceResources`, `RegistryResources`, `ServiceProfileResources`, `DelegationResources`, `CompositeResources` | Shipped; resource-level scope filtering is authenticated-only |
| ACP core | `core/acp` | `AcpAgent`, `AcpClient`, `AcpConnection`, `PromptHandler` | Shipped |
| ACP agent over the catalog | `surface/acp` | `ProtoMoltAcpAgent`, `CatalogLineRunner` | Shipped |
| Action catalog | `surface/actions` | `ActionCatalog`, `ProtoAction`, `StreamingAction` | Shipped |
| CLI, JSON in and JSON out | `apps/cli` | `ProtoMoltCli` | Shipped |
| CLI interactive console | `apps/cli` | `Console`, prompt `protomolt>` | Shipped |
| GraalVM native CLI | `apps/cli` | `:protomolt-cli:nativeCompile` | Shipped |
| Confluent-protocol schema registry HTTP | `schema/registry/service` | `SchemaRegistryServer` | Shipped |
| Registry native actions route (`{prefix}/actions`, default `/protomolt`) | `schema/registry/service` | `SchemaRegistryServer` | Shipped; the only HTTP home for the 28 contributed verbs |
| Kafka Connect sink driving gRPC | `sink/kafka/connect` | `GrpcSinkConnector`, `GrpcSinkTask` | Shipped |
| Kafka Connect source from server streams | `sink/kafka/connect` | `GrpcSourceConnector`, `GrpcSourceTask` | Shipped |
| Kafka Connect transforms | `sink/kafka/connect` | `ValidateMessage`, `MapMessage`, `RedactMessage`, `CelFilter` | Shipped |
| Kafka Connect Iceberg sink | `sink/kafka/connect-iceberg` | `IcebergSinkConnector` | Shipped |
| Kafka Connect OpenSearch sink | `sink/kafka/connect-opensearch` | `OpenSearchSinkConnector` | Shipped |
| Kafka protobuf serde (Confluent wire) | `sink/kafka/serde` | `ProtoMoltProtobufSerializer`, `ProtoMoltProtobufDeserializer` | Shipped |
| Kafka Streams `Serde` | `sink/kafka/serde` | `ProtoMoltSerde` | Shipped |
| Validating gRPC interceptors | `surface/grpc/validation` | `ValidatingServerInterceptor`, `ValidatingClientInterceptor` | Shipped |
| Micrometer observability bindings | `surface/grpc/validation-micrometer`, `sink/kafka/serde-micrometer` | `MicrometerGrpcValidationMetrics`, `MicrometerSerdeMetrics` | Shipped |
| Task console at `/console` | `apps/serve` | `ConsoleHandler`, `TaskConsoleApiHandler` | Shipped |
| Search console, port 8096 | `search/console` | `SearchConsoleServer`, `SearchConsolePage` | Working, thin |
| Parser playground, port 8095 | `parse/playground` | `ParsePlaygroundServer` | Shipped |
| Zero-dependency record verifier CLI | `apps/record-verifier` | `Main`, `ExternalVerifier` | Shipped |
| Code generation for 8 languages plus grpc-java | `core/codegen` | `GenerateStubsAction`, protoc as WebAssembly | Shipped |
| Docker images (serve, cli multi-arch, acp) | `docker-compose.yml`, `deploy/` | services `serve` and `acp` | Shipped |

Beyond the verb catalog, each service in section 2 is its own surface: intake
gRPC and `POST /v1/intake:upload`, repo gRPC and `POST /v1/documents:upload`,
the parse coordinator, search, metrics, inference, accounts and the Confluence
proxy.

---

# The nine annotation families

Nine option namespaces extend `google.protobuf.MessageOptions` and
`FieldOptions`, all allocated out of one contiguous private extension block, ten
numbers per family, field extension first. A tenth vocabulary, `buf.validate`,
is vendored upstream and uses upstream's numbers.

| # | Family | Extensions | Declares | Runtime consumer |
|---|---|---|---|---|
| 1 | indexing hints (`index.hints.v1`) | `index = 59100471` (field), `index_message = 59100472` (message) | Index field type, stored and indexed flags, name override, vector dims and similarity, HNSW parameters, sub-fields, analyzers, null value, sortable and facetable, map mode, date format and resolution, per-engine escape hatch, block-join role, chunk-and-embed policy, `Any` payload gating | `ProtoOptionsIndexingHintSource` into `IndexMappingFactory` (`search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/`) |
| 2 | validation (`validate.v1`) | `field = 59100481`, `message = 59100482` | `FieldRules` (36 string tags, numeric, bool, bytes, enum, repeated, map, timestamp, duration, CEL, `ignore_if_zero`, `taxonomy`) and `MessageRules` (CEL, `skip_when`) | `ProtomoltRuleSource` into `ProtoValidator` (`protobuf/validation/src/main/java/ai/protomolt/proto/validate/`) |
| 3 | metadata (`meta.v1`) | `field = 59100491`, `message = 59100492` | Description, display name, owner, sensitivity class, labels, and a round-trip-safe `json_name` | `DescriptorMetadata` (`protobuf/metadata/src/main/java/ai/protomolt/proto/meta/DescriptorMetadata.java`), plus `SchemaInferrer` and `ShapeSynthesizer` |
| 4 | quality (`quality.v1`) | `quality = 59100501` (message only) | CEL-scored dimensions with weights, composed as a weighted mean | `QualityScorer` (`protobuf/quality/src/main/java/ai/protomolt/proto/quality/QualityScorer.java`) |
| 5 | projection (`projection.v1`) | `sources = 59100511` (message), `from = 59100512`, `default_from = 59100513` (field) | Declared source types and per-field provenance: candidate paths, CEL, or a literal | `MessageProjection` (`transform/projection/src/main/java/ai/protomolt/proto/projection/MessageProjection.java`) |
| 6 | LLM (`llm.v1`) | `field = 59100521`, `message = 59100522` | Fill instructions, safeguards, and a volatility flag | `DescriptorLlm` into `InstructionRenderer` (`protobuf/llm`, `protobuf/prompt`) |
| 7 | mesh (`mesh.v1`) | `field = 59100531`, `message = 59100532` | Processing and route profiles by reference, result type, capabilities, recursion limits, LLM permission, PII scan requirement, approval and evidence policy; nine independent field roles | **None in main source.** The only reader is `mesh/contracts/src/test/java/ai/protomolt/proto/mesh/v1/MeshOptionsRoundTripTest.java`, which pins the vocabulary. The contract exists and is tested; the processors that would honor it do not read it yet |
| 8 | metric (`metric.v1`) | `metric = 59100541` (field), `metric_message = 59100542` (message) | Member role, aggregate, public name, per-measure row filter, calculated measures, default time grain, subject, identity field, synthetic members | `ProtoOptionsMetricHintSource` into `MetricMapping` (`metric/spi/src/main/java/ai/protomolt/proto/metric/spi/`). **No `.proto` in the tree uses these options**; the only annotated instances are built programmatically in tests |
| 9 | protovalidate (`buf.validate`, vendored) | upstream numbers 1159 and 1160 | The full upstream rule vocabulary, vendored verbatim at v1.2.2 | `ProtovalidateRuleSource`, ServiceLoader-registered (`protobuf/validation-protovalidate`) |

Two more things worth stating plainly:

- **`index_message` (59100472) has no reader anywhere**, main or test.
  `IndexMappingFactory` walks nested fields unconditionally.
- **Extension allocation is a convention, not a checked invariant.** There is no
  registry file, no test and no lint rule pinning the block.

Six families implement the same three-step option read, and it is load-bearing
rather than defensive: prefer `hasExtension`; treat an absent unknown field as
genuinely absent; otherwise reparse the options bytes against a registry that
knows the extension, because a dropped hint would not just lose an analyzer, it
would revert a deliberate schema decision. Metadata, LLM and projection read
`hasExtension` only, so an option carried on an unknown-field descriptor is
invisible to them. The verb boundary papers over this for catalog callers:
`SchemaResolver` re-parses inline and descriptor-set schemas with the toolkit's
extensions registered.

There is exactly one CEL module,
`transform/mapper/cel` (`ai.protomolt.proto.cel`), and validation, quality,
projection, shapes, metadata mapping, metric filters, the parse coordinator, the
Kafka Connect transforms and the action catalog all compile through it. Three
root variable names are in use: validation and quality bind `this`, projection
binds `source`, the mapper binds `input` by default and the pipeline executor
overrides it to `target`.

Documentation of record: [validation](docs/transform/validation.md),
[indexing](docs/search/indexing.md), [projection](docs/transform/projection.md),
[quality](docs/transform/quality.md), [metadata](docs/schema/metadata.md),
[masking](docs/transform/masking.md),
[well-known types](docs/design/well-known-types.md),
[metric mapping](docs/design/metric-mapping.md).

---

# Role nodes

`apps/document-platform` boots one binary as any subset of twelve roles.
`PROTOMOLT_ROLES` selects them (comma-separated); `metrics` and `parser-text`
are accepted aliases for `metric` and `parse-text`; an unknown role is refused
at boot listing the known set. Absent roles are reached over
`PROTOMOLT_<ROLE>_TARGET`.

`DEFAULT_ROLES` is the full one-container preset, in canonical mount order:
`repo, parse-text, registry, parse, jobs, intake, playground, search, metric,
search-console`. `KNOWN_ROLES` adds `acquire-s3` and `acquire-jdbc`. Both sets
name each mounting module's own `ROLE` constant rather than repeating the
strings, so a renamed role renames them with it.

| Role | Mounted by | Port | Surface |
|---|---|---|---|
| `repo` | `repo/service` `RepoServiceModule` | 9090 gRPC, 8080 HTTP | `DocumentService`, `DriveService`, health, reflection, `POST /v1/documents:upload` |
| `intake` | `intake/service` `IntakeModule` | 9092 gRPC | `IntakeService`, API-key authenticated, plus the optional HTTP upload lane |
| `parse` | `parse/service` `ParseModule` | 9093 gRPC | `ParseCoordinatorService` |
| `parse-text` | `parse/text` `TextParserModule` | in-process | `ParserPluginService`, the reference parser |
| `search` | `search/service` `SearchServiceModule` | 9094 gRPC | `SearchService`, `SearchIndexService` |
| `metric` | `metric/lucene` `MetricServiceModule` | 9095 gRPC | `MetricService` |
| `registry` | `schema/registry/service` `RegistryModule` | 8081 HTTP | Confluent subjects protocol plus `/protomolt/actions` |
| `jobs` | `jobs/service` `JobsModule` | in-process plus Kafka and Postgres | The durable workflow worker and outbox relay; the four job verbs |
| `playground` | `parse/playground` `PlaygroundModule` | 8095 HTTP | The streaming parser playground |
| `search-console` | `search/console` `SearchConsoleModule` | 8096 HTTP | The search console |
| `acquire-s3` | `acquire/s3` `S3PullModule` | none | Contributes `pull-s3`; the mount is inert |
| `acquire-jdbc` | `acquire/jdbc` `JdbcPullModule` | none | Contributes `pull-jdbc`; the mount is inert |

Co-mounting constraints, because contributions travel in-JVM:

- `registry`, `jobs` and `parse` belong on one node. The job verbs, the
  parse-and-index workflow and `replay-documents` all ride the registry's
  actions route.
- The acquire roles belong with `intake`, which they require.
- **`metric` requires `search` on the same node and refuses to boot without
  it**, because the metric executor borrows the search service's live
  `LuceneSearchStore`.
- `repo` splits off cleanly, and that split is the proven topology
  (`deploy/document-platform/compose-roles.yml`, `PlatformRoleNodeIT`).

A remote read-only metrics node is `PROTOMOLT_ROLES=search,metric` with
`DOCUMENT_PLATFORM_SEARCH_READ_ONLY=true` and the
`DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_*` family pointing at the writer's bucket.
It restores from the bucket on boot, needs no repo role or target, answers
`UNIMPLEMENTED` on `SearchIndexService`, and never writes to the bucket.

Node-wide security: `PROTOMOLT_API_TOKEN` makes every network surface a serving
role exposes demand it, and makes the node's own remote-role channels present
it. `PROTOMOLT_ACCESS_POLICY` loads the principal document, and the same
document is re-scopable at runtime through the config-lane subject
`access-policy`.

See [role nodes](docs/apps/role-nodes.md) and
[the document platform](docs/apps/document-platform.md).

---

# Adjacent projects

These are separate repositories. They are named here because protomolt either
depends on them, is depended on by them, or shares a design lineage. Detail
lives in the survey documents, not in this README.

**Apache OpenNLP.** The project owner is a primary committer on Apache OpenNLP
3.x. Protomolt consumes a preview build published as
`ai.pipestream:opennlp-*`, used in exactly two places: `transform/screening`
(the only module that pulls OpenNLP, for `TokenNameFinder`-backed detection) and
`search/chunk` plus `search/embedding/model2vec` (sentence boundaries and
Model2Vec static embeddings). The upstream programme covers offset tracking,
speed, pure-Java static embeddings, a pluggable gRPC embedding surface and a
gRPC server so non-Java teams can use OpenNLP. See
`/work/shared-docs/system-survey/03-opennlp-grpc-and-vectors.md`.

**The gRParse parsing fleet.** Seventeen independent repositories, one gRPC
service each, sharing one document model that is a field-for-field port of
docling's `DoclingDocument` v2, with gRParse as the C++ scatter-gather
coordinator. Protomolt does not talk to the individual services; it talks to
gRParse through `parse/grparse`, and `parse/document` holds the canonical copy
of the shared document proto that the fleet repos re-vendor byte-identical. See
`/work/shared-docs/system-survey/04-grparse-and-parsing-fleet.md`.

**distributed-search (knn-node).** A streamed cross-shard vector search service
on a forked Apache Lucene, whose distinguishing feature is a shared, monotonic
score floor that lets uncompetitive HNSW paths stop early (Apache Lucene PR
16357). It is already a consumer of protomolt's mapping SPI: its
`BlockJoinLuceneMapper` implements `SearchEngineIndexer` under engine id
`distributed-lucene` and delegates flat field mapping to protomolt's
`ProtoLuceneMapper`. It is not integrated on the query side, because protomolt
has no query-side index SPI. The service describes itself as a developer
preview. See `/work/shared-docs/system-survey/05-search-vector-distributed.md`.

**turbovec.** A Rust scalar-quantization codec plus a SIMD exhaustive-scan
kernel implementing Google Research's TurboQuant, with a gRPC sharding facade
and a separate full search product built on it. It is exact by construction
because there is no graph to be layout-dependent about. There are zero
references to turbovec anywhere in protomolt today, though its own documents
state an intent to grow into a protomolt search provider. Same survey document.

---

# Naming

`ProtoMolt` is the project and artifact name. Maven coordinates keep the
`ai.pipestream` group id: dependencies are `ai.pipestream:protomolt-*`. Java
packages and the wire-level proto namespace — package declarations and the
descriptor-option dialects — both use `ai.protomolt.proto.*`:
`ai.protomolt.proto.{meta,validate,llm,quality,metric,index.hints,projection,mesh}.v1`,
beside the vendored `buf.validate` compatibility dialect. Configuration
properties use the `protomolt.*` prefix and environment variables the
`PROTOMOLT_*` prefix (framework-integration glue exposes a handful of
`pipestream.*` properties). Module and package derivation is ADR-002 in
[AGENTS.md](AGENTS.md).

Vocabulary note: the request-gating component is a **service**, **role** or
**gate**. "Door" is not project vocabulary.

# Requirements

- JDK 25 or newer at runtime (the build itself runs on any JDK via Gradle
  toolchains; GraalVM CE 25 works as the build JDK)
- Gradle 9.6+ for building from source (wrapper included)
- GraalVM JDK 25 only when building the native CLI with
  `:protomolt-cli:nativeCompile`: the resulting binary needs no JDK at all

# Building

`./gradlew build` compiles everything and runs the full test suite. Versions are
derived from `v*` tags (Axion); an untagged checkout builds as a snapshot.
Integration tests against real schema registries are opt-in and skip
automatically when no registry is reachable. CI additionally runs `buf lint`,
`buf breaking` on pull requests, and buf's protovalidate conformance suite bare,
with no skip list. See [building and testing](docs/operations/building.md).

# Runtime disk footprint

ProtoMolt never writes message data to disk from the toolkit modules. The only
runtime writes from `apps/serve` are declared schema storage at locations the
operator chooses: the registry's git repository (`--registry-git`, or a
temporary directory in `--demo` mode) and `gather-git`'s persistent clone cache
(`--gather-cache` / `PROTOMOLT_GATHER_CACHE`, defaulting to
`~/.cache/protomolt/gather/git`). Cache placement is server configuration, never
request input. Everything else, including compilation of `.proto` text to
descriptors, runs entirely in memory.

The document platform is different by design: `repo/service` writes document
parts and blobs to object storage and rows to Postgres, `search/service` writes
a Lucene index, and `jobs/service` writes to Postgres and Kafka. Those are the
services whose job is durable state.

# Documentation

The [documentation index](docs/README.md) follows the repository's module
layout. Start with these workflows:

- [Connect an agent over MCP](docs/surface/mcp.md)
- [Register and inspect a gRPC service](docs/surface/service-workspace.md)
- [Build, replay, and promote a workflow](docs/transform/workflows.md)
- [Run a checked streaming pipeline](docs/transform/pipeline.md)
- [Serve JSON/REST over protobuf](docs/surface/rest-gateway.md)
- [Index and search documents](docs/search/service.md)
- [Query metrics over the same index](docs/metric/metrics.md)
- [Split the platform into role nodes](docs/apps/role-nodes.md)
- [Pull documents from S3 or JDBC](docs/acquire/pull-connectors.md)
- [Generate a Python client without protoc](docs/tutorials/python.md)
- [Build and test ProtoMolt](docs/operations/building.md)

Known gaps in what is written down, so you do not go looking: there is no
`docs/parse/`, `docs/intake/`, `docs/inference/` or `docs/host/` chapter, and
the six `mesh-*` verbs appear in no document. Work that is designed but not
shipped is tracked in [planned work](docs/design/planned-work.md).

# License

[Apache License 2.0](LICENSE) (c) 2026 ai.pipestream
