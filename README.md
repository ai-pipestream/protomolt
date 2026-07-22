# ProtoMolt

ProtoMolt is a modular Java toolkit for working with Protocol Buffers at
runtime. It loads descriptors from schema registries and other sources,
validates messages against rules carried on those descriptors, reshapes
messages with mapping rules and CEL, projects them into search indexes, and
exposes them over JSON/REST with generated OpenAPI and JSON Schema.

Everything operates on descriptors (`Descriptor` / `FileDescriptor`), not on
generated classes. The same code paths work for `DynamicMessage`s resolved
from a registry at runtime as for compiled-in types, and no module is coupled
to any particular message type.

## Modules

Modules are grouped by domain; the table covers every published artifact.
Maven artifact IDs carry the `protomolt-` prefix; Java packages use the
`ai.pipestream.proto.*` namespace (see [Naming](#naming)).

| Group | Artifacts | Purpose |
|---|---|---|
| `core/` | `descriptors`, `helpers`, `proto-sources` | `DescriptorRegistry` and loader SPI; `Any`/`Struct` handling, type conversion, message diff, schema hygiene checks; proto source-set model, runtime `.proto` compilation, publisher SPI |
| `core/formats/` | `formats` | Zero-dependency RFC validators (email, hostname, IP, URI) backing the validation string formats |
| `core/codegen/` | `codegen` | Live code generation: every libprotoc generator (8 languages) and the grpc-java plugin as WebAssembly, no native toolchain |
| `core/compat/` | `compat` | Breaking-change detection: typed schema diffs and backward/forward/full compatibility policies |
| `acquire/gather/` | `gather`, `gather-git`, `gather-maven` | Acquire `.proto` sources from directories, jars, Git repositories, and Maven coordinates; adapt any gatherer to a descriptor loader |
| `acquire/msgraph/` | `msgraph` | Microsoft Graph: OneDrive/SharePoint files and list-item metadata, and Copilot connector ingestion over the external connections API |
| `acquire/connector/` | `connector` | Push-style streaming inputs behind one bounded, pausable SPI: gRPC server streams and Kafka topics feed a synchronous pipeline through the `SourcePump` bridge |
| `schema/registry/` | `registry`, `registry-server` | Git-backed schema registry: subject/version store with compatibility-gated writes, served over the Confluent protocol |
| `schema/` | `schema-apicurio`, `schema-apicurio-deployment`, `schema-confluent` | Descriptor loaders and schema publishers for Apicurio Registry and Confluent-compatible schema registries; the Apicurio loader's Quarkus build-time half |
| `protobuf/` | `protobuf-metadata`, `protobuf-quality`, `protobuf-validation`, `protobuf-validation-protovalidate`, `protobuf-validation-conformance`, `protobuf-indexing` | Descriptor-option standards for metadata, validation, and indexing; CEL-scored quality dimensions declared as message options; protovalidate dialect and conformance harness |
| `transform/mapper/` | `mapper-core`, `mapper-cel`, `metadata` | Text mapping rules, CEL filters and selectors, CEL-driven metadata extraction |
| `transform/shapes/` | `shapes` | Joins, unions, and derived shapes: multi-source mapping scopes, runtime message-type synthesis (envelope, projection, tagged union), schema merging with clash resolution, and struct-to-proto inference |
| `transform/projection/` | `projection` | Self-describing message-to-message projections: per-field provenance (candidate paths, CEL, literals) carried as descriptor options on the target message, so one target can join differently-shaped sources |
| `transform/pipeline/` | `pipeline` | The pipeline schema: chained gRPC calls, projections, and CEL steps as one protobuf message |
| `transform/chain/` | `chain` | The chain manager: configured, type-checked compositions of gRPC calls (verify statically, run serially with gates and deadlines, store named chains in the registry) — plus keyed/zip joins over two live gRPC streams |
| `search/index/` | `index-spi`, `index-ndjson`, `index-lucene`, `index-opensearch`, `index-solr` | Indexing plans and hints; NDJSON output; engine plugins |
| `search/embeddings/` | `embeddings` | Embedding-provider SPI and the plan-driven embedder that fills a document's VECTOR field from its TEXT field |
| `search/embeddings/providers/` | `embeddings-model2vec` | A Model2Vec static-embedding provider backed by OpenNLP |
| `search/embeddings/providers/` | `embeddings-tei` | Remote provider for Hugging Face Text Embeddings Inference over gRPC |
| `search/embeddings/providers/` | `embeddings-ovms` | Remote provider for OpenVINO Model Server over the KServe v2 gRPC protocol |
| `search/embeddings/` | `embeddings-harness` | Pairwise cosine-equivalence certification for two providers serving the same model |
| `search/rerank/` | `rerank` | Rerank-provider SPI: score a query's candidate texts so pipelines can re-order search hits |
| `search/rerank/providers/` | `rerank-tei` | Remote rerank provider for Hugging Face Text Embeddings Inference over gRPC |
| `search/rerank/providers/` | `rerank-ovms` | Remote rerank provider for OpenVINO Model Server over the REST rerank endpoint |
| `search/rerank/` | `rerank-harness` | Ranked-list equivalence certification (Kendall tau-b plus top-1 agreement) for two providers serving the same model |
| `sink/emit/` | `emit`, `emit-okf`, `emit-parquet` | Bundles of rendered files and the sinks that deliver them (directory, git, zip); the OKF v0.1 knowledge-bundle renderer; descriptor-driven Parquet with no generated classes and no native Hadoop |
| `sink/` | `iceberg`, `iceberg-s3` | Apache Iceberg: descriptor-driven table schemas and an append sink writing ProtoMolt Parquet through any catalog; `S3FileIO` wiring so tables live on any S3-compatible store |
| `sink/kafka/` | `connect`, `connect-iceberg`, `serde`, `serde-micrometer` | Kafka Connect plugin: the sink drives any gRPC method from topics, the source feeds topics from server streams with CEL resume-token offsets, and four protobuf-aware transforms (validate, map, redact, CEL filter) drop into any pipeline; a separate sink lands records as Iceberg snapshots; a protobuf serde speaking the Confluent wire format, enforcing declared rules on write, with a Micrometer metrics binding |
| `surface/grpc/` | `grpc-invoke`, `grpc-service`, `grpc-validation`, `grpc-validation-micrometer` | Dynamic gRPC invocation and server reflection from descriptors; the action catalog itself as a typed gRPC service, served descriptor-natively; validating interceptors that enforce declared rules at the call boundary, with a Micrometer binding for their metrics SPI |
| `surface/http/` | `json`, `rest`, `openapi`, `jsonschema` | Protobuf/JSON transcoding, framework-agnostic REST gateway, OpenAPI 3 and JSON Schema generation |
| `surface/mcp/` | `mcp` | Model Context Protocol server over the action catalog and registry: plain-Java stdio, no framework |
| `surface/acp/` | `acp` | The action catalog as an Agent Client Protocol agent: run verbs from ACP-capable IDEs (JetBrains AI chat, Zed) over stdio |
| `surface/actions/` | `actions` | Self-describing verb catalog (compile, validate, diff, check-compat, render, evaluate) for consoles and LLM tooling |
| `host/servers/` | `server-common`, `server-jdk`, `server-vertx`, `server-netty`, `server-spring`, `server-micronaut`, `server-quarkus` | HTTP hosts for the REST gateway, over shared config and helpers |
| `host/integrations/` | `spring`, `quarkus`, `quarkus-deployment` | Dependency-injection wiring (beans and producers, not HTTP hosts); the Quarkus extension's build-time half |
| `apps/serve/` | `serve` | One-process server: the gRPC service with reflection, the verbs over JSON/REST with OpenAPI and Swagger UI, MCP on streamable HTTP, optional registry |
| `apps/cli/` | `cli` | The command line: run any catalog verb from the terminal (JSON in, JSON out), list the verbs, or open an interactive console over the same catalog |
| `bom/` | `bom` | Version alignment for all published artifacts |

## Getting started

Run the whole server — gRPC with reflection, JSON/REST with Swagger UI, MCP,
and a registry — in one container, with a sample schema seeded:

```shell
docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
```

Then exercise it (or open the console at http://localhost:8080/console —
Swagger UI lives at http://localhost:8080/docs):

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

From a clone, `docker compose up` builds and runs the same server, and
`docker compose run --rm acp` is the ACP agent an IDE drives over stdio;
`./scripts/docker-smoke.sh` brings the stack up and proves both the MCP and
ACP surfaces answer. See [Running in Docker](docs/docker.md).

Prefer a process over a container? Every release attaches runnable
`protomolt-serve` and `protomolt-mcp` zips (JRE 21+ is the only
prerequisite), or build from a clone:

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew :protomolt-serve:installDist
serve/build/install/protomolt-serve/bin/protomolt-serve --demo
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

Each of these is covered in depth in the documentation below.

## Documentation

- [Descriptor sources](docs/descriptor-sources.md) — the loader SPI;
  classpath, descriptor-set, Apicurio, and Confluent sources; schema hygiene
- [Gathering proto sources](docs/gathering.md) — filesystem, jar, Git, and
  Maven gatherers; the descriptor-loader adapter
- [Publishing schemas](docs/publishing.md) — registering gathered sources
  with Apicurio and Confluent-compatible registries
- [Compatibility checking](docs/compatibility.md) — breaking-change
  detection with backward/forward/full policies
- [The registry](docs/registry.md) — git-backed schema storage behind the
  Confluent protocol, with compatibility-gated writes
- [Actions](docs/actions.md) — the verb catalog for consoles and LLM
  tooling
- [MCP server](docs/mcp.md) — the catalog and registry over the Model
  Context Protocol for AI agents; reflect, invoke, and generate against any
  gRPC service
- [ACP agent](docs/acp.md) — the catalog as an Agent Client Protocol agent:
  run verbs from ACP-capable IDEs (JetBrains AI chat, Zed) over stdio
- [Stream connectors](docs/connector.md) — the `StreamSource` SPI: gRPC
  server streams and Kafka topics with pause/resume flow control, bridged to
  synchronous consumers by the bounded `SourcePump`
- [Streaming demo](docs/demo-streaming.md) — a server-streaming gRPC call
  rendered live through the ACP agent, in the terminal or an IDE
- [The gRPC service](docs/grpc-service.md) — the catalog as
  `ProtoMoltService`: typed RPCs with reflection, the same verbs over
  JSON/REST with OpenAPI and Swagger UI, one launcher for all of it
- [Operating an OpenVINO server](docs/tutorials/openvino.md) — a full
  gRPC-agent walkthrough: reflect, register the KServe schema, introspect
  models, run a text → embedding inference
- [Python clients without protoc](docs/tutorials/python.md) — reflect a
  server, generate the `_pb2.py` modules, call it with plain grpcio
- [Kafka Connect](docs/kafka-connect.md) — the sink (topics drive gRPC
  methods), the source (server streams feed topics, resumable via CEL
  tokens), and the four protobuf-aware transforms
- [Kafka serde](docs/kafka-serde.md) — a protobuf serializer and
  deserializer speaking the Confluent wire format, enforcing the schema's
  declared rules on write
- [Joins and derived shapes](docs/design/join-shapes.md) — multi-source
  mapping scopes, envelope/projection/oneof output shapes, schema merging
  with clash resolution, derived schemas as registry subjects
- [Field mapping](docs/mapping.md) — text rule syntax, CEL filters and
  selectors
- [Projections](docs/projection.md) — self-describing message-to-message
  mappings as descriptor options on the target message
- [Validation](docs/validation.md) — the rule surface, dialect SPI,
  protovalidate interoperability, conformance
- [Quality scoring](docs/quality.md) — CEL-scored quality dimensions
  declared as message options; weighted composites, measured and optionally
  gated in the Kafka serde
- [Schema metadata](docs/metadata.md) — descriptor-option metadata and
  runtime extraction
- [JSON Schema generation](docs/json-schema.md) — draft 2020-12 schemas from
  descriptors and validation rules
- [Search indexing](docs/indexing.md) — indexing hints, plans, NDJSON, and
  the Lucene/OpenSearch/Solr plugins
- [Text embeddings](docs/embeddings.md) — the `EmbeddingProvider` SPI,
  plan-driven document embedding, and the Model2Vec provider
- [Reranking](docs/rerank.md) — the `RerankProvider` SPI, the TEI and OVMS
  providers, and ranked-list equivalence certification
- [Emitting bundles](docs/emitting.md) — the bundle/sink SPI (directory,
  git, zip), OKF knowledge bundles, and descriptor-driven Parquet
- [Microsoft Graph](docs/msgraph.md) — OneDrive/SharePoint files and
  metadata, and agentless Copilot connector ingestion
- [Apache Iceberg](docs/iceberg.md) — descriptor-driven table schemas and
  snapshot appends through any Iceberg catalog
- [REST gateway and servers](docs/rest-gateway.md) — JSON transcoding, the
  gateway, OpenAPI generation, and the six server hosts
- [Framework integrations](docs/framework-integrations.md) — Spring Boot
  auto-configuration and the Quarkus extension
- [Core utilities](docs/helpers.md) — `Any`/`Struct` helpers, type
  conversion, message diff
- [Building and testing](docs/building.md) — build, integration tests,
  linting, publishing
- [Chain manager design](docs/design/chain-manager.md) — typed, registered
  compositions of gRPC calls: one endpoint in, one composed answer out; a
  sidecar to existing pipelines, deliberately not a pipeline
- [Pipeline design](docs/design/pipeline.md) — a pipeline as a protobuf
  message chaining steps: any gRPC service, alongside projections, CEL
  filters and selects, unnest, and collect
- [Roadmap](docs/roadmap.md) — toward a schema registry over Git and Maven

## Requirements

- JDK 21 or newer at runtime (the build itself runs on any JDK via Gradle
  toolchains)
- Gradle 9.6+ for building from source (wrapper included)

## Building

`./gradlew build` compiles everything and runs the full test suite. Versions
are derived from `v*` tags (Axion); an untagged checkout builds as a
snapshot. Integration tests against real schema registries are opt-in and
skip automatically when no registry is reachable; see
[Building and testing](docs/building.md).

## Runtime disk footprint

ProtoMolt never writes message data to disk. The only runtime writes are
declared schema storage, at locations the operator chooses: the registry's
Git repository (`--registry-git`, or a temporary directory in `--demo`
mode) and `gather-git`'s persistent clone cache (`--gather-cache` /
`PROTOMOLT_GATHER_CACHE`, defaulting to `~/.cache/protomolt/gather/git`).
Cache placement is server configuration, never request input. Everything
else — compilation of `.proto` text to descriptors included — runs
entirely in memory.

## Naming

`ProtoMolt` is the project and artifact name; the code namespace is
`ai.pipestream`. In practice: dependencies are `ai.pipestream:protomolt-*`,
imports are `ai.pipestream.proto.*`, configuration properties use the
`pipestream.*` prefix, and the descriptor-option extensions live under
`ai.pipestream.proto.{meta,validate,index.hints}.v1`.

## License

[Apache License 2.0](LICENSE) © 2026 ai.pipestream
