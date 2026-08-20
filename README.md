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
| `core/` | `descriptors`, `helpers`, `sources` | `DescriptorRegistry` and loader SPI; `Any`/`Struct` handling, type conversion, message diff, schema hygiene checks; proto source-set model, runtime `.proto` compilation, publisher SPI |
| `core/formats/` | `formats` | Zero-dependency RFC validators (email, hostname, IP, URI) backing the validation string formats |
| `core/codegen/` | `codegen` | Live code generation: every libprotoc generator (8 languages) and the grpc-java plugin as WebAssembly, no native toolchain |
| `core/compat/` | `compat` | Breaking-change detection: typed schema diffs and backward/forward/full compatibility policies |
| `core/acp/` | `acp` | Agent Client Protocol core: newline-delimited JSON-RPC 2.0 on virtual threads, a blocking client, and an agent runtime, with no reactive runtime |
| `acquire/gather/` | `acquire-gather`, `acquire-gather-git`, `acquire-gather-maven` | Acquire `.proto` sources from directories, jars, Git repositories, and Maven coordinates; adapt any gatherer to a descriptor loader |
| `acquire/msgraph/` | `acquire-msgraph` | Microsoft Graph: OneDrive/SharePoint files and list-item metadata, and Copilot connector ingestion over the external connections API |
| `acquire/connector/` | `acquire-connector` | Push-style streaming inputs behind one bounded, pausable SPI: gRPC server streams and Kafka topics feed a synchronous pipeline through the `SourcePump` bridge |
| `schema/registry/` | `registry`, `registry-server` | Git-backed schema registry: subject/version store with compatibility-gated writes, served over the Confluent protocol |
| `schema/` | `schema-apicurio`, `schema-apicurio-deployment`, `schema-confluent` | Descriptor loaders and schema publishers for Apicurio Registry and Confluent-compatible schema registries; the Apicurio loader's Quarkus build-time half |
| `protobuf/` | `protobuf-metadata`, `protobuf-quality`, `protobuf-validation`, `protobuf-validation-protovalidate`, `protobuf-validation-conformance`, `protobuf-indexing` | Descriptor-option standards for metadata, validation, and indexing; CEL-scored quality dimensions declared as message options; protovalidate dialect and conformance harness |
| `mesh/cluster/` | `mesh-cluster` | In-memory cluster discovery with fenced presence and capacity, encrypted repository-service event persistence, and restart replay |
| `transform/mapper/` | `mapper-core`, `mapper-cel`, `metadata` | Text mapping rules, CEL filters and selectors, CEL-driven metadata extraction |
| `transform/shapes/` | `shapes` | Joins, unions, and derived shapes: multi-source mapping scopes, runtime message-type synthesis (envelope, projection, tagged union), schema merging with clash resolution, and struct-to-proto inference |
| `transform/projection/` | `projection` | Self-describing message-to-message projections: per-field provenance (candidate paths, CEL, literals) carried as descriptor options on the target message, so one target can join differently-shaped sources |
| `transform/pipeline/` | `pipeline` | Checked pipeline execution across every gRPC streaming shape, with typed edges, structured generation, unnest, collect, and bounded fan-out |
| `transform/workflow/` | `workflow` | Checked serial gRPC compositions with gates, deadlines, named registry storage, and keyed or zip joins over two live streams |
| `transform/delegation/` | `delegation` | Coordinator and worker bidirectional contract, transcript reduction, encrypted repository-service persistence, and restart restoration |
| `jobs/` | `jobs-proto`, `jobs-service` | Durable workflow runs with step checkpoints, external completion, Kafka request and event topics, typed failures, and retries |
| `search/index/` | `index-spi`, `index-ndjson`, `index-lucene`, `index-opensearch`, `index-solr`, `index-qdrant` | Index mappings and hints; NDJSON output; engine plugins |
| `search/embeddings/` | `embeddings` | Embedding-provider SPI and the mapping-driven embedder that fills a document's VECTOR field from its TEXT field |
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
| `surface/grpc/` | `grpc-channel-policy`, `grpc-invoke`, `grpc-service-profile`, `grpc-service-workspace`, `grpc-workflow`, `grpc-service`, `grpc-validation`, `grpc-validation-micrometer` | Validated, host-configurable outbound gRPC channel policy; dynamic gRPC invocation and server reflection; durable service profiles and content-addressed descriptor artifacts; protobuf workflow and run-evidence contracts; actions for registering, listing, inspecting, and refreshing services; the catalog as a typed gRPC service; validating interceptors with a Micrometer binding |
| `surface/http/` | `json`, `rest`, `openapi`, `jsonschema` | Protobuf/JSON transcoding, framework-agnostic REST gateway, OpenAPI 3 and JSON Schema generation |
| `surface/mcp/` | `mcp` | Model Context Protocol server over the action catalog and registry: plain-Java stdio, no framework |
| `surface/acp/` | `acp-agent` | The action catalog as an Agent Client Protocol agent: run verbs from ACP-capable IDEs (JetBrains AI chat, Zed) over stdio, on the `acp` transport |
| `surface/actions/` | `actions` | Self-describing verb catalog (compile, validate, diff, check-compat, render, evaluate) for consoles and LLM tooling |
| `host/server/` | `server-common`, `server-jdk`, `server-vertx`, `server-netty`, `server-spring`, `server-micronaut`, `server-quarkus` | HTTP hosts for the REST gateway, over shared config and helpers |
| `host/integration/` | `integration-spring`, `integration-quarkus`, `integration-quarkus-deployment` | Dependency-injection wiring (beans and producers, not HTTP hosts); the Quarkus extension's build-time half |
| `apps/serve/` | `serve` | One-process server: the gRPC service with reflection, the verbs over JSON/REST with OpenAPI and Swagger UI, MCP on streamable HTTP, optional registry |
| `apps/cli/` | `cli` | The command line: run any catalog verb from the terminal (JSON in, JSON out), list the verbs, or open an interactive console over the same catalog |
| `apps/agent-host/` | `agent-host` | Persistent Codex and Kimi processes attached to delegation over MCP, with structured command gates, cursor recovery, and provider session resume |
| `bom/` | `bom` | Version alignment for all published artifacts |

## Getting started

Run gRPC with reflection, JSON/REST with Swagger UI, MCP, a registry, and a
sample schema in one container:

```shell
docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
```

Then exercise it. The console is at http://localhost:8080/console and Swagger
UI is at http://localhost:8080/docs.

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
ACP surfaces answer. See [Running in Docker](docs/apps/docker.md).

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

## Native CLI (GraalVM)

`protomolt-cli` also builds as a GraalVM native image: sub-10 ms startup and
~35 MB RSS for catalog verbs, no JRE needed on the target machine. Building
the native binary is the only task that requires GraalVM (a GraalVM JDK 25,
e.g. `sdk install java 25.1.3-graalce`); every other build and test task runs
on any JDK via Gradle toolchains, and GraalVM CE can serve as the everyday
build JDK too.

```shell
JAVA_HOME=<graalvm-home> ./gradlew :protomolt-cli:nativeCompile
apps/cli/build/native/nativeCompile/protomolt-cli list
```

Releases also publish it as a multi-arch container (linux/amd64 +
linux/arm64; native-image does not cross-compile, so each architecture builds
on a matching runner and a manifest joins them):

```shell
docker run --rm ghcr.io/ai-pipestream/protomolt-cli list
```

All `generate-stubs` generators (java, kotlin, grpc-java, python, cpp, csharp,
ruby, php, objc) work natively. The generators are one bundled WebAssembly
module; inside a native image it runs on Chicory's interpreter rather than
being compiled to JVM bytecode at first use: same module, same generators,
slower per-invocation execution. (On the JVM nothing changes.)

Only the CLI is a native-image target today. `apps/serve` is deliberately
JVM-only for now: it is a runnable *example* of embedding the toolkit (gRPC
service, REST gateway, MCP, registry) into a server build, not a supported
native artifact. Native servers are planned through the framework
integrations instead: the Spring, Quarkus, and Micronaut modules are the
supported embedding paths, and following their native-image tooling (Spring
AOT, Quarkus native, Micronaut AOT) is the intended route to a native server
in the future.

## Documentation

The [documentation index](docs/README.md) follows the repository's module
layout. Start with these workflows:

- [Connect an agent over MCP](docs/surface/mcp.md)
- [Register and inspect a gRPC service](docs/surface/service-workspace.md)
- [Build, replay, and promote a workflow](docs/transform/workflows.md)
- [Run a checked streaming pipeline](docs/transform/pipeline.md)
- [Generate a Python client without protoc](docs/tutorials/python.md)
- [Build and test ProtoMolt](docs/operations/building.md)

## Requirements

- JDK 21 or newer at runtime (the build itself runs on any JDK via Gradle
  toolchains; GraalVM CE 25 works as the build JDK)
- Gradle 9.6+ for building from source (wrapper included)
- GraalVM JDK 25 (e.g. CE 25.1.3) only when building the native CLI with
  `:protomolt-cli:nativeCompile`: the resulting binary needs no JDK at all

## Building

`./gradlew build` compiles everything and runs the full test suite. Versions
are derived from `v*` tags (Axion); an untagged checkout builds as a
snapshot. Integration tests against real schema registries are opt-in and
skip automatically when no registry is reachable; see
[Building and testing](docs/operations/building.md).

## Runtime disk footprint

ProtoMolt never writes message data to disk. The only runtime writes are
declared schema storage, at locations the operator chooses: the registry's
Git repository (`--registry-git`, or a temporary directory in `--demo`
mode) and `gather-git`'s persistent clone cache (`--gather-cache` /
`PROTOMOLT_GATHER_CACHE`, defaulting to `~/.cache/protomolt/gather/git`).
Cache placement is server configuration, never request input. Everything
else, including compilation of `.proto` text to descriptors, runs
entirely in memory.

## Naming

`ProtoMolt` is the project and artifact name; the code namespace is
`ai.pipestream`. In practice: dependencies are `ai.pipestream:protomolt-*`,
imports are `ai.pipestream.proto.*`, configuration properties use the
`protomolt.*` prefix (framework-integration glue exposes a handful of
`pipestream.*` properties), and the descriptor-option dialects live under
`ai.pipestream.proto.{meta,validate,llm,quality,metric,index.hints,projection,mesh}.v1`,
beside the vendored `buf.validate` compatibility dialect. Module and
package derivation is ADR-002 in [AGENTS.md](AGENTS.md).

## License

[Apache License 2.0](LICENSE) © 2026 ai.pipestream
