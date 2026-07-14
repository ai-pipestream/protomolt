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

Modules are grouped by domain. Maven artifact IDs carry the `protomolt-`
prefix; Java packages use the `ai.pipestream.proto.*` namespace (see
[Naming](#naming)).

| Group | Artifacts | Purpose |
|---|---|---|
| `core/` | `descriptors`, `helpers`, `proto-sources` | `DescriptorRegistry` and loader SPI; `Any`/`Struct` handling, type conversion, message diff, schema hygiene checks; proto source-set model, runtime `.proto` compilation, publisher SPI |
| `formats/` | `formats` | Zero-dependency RFC validators (email, hostname, IP, URI) backing the validation string formats |
| `gather/` | `gather`, `gather-git`, `gather-maven` | Acquire `.proto` sources from directories, jars, Git repositories, and Maven coordinates; adapt any gatherer to a descriptor loader |
| `compat/` | `compat` | Breaking-change detection: typed schema diffs and backward/forward/full compatibility policies |
| `registry/` | `registry`, `registry-server` | Git-backed schema registry: subject/version store with compatibility-gated writes, served over the Confluent protocol |
| `actions/` | `actions` | Self-describing verb catalog (compile, validate, diff, check-compat, render, evaluate) for consoles and LLM tooling |
| `mcp/` | `mcp` | Model Context Protocol server over the action catalog and registry: plain-Java stdio, no framework |
| `grpc/` | `grpc-invoke`, `grpc-service` | Dynamic gRPC invocation and server reflection from descriptors; the action catalog itself as a typed gRPC service, served descriptor-natively |
| `serve/` | `serve` | One-process server: the gRPC service with reflection, the verbs over JSON/REST with OpenAPI and Swagger UI, optional registry |
| `codegen/` | `codegen` | Live code generation: every libprotoc generator (8 languages) and the grpc-java plugin as WebAssembly, no native toolchain |
| `mapper/` | `mapper-core`, `mapper-cel`, `metadata` | Text mapping rules, CEL filters and selectors, CEL-driven metadata extraction |
| `protobuf/` | `protobuf-metadata`, `protobuf-validation`, `protobuf-validation-protovalidate`, `protobuf-validation-conformance`, `protobuf-indexing` | Descriptor-option standards for metadata, validation, and indexing; protovalidate dialect and conformance harness |
| `schema/` | `schema-apicurio`, `schema-confluent` | Descriptor loaders and schema publishers for Apicurio Registry and Confluent-compatible schema registries |
| `index/` | `index-spi`, `index-ndjson`, `index-lucene`, `index-opensearch`, `index-solr` | Indexing plans and hints; NDJSON output; engine plugins |
| `http/` | `json`, `rest`, `openapi`, `jsonschema` | Protobuf/JSON transcoding, framework-agnostic REST gateway, OpenAPI 3 and JSON Schema generation |
| `integrations/` | `spring`, `quarkus` | Dependency-injection wiring (beans and producers, not HTTP hosts) |
| `servers/` | `server-jdk`, `server-vertx`, `server-netty`, `server-spring`, `server-micronaut`, `server-quarkus` | HTTP hosts for the REST gateway |
| `bom/` | `bom` | Version alignment for all published artifacts |

## Getting started

Clone the repository, build it, and publish the artifacts to your local Maven
repository:

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew build
./gradlew publishToMavenLocal
```

The build runs the full test suite, including a protovalidate conformance
gate. To see the toolkit working immediately, start the sample JSON/REST
server and call it:

```shell
./gradlew :samples:runJsonRestServer
curl -H 'api_token: secret' -H 'content-type: application/json' \
  -d '{"name":"Ada"}' http://127.0.0.1:8080/grpc-json/Echo/echo
```

Then depend on the artifacts from `mavenLocal()`:

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
- [The gRPC service](docs/grpc-service.md) — the catalog as
  `ProtoMoltService`: typed RPCs with reflection, the same verbs over
  JSON/REST with OpenAPI and Swagger UI, one launcher for all of it
- [Operating an OpenVINO server](docs/tutorials/openvino.md) — a full
  gRPC-agent walkthrough: reflect, register the KServe schema, introspect
  models, run a text → embedding inference
- [Field mapping](docs/mapping.md) — text rule syntax, CEL filters and
  selectors
- [Validation](docs/validation.md) — the rule surface, dialect SPI,
  protovalidate interoperability, conformance
- [Schema metadata](docs/metadata.md) — descriptor-option metadata and
  runtime extraction
- [JSON Schema generation](docs/json-schema.md) — draft 2020-12 schemas from
  descriptors and validation rules
- [Search indexing](docs/indexing.md) — indexing hints, plans, NDJSON, and
  the Lucene/OpenSearch/Solr plugins
- [REST gateway and servers](docs/rest-gateway.md) — JSON transcoding, the
  gateway, OpenAPI generation, and the six server hosts
- [Framework integrations](docs/framework-integrations.md) — Spring Boot
  auto-configuration and the Quarkus extension
- [Core utilities](docs/helpers.md) — `Any`/`Struct` helpers, type
  conversion, message diff
- [Building and testing](docs/building.md) — build, integration tests,
  linting, publishing
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

## Naming

`ProtoMolt` is the project and artifact name; the code namespace is
`ai.pipestream`. In practice: dependencies are `ai.pipestream:protomolt-*`,
imports are `ai.pipestream.proto.*`, configuration properties use the
`pipestream.*` prefix, and the descriptor-option extensions live under
`ai.pipestream.proto.{meta,validate,index.hints}.v1`.

## License

[MIT](LICENSE) © 2026 ai.pipestream
