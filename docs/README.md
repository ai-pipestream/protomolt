# ProtoMolt documentation

Start with the [project README](../README.md) for an overview and a
build-from-clone quick start. The guides here cover each subsystem in depth.

## Guides

| Guide | Covers |
|---|---|
| [Descriptor sources](descriptor-sources.md) | The `DescriptorLoader` SPI; classpath, descriptor-set, Apicurio, and Confluent sources; schema hygiene checks |
| [Gathering proto sources](gathering.md) | The `ProtoGatherer` SPI; filesystem, jar, Git, and Maven gatherers; the descriptor-loader adapter |
| [Publishing schemas](publishing.md) | The `SchemaPublisher` SPI; Apicurio and Confluent publishers; naming, idempotency, dry runs |
| [Compatibility checking](compatibility.md) | Typed schema diffs; backward/forward/full/transitive policy evaluation; wire, JSON, and source rule layers |
| [The registry](registry.md) | Git-backed schema storage; the Confluent-protocol server; compatibility-gated writes; descriptor-set serving |
| [Actions](actions.md) | The verb catalog — compile, validate, diff, check, render, evaluate — self-describing for consoles and LLM tools |
| [MCP server](mcp.md) | The catalog as MCP tools and the registry as MCP resources; the gRPC agent workflow (reflect, invoke, generate); plain-Java stdio transport |
| [The gRPC service](grpc-service.md) | The catalog as `ProtoMoltService` — typed RPCs served descriptor-natively with reflection; JSON/REST with OpenAPI and Swagger UI; the `protomolt-serve` launcher |
| [Kafka Connect](kafka-connect.md) | The sink (topics drive gRPC methods), the source (server streams feed topics, resumable via CEL tokens), and protobuf-aware transforms (validate, map, CEL filter) |
| [Kafka serde](kafka-serde.md) | A protobuf serializer and deserializer speaking the Confluent wire format, enforcing the schema's declared rules on write, with the packaged descriptor set as a floor under the registry |
| [Joins and derived shapes](design/join-shapes.md) | Multi-source mapping scopes; envelope/projection/oneof output shapes; schema merging with clash resolution; derived schemas as registry subjects; the shape verbs |
| [Field mapping](mapping.md) | Text rule syntax; CEL filters, selectors, and environments |
| [Validation](validation.md) | The rule surface; dialect SPI; protovalidate interoperability and conformance; validating gRPC interceptors |
| [Quality scoring](quality.md) | CEL-scored quality dimensions declared as message options; weighted composites, measured (and optionally gated) in the Kafka serde |
| [Schema metadata](metadata.md) | Declared descriptor-option metadata; CEL-based runtime extraction |
| [JSON Schema generation](json-schema.md) | Draft 2020-12 schemas from descriptors and validation rules |
| [Search indexing](indexing.md) | Indexing hints, plans, NDJSON output, and the Lucene/OpenSearch/Solr plugins |
| [Emitting bundles](emitting.md) | The bundle/sink SPI (directory, git, zip), OKF knowledge bundles, and descriptor-driven Parquet |
| [Microsoft Graph](msgraph.md) | OneDrive/SharePoint files and metadata, and agentless Copilot connector ingestion |
| [Apache Iceberg](iceberg.md) | Descriptor-driven table schemas and snapshot appends through any Iceberg catalog |
| [REST gateway and servers](rest-gateway.md) | JSON transcoding, the gateway, OpenAPI, and the six server hosts |
| [Framework integrations](framework-integrations.md) | Spring Boot auto-configuration and the Quarkus extension |
| [Core utilities](helpers.md) | `Any`/`Struct` handling, type conversion, message diff, schema hygiene |
| [Building and testing](building.md) | Building from a clone, integration tests, linting, publishing |

## Tutorials

- [Operating an OpenVINO server](tutorials/openvino.md) — reflect, fall back to
  the KServe schema, introspect models, and run a text → embedding inference,
  all from an AI agent through the MCP server
- [Python clients without protoc](tutorials/python.md) — reflect a server,
  have ProtoMolt generate the `_pb2.py` modules, and call it with plain
  grpcio; no protoc, no grpcio-tools

## Project direction

- [Roadmap](roadmap.md) — toward a schema registry over Git and Maven
- [Chain manager design](design/chain-manager.md) — typed, registered
  compositions of gRPC calls: one endpoint in, one composed answer out; a
  sidecar to existing pipelines, deliberately not a pipeline

## Records

- [reviews/](reviews/) — dated correctness and security review records
