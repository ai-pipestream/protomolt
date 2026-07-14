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
| [Field mapping](mapping.md) | Text rule syntax; CEL filters, selectors, and environments |
| [Validation](validation.md) | The rule surface; dialect SPI; protovalidate interoperability and conformance |
| [Schema metadata](metadata.md) | Declared descriptor-option metadata; CEL-based runtime extraction |
| [JSON Schema generation](json-schema.md) | Draft 2020-12 schemas from descriptors and validation rules |
| [Search indexing](indexing.md) | Indexing hints, plans, NDJSON output, and the Lucene/OpenSearch/Solr plugins |
| [REST gateway and servers](rest-gateway.md) | JSON transcoding, the gateway, OpenAPI, and the six server hosts |
| [Framework integrations](framework-integrations.md) | Spring Boot auto-configuration and the Quarkus extension |
| [Core utilities](helpers.md) | `Any`/`Struct` handling, type conversion, message diff, schema hygiene |
| [Building and testing](building.md) | Building from a clone, integration tests, linting, publishing |

## Project direction

- [Roadmap](roadmap.md) — toward a schema registry over Git and Maven

## Records

- [reviews/](reviews/) — dated correctness and security review records
