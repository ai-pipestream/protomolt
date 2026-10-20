# schema/

Registry storage and third-party registry clients: the subject and version
store with its git-backed implementation and HTTP server, plus loaders and
publishers for Confluent-protocol and Apicurio registries.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-registry-proto` | `schema/registry/proto` | The descriptor exchange contract: content-addressed descriptor-set transfer between services |
| `:protomolt-registry` | `schema/registry/core` | Schema registry contracts, validation and in-memory storage |
| `:protomolt-registry-service` | `schema/registry/service` | The registry HTTP server: Confluent subjects protocol, descriptor-set serving, and the `{prefix}/actions` route |
| `:protomolt-schema-registry-git` | `schema/registry/git` | Git schema registry storage, federation and workflow version storage |
| `:protomolt-schema-registry-git-service` | `schema/registry/git/service` | Git registry service assembly and federation actions |
| `:protomolt-schema-registry-composer` | `schema/registry/composer` | Provider-backed registry HTTP role for the service composer |
| `:protomolt-schema-confluent` | `schema/confluent` | Confluent schema registry client: loader, descriptor-set source, publisher |
| `:protomolt-schema-apicurio` | `schema/apicurio/runtime` | Apicurio Registry v3 loader and publisher, shipped as a Quarkus extension runtime |
| `:protomolt-schema-apicurio-deployment` | `schema/apicurio/deployment` | The Quarkus build-time half of that extension |

Docs: [registry](../docs/schema/registry.md),
[publishing schemas](../docs/schema/publishing.md),
[compatibility checking](../docs/schema/compatibility.md),
[schema metadata](../docs/schema/metadata.md),
[JSON Schema generation](../docs/schema/json-schema.md).
