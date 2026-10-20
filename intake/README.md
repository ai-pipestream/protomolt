# intake/

The only authenticated way into the document platform: ingest lanes (unary,
client-streaming and one HTTP upload route) in front of repo-service. An API
key resolves to an `IntakeScope` (account, datasources, drives, mime types,
payload cap); a request may narrow within its scope and never widen it.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-intake-proto` | `intake/proto` | The intake contract: `IngestDocument` unary and `IngestStream` client-streaming, plus the HTTP upload lane |
| `:protomolt-intake-service` | `intake/service` | The intake service: API-key scope resolution behind a key-store SPI (in-memory, JDBC, OIDC introspection), embeddable in-JVM or standalone over Netty |

There is no `docs/intake` chapter; the architecture is in
[intake and parsing](../docs/design/intake-and-parsing.md), and the pull
connectors that feed intake are documented in
[pull connectors](../docs/acquire/pull-connectors.md).
