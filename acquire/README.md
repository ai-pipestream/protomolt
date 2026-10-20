# acquire/

How protos and documents get in. Three unrelated families share this
directory: `gather/*` acquires schema (`.proto` sources), `connector`
acquires live message streams, and `pull`, `s3`, `jdbc`, `msgraph` and
`confluence` acquire content. There is no crawl engine and no scheduler:
every content connector is a single bounded pass triggered by a call, and
each returns its resumption position to the caller.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-acquire-gather` | `acquire/gather/core` | Proto gatherer SPI: filesystem and jar sources, staging, the descriptor-loader adapter |
| `:protomolt-acquire-gather-git` | `acquire/gather/git` | Git proto gatherer: clone or fetch repositories and stage proto trees (JGit) |
| `:protomolt-acquire-gather-maven` | `acquire/gather/maven` | Maven proto gatherer: resolve artifacts by coordinate and stage protos from their jars |
| `:protomolt-acquire-connector` | `acquire/connector` | Push-style streaming inputs behind one bounded, pausable SPI: gRPC server streams and Kafka topics |
| `:protomolt-acquire-pull` | `acquire/pull` | Shared pull-connector core: the intake feed seam, the stable-identity document wrap, the pull report |
| `:protomolt-acquire-s3` | `acquire/s3` | S3 pull connector: changed objects past a watermark, fed through the intake service |
| `:protomolt-acquire-jdbc` | `acquire/jdbc` | JDBC pull connector: a watermark query, each row wrapped as a stable-identity document |
| `:protomolt-acquire-msgraph` | `acquire/msgraph` | Microsoft Graph: OAuth2, OneDrive and SharePoint files, Copilot connector ingestion |
| `:protomolt-acquire-confluence` | `acquire/confluence` | Confluence Cloud domain model, crawler, `ChangeSink` SPI and gRPC facade (has its own README) |

Docs: [stream connectors](../docs/acquire/connector.md),
[pull connectors](../docs/acquire/pull-connectors.md),
[gathering proto sources](../docs/acquire/gathering.md),
[Microsoft Graph](../docs/acquire/msgraph.md).
