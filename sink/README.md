# sink/

Where rendered output goes: emitted file bundles, Parquet and Apache
Iceberg, and the Kafka family (wire format, serde, Connect sinks, sources
and transforms).

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-emit` | `sink/emit/core` | Emit SPI, rendered bundles, directory delivery and zip support |
| `:protomolt-emit-git` | `sink/emit/git` | Git delivery for emitted bundles (JGit) |
| `:protomolt-emit-okf` | `sink/emit/okf` | Open Knowledge Format renderer and the `emit-okf` verb |
| `:protomolt-emit-parquet` | `sink/emit/parquet` | Descriptor-driven Parquet, no generated classes and no Hadoop |
| `:protomolt-emit-parquet-s3` | `sink/emit/parquet-s3` | Parquet upload to any S3-compatible store through the AWS SDK |
| `:protomolt-iceberg` | `sink/iceberg` | Descriptor-driven Iceberg table schemas and an append sink through any catalog |
| `:protomolt-iceberg-s3` | `sink/iceberg-s3` | Wires Iceberg `S3FileIO` so tables live on any S3-compatible store |
| `:protomolt-kafka-wire` | `sink/kafka/wire` | The Confluent protobuf wire format: a framework-neutral frame reader and writer |
| `:protomolt-kafka-serde` | `sink/kafka/serde` | Kafka protobuf serde speaking the Confluent wire format, declared rules enforced on write |
| `:protomolt-kafka-serde-micrometer` | `sink/kafka/serde-micrometer` | Micrometer binding for the serde metrics SPI |
| `:protomolt-kafka-connect` | `sink/kafka/connect` | Kafka Connect gRPC sink, gRPC-stream source and protobuf-aware transforms |
| `:protomolt-kafka-connect-iceberg` | `sink/kafka/connect-iceberg` | Connect sink landing topic records as Iceberg snapshots |
| `:protomolt-kafka-connect-opensearch` | `sink/kafka/connect-opensearch` | Connect sink driving topic records into OpenSearch through the index SPI |

Docs: [emitting bundles](../docs/sink/emitting.md),
[Apache Iceberg](../docs/sink/iceberg.md),
[Kafka serde](../docs/sink/kafka-serde.md),
[Kafka Connect](../docs/sink/kafka-connect.md),
[Kafka Connect: OpenSearch](../docs/sink/kafka-connect-opensearch.md).
