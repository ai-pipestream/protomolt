/**
 * A Kafka Connect sink that lands topic records as Apache Iceberg table snapshots.
 *
 * <p>{@link ai.pipestream.proto.kafka.connect.iceberg.IcebergSinkConnector} is the entry point a
 * worker registers, {@link ai.pipestream.proto.kafka.connect.iceberg.IcebergSinkConfig} declares
 * its settings, and {@link ai.pipestream.proto.kafka.connect.iceberg.IcebergSinkTask} decodes each
 * delivered batch into row messages and commits it as one snapshot through
 * {@link ai.pipestream.proto.lake.iceberg.IcebergSink}. Rows are written by ProtoMolt's
 * descriptor-driven Parquet emitter, so no generated stubs and no Hadoop classes are needed on the
 * worker.</p>
 *
 * <p>The row message type is resolved from a serialized {@code google.protobuf.FileDescriptorSet}
 * given in configuration; record values are read as raw protobuf, Confluent-framed bytes, or
 * proto3 JSON. The catalog is configured with {@code iceberg.catalog.*} keys handed straight to
 * Iceberg's catalog builder, so any catalog Iceberg supports works without a code change.</p>
 *
 * <p>This module packages as its own plugin directory, separate from the gRPC connectors and
 * transforms in {@code ai.pipestream.proto.kafka.connect}, so a worker can install either without
 * the other's dependencies.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/iceberg.md">Iceberg
 * guide</a> for catalog setup and sink configuration.</p>
 */
package ai.pipestream.proto.kafka.connect.iceberg;
