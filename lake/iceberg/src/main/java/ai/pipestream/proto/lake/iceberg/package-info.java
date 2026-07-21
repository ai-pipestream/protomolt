/**
 * Writes protobuf messages into Apache Iceberg tables and converts between descriptors and table
 * schemas.
 *
 * <p>{@link IcebergSink} is the entry point: it loads or creates a table, writes the batch as one
 * Parquet data file through {@link ai.pipestream.proto.emit.parquet.ParquetEmitter}, delivers it
 * with the table's {@code FileIO}, and commits an append — a snapshot any engine can read. It
 * works against any Iceberg catalog implementation, REST and JDBC included.
 * {@link IcebergSchemas} converts in both directions: a descriptor becomes the table schema the
 * sink creates, and a table schema becomes registrable {@code .proto} source.</p>
 *
 * <p>{@link IcebergPartitions} builds a {@code PartitionSpec} by column name and reads a message's
 * source-column values so the sink can route rows to partitions. {@link LocalFileIO} is a
 * {@code FileIO} for {@code file://} warehouses built on {@code java.nio}, named through the
 * catalog's {@code io-impl} property; the {@code ai.pipestream.proto.lake.iceberg.s3} package in
 * the sibling module does the same for S3-compatible object stores.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/iceberg.md">Apache
 * Iceberg guide</a> for the type mapping and catalog setup.</p>
 */
package ai.pipestream.proto.lake.iceberg;
