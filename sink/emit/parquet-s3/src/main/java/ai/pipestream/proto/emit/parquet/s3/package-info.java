/**
 * Lands {@link ai.pipestream.proto.emit.parquet.ParquetEmitter}'s Parquet bytes on an
 * S3-compatible object store.
 *
 * <p>{@link ai.pipestream.proto.emit.parquet.s3.S3ParquetSink} takes a descriptor, a
 * batch of messages and an object key, renders the file in memory and uploads it with
 * one {@code putObject}. {@link ai.pipestream.proto.emit.parquet.s3.S3Clients} builds the
 * client: path-style with an endpoint override for self-hosted stores (RustFS,
 * SeaweedFS, Ceph), region-only for AWS S3 - the same store configuration idiom as
 * protomolt-iceberg-s3's {@code S3Catalogs}, on the raw SDK client rather than Iceberg's
 * {@code S3FileIO}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/emitting.md">Emitting
 * bundles guide</a> for the Parquet renderer itself.</p>
 */
package ai.pipestream.proto.emit.parquet.s3;
