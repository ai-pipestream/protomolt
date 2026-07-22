/**
 * Writes protobuf messages as Parquet files, driven by the descriptor alone.
 *
 * <p>{@link ParquetEmitter} is the entry point: it takes a descriptor and a batch of messages —
 * dynamic or generated, only the descriptor matters — and returns the file as bytes or as an
 * {@link ai.pipestream.proto.emit.Bundle} entry. No code generation is involved, the file is
 * produced in memory, and no native Hadoop runtime is required, so where the data lands stays
 * the caller's explicit act through a sink.</p>
 *
 * <p>{@link ProtoParquetSchemas} defines the descriptor-to-Parquet mapping and holds
 * {@link ProtoParquetSchemas.FieldIdResolver}, the hook table formats use to stamp their own
 * column ids into the file schema; the {@code ai.pipestream.proto.lake.iceberg} package supplies
 * one so Iceberg readers resolve columns natively. {@link ParquetExportOptions} carries the two export
 * controls: projecting columns out of the file entirely, and masking values by sensitivity class
 * through {@link ai.pipestream.proto.meta.SensitivityMasker}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/emitting.md">Emitting
 * bundles guide</a> for the type mapping and the export options.</p>
 */
package ai.pipestream.proto.emit.parquet;
