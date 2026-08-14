/**
 * Search-index SPI: the engine-agnostic indexing plan, the hint sources that build it, and the
 * ServiceLoader contract engine plugins implement.
 *
 * <p>{@link IndexingPlanFactory} walks a message descriptor and produces an
 * {@link IndexingPlan}: the indexable fields, their resolved {@link IndexFieldKind}, and their
 * dotted protobuf paths. Per-field settings are carried by {@link ResolvedFieldHint}, which also
 * holds vector, range, map, date, sub-field, and engine-scoped escape-hatch parameters. Planning
 * errors are reported as {@link IndexingPlanException} with the offending field path.
 *
 * <p>{@link IndexingHintSource} is the extension point for where hints come from; sources compose
 * with {@link IndexingHintSource#orElse(IndexingHintSource)}. Three implementations ship here:
 * {@link ProtoOptionsIndexingHintSource} reads the indexing {@code FieldOptions} extensions baked
 * into the descriptor, {@link CatalogIndexingHintSource} supplies programmatic side-car hints for
 * schemas that cannot be annotated, and {@link InferringIndexingHintSource} derives a field kind
 * from the protobuf type when nothing else matches.
 *
 * <p>Engine plugins implement {@link SearchEngineIndexer} and register a
 * {@link SearchEngineIndexerProvider} for discovery through {@link SearchEngineIndexers},
 * receiving the shared {@link ai.pipestream.proto.mapper.ProtoFieldMapper} and
 * {@link ai.pipestream.proto.descriptors.DescriptorRegistry} by way of {@link IndexerContext}.
 * {@link AnyIndexing} unpacks {@code google.protobuf.Any} at write time against that registry
 * and offers every payload to the {@link AnyPayloadValidator}s on the classpath.
 * The Lucene, OpenSearch, and Solr modules are such plugins; NDJSON output is not an engine
 * and ignores the plan entirely.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/search/indexing.md">Search
 * indexing guide</a> for the hint surface and end-to-end usage.
 */
package ai.pipestream.proto.index.spi;
