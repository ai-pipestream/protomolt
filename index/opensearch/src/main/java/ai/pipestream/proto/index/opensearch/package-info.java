/**
 * OpenSearch plugin for the search-index SPI: maps protobuf messages into OpenSearch document
 * maps and generates the index mappings those documents require.
 *
 * <p>{@link OpenSearchDocumentMapper} implements
 * {@link ai.pipestream.proto.index.spi.SearchEngineIndexer} and turns a message plus an
 * {@link ai.pipestream.proto.index.spi.IndexingPlan} into a {@code Map}, leaving serialization
 * and transport to the caller's client. {@link OpenSearchIndexerProvider} is the
 * {@code ServiceLoader} entry registered under
 * {@link ai.pipestream.proto.index.spi.SearchEngineIndexerProvider}.
 *
 * <p>{@link OpenSearchMappingGenerator} derives the index {@code mappings} body from the same
 * plan, covering field types, sub-fields, doc values, date formats, range types, and
 * {@code knn_vector} parameters. It is the OpenSearch counterpart of the schema generator in the
 * Solr module and of the per-field spec report in the Lucene module.
 *
 * <p>{@link OpenSearchSink} is the transport: a thin {@code java.net.http} sink that creates an
 * index from a plan's generated mappings (with {@code index.knn} when the plan has a vector
 * field) and writes document maps through the {@code _bulk} endpoint, surfacing per-item
 * failures. Engine-agnostic NDJSON emission stays in the separate {@code protomolt-index-ndjson}
 * module, which does not interpret the plan.
 *
 * <p>{@link OpenSearchSearch} is the read-side sibling of the sink, running kNN queries and
 * parsing hits. {@link RerankedSemanticSearch} builds on it: embed a query, recall a deep
 * candidate set with kNN, score the candidates with a rerank provider, and answer the
 * reordered top-k.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/indexing.md">Search
 * indexing guide</a> for the hint surface and for how declared sensitivity classes are applied to
 * the search layer.
 */
package ai.pipestream.proto.index.opensearch;
