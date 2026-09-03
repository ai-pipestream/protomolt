/**
 * Solr plugin for the search-index SPI: maps protobuf messages into Solr document maps and
 * generates the managed-schema definitions those documents require.
 *
 * <p>{@link SolrDocumentMapper} implements
 * {@link ai.protomolt.proto.search.index.spi.SearchEngineIndexer} and turns a message plus an
 * {@link ai.protomolt.proto.search.index.spi.IndexMapping} into a {@code SolrInputDocument}-compatible
 * {@code Map}, so callers may use SolrJ or the JSON update handler without this module taking a
 * dependency on either. {@link SolrIndexerProvider} is the {@code ServiceLoader} entry registered
 * under {@link ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider}.
 *
 * <p>{@link SolrSchemaGenerator} derives the matching {@code field}, {@code fieldType}, and
 * {@code copyField} definitions from the same mapping, for posting to the Schema API. It is the Solr
 * counterpart of the mappings generator in the OpenSearch module and of the per-field spec report
 * in the Lucene module.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/search/indexing.md">Search
 * indexing guide</a> for the hint surface shared by all engines.
 */
package ai.protomolt.proto.search.index.solr;
