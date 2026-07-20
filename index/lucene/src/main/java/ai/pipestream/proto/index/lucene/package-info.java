/**
 * Lucene plugin for the search-index SPI: maps protobuf messages into Lucene documents and
 * reports the per-field index settings a plan implies.
 *
 * <p>{@link ai.pipestream.proto.index.lucene.ProtoLuceneMapper} implements
 * {@link ai.pipestream.proto.index.spi.SearchEngineIndexer} and turns a message plus an
 * {@link ai.pipestream.proto.index.spi.IndexingPlan} into an
 * {@code org.apache.lucene.document.Document}, emitting KNN vector fields, point and doc-values
 * fields, and sub-fields named {@code field.sub}.
 * {@link ai.pipestream.proto.index.lucene.LuceneIndexerProvider} is the
 * {@code ServiceLoader} entry registered under
 * {@link ai.pipestream.proto.index.spi.SearchEngineIndexerProvider}.
 *
 * <p>Lucene has no schema artifact, so
 * {@link ai.pipestream.proto.index.lucene.LuceneFieldSpecs} takes the place of the mappings
 * file the OpenSearch and Solr plugins generate: a typed per-field report of analyzers, vector
 * encoding and similarity, and expected doc-values types, which the caller applies when
 * configuring the {@code IndexWriter}.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/indexing.md">Search
 * indexing guide</a> for the hint surface shared by all engines.
 */
package ai.pipestream.proto.index.lucene;
