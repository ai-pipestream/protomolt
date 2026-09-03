package ai.protomolt.proto.search.index.spi;

import com.google.protobuf.Message;

/**
 * ServiceLoader SPI: one implementation per search engine.
 *
 * <p>NDJSON is not an engine — use {@code protomolt-search-index-ndjson}.
 * Engines consume an {@link IndexMapping} built from descriptor indexing hints.
 */
public interface SearchEngineIndexer {
    /** Stable id, e.g. {@code lucene}, {@code opensearch}, {@code solr}. */
    String engineId();

    /** Maps {@code message} into an engine-native document using the shared mapping. */
    Object map(Message message, IndexMapping mapping) throws Exception;
}
