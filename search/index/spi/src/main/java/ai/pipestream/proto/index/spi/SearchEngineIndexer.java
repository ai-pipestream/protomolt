package ai.pipestream.proto.index.spi;

import com.google.protobuf.Message;

/**
 * ServiceLoader SPI: one implementation per search engine.
 *
 * <p>NDJSON is not an engine — use {@code protomolt-index-ndjson}.
 * Engines consume an {@link IndexingPlan} built from descriptor indexing hints.
 */
public interface SearchEngineIndexer {
    /** Stable id, e.g. {@code lucene}, {@code opensearch}, {@code solr}. */
    String engineId();

    /** Maps {@code message} into an engine-native document using the shared plan. */
    Object map(Message message, IndexingPlan plan) throws Exception;
}
