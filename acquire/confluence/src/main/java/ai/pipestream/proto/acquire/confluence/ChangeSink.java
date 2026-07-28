package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;

/**
 * Where the crawler's output goes. Implementations must be thread-safe: the
 * crawler emits from virtual threads concurrently. The Kafka / repo-service /
 * Lucene wiring lands behind this SPI in a later change; this module ships a
 * {@link LoggingChangeSink} and an {@link InMemoryChangeSink} for tests.
 */
public interface ChangeSink {

    /** One upsert or delete against the Confluence mirror. */
    void emit(ConfluenceChange change);

    /** The full-sync marker for one completed space crawl. */
    void snapshot(ConfluenceSnapshot snapshot);
}
