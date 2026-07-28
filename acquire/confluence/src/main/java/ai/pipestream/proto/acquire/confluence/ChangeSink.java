package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;

/**
 * Where the crawler's output goes. Implementations must be thread-safe: the
 * crawler emits from virtual threads concurrently. The shipped
 * implementations: {@link KafkaChangeSink} (publishes to Kafka through the
 * protomolt serde, activated by {@code CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS}),
 * {@link RepoChangeSink} (saves pages, blog posts and attachments into the
 * repo service as Documents, activated by {@code CONFLUENCE_REPO_TARGET}),
 * {@link CompositeChangeSink} (fan-out when several are active), plus
 * {@link LoggingChangeSink} and {@link InMemoryChangeSink} for tests. The
 * Lucene projection of the Kafka feed is {@link ConfluenceLuceneProjector},
 * a consumer app rather than a sink.
 */
public interface ChangeSink {

    /** One upsert or delete against the Confluence mirror. */
    void emit(ConfluenceChange change);

    /** The full-sync marker for one completed space crawl. */
    void snapshot(ConfluenceSnapshot snapshot);
}
