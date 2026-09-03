/**
 * The Confluence Cloud crawler core: read a workspace over the REST v2 API and emit the
 * domain protos.
 *
 * <p>{@link ai.protomolt.proto.acquire.confluence.ConfluenceClient} is the transport client -
 * basic auth from the account email plus API token, cursor pagination over
 * {@code _links.next}, the throttling contract honored (429 retried after
 * {@code Retry-After} with jitter), and a politeness gap between requests.
 * {@link ai.protomolt.proto.acquire.confluence.ConfluenceMapper} turns the REST JSON into
 * the {@code ai.pipestream.proto.acquire.confluence.v1} domain protos, never throwing on
 * unknown enum wire values.
 * {@link ai.protomolt.proto.acquire.confluence.ConfluenceCrawler} orchestrates the sweep on
 * virtual threads: spaces, then per space pages and blog posts with their comments,
 * attachments, labels, and properties, emitting
 * {@code ConfluenceChange}/{@code ConfluenceSnapshot} into a
 * {@link ai.protomolt.proto.acquire.confluence.ChangeSink}. The shipped sinks:
 * {@link ai.protomolt.proto.acquire.confluence.KafkaChangeSink} publishes to Kafka through the
 * protomolt serde, {@link ai.protomolt.proto.acquire.confluence.RepoChangeSink} saves pages,
 * blog posts and attachments into the repo service as Documents, and
 * {@link ai.protomolt.proto.acquire.confluence.CompositeChangeSink} fans out when several are
 * active; the Lucene projection of the Kafka feed is
 * {@link ai.protomolt.proto.acquire.confluence.ConfluenceLuceneProjector}. A logging sink and
 * an in-memory collector remain for tests.</p>
 *
 * <p>Configuration is the env-driven
 * {@link ai.protomolt.proto.acquire.confluence.ConfluenceConnectorConfig}
 * ({@code CONFLUENCE_BASE_URL} / _EMAIL / _API_TOKEN / _SPACES / _PAGE_SIZE /
 * _BODY_FORMAT; {@code CONFLUENCE_USER} and {@code CONFLUENCE_TOKEN} alias the
 * two credential variables; {@code CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS} activates the Kafka
 * sink, {@code CONFLUENCE_REPO_TARGET} the repo sink); secrets never reach a log line.</p>
 *
 * <p>The gRPC facade sits on top:
 * {@link ai.protomolt.proto.acquire.confluence.ConfluenceGrpcService} implements the
 * {@code ConfluenceService} rpcs as thin delegations to the crawler core, and
 * {@link ai.protomolt.proto.acquire.confluence.ConfluenceProxyServer} serves them over
 * Netty with reflection and health on, handlers on virtual threads.</p>
 */
package ai.protomolt.proto.acquire.confluence;
