/**
 * The Confluence Cloud crawler core: read a workspace over the REST v2 API and emit the
 * domain protos.
 *
 * <p>{@link ai.pipestream.proto.acquire.confluence.ConfluenceClient} is the transport door -
 * basic auth from the account email plus API token, cursor pagination over
 * {@code _links.next}, the throttling contract honored (429 retried after
 * {@code Retry-After} with jitter), and a politeness gap between requests.
 * {@link ai.pipestream.proto.acquire.confluence.ConfluenceMapper} turns the REST JSON into
 * the {@code ai.pipestream.proto.acquire.confluence.v1} domain protos, never throwing on
 * unknown enum wire values.
 * {@link ai.pipestream.proto.acquire.confluence.ConfluenceCrawler} orchestrates the sweep on
 * virtual threads: spaces, then per space pages and blog posts with their comments,
 * attachments, labels, and properties, emitting
 * {@code ConfluenceChange}/{@code ConfluenceSnapshot} into a
 * {@link ai.pipestream.proto.acquire.confluence.ChangeSink}. The sink SPI is where the Kafka
 * and repo-service wiring lands in a later change; this module ships a logging sink and an
 * in-memory collector for tests.</p>
 *
 * <p>Configuration is the env-driven
 * {@link ai.pipestream.proto.acquire.confluence.ConfluenceConnectorConfig}
 * ({@code CONFLUENCE_BASE_URL} / _EMAIL / _API_TOKEN / _SPACES / _PAGE_SIZE /
 * _BODY_FORMAT); secrets never reach a log line.</p>
 */
package ai.pipestream.proto.acquire.confluence;
