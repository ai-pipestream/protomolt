/**
 * Microsoft Graph integration spoken directly over the REST API, with no Microsoft SDK and no
 * Windows agent.
 *
 * <p>{@link GraphClient} is the entry point: an authorized JSON-in, JSON-out door to Graph
 * that honors the service's throttling contract. It takes its bearer token from a supplier,
 * so token acquisition stays in {@link GraphAuth}, which implements the two flows a headless
 * toolkit needs — client credentials for service work and device code for an operator signing
 * in from a terminal.</p>
 *
 * <p>Two lanes sit on that client. {@link GraphFiles} reads and writes OneDrive and SharePoint
 * Online through the shared {@code driveItem} model, including the SharePoint column values of
 * a document, which are data-rich JSON that {@code infer-schema} can turn into a typed
 * message.
 * {@link GraphConnections} drives the Copilot connectors (external connections) API — create a
 * connection, register its schema, and push external items — with {@link GraphSchemas}
 * rendering that schema from an {@link ai.pipestream.proto.index.spi.IndexingPlan}, the same
 * indexing hints the OpenSearch, Solr, and Lucene generators read.</p>
 *
 * <p>{@link GraphProbe} is a standalone command that reports which of these surfaces a given
 * tenant actually permits; every probe is a read.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/msgraph.md">Microsoft
 * Graph guide</a> for the permissions each lane requires.</p>
 */
package ai.pipestream.proto.graph;
