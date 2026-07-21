/**
 * Facade chaining optional validation, indexing-plan resolution, and NDJSON projection.
 *
 * <p>{@link ProtobufIndexer} is the single entry point. It resolves a message type's
 * {@link ai.pipestream.proto.index.spi.IndexingPlan} through an
 * {@link ai.pipestream.proto.index.spi.IndexingPlanFactory}, validates instances with an
 * optional {@link ai.pipestream.proto.validate.ProtoValidator}, and writes NDJSON lines — plain
 * or bulk-index pairs — with {@link ai.pipestream.proto.index.ndjson.ProtoNdjsonWriter}. The
 * two concerns stay independent: pass a validator only when a caller wants the two chained.</p>
 *
 * <p>The hint sources are the extension point and live in
 * {@code ai.pipestream.proto.index.spi}: {@link ProtobufIndexer#create()} infers from the
 * descriptor alone, while {@link ProtobufIndexer#defaults} consults a catalog, then the
 * {@code ai.pipestream.proto.index.hints.v1} descriptor options, then inference. Descriptor sets
 * parsed at runtime need the hint and validation extensions registered first, which
 * {@link ProtobufIndexer#registerExtensions} does in one call.</p>
 *
 * <p>NDJSON is engine-agnostic and does not interpret the plan; the engine plugins that do —
 * Lucene, OpenSearch, Solr — read the same plan model from sibling modules.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/indexing.md">Search
 * indexing guide</a> for the hint vocabulary and the engine split.</p>
 */
package ai.pipestream.proto.indexing;
