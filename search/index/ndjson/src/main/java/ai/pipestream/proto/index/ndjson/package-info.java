/**
 * Engine-agnostic encoding of protobuf messages as NDJSON, including bulk-action pairs.
 *
 * <p>{@link ProtoNdjsonWriter} is the entry point. It renders each message as one line of proto3
 * JSON using the message descriptor, and writes the {@code index}, {@code create}, and
 * {@code delete} action envelopes that precede a document in an OpenSearch-style bulk request.
 * A {@link ai.pipestream.proto.descriptors.DescriptorRegistry} may be supplied so that
 * {@code Any} fields resolve against known types.
 *
 * <p>{@link NdjsonOptions} controls field naming and default-value inclusion. Whitespace omission
 * is required rather than optional, because pretty-printed JSON is not valid line-oriented output.
 *
 * <p>This package is not a search-engine plugin: it does not read an indexing plan and does not
 * interpret indexing hints. The Lucene, OpenSearch, and Solr modules cover that path through the
 * {@code protomolt-index-spi} service interfaces.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/indexing.md">Search
 * indexing guide</a> for how the output path fits with the engine plugins.
 */
package ai.pipestream.proto.index.ndjson;
