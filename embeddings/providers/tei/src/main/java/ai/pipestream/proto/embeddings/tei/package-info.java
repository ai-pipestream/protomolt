/**
 * Hugging Face Text Embeddings Inference (TEI) provider.
 *
 * <p>{@link ai.pipestream.proto.embeddings.tei.TeiEmbeddingProvider} registers under the id
 * {@code tei} and embeds through a TEI server's gRPC Embed API. Construct it with a
 * {@code host:port} target, or let the ServiceLoader constructor resolve one from the
 * {@code protomolt.embeddings.tei.target} system property or the
 * {@code PROTOMOLT_TEI_TARGET} environment variable. The server-side model is a TEI process
 * property, not client configuration. Overlength inputs truncate server side by default;
 * setting the {@code protomolt.embeddings.tei.truncate} system property or the
 * {@code PROTOMOLT_TEI_TRUNCATE} environment variable to {@code false} rejects them instead.
 */
package ai.pipestream.proto.embeddings.tei;
