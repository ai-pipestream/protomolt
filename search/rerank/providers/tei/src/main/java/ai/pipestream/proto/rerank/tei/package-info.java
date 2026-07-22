/**
 * Hugging Face Text Embeddings Inference (TEI) rerank provider.
 *
 * <p>{@link ai.pipestream.proto.rerank.tei.TeiRerankProvider} registers under the id
 * {@code tei} and reranks through a TEI server's gRPC Rerank API. Construct it with a
 * {@code host:port} target, or let the ServiceLoader constructor resolve one from the
 * {@code protomolt.rerank.tei.target} system property or the
 * {@code PROTOMOLT_RERANK_TEI_TARGET} environment variable. The server-side model is a TEI
 * process property, not client configuration.
 */
package ai.pipestream.proto.rerank.tei;
