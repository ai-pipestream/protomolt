/**
 * Model2Vec static-embedding provider.
 *
 * <p>{@link ai.pipestream.proto.embeddings.model2vec.Model2VecEmbeddingProvider} registers
 * under the id {@code model2vec} and wraps the OpenNLP {@code StaticEmbeddingModel}: a
 * distilled per-token vector table in the Model2Vec release layout, embedded by subword
 * tokenization and mean pooling with no model forward pass, so there is no inference runtime
 * to ship. Construct it with a model directory, or let the ServiceLoader constructor resolve
 * one from the {@code protomolt.embeddings.model2vec.path} system property or the
 * {@code PROTOMOLT_MODEL2VEC_PATH} environment variable.
 */
package ai.pipestream.proto.embeddings.model2vec;
