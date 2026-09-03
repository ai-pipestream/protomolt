/**
 * Embedding-provider SPI and the mapping-driven document embedder.
 *
 * <p>{@link EmbeddingProvider} is the ServiceLoader contract: a stable provider id, a fixed
 * vector dimension, and text-to-vector embedding. {@link EmbeddingProviders} discovers
 * implementations and resolves them by id; the {@code protomolt-search-embedding-model2vec} module
 * ships one.
 *
 * <p>{@link MappingEmbedder} joins a provider to an
 * {@link ai.protomolt.proto.search.index.spi.IndexMapping}: it reads a TEXT source field from an
 * engine-neutral mapped document, validates the provider's dimension against the vector
 * field's {@code vector_dims} hint, and writes the embedded vector into the mapping's VECTOR
 * field in the shape engine mappers use for repeated floats.
 */
package ai.protomolt.proto.search.embedding;
