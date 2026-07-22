/**
 * Rerank-provider SPI.
 *
 * <p>{@link ai.pipestream.proto.rerank.RerankProvider} is the ServiceLoader contract: a
 * stable provider id and query-to-scores ranking of candidate texts. Score scales are
 * provider-specific, so consumers compare only the order a provider produces.
 * {@link ai.pipestream.proto.rerank.RerankProviders} discovers implementations and resolves
 * them by id; the {@code protomolt-rerank-tei} and {@code protomolt-rerank-ovms} modules ship
 * providers.
 */
package ai.pipestream.proto.rerank;
