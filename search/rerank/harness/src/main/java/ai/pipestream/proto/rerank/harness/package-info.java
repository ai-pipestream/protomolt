/**
 * Equivalence certification for rerank providers.
 *
 * <p>{@link ai.pipestream.proto.rerank.harness.RerankEquivalence} compares two providers
 * serving the same model over query cases and reduces the per-query Kendall tau-b
 * correlations (computed by {@link ai.pipestream.proto.rerank.harness.KendallTau}) plus the
 * top-1 agreement count to a
 * {@link ai.pipestream.proto.rerank.harness.RerankEquivalenceReport}: certified when the
 * worst query clears the threshold. Score scales are provider-specific, so certification is
 * on ranking, never on raw score values.
 */
package ai.pipestream.proto.rerank.harness;
