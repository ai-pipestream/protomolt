/**
 * Equivalence certification for embedding providers.
 *
 * <p>{@link ai.protomolt.proto.search.embedding.harness.EmbeddingEquivalence} compares two
 * providers serving the same model over a corpus and reduces the per-text cosine similarities
 * and norm ratios (computed with {@link ai.protomolt.proto.search.embedding.harness.Cosines}) to an
 * {@link ai.protomolt.proto.search.embedding.harness.EquivalenceReport}: certified when the worst
 * text clears the threshold, so a runtime can mix the pair.
 */
package ai.protomolt.proto.search.embedding.harness;
