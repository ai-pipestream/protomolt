/**
 * Equivalence certification for embedding providers.
 *
 * <p>{@link ai.pipestream.proto.embeddings.harness.EmbeddingEquivalence} compares two
 * providers serving the same model over a corpus and reduces the per-text cosine similarities
 * and norm ratios (computed with {@link ai.pipestream.proto.embeddings.harness.Cosines}) to an
 * {@link ai.pipestream.proto.embeddings.harness.EquivalenceReport}: certified when the worst
 * text clears the threshold, so a runtime can mix the pair.
 */
package ai.pipestream.proto.embeddings.harness;
