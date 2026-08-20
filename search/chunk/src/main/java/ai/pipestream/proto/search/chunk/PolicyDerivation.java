package ai.pipestream.proto.search.chunk;

import ai.pipestream.proto.search.embedding.EmbeddingProvider;
import ai.pipestream.proto.search.embedding.EmbeddingProviders;
import ai.pipestream.proto.search.index.spi.ChunkingPolicy;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes a whole {@link ChunkingPolicy}: chunk under the policy's
 * chunking spec, embed each chunk under its embedding spec. This is the
 * derivation the policy digest identifies; the provider is validated
 * against the spec by name and dimension before any text is touched, so a
 * corpus can never be silently derived with the wrong model.
 */
public final class PolicyDerivation {

    private final SentencePackedChunker chunker = new SentencePackedChunker();
    private final EmbeddingProvider provider;

    /**
     * Creates the derivation over a provider.
     *
     * @param provider the embedding provider the policy will be checked
     *        against
     */
    public PolicyDerivation(EmbeddingProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException("provider must not be null");
        }
        this.provider = provider;
    }

    /**
     * Creates the derivation for a policy by discovering the embedding
     * provider the policy names on the classpath
     * ({@link EmbeddingProviders#forSpec}). This is the whole lane wiring: a
     * policy served with a shape resolves to its chunker and its embedding
     * model with no further configuration, and the resolution fails loudly
     * here, before any text, when the provider is absent, misconfigured, or
     * of the wrong dimension.
     *
     * @param policy the policy to derive under
     * @return a derivation whose provider satisfies the policy's embedding
     *         spec
     */
    public static PolicyDerivation discover(ChunkingPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("policy must not be null");
        }
        return new PolicyDerivation(EmbeddingProviders.forSpec(policy.embedding()));
    }

    /**
     * One chunk with its derived vector.
     *
     * @param chunk the chunk
     * @param vector the chunk's vector, {@code dims} components,
     *        L2-normalized when the policy says so
     */
    public record DerivedChunk(Chunk chunk, float[] vector) {
    }

    /**
     * Derives chunks and vectors from the text under the policy.
     *
     * @param text the source text; blank derives nothing
     * @param policy the policy; its embedding model must be this
     *        provider's id and its dims the provider's dimension, refused
     *        loudly otherwise
     * @return the derived chunks, in chunk order
     */
    public List<DerivedChunk> derive(String text, ChunkingPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("policy must not be null");
        }
        ChunkingPolicy.EmbeddingSpec embedding = policy.embedding();
        if (!provider.providerId().equals(embedding.model())) {
            throw new IllegalArgumentException("the policy embeds with model "
                    + embedding.model() + ", this provider is " + provider.providerId());
        }
        if (provider.dimension() != embedding.dims()) {
            throw new IllegalArgumentException("the policy pins " + embedding.dims()
                    + " dims, provider " + provider.providerId()
                    + " produces " + provider.dimension());
        }
        List<Chunk> chunks = chunker.chunk(text, policy.chunking());
        List<DerivedChunk> derived = new ArrayList<>(chunks.size());
        for (Chunk chunk : chunks) {
            float[] vector = provider.embed(chunk.text());
            if (vector == null || vector.length != embedding.dims()) {
                throw new IllegalStateException("provider " + provider.providerId()
                        + " returned " + (vector == null ? "null" : vector.length + " dims")
                        + " for a " + embedding.dims() + "-dim policy");
            }
            if (embedding.normalize()) {
                normalize(vector);
            }
            derived.add(new DerivedChunk(chunk, vector));
        }
        return List.copyOf(derived);
    }

    private static void normalize(float[] vector) {
        double sum = 0;
        for (float component : vector) {
            sum += (double) component * component;
        }
        if (sum == 0) {
            return;
        }
        float norm = (float) Math.sqrt(sum);
        for (int i = 0; i < vector.length; i++) {
            vector[i] /= norm;
        }
    }
}
