package ai.pipestream.proto.index.spi;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * An index recipe: derive chunks and vectors from a source text field at
 * index time (chunk + embed on the server).
 *
 * <p>The {@linkplain #digest() digest} identifies the derivation pipeline the
 * way a schema pin identifies a message shape: two indexes agree on chunk
 * boundaries and vector spaces exactly when their recipe digests agree, so
 * consumers pin the digest and re-derive only when it changes. Every
 * component that can move a chunk boundary or change a vector participates,
 * including the chunker's implementation version.
 *
 * @param chunking deterministic chunking configuration (required)
 * @param embedding embedding configuration for the derived vectors (required)
 * @param vectorField index field name for the derived vector; empty = engine
 *        convention ({@code "<field>#<model>"})
 * @param storeChunkText store chunk text on child documents for highlighting
 */
public record ChunkRecipe(
        ChunkingSpec chunking,
        EmbeddingSpec embedding,
        String vectorField,
        boolean storeChunkText) {

    public ChunkRecipe {
        Objects.requireNonNull(chunking, "chunking");
        Objects.requireNonNull(embedding, "embedding");
        vectorField = vectorField == null ? "" : vectorField;
    }

    /** Deterministic chunking configuration; every value participates in the digest. */
    public record ChunkingSpec(
            String strategy,
            int strategyVersion,
            int targetTokens,
            int overlapTokens,
            int minTokens,
            int maxTokens,
            String boundary) {

        public ChunkingSpec {
            Objects.requireNonNull(strategy, "strategy");
            Objects.requireNonNull(boundary, "boundary");
            if (strategy.isBlank()) {
                throw new IllegalArgumentException("chunking strategy must not be blank");
            }
        }
    }

    /** Embedding configuration for derived vectors; participates in the digest. */
    public record EmbeddingSpec(
            String model,
            int dims,
            VectorSimilarity similarity,
            boolean normalize) {

        public EmbeddingSpec {
            Objects.requireNonNull(model, "model");
            if (model.isBlank()) {
                throw new IllegalArgumentException("embedding model must not be blank");
            }
            if (dims <= 0) {
                throw new IllegalArgumentException("embedding dims must be positive, got " + dims);
            }
            similarity = similarity == null ? VectorSimilarity.COSINE : similarity;
        }
    }

    /**
     * SHA-256 over a canonical, versioned rendering of every component. The
     * rendering is part of the public contract: it never changes for existing
     * fields, and new fields append (bumping the leading version marker), so
     * a digest computed today matches one computed by a future release for
     * the same recipe.
     */
    public String digest() {
        String canonical = "chunk-recipe/1\n"
                + "chunking.strategy=" + chunking.strategy() + "\n"
                + "chunking.strategyVersion=" + chunking.strategyVersion() + "\n"
                + "chunking.targetTokens=" + chunking.targetTokens() + "\n"
                + "chunking.overlapTokens=" + chunking.overlapTokens() + "\n"
                + "chunking.minTokens=" + chunking.minTokens() + "\n"
                + "chunking.maxTokens=" + chunking.maxTokens() + "\n"
                + "chunking.boundary=" + chunking.boundary() + "\n"
                + "embedding.model=" + embedding.model() + "\n"
                + "embedding.dims=" + embedding.dims() + "\n"
                + "embedding.similarity=" + embedding.similarity() + "\n"
                + "embedding.normalize=" + embedding.normalize() + "\n"
                + "vectorField=" + vectorField + "\n"
                + "storeChunkText=" + storeChunkText + "\n";
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is a mandatory JCA algorithm", e);
        }
    }
}
