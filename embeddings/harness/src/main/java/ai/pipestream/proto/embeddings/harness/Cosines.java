package ai.pipestream.proto.embeddings.harness;

import java.util.Objects;

/**
 * Cosine similarity and L2 norms of embedding vectors.
 */
public final class Cosines {

    private Cosines() {
    }

    /**
     * The cosine of the angle between {@code a} and {@code b}, in [-1, 1].
     *
     * <p>When either vector is the zero vector the result is 0.0 rather than NaN: a zero
     * vector has no direction, so it is treated as maximally dissimilar instead of poisoning
     * the comparison with a non-number.
     *
     * @throws IllegalArgumentException when the vectors differ in length
     */
    public static double cosine(float[] a, float[] b) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vector length mismatch: " + a.length
                    + " vs " + b.length);
        }
        double dot = 0;
        double normA = 0;
        double normB = 0;
        for (int d = 0; d < a.length; d++) {
            dot += (double) a[d] * b[d];
            normA += (double) a[d] * a[d];
            normB += (double) b[d] * b[d];
        }
        if (normA == 0 || normB == 0) {
            return 0.0;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * The L2 (Euclidean) norm of {@code v}: the square root of the sum of squared components,
     * 0.0 for the zero vector.
     */
    public static double norm(float[] v) {
        Objects.requireNonNull(v, "v");
        double sum = 0;
        for (float component : v) {
            sum += (double) component * component;
        }
        return Math.sqrt(sum);
    }
}
