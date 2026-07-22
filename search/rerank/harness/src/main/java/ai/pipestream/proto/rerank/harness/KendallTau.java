package ai.pipestream.proto.rerank.harness;

import java.util.Objects;

/**
 * Kendall's tau rank correlation between two score vectors over the same candidates.
 */
public final class KendallTau {

    private KendallTau() {
    }

    /**
     * The tau-b coefficient between {@code a} and {@code b} with exact-equality ties:
     * equivalent to {@link #tauB(double[], double[], double)} with epsilon 0.
     *
     * @throws IllegalArgumentException when the vectors differ in length or carry fewer than
     *         two elements
     */
    public static double tauB(double[] a, double[] b) {
        return tauB(a, b, 0.0);
    }

    /**
     * The tau-b coefficient between {@code a} and {@code b}, in [-1, 1]:
     * {@code (C - D) / sqrt((n0 - tiesA) * (n0 - tiesB))} over all pairs, where C and D count
     * concordant and discordant pairs, n0 = n(n-1)/2, and tiesX counts the pairs tied on one
     * side. Tied pairs are excluded from the matching side's denominator term, so a few equal
     * scores do not drag the coefficient toward zero.
     *
     * <p>A pair is tied on a side when its score difference there is at or below
     * {@code epsilon}; only pairs that differ by more than {@code epsilon} on BOTH sides
     * count as concordant or discordant. The epsilon models the noise floor of a provider's
     * scoring scale: two sigmoid scores that differ in the seventh decimal are the same
     * relevance, not a disagreement.
     *
     * <p>A constant side (every pair within {@code epsilon}) carries no ranking at all, which
     * zeroes the denominator: when BOTH sides are constant there is no ranking to disagree
     * about and the result is 1.0, while a constant side against a varied one has no
     * agreement to speak of and the result is 0.0.
     *
     * @throws IllegalArgumentException when {@code epsilon} is negative, when the vectors
     *         differ in length, or when they carry fewer than two elements
     */
    public static double tauB(double[] a, double[] b, double epsilon) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vector length mismatch: " + a.length
                    + " vs " + b.length);
        }
        if (epsilon < 0) {
            throw new IllegalArgumentException("epsilon must not be negative: " + epsilon);
        }
        int n = a.length;
        if (n < 2) {
            throw new IllegalArgumentException("tau-b needs at least two elements, got " + n);
        }
        long concordant = 0;
        long discordant = 0;
        long tiesA = 0;
        long tiesB = 0;
        for (int i = 0; i < n - 1; i++) {
            for (int j = i + 1; j < n; j++) {
                double da = a[i] - a[j];
                double db = b[i] - b[j];
                boolean tieA = Math.abs(da) <= epsilon;
                boolean tieB = Math.abs(db) <= epsilon;
                if (tieA) {
                    tiesA++;
                }
                if (tieB) {
                    tiesB++;
                }
                if (!tieA && !tieB) {
                    if (Math.signum(da) == Math.signum(db)) {
                        concordant++;
                    } else {
                        discordant++;
                    }
                }
            }
        }
        long n0 = (long) n * (n - 1) / 2;
        if (n0 - tiesA == 0 || n0 - tiesB == 0) {
            // At least one side is constant within epsilon. Both constant means no ranking to
            // disagree about; one constant side means no ranking agreement to speak of.
            return n0 - tiesA == 0 && n0 - tiesB == 0 ? 1.0 : 0.0;
        }
        double denominator = Math.sqrt((double) (n0 - tiesA) * (n0 - tiesB));
        return (concordant - discordant) / denominator;
    }
}
