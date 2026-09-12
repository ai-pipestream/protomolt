package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;

import java.util.List;

/**
 * A whole signature: an ordered run of subsequences placed relative to an
 * anchor, each optionally flanked by fragments.
 *
 * <p>Format registries publish patterns this way because a real signature
 * is rarely one contiguous run. A header magic, then eight bytes of
 * anything, then a version marker somewhere in the next kilobyte, is three
 * pieces with stated distances between them, and compiling the distances
 * into the pattern would multiply out into thousands of alternatives.
 *
 * <p>An anchor says where the first piece is measured from: the start of
 * the content, the end of it, or nowhere in particular. Distances for the
 * pieces after the first are measured from the end of the piece before.
 * Fragments sharing a position are alternatives of each other; fragments
 * at increasing positions chain outward from the piece they flank.
 *
 * <p>The result of a match is one of three things, not two. A signature
 * whose pieces all sit inside the windows either matched or did not; a
 * signature reaching outside them was not evaluated, and reporting that
 * as "did not match" would turn a blind spot into a false negative.
 */
public final class SignatureSequence {

    /** Where the first subsequence is measured from. */
    public enum Anchor {
        /** Distances run from the start of the content. */
        LEADING,
        /** Distances run back from the end of the content. */
        TRAILING,
        /** No fixed distance; the pattern is searched for. */
        ANYWHERE
    }

    /** What a match attempt established. */
    public enum Outcome {
        /** The signature is present. */
        MATCHED,
        /** The signature is absent, and the bytes to prove it were read. */
        NOT_MATCHED,
        /** The bytes needed to decide were outside the windows. */
        NOT_EVALUABLE
    }

    /**
     * A pattern flanking a subsequence, at a distance from it.
     *
     * @param position 1 for the fragment nearest the subsequence, counting
     *        outward
     * @param minOffset the shortest distance from what it flanks
     * @param maxOffset the longest distance from what it flanks
     * @param expression the pattern
     */
    public record Fragment(int position, int minOffset, int maxOffset,
                           SignatureExpression expression) {
    }

    /**
     * One piece of a signature.
     *
     * @param minOffset the shortest distance from the anchor, or from the
     *        end of the piece before
     * @param maxOffset the longest such distance
     * @param expression the piece's own pattern
     * @param before fragments to the left, nearest first
     * @param after fragments to the right, nearest first
     */
    public record Subsequence(int minOffset, int maxOffset, SignatureExpression expression,
                              List<Fragment> before, List<Fragment> after) {

        /** Validates and copies. */
        public Subsequence {
            if (expression == null) {
                throw new IllegalArgumentException("a subsequence needs a pattern");
            }
            if (minOffset < 0 || maxOffset < minOffset) {
                throw new IllegalArgumentException("offsets must be a non-negative range");
            }
            before = List.copyOf(before);
            after = List.copyOf(after);
        }

        /** The shortest run of bytes the left-hand fragments occupy. */
        int minLeading() {
            return extent(before, true);
        }

        /** The longest run of bytes the left-hand fragments occupy. */
        int maxLeading() {
            return extent(before, false);
        }

        /** The shortest run from the pattern's start to the extent's end. */
        int minTrailing() {
            return expression.minLength() + extent(after, true);
        }

        /** The longest run from the pattern's start to the extent's end. */
        int maxTrailing() {
            return expression.maxLength() + extent(after, false);
        }

        private static int extent(List<Fragment> fragments, boolean shortest) {
            int total = 0;
            int position = 0;
            int best = -1;
            for (Fragment fragment : sortedByPosition(fragments)) {
                if (fragment.position() != position) {
                    if (best >= 0) {
                        total += best;
                    }
                    position = fragment.position();
                    best = -1;
                }
                int span = shortest
                        ? fragment.minOffset() + fragment.expression().minLength()
                        : fragment.maxOffset() + fragment.expression().maxLength();
                best = best < 0 ? span : (shortest ? Math.min(best, span) : Math.max(best, span));
            }
            return best >= 0 ? total + best : total;
        }
    }

    private final Anchor anchor;
    private final List<Subsequence> subsequences;

    /**
     * Creates a signature.
     *
     * @param anchor where the first subsequence is measured from
     * @param subsequences the pieces, in the order they appear in the
     *        content; must not be empty
     */
    public SignatureSequence(Anchor anchor, List<Subsequence> subsequences) {
        if (anchor == null) {
            throw new IllegalArgumentException("a signature needs an anchor");
        }
        if (subsequences == null || subsequences.isEmpty()) {
            throw new IllegalArgumentException("a signature needs at least one subsequence");
        }
        this.anchor = anchor;
        this.subsequences = List.copyOf(subsequences);
    }

    /** Where the first subsequence is measured from. */
    public Anchor anchor() {
        return anchor;
    }

    /** The pieces, in content order. */
    public List<Subsequence> subsequences() {
        return subsequences;
    }

    /** Whether every piece has a bounded pattern. */
    public boolean bounded() {
        for (Subsequence subsequence : subsequences) {
            if (!subsequence.expression().bounded()) {
                return false;
            }
            for (Fragment fragment : subsequence.before()) {
                if (!fragment.expression().bounded()) {
                    return false;
                }
            }
            for (Fragment fragment : subsequence.after()) {
                if (!fragment.expression().bounded()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Matches the signature.
     *
     * @param bytes the content's windows
     * @return what the attempt established
     */
    public Outcome matchIn(ByteWindows bytes) {
        if (!bounded()) {
            return Outcome.NOT_EVALUABLE;
        }
        Attempt attempt = new Attempt(bytes);
        boolean matched = anchor == Anchor.TRAILING
                ? attempt.matchTrailing()
                : attempt.matchForward();
        if (matched) {
            return Outcome.MATCHED;
        }
        return attempt.blind ? Outcome.NOT_EVALUABLE : Outcome.NOT_MATCHED;
    }

    private static List<Fragment> sortedByPosition(List<Fragment> fragments) {
        return fragments.stream()
                .sorted((left, right) -> Integer.compare(left.position(), right.position()))
                .toList();
    }

    /** One match attempt, tracking whether anything went unread. */
    private final class Attempt {

        private final ByteWindows bytes;
        private boolean blind;

        Attempt(ByteWindows bytes) {
            this.bytes = bytes;
        }

        /** LEADING and ANYWHERE both place pieces left to right. */
        boolean matchForward() {
            if (anchor == Anchor.LEADING) {
                return placeFrom(0, 0, 0);
            }
            // An unanchored pattern is searched for, and searching is only
            // possible where bytes are resident: the two windows, not the
            // unread span between them.
            for (long[] span : residentSpans()) {
                if (placeFrom(0, span[0], span[1])) {
                    return true;
                }
            }
            return false;
        }

        /** The spans an unanchored search may start in. */
        private List<long[]> residentSpans() {
            long headEnd = bytes.head().length;
            if (!bytes.sizeKnown() || bytes.tail().length == 0) {
                return List.of(new long[] {0, Math.max(0, headEnd - 1)});
            }
            long tailStart = bytes.sizeBytes() - bytes.tail().length;
            long tailEnd = bytes.sizeBytes();
            if (tailStart <= headEnd) {
                return List.of(new long[] {0, Math.max(0, tailEnd - 1)});
            }
            return List.of(new long[] {0, Math.max(0, headEnd - 1)},
                    new long[] {tailStart, Math.max(tailStart, tailEnd - 1)});
        }

        /**
         * Places subsequence {@code index} and everything after it, given
         * that the extent may begin anywhere in {@code [from, to]}.
         *
         * <p>The distance is measured to the extent's near edge, which is
         * where the outermost left-hand fragment begins, not to the
         * pattern itself. The pattern's own start is therefore searched
         * over the span the fragments could occupy, and a placement counts
         * only when the extent lands where the distance says.
         */
        private boolean placeFrom(int index, long from, long to) {
            Subsequence subsequence = subsequences.get(index);
            long low = from + subsequence.minOffset();
            long high = to + subsequence.maxOffset();
            long first = Math.max(0, low + subsequence.minLeading());
            long last = Math.min(high + subsequence.maxLeading(), lastStart());
            for (long start = first; start <= last; start++) {
                long[] extent = place(subsequence, start);
                if (extent == null || extent[0] < low || extent[0] > high) {
                    continue;
                }
                if (index + 1 == subsequences.size()) {
                    return true;
                }
                if (placeFrom(index + 1, extent[1], extent[1])) {
                    return true;
                }
            }
            return false;
        }

        /** The furthest start worth trying: the last byte that was captured. */
        private long lastStart() {
            return bytes.sizeKnown() ? bytes.sizeBytes() : bytes.head().length;
        }

        /** TRAILING places pieces right to left, from the content's end. */
        boolean matchTrailing() {
            if (!bytes.sizeKnown()) {
                blind = true;
                return false;
            }
            return placeBackFrom(0, bytes.sizeBytes());
        }

        private boolean placeBackFrom(int index, long cursor) {
            Subsequence subsequence = subsequences.get(index);
            for (int offset = subsequence.minOffset();
                    offset <= subsequence.maxOffset(); offset++) {
                long edge = cursor - offset;
                if (edge < 0) {
                    break;
                }
                // The distance is measured to the extent's far edge, so the
                // pattern's start is that edge less the pattern and any
                // right-hand fragments.
                long first = Math.max(0, edge - subsequence.maxTrailing());
                long last = edge - subsequence.minTrailing();
                for (long start = first; start <= last; start++) {
                    long[] extent = place(subsequence, start);
                    if (extent == null || extent[1] != edge) {
                        continue;
                    }
                    if (index + 1 == subsequences.size()) {
                        return true;
                    }
                    if (placeBackFrom(index + 1, extent[0])) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Places one subsequence with its main pattern starting at
         * {@code start}.
         *
         * @return the extent as {@code {start, end}} including fragments,
         *         or null when it does not fit here
         */
        private long[] place(Subsequence subsequence, long start) {
            note(subsequence.expression(), start);
            int length = subsequence.expression().matchAt(bytes, start);
            if (length == SignatureExpression.NO_MATCH) {
                return null;
            }
            long rightEnd = placeFragments(subsequence.after(), start + length, true);
            if (rightEnd == SignatureExpression.NO_MATCH) {
                return null;
            }
            long leftStart = placeFragments(subsequence.before(), start, false);
            if (leftStart == SignatureExpression.NO_MATCH) {
                return null;
            }
            return new long[] {leftStart, rightEnd};
        }

        /**
         * Chains one side's fragments outward from a cursor.
         *
         * @param rightward true for fragments after the subsequence
         * @return the outermost boundary, or NO_MATCH
         */
        private long placeFragments(List<Fragment> fragments, long cursor, boolean rightward) {
            if (fragments.isEmpty()) {
                return cursor;
            }
            List<Fragment> ordered = sortedByPosition(fragments);
            int position = ordered.getFirst().position();
            boolean placedThisPosition = false;
            for (Fragment fragment : ordered) {
                if (fragment.position() != position) {
                    if (!placedThisPosition) {
                        return SignatureExpression.NO_MATCH;
                    }
                    position = fragment.position();
                    placedThisPosition = false;
                }
                if (placedThisPosition) {
                    continue;
                }
                long boundary = placeFragment(fragment, cursor, rightward);
                if (boundary != SignatureExpression.NO_MATCH) {
                    cursor = boundary;
                    placedThisPosition = true;
                }
            }
            return placedThisPosition ? cursor : SignatureExpression.NO_MATCH;
        }

        /** One fragment at its permitted distances; the new boundary or NO_MATCH. */
        private long placeFragment(Fragment fragment, long cursor, boolean rightward) {
            for (int offset = fragment.minOffset(); offset <= fragment.maxOffset(); offset++) {
                if (rightward) {
                    long at = cursor + offset;
                    note(fragment.expression(), at);
                    int length = fragment.expression().matchAt(bytes, at);
                    if (length != SignatureExpression.NO_MATCH) {
                        return at + length;
                    }
                } else {
                    // A left fragment ends at the cursor less the distance,
                    // so where it starts depends on how long it turns out
                    // to be; each possible length is tried in turn.
                    long end = cursor - offset;
                    for (int length = fragment.expression().minLength();
                            length <= fragment.expression().maxLength(); length++) {
                        long at = end - length;
                        if (at < 0) {
                            break;
                        }
                        note(fragment.expression(), at);
                        if (fragment.expression().matchAt(bytes, at) == length) {
                            return at;
                        }
                    }
                }
            }
            return SignatureExpression.NO_MATCH;
        }

        /**
         * Records that a pattern needed bytes that were not captured.
         *
         * <p>Running off the end of the content is not a blind spot: the
         * content really does stop there, so the pattern really does not
         * fit. Only a reach into the span between the windows leaves the
         * question open.
         */
        private void note(SignatureExpression expression, long at) {
            int length = expression.maxLength();
            if (bytes.observable(at, length)) {
                return;
            }
            if (bytes.sizeKnown() && at + length > bytes.sizeBytes()) {
                return;
            }
            blind = true;
        }
    }
}
