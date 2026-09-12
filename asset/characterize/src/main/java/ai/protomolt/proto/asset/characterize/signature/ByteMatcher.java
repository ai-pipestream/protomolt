package ai.protomolt.proto.asset.characterize.signature;

import java.util.List;

/**
 * A test on one byte. The signature syntax builds patterns out of these:
 * a literal value, a value inside a range, a value with certain bits set,
 * a value drawn from a set, or any value at all.
 *
 * <p>Bytes arrive as unsigned ints in {@code 0..255}, because a signature
 * that speaks of {@code FF} means 255, not -1.
 */
@FunctionalInterface
public interface ByteMatcher {

    /** Matches any byte. */
    ByteMatcher ANY = value -> true;

    /**
     * Whether a byte satisfies this test.
     *
     * @param value the byte as an unsigned int in {@code 0..255}
     * @return true when it matches
     */
    boolean matches(int value);

    /**
     * One exact value.
     *
     * @param value the byte to match
     * @return the matcher
     */
    static ByteMatcher exact(int value) {
        int wanted = value & 0xFF;
        return candidate -> candidate == wanted;
    }

    /**
     * An inclusive range of values.
     *
     * @param low the lowest value that matches
     * @param high the highest value that matches
     * @return the matcher
     */
    static ByteMatcher range(int low, int high) {
        int from = Math.min(low & 0xFF, high & 0xFF);
        int to = Math.max(low & 0xFF, high & 0xFF);
        return candidate -> candidate >= from && candidate <= to;
    }

    /**
     * Every value sharing the bits the mask sets.
     *
     * @param mask the bits that must be set
     * @return the matcher
     */
    static ByteMatcher bitmask(int mask) {
        int bits = mask & 0xFF;
        return candidate -> (candidate & bits) == bits;
    }

    /**
     * The union of several tests.
     *
     * @param members the tests to union; must not be empty
     * @return the matcher
     */
    static ByteMatcher anyOf(List<ByteMatcher> members) {
        List<ByteMatcher> copy = List.copyOf(members);
        if (copy.size() == 1) {
            return copy.getFirst();
        }
        return candidate -> {
            for (ByteMatcher member : copy) {
                if (member.matches(candidate)) {
                    return true;
                }
            }
            return false;
        };
    }

    /**
     * The complement of a test.
     *
     * @param matcher the test to invert
     * @return the matcher
     */
    static ByteMatcher not(ByteMatcher matcher) {
        return candidate -> !matcher.matches(candidate);
    }
}
