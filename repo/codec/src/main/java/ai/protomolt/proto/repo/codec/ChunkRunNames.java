package ai.protomolt.proto.repo.codec;

import java.util.HashMap;
import java.util.HashSet;

/** Existing first-unused suffix rules, shared by splitting and validation. */
final class ChunkRunNames {
    private final HashSet<String> used = new HashSet<>();
    private final HashMap<String, Long> nextSuffix = new HashMap<>();

    static String key(String raw, long index) { return raw.isBlank() ? "set-" + index : raw; }

    String claim(String raw) {
        if (used.add(raw)) return raw;
        long suffix = nextSuffix.getOrDefault(raw, 2L);
        String candidate;
        do { candidate = raw + "#" + suffix++; } while (!used.add(candidate));
        nextSuffix.put(raw, suffix);
        return candidate;
    }
}
