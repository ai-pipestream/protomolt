package ai.protomolt.proto.emit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An ordered set of rendered files — the unit every emitter produces and every
 * {@link BundleSink} delivers. The mirror image of the gather side's source set: gatherers
 * turn a place into sources, renderers turn schemas (or messages) into a bundle, sinks turn
 * a bundle into a place.
 *
 * <p>Paths are bundle-relative with forward slashes, validated on entry: no leading slash,
 * no {@code ..} segments, no blanks, no duplicates — so no sink ever writes outside the
 * destination the caller chose.</p>
 */
public final class Bundle {

    private final Map<String, byte[]> files;

    /**
     * Package-private rather than private so a test can build a bundle that skips
     * {@link Builder#validate}, which is the only way to drive a sink's own path-escape guard —
     * {@link Builder} refuses those paths, so the guard is unreachable through the public API.
     * Callers outside this package go through {@link #builder()} and its validation.
     */
    Bundle(Map<String, byte[]> files) {
        this.files = files;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Bundle-relative paths in insertion order. */
    public Set<String> paths() {
        return files.keySet();
    }

    /** The file's bytes, or {@code null} when the path is absent. */
    public byte[] file(String path) {
        byte[] content = files.get(path);
        return content == null ? null : content.clone();
    }

    /** The file decoded as UTF-8, or {@code null} when the path is absent. */
    public String text(String path) {
        byte[] content = files.get(path);
        return content == null ? null : new String(content, StandardCharsets.UTF_8);
    }

    public int size() {
        return files.size();
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }

    /** Visits every file in insertion order. */
    public void forEach(java.util.function.BiConsumer<String, byte[]> visitor) {
        files.forEach(visitor);
    }

    public static final class Builder {
        private final Map<String, byte[]> files = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder add(String path, byte[] content) {
            String validated = validate(path);
            Objects.requireNonNull(content, "content");
            if (files.putIfAbsent(validated, content.clone()) != null) {
                throw new IllegalArgumentException("Duplicate bundle path: " + validated);
            }
            return this;
        }

        public Builder add(String path, String utf8Content) {
            return add(path, Objects.requireNonNull(utf8Content, "utf8Content")
                    .getBytes(StandardCharsets.UTF_8));
        }

        public Bundle build() {
            return new Bundle(Collections.unmodifiableMap(new LinkedHashMap<>(files)));
        }

        private static String validate(String path) {
            Objects.requireNonNull(path, "path");
            if (path.isBlank()) {
                throw new IllegalArgumentException("Bundle paths must not be blank");
            }
            String normalized = path.replace('\\', '/');
            if (normalized.startsWith("/") || normalized.endsWith("/")) {
                throw new IllegalArgumentException(
                        "Bundle paths are relative files, not absolute or directories: " + path);
            }
            for (String segment : splitOnSlash(normalized)) {
                if (segment.isBlank() || segment.equals(".") || segment.equals("..")) {
                    throw new IllegalArgumentException(
                            "Bundle path segment '" + segment + "' is not allowed: " + path);
                }
            }
            return normalized;
        }

        /**
         * {@link String#split(String)} (limit 0) on {@code '/'} without regex: an empty
         * input is one empty token, trailing empty tokens are dropped, leading and interior
         * empty tokens are kept.
         */
        private static List<String> splitOnSlash(String path) {
            if (path.isEmpty()) {
                return List.of("");
            }
            List<String> segments = new ArrayList<>();
            int start = 0;
            int slash;
            while ((slash = path.indexOf('/', start)) >= 0) {
                segments.add(path.substring(start, slash));
                start = slash + 1;
            }
            segments.add(path.substring(start));
            int end = segments.size();
            while (end > 0 && segments.get(end - 1).isEmpty()) {
                end--;
            }
            return segments.subList(0, end);
        }
    }
}
