package ai.protomolt.proto.authz;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The headers of one inbound request, as a {@link RequestCallerResolver} reads them. Names
 * match case-insensitively, as HTTP header names do, and a header the request did not carry
 * reads as an empty list, never null. A host adapts its own request type to this view, so a
 * resolver needs no particular HTTP server on its classpath.
 */
@FunctionalInterface
public interface RequestHeaders {

    /**
     * Every value the request carried for a header, in arrival order.
     *
     * @param name the header name, matched case-insensitively
     * @return the values, or an empty list when the request did not carry the header
     */
    List<String> values(String name);

    /**
     * The first value the request carried for a header.
     *
     * @param name the header name, matched case-insensitively
     * @return the first value, or empty when the request did not carry the header
     */
    default Optional<String> first(String name) {
        List<String> values = values(name);
        return values.isEmpty() ? Optional.empty() : Optional.of(values.getFirst());
    }

    /**
     * An immutable view over {@code headers}, with names folded case-insensitively. Values
     * under names that differ only in case are concatenated in iteration order.
     *
     * @param headers header name to its values; no null names, lists or values
     * @return the view
     */
    static RequestHeaders of(Map<String, ? extends List<String>> headers) {
        Objects.requireNonNull(headers, "headers");
        Map<String, List<String>> folded = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        headers.forEach((name, values) -> {
            Objects.requireNonNull(name, "header name");
            Objects.requireNonNull(values, "values of header " + name);
            for (String value : values) {
                Objects.requireNonNull(value, "value of header " + name);
            }
            folded.computeIfAbsent(name, key -> new ArrayList<>()).addAll(values);
        });
        Map<String, List<String>> frozen = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        folded.forEach((name, values) -> frozen.put(name, List.copyOf(values)));
        return name -> frozen.getOrDefault(Objects.requireNonNull(name, "name"), List.of());
    }
}
