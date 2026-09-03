package ai.protomolt.proto.sources;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * An immutable set of {@code .proto} sources keyed by import path — the unit of work shared by
 * gatherers (which produce one), the compiler (which turns one into descriptors) and schema
 * publishers (which register one with a registry).
 *
 * <p>Import paths inside the set resolve against each other, so a set is self-contained apart
 * from {@code google/protobuf/*} well-known imports, which the compiler supplies. Insertion
 * order is preserved and deterministic.</p>
 */
public final class ProtoSourceSet {

    private final Map<String, ProtoSource> sources;

    private ProtoSourceSet(Map<String, ProtoSource> sources) {
        this.sources = sources;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Creates a set from the given sources; duplicate paths with differing content fail. */
    public static ProtoSourceSet of(Collection<ProtoSource> sources) {
        Builder builder = builder();
        sources.forEach(builder::add);
        return builder.build();
    }

    public static ProtoSourceSet empty() {
        return new ProtoSourceSet(Map.of());
    }

    public boolean isEmpty() {
        return sources.isEmpty();
    }

    public int size() {
        return sources.size();
    }

    public Set<String> paths() {
        return sources.keySet();
    }

    public Collection<ProtoSource> sources() {
        return sources.values();
    }

    public Optional<ProtoSource> get(String path) {
        return Optional.ofNullable(sources.get(path));
    }

    public boolean contains(String path) {
        return sources.containsKey(path);
    }

    /**
     * Imports declared by each file in the set, including imports the set does not contain
     * (typically {@code google/protobuf/*}).
     */
    public Map<String, List<String>> importGraph() {
        Map<String, List<String>> graph = LinkedHashMap.newLinkedHashMap(sources.size());
        for (ProtoSource source : sources.values()) {
            graph.put(source.path(), ProtoImports.of(source.content()));
        }
        return graph;
    }

    /**
     * Paths in reverse-topological order: every file appears after all of its imports that are
     * present in this set. This is the registration order schema registries require —
     * references first. Fails on import cycles within the set.
     *
     * @throws IllegalStateException on an import cycle, naming the cycle members
     */
    public List<String> topologicalOrder() {
        Map<String, List<String>> graph = importGraph();
        List<String> order = new ArrayList<>(sources.size());
        Set<String> done = new HashSet<>();
        Deque<String> inProgress = new ArrayDeque<>();
        for (String path : sources.keySet()) {
            visit(path, graph, done, inProgress, order);
        }
        return List.copyOf(order);
    }

    private void visit(String path, Map<String, List<String>> graph, Set<String> done,
                       Deque<String> inProgress, List<String> order) {
        if (done.contains(path) || !sources.containsKey(path)) {
            return;
        }
        if (inProgress.contains(path)) {
            throw new IllegalStateException("Import cycle involving " + path
                    + " (chain: " + inProgress + ")");
        }
        inProgress.push(path);
        try {
            for (String dependency : graph.getOrDefault(path, List.of())) {
                visit(dependency, graph, done, inProgress, order);
            }
        } finally {
            inProgress.pop();
        }
        done.add(path);
        order.add(path);
    }

    /**
     * The subset reachable from {@code rootPath} by following imports present in this set
     * (including the root itself).
     */
    public ProtoSourceSet reachableFrom(String rootPath) {
        ProtoSource root = sources.get(rootPath);
        if (root == null) {
            throw new IllegalArgumentException("No source for path " + rootPath);
        }
        Set<String> reachable = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>();
        frontier.push(rootPath);
        while (!frontier.isEmpty()) {
            String path = frontier.pop();
            ProtoSource source = sources.get(path);
            if (source == null || !reachable.add(path)) {
                continue;
            }
            ProtoImports.of(source.content()).forEach(frontier::push);
        }
        Builder builder = builder();
        // Preserve this set's insertion order rather than traversal order.
        for (ProtoSource source : sources.values()) {
            if (reachable.contains(source.path())) {
                builder.add(source);
            }
        }
        return builder.build();
    }

    /**
     * Merges this set with another. The same path may appear in both only with identical
     * content (first origin wins); differing content is a conflict.
     *
     * @throws IllegalStateException when both sets define a path with different content
     */
    public ProtoSourceSet merge(ProtoSourceSet other) {
        Builder builder = builder();
        sources.values().forEach(builder::add);
        other.sources.values().forEach(builder::add);
        return builder.build();
    }

    @Override
    public String toString() {
        return "ProtoSourceSet" + sources.keySet();
    }

    public static final class Builder {

        private final Map<String, ProtoSource> sources = new LinkedHashMap<>();

        public Builder add(ProtoSource source) {
            Objects.requireNonNull(source, "source");
            ProtoSource existing = sources.get(source.path());
            if (existing != null) {
                if (!existing.content().equals(source.content())) {
                    throw new IllegalStateException("Conflicting content for path "
                            + source.path() + " (origins: " + existing.origin()
                            + " vs " + source.origin() + ")");
                }
                return this; // identical duplicate: first origin wins
            }
            sources.put(source.path(), source);
            return this;
        }

        public Builder add(String path, String content, String origin) {
            return add(new ProtoSource(path, content, origin));
        }

        public ProtoSourceSet build() {
            // Map.copyOf would drop insertion order; the class promises determinism.
            return new ProtoSourceSet(Collections.unmodifiableMap(new LinkedHashMap<>(sources)));
        }
    }
}
