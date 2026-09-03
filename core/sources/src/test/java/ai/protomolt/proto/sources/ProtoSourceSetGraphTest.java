package ai.protomolt.proto.sources;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Graph-shaped coverage of {@link ProtoSourceSet}: the import graph view, diamond and cyclic
 * topologies, reachability order, merge semantics, and the immutability of every view the set
 * hands out.
 */
class ProtoSourceSetGraphTest {

    private static ProtoSource proto(String path, String... imports) {
        StringBuilder content = new StringBuilder("syntax = \"proto3\";\n");
        for (String imported : imports) {
            content.append("import \"").append(imported).append("\";\n");
        }
        content.append("message M").append(Math.abs(path.hashCode())).append(" {}\n");
        return new ProtoSource(path, content.toString(), "test:" + path);
    }

    // ---------------------------------------------------------------- import graph

    @Test
    void importGraphIncludesImportsOutsideTheSet() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("app.proto", "common/base.proto", "google/protobuf/timestamp.proto"))
                .add(proto("common/base.proto"))
                .build();
        Map<String, List<String>> graph = set.importGraph();
        assertThat(graph).containsOnlyKeys("app.proto", "common/base.proto");
        assertThat(graph.get("app.proto"))
                .containsExactly("common/base.proto", "google/protobuf/timestamp.proto");
        assertThat(graph.get("common/base.proto")).isEmpty();
    }

    @Test
    void importGraphOfTheEmptySetIsEmpty() {
        assertThat(ProtoSourceSet.empty().importGraph()).isEmpty();
        assertThat(ProtoSourceSet.empty().topologicalOrder()).isEmpty();
    }

    // ---------------------------------------------------------------- topological order

    @Test
    void topologicalOrderVisitsDiamondDependenciesOnce() {
        //     app
        //    /   \
        //   b     c
        //    \   /
        //      d
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("app.proto", "b.proto", "c.proto"))
                .add(proto("b.proto", "d.proto"))
                .add(proto("c.proto", "d.proto"))
                .add(proto("d.proto"))
                .build();
        List<String> order = set.topologicalOrder();
        assertThat(order).containsExactlyInAnyOrder("app.proto", "b.proto", "c.proto", "d.proto");
        assertThat(order.indexOf("d.proto")).isLessThan(order.indexOf("b.proto"));
        assertThat(order.indexOf("d.proto")).isLessThan(order.indexOf("c.proto"));
        assertThat(order.indexOf("b.proto")).isLessThan(order.indexOf("app.proto"));
        assertThat(order.indexOf("c.proto")).isLessThan(order.indexOf("app.proto"));
    }

    @Test
    void topologicalOrderIsIndependentOfInsertionOrder() {
        ProtoSourceSet dependenciesFirst = ProtoSourceSet.builder()
                .add(proto("base.proto"))
                .add(proto("app.proto", "base.proto"))
                .build();
        ProtoSourceSet dependentsFirst = ProtoSourceSet.builder()
                .add(proto("app.proto", "base.proto"))
                .add(proto("base.proto"))
                .build();
        assertThat(dependenciesFirst.topologicalOrder())
                .containsExactly("base.proto", "app.proto");
        assertThat(dependentsFirst.topologicalOrder())
                .containsExactly("base.proto", "app.proto");
    }

    @Test
    void selfImportIsACycle() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("self.proto", "self.proto"))
                .build();
        assertThatThrownBy(set::topologicalOrder)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("self.proto");
    }

    @Test
    void cycleErrorNamesTheChain() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("a.proto", "b.proto"))
                .add(proto("b.proto", "c.proto"))
                .add(proto("c.proto", "a.proto"))
                .build();
        assertThatThrownBy(set::topologicalOrder)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Import cycle");
    }

    // ---------------------------------------------------------------- reachability

    @Test
    void reachableFromPreservesInsertionOrderNotTraversalOrder() {
        // Traversal from app.proto visits z.proto before a.proto; the result must keep the
        // set's insertion order (a before z) instead.
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("a.proto"))
                .add(proto("z.proto"))
                .add(proto("app.proto", "z.proto", "a.proto"))
                .build();
        assertThat(set.reachableFrom("app.proto").paths())
                .containsExactly("a.proto", "z.proto", "app.proto");
    }

    @Test
    void reachableFromALeafIsJustTheLeaf() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("app.proto", "leaf.proto"))
                .add(proto("leaf.proto"))
                .build();
        ProtoSourceSet reachable = set.reachableFrom("leaf.proto");
        assertThat(reachable.paths()).containsExactly("leaf.proto");
        assertThat(reachable.get("leaf.proto")).isPresent();
    }

    @Test
    void reachableFromToleratesSharedDependencies() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("app.proto", "b.proto", "c.proto"))
                .add(proto("b.proto", "d.proto"))
                .add(proto("c.proto", "d.proto"))
                .add(proto("d.proto"))
                .build();
        assertThat(set.reachableFrom("app.proto").paths())
                .containsExactlyInAnyOrder("app.proto", "b.proto", "c.proto", "d.proto");
    }

    // ---------------------------------------------------------------- merge

    @Test
    void mergeKeepsLeftThenRightInsertionOrder() {
        ProtoSourceSet left = ProtoSourceSet.of(List.of(proto("z.proto"), proto("b.proto")));
        ProtoSourceSet right = ProtoSourceSet.of(List.of(proto("a.proto"), proto("y.proto")));
        assertThat(left.merge(right).paths())
                .containsExactly("z.proto", "b.proto", "a.proto", "y.proto");
    }

    @Test
    void mergeOfIdenticalDuplicatesKeepsTheLeftOrigin() {
        ProtoSource first = new ProtoSource("a.proto", "syntax = \"proto3\";", "left");
        ProtoSource second = new ProtoSource("a.proto", "syntax = \"proto3\";", "right");
        ProtoSourceSet merged = ProtoSourceSet.of(List.of(first))
                .merge(ProtoSourceSet.of(List.of(second)));
        assertThat(merged.size()).isEqualTo(1);
        assertThat(merged.get("a.proto").orElseThrow().origin()).isEqualTo("left");
    }

    // ---------------------------------------------------------------- views are immutable

    @Test
    void pathsAndSourcesViewsAreUnmodifiable() {
        ProtoSourceSet set = ProtoSourceSet.of(List.of(proto("a.proto")));
        assertThatThrownBy(() -> set.paths().add("b.proto"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> set.sources().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void topologicalOrderResultIsImmutable() {
        ProtoSourceSet set = ProtoSourceSet.of(List.of(proto("a.proto")));
        assertThatThrownBy(() -> set.topologicalOrder().add("b.proto"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void builderRejectsNullSources() {
        assertThatThrownBy(() -> ProtoSourceSet.builder().add(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void emptySetBehaviors() {
        ProtoSourceSet empty = ProtoSourceSet.empty();
        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.size()).isZero();
        assertThat(empty.get("nope.proto")).isEmpty();
        assertThat(empty.contains("nope.proto")).isFalse();
        assertThat(empty.toString()).contains("ProtoSourceSet");
    }
}
