package ai.protomolt.proto.sources;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoSourceSetTest {

    private static ProtoSource proto(String path, String... imports) {
        StringBuilder content = new StringBuilder("syntax = \"proto3\";\n");
        for (String imported : imports) {
            content.append("import \"").append(imported).append("\";\n");
        }
        content.append("message M").append(Math.abs(path.hashCode())).append(" {}\n");
        return new ProtoSource(path, content.toString(), "test:" + path);
    }

    @Test
    void preservesInsertionOrder() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("z.proto"))
                .add(proto("a.proto"))
                .add(proto("m.proto"))
                .build();
        assertThat(set.paths()).containsExactly("z.proto", "a.proto", "m.proto");
    }

    @Test
    void identicalDuplicateIsToleratedFirstOriginWins() {
        ProtoSource first = new ProtoSource("a.proto", "syntax = \"proto3\";", "origin-1");
        ProtoSource second = new ProtoSource("a.proto", "syntax = \"proto3\";", "origin-2");
        ProtoSourceSet set = ProtoSourceSet.builder().add(first).add(second).build();
        assertThat(set.size()).isEqualTo(1);
        assertThat(set.get("a.proto").orElseThrow().origin()).isEqualTo("origin-1");
    }

    @Test
    void conflictingContentFails() {
        assertThatIllegalStateException().isThrownBy(() -> ProtoSourceSet.builder()
                        .add(new ProtoSource("a.proto", "syntax = \"proto3\"; message A {}", "one"))
                        .add(new ProtoSource("a.proto", "syntax = \"proto3\"; message B {}", "two"))
                        .build())
                .withMessageContaining("a.proto")
                .withMessageContaining("one")
                .withMessageContaining("two");
    }

    @Test
    void topologicalOrderPutsImportsFirst() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("app.proto", "common/base.proto", "common/extra.proto"))
                .add(proto("common/base.proto"))
                .add(proto("common/extra.proto", "common/base.proto"))
                .build();
        List<String> order = set.topologicalOrder();
        assertThat(order).containsExactlyInAnyOrder(
                "app.proto", "common/base.proto", "common/extra.proto");
        assertThat(order.indexOf("common/base.proto")).isLessThan(order.indexOf("common/extra.proto"));
        assertThat(order.indexOf("common/extra.proto")).isLessThan(order.indexOf("app.proto"));
    }

    @Test
    void topologicalOrderIgnoresImportsOutsideTheSet() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("app.proto", "google/protobuf/timestamp.proto"))
                .build();
        assertThat(set.topologicalOrder()).containsExactly("app.proto");
    }

    @Test
    void importCycleFailsWithChain() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("a.proto", "b.proto"))
                .add(proto("b.proto", "a.proto"))
                .build();
        assertThatThrownBy(set::topologicalOrder)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void reachableFromFollowsTransitiveImports() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(proto("app.proto", "mid.proto"))
                .add(proto("mid.proto", "leaf.proto"))
                .add(proto("leaf.proto"))
                .add(proto("unrelated.proto"))
                .build();
        ProtoSourceSet reachable = set.reachableFrom("app.proto");
        assertThat(reachable.paths()).containsExactly("app.proto", "mid.proto", "leaf.proto");
    }

    @Test
    void reachableFromUnknownRootFails() {
        assertThatThrownBy(() -> ProtoSourceSet.empty().reachableFrom("nope.proto"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mergeCombinesAndDetectsConflicts() {
        ProtoSourceSet left = ProtoSourceSet.of(List.of(proto("a.proto")));
        ProtoSourceSet right = ProtoSourceSet.of(List.of(proto("b.proto")));
        assertThat(left.merge(right).paths()).containsExactly("a.proto", "b.proto");

        ProtoSourceSet conflicting = ProtoSourceSet.of(List.of(
                new ProtoSource("a.proto", "syntax = \"proto3\"; message Other {}", "elsewhere")));
        assertThatIllegalStateException().isThrownBy(() -> left.merge(conflicting));
    }
}
