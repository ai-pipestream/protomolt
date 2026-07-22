package ai.pipestream.proto.registry;

import ai.pipestream.proto.compat.ChangeRules;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompatibilityWriteGateTest {

    private static final String V1 = """
            syntax = "proto3";
            package example;
            message Person {
              string name = 1;
              int32 age = 2;
            }
            """;

    /** Compatible with {@link #V1}: adds a field. */
    private static final String V2_COMPATIBLE = """
            syntax = "proto3";
            package example;
            message Person {
              string name = 1;
              int32 age = 2;
              string email = 3;
            }
            """;

    /** Incompatible with {@link #V1}: changes the type of field 2 across wire groups. */
    private static final String V2_INCOMPATIBLE = """
            syntax = "proto3";
            package example;
            message Person {
              string name = 1;
              string age = 2;
            }
            """;

    private static InMemorySchemaRegistryStore storeWithGate() {
        return new InMemorySchemaRegistryStore(new CompatibilityWriteGate());
    }

    @Test
    void firstRegistrationIsNeverGated() throws Exception {
        try (var store = storeWithGate()) {
            assertThatCode(() -> store.register("person", V1, List.of()))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void compatibleEvolutionIsAccepted() throws Exception {
        try (var store = storeWithGate()) {
            store.register("person", V1, List.of());
            StoredSchema v2 = store.register("person", V2_COMPATIBLE, List.of());
            assertThat(v2.version()).isEqualTo(2);
        }
    }

    @Test
    void incompatibleEvolutionIsRejectedWithRuleAndPath() throws Exception {
        try (var store = storeWithGate()) {
            store.register("person", V1, List.of());
            assertThatThrownBy(() -> store.register("person", V2_INCOMPATIBLE, List.of()))
                    .isInstanceOf(IncompatibleRegistrationException.class)
                    .hasMessageContaining(ChangeRules.FIELD_TYPE_CHANGED)
                    .hasMessageContaining("example.Person");
        }
    }

    @Test
    void noneModeSkipsTheGate() throws Exception {
        try (var store = storeWithGate()) {
            store.register("person", V1, List.of());
            store.setCompatibilityMode("person", "NONE");
            assertThatCode(() -> store.register("person", V2_INCOMPATIBLE, List.of()))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void transitiveModeChecksTheWholeHistory() throws Exception {
        String v2Removed = """
                syntax = "proto3";
                package example;
                message Person {
                  string name = 1;
                }
                """;
        String v3Reused = """
                syntax = "proto3";
                package example;
                message Person {
                  string name = 1;
                  string age = 2;
                }
                """;
        // Against the latest only (v2, which lacks field 2) the reuse looks like a plain
        // addition; only the transitive check against v1 sees the type change.
        try (var plain = storeWithGate()) {
            plain.register("person", V1, List.of());
            plain.register("person", v2Removed, List.of());
            assertThatCode(() -> plain.register("person", v3Reused, List.of()))
                    .doesNotThrowAnyException();
        }
        try (var transitive = storeWithGate()) {
            transitive.setGlobalCompatibilityMode("BACKWARD_TRANSITIVE");
            transitive.register("person", V1, List.of());
            transitive.register("person", v2Removed, List.of());
            assertThatThrownBy(() -> transitive.register("person", v3Reused, List.of()))
                    .isInstanceOf(IncompatibleRegistrationException.class)
                    .hasMessageContaining("v1")
                    .hasMessageContaining(ChangeRules.FIELD_TYPE_CHANGED);
        }
    }

    // ------------------------------------------------- degraded comparisons

    /**
     * A candidate whose references do not resolve is a violation, not a thrown error: the gate
     * has to return a list so the store can report every problem at once, and the store's own
     * compile step then raises the typed failure.
     */
    @Test
    void aCandidateThatDoesNotResolveIsReportedAsAViolation() throws Exception {
        try (var store = storeWithGate()) {
            store.register("person", V1, List.of());
            List<StoredSchema> history = List.of(store.latest("person").orElseThrow());
            SchemaReference dangling = new SchemaReference("missing.proto", "missing.proto", 1);

            List<String> violations = new CompatibilityWriteGate().validate(
                    "person", "BACKWARD", history, V2_COMPATIBLE, List.of(dangling), store);

            assertThat(violations).singleElement().asString()
                    .startsWith("candidate does not resolve: ")
                    .contains("missing.proto")
                    .contains("version 1 does not exist in the store");
        }
    }

    /**
     * When one historical version cannot be compared the gate reports that version and keeps
     * going, so a single unreadable version does not mask violations in the others.
     */
    @Test
    void aHistoricalVersionThatCannotBeComparedIsReportedPerVersion() throws Exception {
        try (var store = storeWithGate()) {
            store.register("person", V1, List.of());
            StoredSchema real = store.latest("person").orElseThrow();

            String garbage = "this is not a proto file";
            StoredSchema uncompilable = new StoredSchema("person", 7, 99, garbage, List.of(),
                    SchemaContents.contentHash(garbage, List.of()));
            StoredSchema unresolvable = new StoredSchema("person", 8, 98, V1,
                    List.of(new SchemaReference("gone.proto", "gone.proto", 3)),
                    SchemaContents.contentHash(V1, List.of()));

            List<String> violations = new CompatibilityWriteGate().validate(
                    "person", "BACKWARD_TRANSITIVE",
                    List.of(uncompilable, unresolvable, real),
                    V2_INCOMPATIBLE, List.of(), store);

            assertThat(violations).hasSize(3);
            assertThat(violations.get(0)).startsWith("v7: comparison failed: ")
                    .contains("Failed to compile old schema sources");
            assertThat(violations.get(1)).startsWith("v8: comparison failed: ")
                    .contains("gone.proto");
            // The version that could be compared still yields its real rule violation.
            assertThat(violations.get(2)).startsWith("v1: " + ChangeRules.FIELD_TYPE_CHANGED)
                    .contains("example.Person");
        }
    }

    @Test
    void referencesResolveDuringComparison() throws Exception {
        String common = """
                syntax = "proto3";
                package common;
                message Id { string value = 1; }
                """;
        String appV1 = """
                syntax = "proto3";
                package app;
                import "common.proto";
                message Doc { common.Id id = 1; }
                """;
        String appV2Incompatible = """
                syntax = "proto3";
                package app;
                import "common.proto";
                message Doc { string id = 1; }
                """;
        try (var store = storeWithGate()) {
            store.register("common.proto", common, List.of());
            List<SchemaReference> refs = List.of(new SchemaReference("common.proto", "common.proto", 1));
            store.register("app", appV1, refs);
            assertThatThrownBy(() -> store.register("app", appV2Incompatible, refs))
                    .isInstanceOf(IncompatibleRegistrationException.class)
                    .hasMessageContaining(ChangeRules.FIELD_TYPE_CHANGED);
        }
    }
}
