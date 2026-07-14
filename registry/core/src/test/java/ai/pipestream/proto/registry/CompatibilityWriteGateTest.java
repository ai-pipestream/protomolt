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
