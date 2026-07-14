package ai.pipestream.proto.compat;

import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.diff;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;

/** Proto2 {@code required} semantics; sources compile through Wire's proto2 support. */
class Proto2RuleTest {

    private static String proto2(String fields) {
        return """
                syntax = "proto2";
                package legacy;
                message Record {
                %s
                }
                """.formatted(fields);
    }

    private static final String BASE = proto2("  required string id = 1;");

    @Test
    void newRequiredFieldBreaksBackward() throws Exception {
        String updated = proto2("  required string id = 1;\n  required string owner = 2;");

        SchemaChange change = single(diff(BASE, updated), "FIELD_REQUIRED_ADDED");
        assertThat(change.path()).isEqualTo("legacy.Record.owner");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.WIRE_BACKWARD, Impact.SOURCE);
    }

    @Test
    void newOptionalFieldInProto2IsInformational() throws Exception {
        String updated = proto2("  required string id = 1;\n  optional string owner = 2;");

        List<SchemaChange> changes = diff(BASE, updated);
        assertThat(single(changes, "FIELD_ADDED").isInformational()).isTrue();
        assertThat(changes).hasSize(1);
    }

    @Test
    void requiredToOptionalBreaksForwardOnly() throws Exception {
        String updated = proto2("  optional string id = 1;");

        SchemaChange change = single(diff(BASE, updated), "FIELD_LABEL_CHANGED");
        assertThat(change.path()).isEqualTo("legacy.Record.id");
        assertThat(change.impacts()).containsExactly(Impact.WIRE_FORWARD);
    }

    @Test
    void optionalToRequiredBreaksBackwardOnly() throws Exception {
        String old = proto2("  optional string id = 1;");

        SchemaChange change = single(diff(old, BASE), "FIELD_LABEL_CHANGED");
        assertThat(change.impacts()).containsExactly(Impact.WIRE_BACKWARD);
    }

    @Test
    void checkerDirectionsHonorRequiredTransitions() throws Exception {
        CompatibilityChecker checker = CompatibilityChecker.create();
        var oldSet = TestSchemas.compile(proto2("  optional string id = 1;"));
        var newSet = TestSchemas.compile(BASE);

        assertThat(checker.check(oldSet, newSet, CompatibilityMode.BACKWARD).isCompatible())
                .isFalse();
        assertThat(checker.check(oldSet, newSet, CompatibilityMode.FORWARD).isCompatible())
                .isTrue();
        assertThat(checker.check(newSet, oldSet, CompatibilityMode.FORWARD).isCompatible())
                .isFalse();
        assertThat(checker.check(newSet, oldSet, CompatibilityMode.BACKWARD).isCompatible())
                .isTrue();
    }
}
