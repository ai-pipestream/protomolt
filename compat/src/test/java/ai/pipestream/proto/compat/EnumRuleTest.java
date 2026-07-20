package ai.pipestream.proto.compat;

import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.all;
import static ai.pipestream.proto.compat.TestSchemas.diff;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;

/** Enum value rules: number-matched with a name-keyed pass to spot renumbering. */
class EnumRuleTest {

    private static String status(String values) {
        return """
                syntax = "proto3";
                package example;
                enum Status {
                %s
                }
                """.formatted(values);
    }

    @Test
    void enumValueAddedIsInformational() throws Exception {
        List<SchemaChange> changes = diff(
                status("  STATUS_UNSPECIFIED = 0;"),
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_ACTIVE = 1;"));

        SchemaChange change = single(changes, "ENUM_VALUE_ADDED");
        assertThat(change.path()).isEqualTo("example.Status.STATUS_ACTIVE");
        assertThat(change.isInformational()).isTrue();
    }

    @Test
    void enumValueRemovedIsWireCleanButJsonBreaking() throws Exception {
        List<SchemaChange> changes = diff(
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_OLD = 1;"),
                status("  STATUS_UNSPECIFIED = 0;"));

        SchemaChange change = single(changes, "ENUM_VALUE_REMOVED");
        assertThat(change.path()).isEqualTo("example.Status.STATUS_OLD");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.JSON_BACKWARD, Impact.SOURCE);
        assertThat(change.impacts())
                .doesNotContain(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }

    @Test
    void enumValueNumberChangedBreaksEverything() throws Exception {
        List<SchemaChange> changes = diff(
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_ACTIVE = 1;"),
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_ACTIVE = 2;"));

        SchemaChange change = single(changes, "ENUM_VALUE_NUMBER_CHANGED");
        assertThat(change.path()).isEqualTo("example.Status.STATUS_ACTIVE");
        assertThat(change.before()).isEqualTo("STATUS_ACTIVE = 1");
        assertThat(change.after()).isEqualTo("STATUS_ACTIVE = 2");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
        // Not double-reported as a removal plus an addition.
        assertThat(all(changes, "ENUM_VALUE_REMOVED")).isEmpty();
        assertThat(all(changes, "ENUM_VALUE_ADDED")).isEmpty();
    }

    @Test
    void enumValueNameChangedBreaksJsonAndSource() throws Exception {
        List<SchemaChange> changes = diff(
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_OLD = 1;"),
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_NEW = 1;"));

        SchemaChange change = single(changes, "ENUM_VALUE_NAME_CHANGED");
        assertThat(change.path()).isEqualTo("example.Status.STATUS_NEW");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.JSON_BACKWARD,
                Impact.JSON_FORWARD, Impact.SOURCE);
        assertThat(all(changes, "ENUM_VALUE_REMOVED")).isEmpty();
        assertThat(all(changes, "ENUM_VALUE_ADDED")).isEmpty();
    }

    /**
     * Values are matched by number, and under {@code allow_alias} several names share one
     * number, so a dropped alias is invisible to the by-number pass. JSON payloads carry the
     * name, so the removal has to be reported.
     */
    @Test
    void removedEnumAliasIsReportedAsARemoval() throws Exception {
        List<SchemaChange> changes = diff(
                status("  option allow_alias = true;\n  STATUS_UNSPECIFIED = 0;\n"
                        + "  STATUS_ACTIVE = 1;\n  STATUS_ON = 1;"),
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_ACTIVE = 1;"));

        SchemaChange change = single(changes, "ENUM_VALUE_REMOVED");
        assertThat(change.path()).isEqualTo("example.Status.STATUS_ON");
        assertThat(change.before()).isEqualTo("STATUS_ON = 1");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.JSON_BACKWARD, Impact.SOURCE);
    }

    @Test
    void removedFirstAliasIsARemovalNotARename() throws Exception {
        List<SchemaChange> changes = diff(
                status("  option allow_alias = true;\n  STATUS_UNSPECIFIED = 0;\n"
                        + "  STATUS_ACTIVE = 1;\n  STATUS_ON = 1;"),
                status("  STATUS_UNSPECIFIED = 0;\n  STATUS_ON = 1;"));

        assertThat(single(changes, "ENUM_VALUE_REMOVED").path())
                .isEqualTo("example.Status.STATUS_ACTIVE");
        assertThat(all(changes, "ENUM_VALUE_NAME_CHANGED")).isEmpty();
    }

    @Test
    void keptEnumAliasProducesNoChanges() throws Exception {
        String schema = status("  option allow_alias = true;\n  STATUS_UNSPECIFIED = 0;\n"
                + "  STATUS_ACTIVE = 1;\n  STATUS_ON = 1;");
        assertThat(diff(schema, schema)).isEmpty();
    }

    @Test
    void identicalEnumProducesNoChanges() throws Exception {
        String schema = status("  STATUS_UNSPECIFIED = 0;\n  STATUS_ACTIVE = 1;");
        assertThat(diff(schema, schema)).isEmpty();
    }

    @Test
    void nestedEnumMatchedByFullyQualifiedName() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Doc {
                  enum Kind { KIND_UNSPECIFIED = 0; }
                  Kind kind = 1;
                }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Doc {
                  enum Kind { KIND_UNSPECIFIED = 0; KIND_TEXT = 1; }
                  Kind kind = 1;
                }
                """;

        SchemaChange change = single(diff(old, updated), "ENUM_VALUE_ADDED");
        assertThat(change.path()).isEqualTo("example.Doc.Kind.KIND_TEXT");
    }
}
