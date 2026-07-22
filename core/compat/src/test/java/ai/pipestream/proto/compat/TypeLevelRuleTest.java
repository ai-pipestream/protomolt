package ai.pipestream.proto.compat;

import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.all;
import static ai.pipestream.proto.compat.TestSchemas.diff;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;

/** Message-, enum- and file-level rules: type add/remove and FQN-based matching. */
class TypeLevelRuleTest {

    private static final String TWO_MESSAGES = """
            syntax = "proto3";
            package example;
            message Person { string name = 1; }
            message Extra { int32 value = 1; }
            """;
    private static final String ONE_MESSAGE = """
            syntax = "proto3";
            package example;
            message Person { string name = 1; }
            """;

    @Test
    void messageRemoved() throws Exception {
        SchemaChange change = single(diff(TWO_MESSAGES, ONE_MESSAGE), "MESSAGE_REMOVED");

        assertThat(change.path()).isEqualTo("example.Extra");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    }

    @Test
    void messageAddedIsInformational() throws Exception {
        SchemaChange change = single(diff(ONE_MESSAGE, TWO_MESSAGES), "MESSAGE_ADDED");

        assertThat(change.path()).isEqualTo("example.Extra");
        assertThat(change.isInformational()).isTrue();
    }

    @Test
    void enumRemoved() throws Exception {
        String withEnum = ONE_MESSAGE + "enum Status { STATUS_UNSPECIFIED = 0; }\n";
        SchemaChange change = single(diff(withEnum, ONE_MESSAGE), "ENUM_REMOVED");

        assertThat(change.path()).isEqualTo("example.Status");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    }

    @Test
    void enumAddedIsInformational() throws Exception {
        String withEnum = ONE_MESSAGE + "enum Status { STATUS_UNSPECIFIED = 0; }\n";
        SchemaChange change = single(diff(ONE_MESSAGE, withEnum), "ENUM_ADDED");

        assertThat(change.path()).isEqualTo("example.Status");
        assertThat(change.isInformational()).isTrue();
    }

    @Test
    void identicalSchemasProduceNoChanges() throws Exception {
        assertThat(diff(TWO_MESSAGES, TWO_MESSAGES)).isEmpty();
    }

    @Test
    void typeMovedBetweenFilesWithUnchangedShapeProducesNoChanges() throws Exception {
        var oldSet = TestSchemas.compileFiles(
                "a.proto", """
                        syntax = "proto3";
                        package example;
                        message Person { string name = 1; repeated int32 scores = 2; }
                        """,
                "b.proto", """
                        syntax = "proto3";
                        package example;
                        message Other { string id = 1; }
                        """);
        var newSet = TestSchemas.compileFiles(
                "a.proto", """
                        syntax = "proto3";
                        package example;
                        message Other { string id = 1; }
                        """,
                "b.proto", """
                        syntax = "proto3";
                        package example;
                        message Person { string name = 1; repeated int32 scores = 2; }
                        """);

        assertThat(SchemaDiff.diff(oldSet, newSet)).isEmpty();
    }

    @Test
    void nestedMessageMatchedByFullyQualifiedName() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Outer {
                  message Inner { string id = 1; }
                  Inner inner = 1;
                }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Outer {
                  message Inner { int64 id = 1; }
                  Inner inner = 1;
                }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_TYPE_CHANGED");
        assertThat(change.path()).isEqualTo("example.Outer.Inner.id");
    }

    @Test
    void syntheticMapEntryIsNotReportedAsMessageRemoval() throws Exception {
        String withMap = """
                syntax = "proto3";
                package example;
                message Doc { map<string, int32> attrs = 1; }
                """;
        String without = """
                syntax = "proto3";
                package example;
                message Doc { reserved 1; }
                """;

        List<SchemaChange> changes = diff(withMap, without);
        assertThat(all(changes, "MESSAGE_REMOVED")).isEmpty();
        assertThat(single(changes, "FIELD_REMOVED").path()).isEqualTo("example.Doc.attrs");
    }
}
