package ai.pipestream.proto.compat;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.all;
import static ai.pipestream.proto.compat.TestSchemas.compile;
import static ai.pipestream.proto.compat.TestSchemas.diff;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;

/** Field-level rules: add/remove, names, presence and labels. */
class FieldRuleTest {

    private static final String BASE = """
            syntax = "proto3";
            package example;
            message Person {
              string name = 1;
              int32 age = 2;
            }
            """;

    @Test
    void fieldAddedIsInformational() throws Exception {
        String updated = """
                syntax = "proto3";
                package example;
                message Person {
                  string name = 1;
                  int32 age = 2;
                  string email = 3;
                }
                """;

        List<SchemaChange> changes = diff(BASE, updated);
        SchemaChange change = single(changes, "FIELD_ADDED");
        assertThat(change.path()).isEqualTo("example.Person.email");
        assertThat(change.isInformational()).isTrue();
        assertThat(changes).hasSize(1);
    }

    @Test
    void fieldRemovedBreaksJsonBackwardAndSourceOnly() throws Exception {
        String updated = """
                syntax = "proto3";
                package example;
                message Person { string name = 1; }
                """;

        SchemaChange change = single(diff(BASE, updated), "FIELD_REMOVED");
        assertThat(change.path()).isEqualTo("example.Person.age");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.JSON_BACKWARD, Impact.SOURCE);
    }

    @Test
    void fieldRemovedWithoutReservationEmitsAdvisory() throws Exception {
        String updated = """
                syntax = "proto3";
                package example;
                message Person { string name = 1; }
                """;

        SchemaChange advisory = single(diff(BASE, updated), "FIELD_REMOVED_NOT_RESERVED");
        assertThat(advisory.path()).isEqualTo("example.Person.age");
        assertThat(advisory.isInformational()).isTrue();
    }

    @Test
    void fieldRemovedWithNumberReservedHasNoAdvisory() throws Exception {
        FileDescriptorSet newSet = TestSchemas.reserveNumbers(compile("""
                syntax = "proto3";
                package example;
                message Person { string name = 1; }
                """), "Person", 2);

        List<SchemaChange> changes = SchemaDiff.diff(compile(BASE), newSet);
        assertThat(all(changes, "FIELD_REMOVED")).hasSize(1);
        assertThat(all(changes, "FIELD_REMOVED_NOT_RESERVED")).isEmpty();
    }

    @Test
    void fieldRemovedWithNameReservedHasNoAdvisory() throws Exception {
        FileDescriptorSet newSet = TestSchemas.reserveNames(compile("""
                syntax = "proto3";
                package example;
                message Person { string name = 1; }
                """), "Person", "age");

        assertThat(all(SchemaDiff.diff(compile(BASE), newSet), "FIELD_REMOVED_NOT_RESERVED"))
                .isEmpty();
    }

    @Test
    void fieldNameChangedKeepsWireBreaksJsonAndSource() throws Exception {
        String updated = """
                syntax = "proto3";
                package example;
                message Person {
                  string full_name = 1;
                  int32 age = 2;
                }
                """;

        List<SchemaChange> changes = diff(BASE, updated);
        SchemaChange change = single(changes, "FIELD_NAME_CHANGED");
        assertThat(change.path()).isEqualTo("example.Person.full_name");
        assertThat(change.before()).isEqualTo("name = 1");
        assertThat(change.after()).isEqualTo("full_name = 1");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.JSON_BACKWARD,
                Impact.JSON_FORWARD, Impact.SOURCE);
        assertThat(changes).hasSize(1); // no FIELD_REMOVED/FIELD_ADDED pair
    }

    @Test
    void explicitJsonNameChangeBreaksJsonOnly() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.withJsonName(compile(BASE),
                "Person", "name", "displayName");
        FileDescriptorSet newSet = compile(BASE);

        SchemaChange change = single(SchemaDiff.diff(oldSet, newSet), "FIELD_JSON_NAME_CHANGED");
        assertThat(change.path()).isEqualTo("example.Person.name");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.JSON_BACKWARD, Impact.JSON_FORWARD);
    }

    @Test
    void unchangedJsonNameProducesNoChange() throws Exception {
        FileDescriptorSet withExplicit = TestSchemas.withJsonName(compile(BASE),
                "Person", "name", "name");

        // Explicit json_name equal to the derived name is not a change.
        assertThat(SchemaDiff.diff(withExplicit, compile(BASE))).isEmpty();
    }

    @Test
    void presenceChangeIsSourceOnly() throws Exception {
        String updated = """
                syntax = "proto3";
                package example;
                message Person {
                  string name = 1;
                  optional int32 age = 2;
                }
                """;

        List<SchemaChange> changes = diff(BASE, updated);
        SchemaChange change = single(changes, "FIELD_PRESENCE_CHANGED");
        assertThat(change.path()).isEqualTo("example.Person.age");
        assertThat(change.impacts()).containsExactly(Impact.SOURCE);
        // The synthetic proto3-optional oneof is invisible to the oneof rules.
        assertThat(all(changes, "FIELD_MOVED_INTO_ONEOF")).isEmpty();
        assertThat(all(changes, "ONEOF_ADDED")).isEmpty();
    }

    @Test
    void singularToRepeatedBreaksEverything() throws Exception {
        String updated = """
                syntax = "proto3";
                package example;
                message Person {
                  string name = 1;
                  repeated int32 age = 2;
                }
                """;

        SchemaChange change = single(diff(BASE, updated), "FIELD_LABEL_CHANGED");
        assertThat(change.path()).isEqualTo("example.Person.age");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    }

    @Test
    void repeatedToSingularBreaksEverything() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Person { repeated string name = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Person { string name = 1; }
                """;

        assertThat(single(diff(old, updated), "FIELD_LABEL_CHANGED").impacts())
                .contains(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }
}
