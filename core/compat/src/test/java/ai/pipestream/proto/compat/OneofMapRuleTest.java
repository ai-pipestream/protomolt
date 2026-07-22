package ai.pipestream.proto.compat;

import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.all;
import static ai.pipestream.proto.compat.TestSchemas.diff;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;

/** Oneof membership rules and map key/value diffing through synthetic entry messages. */
class OneofMapRuleTest {

    @Test
    void existingFieldMovedIntoOneofBreaksWire() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Contact {
                  string phone = 1;
                  string fax = 2;
                }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Contact {
                  oneof channel {
                    string phone = 1;
                    string fax = 2;
                  }
                }
                """;

        List<SchemaChange> changes = diff(old, updated);
        assertThat(all(changes, "FIELD_MOVED_INTO_ONEOF"))
                .hasSize(2)
                .allSatisfy(change -> assertThat(change.impacts()).containsExactlyInAnyOrder(
                        Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD, Impact.SOURCE));
    }

    @Test
    void fieldMovedOutOfOneofBreaksWire() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Contact {
                  oneof channel { string phone = 1; }
                }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Contact { string phone = 1; }
                """;

        List<SchemaChange> changes = diff(old, updated);
        SchemaChange moved = single(changes, "FIELD_MOVED_OUT_OF_ONEOF");
        assertThat(moved.path()).isEqualTo("example.Contact.phone");
        assertThat(moved.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.SOURCE);
        assertThat(single(changes, "ONEOF_REMOVED").impacts()).containsExactly(Impact.SOURCE);
    }

    @Test
    void brandNewOneofOfNewFieldsIsInformational() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Contact { string name = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Contact {
                  string name = 1;
                  oneof channel {
                    string phone = 2;
                    string fax = 3;
                  }
                }
                """;

        List<SchemaChange> changes = diff(old, updated);
        assertThat(single(changes, "ONEOF_ADDED").isInformational()).isTrue();
        assertThat(all(changes, "FIELD_MOVED_INTO_ONEOF")).isEmpty();
        assertThat(all(changes, "FIELD_ADDED")).hasSize(2);
        assertThat(changes).allSatisfy(change -> assertThat(change.isInformational()).isTrue());
    }

    @Test
    void fieldMovedBetweenOneofsBreaksWire() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Contact {
                  oneof channel { string phone = 1; }
                  oneof backup { string fax = 2; }
                }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Contact {
                  oneof channel { string phone = 1; string fax = 2; }
                }
                """;

        List<SchemaChange> changes = diff(old, updated);
        SchemaChange moved = single(changes, "FIELD_MOVED_INTO_ONEOF");
        assertThat(moved.path()).isEqualTo("example.Contact.fax");
        assertThat(moved.impacts()).contains(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }

    @Test
    void mapValueTypeChangeSurfacesWithMapValuePath() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Doc { map<string, int32> attrs = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Doc { map<string, string> attrs = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_TYPE_CHANGED");
        assertThat(change.path()).isEqualTo("example.Doc.attrs (map value)");
        assertThat(change.impacts()).contains(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }

    @Test
    void mapKeyTypeChangeSurfacesWithMapKeyPath() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Doc { map<string, int32> attrs = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Doc { map<int32, int32> attrs = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_TYPE_CHANGED");
        assertThat(change.path()).isEqualTo("example.Doc.attrs (map key)");
    }

    @Test
    void mapValueVarintWideningIsWireCompatible() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Doc { map<string, int32> attrs = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Doc { map<string, int64> attrs = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_TYPE_CHANGED_COMPATIBLE");
        assertThat(change.path()).isEqualTo("example.Doc.attrs (map value)");
        assertThat(change.impacts()).doesNotContain(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }

    @Test
    void mapValueMessageTypeChangeIsCaught() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message A { string id = 1; }
                message B { string id = 1; }
                message Doc { map<string, A> attrs = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message A { string id = 1; }
                message B { string id = 1; }
                message Doc { map<string, B> attrs = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_MESSAGE_TYPE_CHANGED");
        assertThat(change.path()).isEqualTo("example.Doc.attrs (map value)");
    }

    @Test
    void mapToRepeatedMessageIsCaught() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Doc { map<string, string> attrs = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Attr {
                  string key = 1;
                  string value = 2;
                }
                message Doc { repeated Attr attrs = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_MAP_ENTRY_CHANGED");
        assertThat(change.path()).isEqualTo("example.Doc.attrs");
        assertThat(change.impacts()).contains(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }

    @Test
    void unchangedMapProducesNoChanges() throws Exception {
        String schema = """
                syntax = "proto3";
                package example;
                message Doc { map<string, int32> attrs = 1; }
                """;

        assertThat(diff(schema, schema)).isEmpty();
    }
}
