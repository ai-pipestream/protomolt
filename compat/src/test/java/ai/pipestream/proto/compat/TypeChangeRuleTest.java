package ai.pipestream.proto.compat;

import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.diff;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;

/** Field type changes: wire-compatibility groups and message/enum type identity. */
class TypeChangeRuleTest {

    private static String schema(String fieldType) {
        return """
                syntax = "proto3";
                package example;
                message Doc { %s value = 1; }
                """.formatted(fieldType);
    }

    private static List<SchemaChange> typeDiff(String oldType, String newType) throws Exception {
        return diff(schema(oldType), schema(newType));
    }

    private static void assertCompatibleChange(String oldType, String newType) throws Exception {
        SchemaChange change = single(typeDiff(oldType, newType), "FIELD_TYPE_CHANGED_COMPATIBLE");
        assertThat(change.path()).isEqualTo("example.Doc.value");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.JSON_BACKWARD,
                Impact.JSON_FORWARD, Impact.SOURCE);
    }

    private static void assertWireBreakingChange(String oldType, String newType) throws Exception {
        SchemaChange change = single(typeDiff(oldType, newType), "FIELD_TYPE_CHANGED");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    }

    @Test
    void varintGroupIsInterchangeable() throws Exception {
        assertCompatibleChange("int32", "int64");
        assertCompatibleChange("int64", "uint32");
        assertCompatibleChange("uint64", "bool");
    }

    @Test
    void zigzagGroupIsInterchangeable() throws Exception {
        assertCompatibleChange("sint32", "sint64");
    }

    @Test
    void fixed32GroupIsInterchangeable() throws Exception {
        assertCompatibleChange("fixed32", "sfixed32");
    }

    @Test
    void fixed64GroupIsInterchangeable() throws Exception {
        assertCompatibleChange("fixed64", "sfixed64");
    }

    @Test
    void int32ToSint32Breaks() throws Exception {
        assertWireBreakingChange("int32", "sint32");
    }

    @Test
    void fixed32ToFixed64Breaks() throws Exception {
        assertWireBreakingChange("fixed32", "fixed64");
    }

    @Test
    void floatToDoubleBreaks() throws Exception {
        assertWireBreakingChange("float", "double");
    }

    @Test
    void int32ToStringBreaks() throws Exception {
        assertWireBreakingChange("int32", "string");
    }

    @Test
    void sameTypeIsNoChange() throws Exception {
        assertThat(typeDiff("int32", "int32")).isEmpty();
    }

    @Test
    void stringToBytesIsWireSafe() throws Exception {
        assertCompatibleChange("string", "bytes");
    }

    @Test
    void bytesToStringBreaksBackwardOnly() throws Exception {
        SchemaChange change = single(typeDiff("bytes", "string"), "FIELD_TYPE_CHANGED");

        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
        assertThat(change.impacts()).doesNotContain(Impact.WIRE_FORWARD);
    }

    @Test
    void enumToIntIsInterchangeable() throws Exception {
        String withEnum = """
                syntax = "proto3";
                package example;
                enum Status { STATUS_UNSPECIFIED = 0; }
                message Doc { Status value = 1; }
                """;
        String withInt = """
                syntax = "proto3";
                package example;
                enum Status { STATUS_UNSPECIFIED = 0; }
                message Doc { int32 value = 1; }
                """;

        SchemaChange change = single(diff(withEnum, withInt), "FIELD_TYPE_CHANGED_COMPATIBLE");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.JSON_BACKWARD,
                Impact.JSON_FORWARD, Impact.SOURCE);
    }

    @Test
    void messageTypeFqnChangeBreaksEverything() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message A { string id = 1; }
                message B { string id = 1; }
                message Doc { A ref = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message A { string id = 1; }
                message B { string id = 1; }
                message Doc { B ref = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_MESSAGE_TYPE_CHANGED");
        assertThat(change.path()).isEqualTo("example.Doc.ref");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    }

    @Test
    void sameMessageTypeIsNoChange() throws Exception {
        String schema = """
                syntax = "proto3";
                package example;
                message A { string id = 1; }
                message Doc { A ref = 1; }
                """;

        assertThat(diff(schema, schema)).isEmpty();
    }

    @Test
    void enumTypeFqnChangeBreaksEverything() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                enum Color { COLOR_UNSPECIFIED = 0; }
                enum Shade { SHADE_UNSPECIFIED = 0; }
                message Doc { Color tint = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                enum Color { COLOR_UNSPECIFIED = 0; }
                enum Shade { SHADE_UNSPECIFIED = 0; }
                message Doc { Shade tint = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_ENUM_TYPE_CHANGED");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    }

    @Test
    void messageToScalarBreaksEverything() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message A { string id = 1; }
                message Doc { A ref = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message A { string id = 1; }
                message Doc { string ref = 1; }
                """;

        SchemaChange change = single(diff(old, updated), "FIELD_TYPE_CHANGED");
        assertThat(change.impacts()).contains(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }
}
