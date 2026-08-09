package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link StructLike} view of a proto message that a {@code PartitionKey} reads: every
 * supported column type converts to Iceberg's internal representation, unset message fields
 * and unknown columns read as null, proto3 scalar defaults come through as values, a struct
 * column is refused as a partition source, and the row rejects writes. The unsigned-widening
 * case lives in {@link IcebergPartitionsTest}; these are the branches it does not reach.
 */
class IcebergPartitionsRowTest {

    private static Descriptor row;
    private static Schema schema;

    @BeforeAll
    static void compile() throws Exception {
        row = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                        .add("pr/row.proto", """
                                syntax = "proto3";
                                package pr;
                                import "google/protobuf/timestamp.proto";
                                enum Color { UNKNOWN = 0; RED = 1; }
                                message Row {
                                  bool flag = 1;
                                  int32 n = 2;
                                  int64 big = 3;
                                  float ratio = 4;
                                  double score = 5;
                                  string name = 6;
                                  Color color = 7;
                                  bytes blob = 8;
                                  google.protobuf.Timestamp at = 9;
                                  Child child = 10;
                                }
                                message Child { string x = 1; }
                                """, "test").build())
                .descriptorFor("pr/row.proto").orElseThrow().findMessageTypeByName("Row");
        schema = IcebergSchemas.fromDescriptor(row);
    }

    private static int pos(String column) {
        List<Types.NestedField> columns = schema.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(column)) {
                return i;
            }
        }
        throw new IllegalArgumentException("no column " + column);
    }

    private static DynamicMessage fullRow() {
        Descriptor timestamp = row.findFieldByName("at").getMessageType();
        return DynamicMessage.newBuilder(row)
                .setField(row.findFieldByName("flag"), true)
                .setField(row.findFieldByName("n"), 42)
                .setField(row.findFieldByName("big"), 9_876_543_210L)
                .setField(row.findFieldByName("ratio"), 1.5f)
                .setField(row.findFieldByName("score"), 2.75)
                .setField(row.findFieldByName("name"), "abc")
                .setField(row.findFieldByName("color"),
                        row.findFieldByName("color").getEnumType().findValueByName("RED"))
                .setField(row.findFieldByName("blob"), ByteString.copyFrom(new byte[]{1, 2, 3}))
                .setField(row.findFieldByName("at"), DynamicMessage.newBuilder(timestamp)
                        .setField(timestamp.findFieldByName("seconds"), 1_700_000_000L)
                        .setField(timestamp.findFieldByName("nanos"), 123_456_789)
                        .build())
                .build();
    }

    @Test
    void theRowHasOneSlotPerSchemaColumn() {
        assertThat(IcebergPartitions.row(fullRow(), schema).size())
                .isEqualTo(schema.columns().size());
    }

    @Test
    void everySupportedTypeConvertsToIcebergsInternalRepresentation() {
        StructLike view = IcebergPartitions.row(fullRow(), schema);

        assertThat(view.get(pos("flag"), Boolean.class)).isEqualTo(true);
        assertThat(view.get(pos("n"), Integer.class)).isEqualTo(42);
        assertThat(view.get(pos("big"), Long.class)).isEqualTo(9_876_543_210L);
        assertThat(view.get(pos("ratio"), Float.class)).isEqualTo(1.5f);
        assertThat(view.get(pos("score"), Double.class)).isEqualTo(2.75);
        assertThat(view.get(pos("name"), String.class)).isEqualTo("abc");
        // Enums partition on their symbolic name, matching the string column they land in.
        assertThat(view.get(pos("color"), String.class)).isEqualTo("RED");
        assertThat(view.get(pos("blob"), ByteBuffer.class))
                .isEqualTo(ByteBuffer.wrap(new byte[]{1, 2, 3}));
        // Timestamps become epoch micros: seconds * 1e6 + nanos / 1000.
        assertThat(view.get(pos("at"), Long.class)).isEqualTo(1_700_000_000_123_456L);
    }

    @Test
    void anUnsetMessageFieldReadsAsNull() {
        DynamicMessage message = DynamicMessage.newBuilder(row)
                .setField(row.findFieldByName("name"), "abc")
                .build();
        assertThat(IcebergPartitions.row(message, schema).get(pos("at"), Object.class)).isNull();
    }

    @Test
    void anUnsetScalarReadsAsItsProto3DefaultNotNull() {
        DynamicMessage empty = DynamicMessage.getDefaultInstance(row);
        StructLike view = IcebergPartitions.row(empty, schema);
        assertThat(view.get(pos("n"), Integer.class)).isEqualTo(0);
        assertThat(view.get(pos("name"), String.class)).isEmpty();
        assertThat(view.get(pos("flag"), Boolean.class)).isEqualTo(false);
    }

    @Test
    void aSchemaColumnTheDescriptorDoesNotHaveReadsAsNull() {
        Schema wider = new Schema(
                Types.NestedField.optional(1, "name", Types.StringType.get()),
                Types.NestedField.optional(2, "ghost", Types.StringType.get()));
        DynamicMessage message = DynamicMessage.newBuilder(row)
                .setField(row.findFieldByName("name"), "abc")
                .build();
        StructLike view = IcebergPartitions.row(message, wider);
        assertThat(view.get(0, String.class)).isEqualTo("abc");
        assertThat(view.get(1, Object.class)).isNull();
    }

    @Test
    void aStructColumnIsRefusedAsAPartitionSource() {
        // The refusal only fires when the struct actually carries a value; an unset message
        // field reads as null before type conversion is ever consulted.
        Descriptor childType = row.findFieldByName("child").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(row)
                .setField(row.findFieldByName("child"), DynamicMessage.newBuilder(childType)
                        .setField(childType.findFieldByName("x"), "nested")
                        .build())
                .build();
        assertThatThrownBy(() ->
                IcebergPartitions.row(message, schema).get(pos("child"), Object.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("child")
                .hasMessageContaining("cannot be a partition source");
    }

    @Test
    void theRowIsReadOnly() {
        assertThatThrownBy(() -> IcebergPartitions.row(fullRow(), schema).set(0, true))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("read-only");
    }
}
