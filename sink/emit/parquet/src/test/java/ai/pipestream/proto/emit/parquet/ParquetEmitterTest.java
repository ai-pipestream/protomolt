package ai.pipestream.proto.emit.parquet;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Descriptor-driven Parquet, round-tripped: dynamic messages written entirely in memory
 * must read back with every column faithful — nested groups, repeated values, map entries,
 * enum names, unsigned widening, and proto3 presence semantics.
 */
class ParquetEmitterTest {

    private static final String PROTO = """
            syntax = "proto3";
            package pq.test;
            import "google/protobuf/timestamp.proto";
            import "google/protobuf/struct.proto";
            message Reading {
              string sensor = 1;
              int64 count = 2;
              double value = 3;
              bool active = 4;
              bytes raw = 5;
              uint32 unsigned = 6;
              optional string note = 7;
              Unit unit = 8;
              repeated string tags = 9;
              map<string, int64> attrs = 10;
              Location location = 11;
              google.protobuf.Timestamp observed = 12;
              google.protobuf.Struct extra = 13;
            }
            message Location { double lat = 1; double lon = 2; }
            enum Unit { UNIT_UNSPECIFIED = 0; UNIT_CELSIUS = 1; }
            message Node { string name = 1; Node parent = 2; }
            message Choice {
              string label = 1;
              oneof pick { string text = 2; int64 number = 3; }
            }
            """;

    private static FileDescriptor file() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("pq/test/pq.proto", PROTO, "test").build());
        return compiled.descriptorFor("pq/test/pq.proto").orElseThrow();
    }

    private static com.google.protobuf.Message timestamp(FileDescriptor file, long seconds,
                                                         int nanos) {
        Descriptor type = file.findMessageTypeByName("Reading")
                .findFieldByName("observed").getMessageType();
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("seconds"), seconds)
                .setField(type.findFieldByName("nanos"), nanos)
                .build();
    }

    private static DynamicMessage reading(Descriptor type, Descriptor location, int i,
                                          boolean withNote) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("sensor"), "s" + i)
                .setField(type.findFieldByName("count"), (long) i)
                .setField(type.findFieldByName("value"), i + 0.5)
                .setField(type.findFieldByName("active"), i % 2 == 0)
                .setField(type.findFieldByName("raw"), ByteString.copyFrom(new byte[]{1, 2}))
                .setField(type.findFieldByName("unsigned"), -1) // uint32 max as an int bit pattern
                .setField(type.findFieldByName("unit"),
                        type.findFieldByName("unit").getEnumType()
                                .findValueByName("UNIT_CELSIUS"))
                .setField(type.findFieldByName("location"), DynamicMessage.newBuilder(location)
                        .setField(location.findFieldByName("lat"), 40.7)
                        .setField(location.findFieldByName("lon"), -74.0)
                        .build())
                .setField(type.findFieldByName("observed"),
                        timestamp(type.getFile(), 1_700_000_000L, 123_000))
                .setField(type.findFieldByName("extra"), structOf(type));
        builder.addRepeatedField(type.findFieldByName("tags"), "a");
        builder.addRepeatedField(type.findFieldByName("tags"), "b");
        Descriptor entry = type.findFieldByName("attrs").getMessageType();
        builder.addRepeatedField(type.findFieldByName("attrs"), DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "retries")
                .setField(entry.findFieldByName("value"), 3L)
                .build());
        if (withNote) {
            builder.setField(type.findFieldByName("note"), "calibrated");
        }
        return builder.build();
    }

    private static com.google.protobuf.Message structOf(Descriptor reading) {
        Descriptor structType = reading.findFieldByName("extra").getMessageType();
        DynamicMessage.Builder struct = DynamicMessage.newBuilder(structType);
        try {
            com.google.protobuf.util.JsonFormat.parser().merge(
                    "{\"source\": \"probe\", \"level\": 7}", struct);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return struct.build();
    }

    /** ParquetReader over a plain InputFile — no Hadoop filesystem anywhere in the test. */
    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }

    @Test
    void roundTripsEveryColumnShape(@TempDir Path dir) throws Exception {
        FileDescriptor file = file();
        Descriptor type = file.findMessageTypeByName("Reading");
        Descriptor location = file.findMessageTypeByName("Location");

        byte[] parquet = ParquetEmitter.toBytes(type, List.of(
                reading(type, location, 0, true),
                reading(type, location, 1, false)));
        assertThat(new String(parquet, 0, 4, java.nio.charset.StandardCharsets.US_ASCII))
                .isEqualTo("PAR1");

        Path fileOnDisk = dir.resolve("readings.parquet");
        Files.write(fileOnDisk, parquet);

        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(fileOnDisk)).build()) {
            Group first = reader.read();
            assertThat(first.getString("sensor", 0)).isEqualTo("s0");
            assertThat(first.getLong("count", 0)).isZero();
            assertThat(first.getDouble("value", 0)).isEqualTo(0.5);
            assertThat(first.getBoolean("active", 0)).isTrue();
            assertThat(first.getBinary("raw", 0).getBytes()).containsExactly(1, 2);
            // uint32 0xFFFFFFFF widened to int64, sign intact.
            assertThat(first.getLong("unsigned", 0)).isEqualTo(4294967295L);
            assertThat(first.getBinary("unit", 0).toStringUsingUTF8())
                    .isEqualTo("UNIT_CELSIUS");
            Group tags = first.getGroup("tags", 0);
            assertThat(tags.getFieldRepetitionCount("list")).isEqualTo(2);
            assertThat(tags.getGroup("list", 1).getString("element", 0)).isEqualTo("b");
            Group attr = first.getGroup("attrs", 0).getGroup("key_value", 0);
            assertThat(attr.getString("key", 0)).isEqualTo("retries");
            assertThat(attr.getLong("value", 0)).isEqualTo(3L);
            Group loc = first.getGroup("location", 0);
            assertThat(loc.getDouble("lat", 0)).isEqualTo(40.7);
            assertThat(first.getString("note", 0)).isEqualTo("calibrated");
            // Timestamp is a real microsecond column, Struct a JSON string column.
            assertThat(first.getLong("observed", 0)).isEqualTo(1_700_000_000_000_123L);
            assertThat(first.getBinary("extra", 0).toStringUsingUTF8())
                    .contains("\"source\":\"probe\"").contains("\"level\":7");

            Group second = reader.read();
            assertThat(second.getString("sensor", 0)).isEqualTo("s1");
            // The optional field was unset: zero occurrences, not an empty string.
            assertThat(second.getFieldRepetitionCount("note")).isZero();

            assertThat(reader.read()).isNull();
        }
    }

    /**
     * Oneof members track presence without carrying the {@code optional} keyword. Encoding them
     * as required columns would write a proto3 default in place of "not set", leaving no way to
     * tell which arm of the oneof a row actually carried.
     */
    @Test
    void oneofMembersAreOptionalColumnsAndUnsetArmsAreNotWritten(@TempDir Path dir)
            throws Exception {
        Descriptor type = file().findMessageTypeByName("Choice");

        MessageType schema = ProtoParquetSchemas.schema(type);
        assertThat(schema.getType("label").getRepetition())
                .isEqualTo(org.apache.parquet.schema.Type.Repetition.REQUIRED);
        assertThat(schema.getType("text").getRepetition())
                .isEqualTo(org.apache.parquet.schema.Type.Repetition.OPTIONAL);
        assertThat(schema.getType("number").getRepetition())
                .isEqualTo(org.apache.parquet.schema.Type.Repetition.OPTIONAL);

        byte[] parquet = ParquetEmitter.toBytes(type, List.of(
                DynamicMessage.newBuilder(type)
                        .setField(type.findFieldByName("label"), "first")
                        .setField(type.findFieldByName("text"), "chosen").build(),
                DynamicMessage.newBuilder(type)
                        .setField(type.findFieldByName("label"), "second")
                        .setField(type.findFieldByName("number"), 7L).build()));

        Path fileOnDisk = dir.resolve("choices.parquet");
        Files.write(fileOnDisk, parquet);
        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(fileOnDisk)).build()) {
            Group first = reader.read();
            assertThat(first.getString("text", 0)).isEqualTo("chosen");
            assertThat(first.getFieldRepetitionCount("number")).isZero();

            Group second = reader.read();
            assertThat(second.getLong("number", 0)).isEqualTo(7L);
            assertThat(second.getFieldRepetitionCount("text")).isZero();
        }
    }

    @Test
    void recursiveTypesAndForeignMessagesAreRejected() throws Exception {
        FileDescriptor file = file();
        Descriptor node = file.findMessageTypeByName("Node");
        assertThatThrownBy(() -> ParquetEmitter.toBytes(node, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive")
                .hasMessageContaining("pq.test.Node");

        Descriptor reading = file.findMessageTypeByName("Reading");
        Descriptor location = file.findMessageTypeByName("Location");
        assertThatThrownBy(() -> ParquetEmitter.toBytes(reading, List.of(
                DynamicMessage.getDefaultInstance(location))))
                .hasMessageContaining("Expected pq.test.Reading");
    }
}
