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
            }
            message Location { double lat = 1; double lon = 2; }
            enum Unit { UNIT_UNSPECIFIED = 0; UNIT_CELSIUS = 1; }
            message Node { string name = 1; Node parent = 2; }
            """;

    private static FileDescriptor file() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("pq/test/pq.proto", PROTO, "test").build());
        return compiled.descriptorFor("pq/test/pq.proto").orElseThrow();
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
                        .build());
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
            assertThat(first.getFieldRepetitionCount("tags")).isEqualTo(2);
            assertThat(first.getString("tags", 1)).isEqualTo("b");
            Group attr = first.getGroup("attrs", 0);
            assertThat(attr.getString("key", 0)).isEqualTo("retries");
            assertThat(attr.getLong("value", 0)).isEqualTo(3L);
            Group loc = first.getGroup("location", 0);
            assertThat(loc.getDouble("lat", 0)).isEqualTo(40.7);
            assertThat(first.getString("note", 0)).isEqualTo("calibrated");

            Group second = reader.read();
            assertThat(second.getString("sensor", 0)).isEqualTo("s1");
            // The optional field was unset: zero occurrences, not an empty string.
            assertThat(second.getFieldRepetitionCount("note")).isZero();

            assertThat(reader.read()).isNull();
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
