package ai.pipestream.proto.kafka.wire;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The wire format is only right if it agrees with the tools that already speak it, so the frames
 * here are cross-checked against Kafka's own {@link ByteUtils#writeVarint}: a round trip through
 * our own encoder and decoder would pass just as happily with both halves wrong, which is exactly
 * how the zigzag bug in the Connect sinks survived.
 */
class ConfluentWireFormatTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.test;
            message First { string a = 1; }
            message Second {
              string b = 1;
              message Inner { string c = 1; }
            }
            message Third { string d = 1; }
            """;

    private static FileDescriptor file;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/test/types.proto", PROTO, "test").build());
        file = compiled.descriptorFor("serde/test/types.proto").orElseThrow();
    }

    /** Byte for byte how Confluent writes the index array. */
    private static byte[] confluentFrame(int schemaId, int[] indexes, byte[] payload) {
        ByteBuffer array = ByteBuffer.allocate(64);
        if (indexes.length == 1 && indexes[0] == 0) {
            array.put((byte) 0);
        } else {
            ByteUtils.writeVarint(indexes.length, array);
            for (int index : indexes) {
                ByteUtils.writeVarint(index, array);
            }
        }
        array.flip();
        ByteBuffer out = ByteBuffer.allocate(5 + array.remaining() + payload.length);
        out.put((byte) 0).putInt(schemaId).put(array).put(payload);
        return out.array();
    }

    @Test
    void framesTheSameBytesConfluentWould() {
        byte[] payload = {1, 2, 3};
        for (int[] indexes : new int[][]{{0}, {1}, {2}, {1, 0}, {2, 3, 1}}) {
            assertThat(ConfluentWireFormat.frame(7, List.of(boxed(indexes)), payload))
                    .as("index path %s", java.util.Arrays.toString(indexes))
                    .isEqualTo(confluentFrame(7, indexes, payload));
        }
    }

    @Test
    void readsBackWhatConfluentWrote() {
        byte[] payload = {(byte) 0xAA, (byte) 0xBB};
        for (int[] indexes : new int[][]{{0}, {1}, {2}, {1, 0}, {2, 3, 1}}) {
            byte[] framed = confluentFrame(99, indexes, payload);
            assertThat(ConfluentWireFormat.schemaId(framed)).isEqualTo(99);
            assertThat(ConfluentWireFormat.messageIndex(framed))
                    .containsExactly(boxed(indexes));
            assertThat(ConfluentWireFormat.payload(framed)).isEqualTo(payload);
        }
    }

    /** A large id uses the high bit: it must stay unsigned across the round trip. */
    @Test
    void carriesLargeSchemaIds() {
        byte[] framed = ConfluentWireFormat.frame(Integer.MAX_VALUE, List.of(0), new byte[]{9});
        assertThat(ConfluentWireFormat.schemaId(framed)).isEqualTo(Integer.MAX_VALUE);
    }

    /** The single-zero-byte optimization is the shape on the wire, not just a decoding detail. */
    @Test
    void writesTheFirstMessageAsALoneZeroByte() {
        byte[] framed = ConfluentWireFormat.frame(1, List.of(0), new byte[]{7});
        assertThat(framed).hasSize(7);
        assertThat(framed[5]).isEqualTo((byte) 0);
        assertThat(ConfluentWireFormat.messageIndex(framed)).containsExactly(0);
    }

    @Test
    void findsTheIndexPathOfATopLevelMessage() {
        assertThat(ConfluentWireFormat.indexPath(file.findMessageTypeByName("First")))
                .containsExactly(0);
        assertThat(ConfluentWireFormat.indexPath(file.findMessageTypeByName("Second")))
                .containsExactly(1);
        assertThat(ConfluentWireFormat.indexPath(file.findMessageTypeByName("Third")))
                .containsExactly(2);
    }

    @Test
    void findsTheIndexPathOfANestedMessage() {
        Descriptor inner = file.findMessageTypeByName("Second").findNestedTypeByName("Inner");
        assertThat(ConfluentWireFormat.indexPath(inner)).containsExactly(1, 0);
        assertThat(ConfluentWireFormat.messageAt(file, List.of(1, 0))).isSameAs(inner);
    }

    @Test
    void resolvesAnIndexPathBackToItsMessage() {
        assertThat(ConfluentWireFormat.messageAt(file, List.of(2)))
                .isSameAs(file.findMessageTypeByName("Third"));
        assertThat(ConfluentWireFormat.messageAt(file, List.of(9))).isNull();
        assertThat(ConfluentWireFormat.messageAt(file, List.of(0, 4))).isNull();
    }

    @Test
    void rejectsBytesThatAreNotFramed() {
        assertThatThrownBy(() -> ConfluentWireFormat.payload(new byte[]{1, 2, 3, 4, 5, 6}))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Not Confluent wire format");
        assertThatThrownBy(() -> ConfluentWireFormat.schemaId(new byte[]{0, 1}))
                .isInstanceOf(ConfluentWireFormatException.class);
    }

    private static Integer[] boxed(int[] values) {
        Integer[] out = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = values[i];
        }
        return out;
    }
}
