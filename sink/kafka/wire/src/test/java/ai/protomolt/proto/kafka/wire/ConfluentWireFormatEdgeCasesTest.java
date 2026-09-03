package ai.protomolt.proto.kafka.wire;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The hostile half of the wire format: frames a broken writer produces, and the index encodings
 * that only differ from the naive reading away from zero. {@link ConfluentWireFormatTest} proves
 * agreement with Confluent on well-formed frames; these pin down what a malformed frame is told,
 * and that zigzag multi-byte and negative indexes survive the round trip.
 */
class ConfluentWireFormatEdgeCasesTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.test.deep;
            message Outer {
              message Middle {
                message Core { string v = 1; }
              }
            }
            message Sibling { string s = 1; }
            """;

    private static FileDescriptor file;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/test/deep/types.proto", PROTO, "test").build());
        file = compiled.descriptorFor("serde/test/deep/types.proto").orElseThrow();
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
    void rejectsNullFrames() {
        assertThatThrownBy(() -> ConfluentWireFormat.schemaId(null))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Not Confluent wire format");
        assertThatThrownBy(() -> ConfluentWireFormat.messageIndex(null))
                .isInstanceOf(ConfluentWireFormatException.class);
        assertThatThrownBy(() -> ConfluentWireFormat.payload(null))
                .isInstanceOf(ConfluentWireFormatException.class);
    }

    /** Five bytes is a magic byte plus a truncated schema id: still not a frame. */
    @Test
    void rejectsFramesTooShortForThePrefixEvenWithTheRightMagic() {
        assertThatThrownBy(() -> ConfluentWireFormat.schemaId(new byte[]{0, 1, 2, 3, 4}))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Not Confluent wire format");
    }

    /**
     * A count beyond {@code MAX_INDEXES} means the bytes are not a frame we are reading
     * correctly; both readers that walk the index array refuse it.
     */
    @Test
    void rejectsAnImplausibleIndexCount() {
        // zigzag(129) = 258, whose varint is 0x82 0x02.
        byte[] framed = {0, 0, 0, 0, 1, (byte) 0x82, 0x02};
        assertThatThrownBy(() -> ConfluentWireFormat.messageIndex(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Implausible message-index count: 129");
        assertThatThrownBy(() -> ConfluentWireFormat.payloadOffset(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Implausible message-index count: 129");
    }

    /** A negative count is as implausible as a huge one; byte 0x01 zigzag-decodes to -1. */
    @Test
    void rejectsANegativeIndexCount() {
        byte[] framed = {0, 0, 0, 0, 1, 0x01};
        assertThatThrownBy(() -> ConfluentWireFormat.messageIndex(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Implausible message-index count: -1");
    }

    @Test
    void rejectsAVarintCutOffMidContinuation() {
        // The count varint opens with a continuation bit and the frame ends.
        byte[] framed = {0, 0, 0, 0, 1, (byte) 0x80};
        assertThatThrownBy(() -> ConfluentWireFormat.messageIndex(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Malformed varint");
    }

    @Test
    void rejectsAVarintThatNeverTerminates() {
        byte[] framed = new byte[15];
        framed[0] = 0; // magic
        framed[4] = 1; // schema id
        for (int i = 5; i < framed.length; i++) {
            framed[i] = (byte) 0x80; // continuation bit set forever
        }
        assertThatThrownBy(() -> ConfluentWireFormat.messageIndex(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Malformed varint");
    }

    /** Indexes of 64 and up take two or more varint bytes; the encoder must match Kafka's. */
    @Test
    void writesMultiByteIndexesTheWayKafkaDoes() {
        byte[] payload = {5};
        for (int[] indexes : new int[][]{{64}, {8192}, {64, 8192, 1_000_000}}) {
            assertThat(ConfluentWireFormat.frame(3, List.of(boxed(indexes)), payload))
                    .as("index path %s", java.util.Arrays.toString(indexes))
                    .isEqualTo(confluentFrame(3, indexes, payload));
        }
    }

    /** Zigzag exists for negatives; a negative index must come back exactly. */
    @Test
    void roundTripsNegativeIndexes() {
        byte[] framed = ConfluentWireFormat.frame(1, List.of(-1, -5), new byte[]{9});
        assertThat(ConfluentWireFormat.messageIndex(framed)).containsExactly(-1, -5);
        assertThat(ConfluentWireFormat.payload(framed)).isEqualTo(new byte[]{9});
    }

    /**
     * The lone-zero-byte shape is only for the single index {@code [0]}: an explicit path of
     * two zeros is a count followed by two indexes, three bytes on the wire.
     */
    @Test
    void doesNotApplyTheZeroByteOptimizationToLongerPathsStartingAtZero() {
        byte[] framed = ConfluentWireFormat.frame(9, List.of(0, 0), new byte[]{7});
        assertThat(framed).hasSize(9);
        // count = zigzag(2) = 4, then one zero byte per index.
        assertThat(framed[5]).isEqualTo((byte) 4);
        assertThat(framed[6]).isEqualTo((byte) 0);
        assertThat(framed[7]).isEqualTo((byte) 0);
        assertThat(ConfluentWireFormat.messageIndex(framed)).containsExactly(0, 0);
        assertThat(ConfluentWireFormat.payload(framed)).isEqualTo(new byte[]{7});
    }

    /** Negative schema ids keep the high bits set across the round trip. */
    @Test
    void carriesNegativeSchemaIds() {
        byte[] framed = ConfluentWireFormat.frame(-1, List.of(0), new byte[]{1});
        assertThat(framed[1]).isEqualTo((byte) 0xFF);
        assertThat(framed[4]).isEqualTo((byte) 0xFF);
        assertThat(ConfluentWireFormat.schemaId(framed)).isEqualTo(-1);
    }

    /** The bound itself is plausible: 128 indexes walk the array, 129 do not. */
    @Test
    void acceptsTheMaximumPlausibleIndexCount() {
        byte[] framed = ConfluentWireFormat.frame(2, Collections.nCopies(128, 0), new byte[]{3});
        assertThat(ConfluentWireFormat.messageIndex(framed)).hasSize(128).containsOnly(0);
        // 5 prefix bytes + 2 for the count varint + 128 single-byte indexes.
        assertThat(ConfluentWireFormat.payloadOffset(framed)).isEqualTo(135);
        assertThat(ConfluentWireFormat.payload(framed)).isEqualTo(new byte[]{3});
    }

    @Test
    void payloadOffsetTracksTheIndexArrayWidth() {
        // [0]: the lone zero byte, so the payload starts at 6.
        assertThat(ConfluentWireFormat.payloadOffset(
                ConfluentWireFormat.frame(1, List.of(0), new byte[4]))).isEqualTo(6);
        // [1]: count byte + index byte.
        assertThat(ConfluentWireFormat.payloadOffset(
                ConfluentWireFormat.frame(1, List.of(1), new byte[4]))).isEqualTo(7);
        // [64]: the index takes two varint bytes.
        assertThat(ConfluentWireFormat.payloadOffset(
                ConfluentWireFormat.frame(1, List.of(64), new byte[4]))).isEqualTo(8);
    }

    @Test
    void roundTripsAnEmptyPayload() {
        byte[] framed = ConfluentWireFormat.frame(5, List.of(0), new byte[0]);
        assertThat(framed).hasSize(6);
        assertThat(ConfluentWireFormat.payload(framed)).isEmpty();
    }

    @Test
    void findsTheIndexPathThreeLevelsDown() {
        Descriptor outer = file.findMessageTypeByName("Outer");
        Descriptor middle = outer.findNestedTypeByName("Middle");
        Descriptor core = middle.findNestedTypeByName("Core");
        assertThat(ConfluentWireFormat.indexPath(core)).containsExactly(0, 0, 0);
        assertThat(ConfluentWireFormat.messageAt(file, List.of(0, 0, 0))).isSameAs(core);
    }

    @Test
    void messageAtRefusesPathsThatLeadNowhere() {
        assertThat(ConfluentWireFormat.messageAt(file, List.of())).isNull();
        assertThat(ConfluentWireFormat.messageAt(file, List.of(-1))).isNull();
        // Sibling declares no nested types, so any descent past it leads nowhere.
        assertThat(ConfluentWireFormat.messageAt(file, List.of(1, 0))).isNull();
    }

    @Test
    void theExceptionCarriesItsCause() {
        IllegalStateException cause = new IllegalStateException("root");
        ConfluentWireFormatException exception =
                new ConfluentWireFormatException("wrapped", cause);
        assertThat(exception.getCause()).isSameAs(cause);
        assertThat(exception.getMessage()).isEqualTo("wrapped");
    }

    private static Integer[] boxed(int[] values) {
        Integer[] out = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = values[i];
        }
        return out;
    }
}
