package ai.pipestream.proto.kafka.connect;

import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The frame is only ours to read if we read it the way Confluent writes it, so these frames are
 * built with Kafka's own {@link ByteUtils#writeVarint} rather than a hand-rolled encoder: the
 * index array is zigzag-encoded, and reading it as unsigned decodes only the single-zero-byte
 * "first message" case, which is exactly the case every other fixture here happened to use.
 */
class ConfluentFramingTest {

    private static final byte[] PAYLOAD = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};

    /** Byte-for-byte how Confluent's MessageIndexes.toByteArray() lays out the frame. */
    private static byte[] framed(int[] indexes, byte[] payload) {
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
        out.put((byte) 0).putInt(42).put(array).put(payload);
        return out.array();
    }

    @Test
    void readsTheFirstMessageOptimization() {
        assertThat(ConfluentFraming.payload(framed(new int[]{0}, PAYLOAD))).isEqualTo(PAYLOAD);
    }

    /** A message that is not declared first in its file: the case zigzag decides. */
    @Test
    void readsAMessageThatIsNotTheFirstInItsFile() {
        assertThat(ConfluentFraming.payload(framed(new int[]{1}, PAYLOAD))).isEqualTo(PAYLOAD);
        assertThat(ConfluentFraming.payload(framed(new int[]{2}, PAYLOAD))).isEqualTo(PAYLOAD);
    }

    /** A nested message: the index array walks the path, so more than one index appears. */
    @Test
    void readsANestedMessagePath() {
        assertThat(ConfluentFraming.payload(framed(new int[]{1, 0}, PAYLOAD))).isEqualTo(PAYLOAD);
        assertThat(ConfluentFraming.payload(framed(new int[]{2, 3, 1}, PAYLOAD))).isEqualTo(PAYLOAD);
    }

    @Test
    void rejectsBytesThatAreNotConfluentFramed() {
        assertThatThrownBy(() -> ConfluentFraming.payload(new byte[]{1, 2, 3, 4, 5, 6}))
                .hasMessageContaining("Not Confluent wire format");
    }
}
