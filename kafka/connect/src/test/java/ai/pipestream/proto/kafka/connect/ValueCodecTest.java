package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The value codec must translate every malformed Confluent frame into a {@link DataException}
 * the worker can route, now that the frame reader lives in another module and throws its own
 * exception. The frames are built with Kafka's own {@link ByteUtils#writeVarint} so the
 * zigzag-encoded index array matches what Confluent writes.
 */
class ValueCodecTest {

    private static final byte[] PAYLOAD = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};

    private static Descriptor orderType;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("codec/test/order.proto", """
                        syntax = "proto3";
                        package codec.test;
                        message Order { string id = 1; }
                        """, "test")
                .build());
        FileDescriptor file = compiled.descriptorFor("codec/test/order.proto").orElseThrow();
        orderType = file.findMessageTypeByName("Order");
    }

    @Test
    void rejectsFramesTooShortToHoldAPrefix() {
        ValueCodec codec = new ValueCodec(orderType, "confluent");
        assertThatThrownBy(() -> codec.decode(new byte[]{0, 1}, "topic orders"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Not Confluent wire format");
    }

    @Test
    void rejectsAWrongMagicByte() {
        ValueCodec codec = new ValueCodec(orderType, "confluent");
        assertThatThrownBy(() -> codec.decode(new byte[]{1, 2, 3, 4, 5, 6}, "topic orders"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Not Confluent wire format");
    }

    /**
     * The count varint is zigzag-decoded, so a malformed frame can present a negative count.
     * Skipping the index array instead of refusing it would hand the index bytes to the message
     * parser as payload.
     */
    @Test
    void rejectsANegativeMessageIndexCount() {
        ByteBuffer count = ByteBuffer.allocate(8);
        ByteUtils.writeVarint(-1, count);
        count.flip();
        ByteBuffer out = ByteBuffer.allocate(5 + count.remaining() + PAYLOAD.length);
        out.put((byte) 0).putInt(42).put(count).put(PAYLOAD);

        ValueCodec codec = new ValueCodec(orderType, "confluent");
        assertThatThrownBy(() -> codec.decode(out.array(), "topic orders"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Implausible message-index count");
    }
}
