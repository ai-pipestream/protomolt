package ai.protomolt.proto.schema.apicurio;

import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Edge-case tests for {@link ApicurioProtobufParseFallback}: constructor validation and
 * malformed wire-format framing. Happy-path framing is covered by
 * {@link ApicurioProtobufParseFallbackTest}.
 */
class ApicurioProtobufParseFallbackEdgeCasesTest {

    @Test
    void nullMessageTypeIsRejected() {
        assertThatThrownBy(() -> new ApicurioProtobufParseFallback(null, 4, false, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("messageType");
    }

    @Test
    void negativeIdSizeIsRejected() {
        assertThatThrownBy(() -> new ApicurioProtobufParseFallback(Struct.class, -1, false, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idSize");
    }

    @Test
    void emptyPayloadParsesToTheDefaultInstance() {
        Struct parsed = ApicurioProtobufParseFallback.forType(Struct.class).parse(new byte[0]);
        assertThat(parsed).isEqualTo(Struct.getDefaultInstance());
    }

    @Test
    void magicByteWithTruncatedIdPrefixFails() {
        // Magic byte present but fewer than idSize id bytes follow.
        byte[] truncated = {ApicurioProtobufParseFallback.MAGIC_BYTE, 0x01, 0x02};
        assertThatThrownBy(() -> ApicurioProtobufParseFallback.forType(Struct.class).parse(truncated))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void typeRefWithTruncatedDelimitedPayloadFails() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ApicurioProtobufParseFallback.MAGIC_BYTE);
        out.writeBytes(ByteBuffer.allocate(4).putInt(9).array());
        out.write(10); // type-ref claims 10 bytes
        out.write(0x55); // ...but supplies only one
        assertThatThrownBy(() -> ApicurioProtobufParseFallback.forType(Struct.class)
                .parse(out.toByteArray()))
                .isInstanceOf(IllegalStateException.class)
                .hasRootCauseInstanceOf(java.io.IOException.class)
                .rootCause().hasMessageContaining("Truncated delimited message");
    }

    @Test
    void typeRefWithNegativeDecodedLengthFails() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ApicurioProtobufParseFallback.MAGIC_BYTE);
        out.writeBytes(ByteBuffer.allocate(4).putInt(9).array());
        // 0xFFFFFFFF as a varint decodes to int -1: a negative delimited length.
        out.writeBytes(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x0F});
        assertThatThrownBy(() -> ApicurioProtobufParseFallback.forType(Struct.class)
                .parse(out.toByteArray()))
                .isInstanceOf(IllegalStateException.class)
                .hasRootCauseInstanceOf(java.io.IOException.class)
                .rootCause().hasMessageContaining("Negative delimited message length");
    }

    @Test
    void typeRefOfZeroLengthIsSkippedAndPayloadParses() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("k", com.google.protobuf.Value.newBuilder().setStringValue("v").build())
                .build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ApicurioProtobufParseFallback.MAGIC_BYTE);
        out.writeBytes(ByteBuffer.allocate(4).putInt(3).array());
        out.write(0); // empty type-ref
        out.write(message.toByteArray());

        Struct parsed = ApicurioProtobufParseFallback.forType(Struct.class).parse(out.toByteArray());
        assertThat(parsed.getFieldsOrThrow("k").getStringValue()).isEqualTo("v");
    }

    @Test
    void messageIndexesOfZeroLengthAreSkipped() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("k", com.google.protobuf.Value.newBuilder().setStringValue("v").build())
                .build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ApicurioProtobufParseFallback.MAGIC_BYTE);
        out.writeBytes(ByteBuffer.allocate(4).putInt(5).array());
        out.write(0); // message-index count 0 (zigzag varint)
        out.write(message.toByteArray());

        ApicurioProtobufParseFallback fallback =
                new ApicurioProtobufParseFallback(Struct.class, 4, true, false);
        Struct parsed = fallback.parse(out.toByteArray());
        assertThat(parsed.getFieldsOrThrow("k").getStringValue())
                .isEqualTo("v");
    }

    @Test
    void skipDelimitedMessageAtEofFails() {
        // Varint length 5 with nothing after it.
        java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(new byte[]{5});
        assertThatThrownBy(() -> ApicurioProtobufParseFallback.skipDelimitedMessage(in))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Truncated");
    }

    @Test
    void zigZagVarintRoundTripsNegativeAndPositiveValues() throws Exception {
        // Zigzag: 0 -> 0, 1 -> -1, 2 -> 1, 3 -> -2.
        assertThat(ApicurioProtobufParseFallback.readZigZagVarint(
                new java.io.ByteArrayInputStream(new byte[]{0}))).isZero();
        assertThat(ApicurioProtobufParseFallback.readZigZagVarint(
                new java.io.ByteArrayInputStream(new byte[]{1}))).isEqualTo(-1);
        assertThat(ApicurioProtobufParseFallback.readZigZagVarint(
                new java.io.ByteArrayInputStream(new byte[]{2}))).isEqualTo(1);
        assertThat(ApicurioProtobufParseFallback.readZigZagVarint(
                new java.io.ByteArrayInputStream(new byte[]{3}))).isEqualTo(-2);
    }
}
