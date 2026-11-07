package ai.protomolt.proto.kafka.wire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Truncation and boundary shapes the hostile-half test does not pin: a frame whose index
 * array is cut short, the negative-count rejection on the offset path, and the schema-id
 * boundaries.
 */
class ConfluentWireFormatTruncationTest {

    @Test
    void payloadOffsetRejectsANegativeIndexCount() {
        byte[] framed = {0, 0, 0, 0, 1, 0x01};
        assertThatThrownBy(() -> ConfluentWireFormat.payloadOffset(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Implausible message-index count: -1");
    }

    @Test
    void messageIndexRejectsATruncatedIndexArray() {
        // Count claims two indexes (zigzag(2) = 4) but only one follows.
        byte[] framed = {0, 0, 0, 0, 1, 0x04, 0x00};
        assertThatThrownBy(() -> ConfluentWireFormat.messageIndex(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Malformed varint");
    }

    @Test
    void payloadOffsetRejectsATruncatedIndexArray() {
        byte[] framed = {0, 0, 0, 0, 1, 0x04, 0x00};
        assertThatThrownBy(() -> ConfluentWireFormat.payloadOffset(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Malformed varint");
    }

    @Test
    void payloadRejectsATruncatedIndexArray() {
        byte[] framed = {0, 0, 0, 0, 1, 0x04, 0x00};
        assertThatThrownBy(() -> ConfluentWireFormat.payload(framed))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Malformed varint");
    }

    @Test
    void carriesTheZeroSchemaId() {
        byte[] framed = ConfluentWireFormat.frame(0, List.of(0), new byte[]{9});
        assertThat(ConfluentWireFormat.schemaId(framed)).isZero();
        assertThat(ConfluentWireFormat.payload(framed)).isEqualTo(new byte[]{9});
    }

    @Test
    void rejectsAnEmptyByteArray() {
        assertThatThrownBy(() -> ConfluentWireFormat.schemaId(new byte[0]))
                .isInstanceOf(ConfluentWireFormatException.class)
                .hasMessageContaining("Not Confluent wire format");
    }
}
