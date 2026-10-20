package ai.protomolt.proto.asset.bridge;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Avro object-container header reader: the magic, the metadata map in both block
 * forms, and a bounded refusal for each kind of corrupt header.
 */
class AvroHeaderTest {

    private static final byte[] MAGIC = {'O', 'b', 'j', 0x01};

    @Test
    void metadataIsReadInFileOrderAcrossBlocks() throws IOException {
        Header h = new Header().magic().count(1).entry("avro.schema", "\"string\"")
                .count(1).entry("avro.codec", "null").count(0);

        assertThat(AvroHeader.metadata(h.in()))
                .containsExactly(Map.entry(AvroHeader.SCHEMA_KEY, "\"string\""),
                        Map.entry("avro.codec", "null"));
    }

    @Test
    void aNegativeBlockCountCarriesABlockSizeThatIsSkipped() throws IOException {
        Header h = new Header().magic().count(-2).count(42)
                .entry("a", "1").entry("b", "2").count(0);

        assertThat(AvroHeader.metadata(h.in())).containsExactly(Map.entry("a", "1"), Map.entry("b", "2"));
    }

    @Test
    void anEmptyMapIsAnEmptyResult() throws IOException {
        assertThat(AvroHeader.metadata(new Header().magic().count(0).in())).isEmpty();
    }

    @Test
    void wrongOrShortMagicIsRefused() {
        assertThatThrownBy(() -> AvroHeader.metadata(new ByteArrayInputStream(new byte[] {'O', 'b', 'j', 0x02})))
                .isInstanceOf(IOException.class)
                .hasMessage("not an Avro object container: the magic bytes are wrong");
        assertThatThrownBy(() -> AvroHeader.metadata(new ByteArrayInputStream(new byte[] {'O', 'b'})))
                .isInstanceOf(IOException.class)
                .hasMessage("not an Avro object container: the magic bytes are wrong");
    }

    @Test
    void aBlockOfMoreThan1024EntriesIsRefused() {
        assertThatThrownBy(() -> AvroHeader.metadata(new Header().magic().count(1025).in()))
                .isInstanceOf(IOException.class)
                .hasMessage("an Avro header block of 1025 entries is out of bounds");
        assertThatThrownBy(() -> AvroHeader.metadata(new Header().magic().count(-1025).count(0).in()))
                .isInstanceOf(IOException.class)
                .hasMessage("an Avro header block of 1025 entries is out of bounds");
    }

    @Test
    void aValueLengthOverOneMebibyteOrNegativeIsRefused() {
        assertThatThrownBy(() -> AvroHeader.metadata(new Header().magic().count(1).count((1 << 20) + 1).in()))
                .isInstanceOf(IOException.class)
                .hasMessage("an Avro header value of " + ((1 << 20) + 1) + " bytes is out of bounds");
        assertThatThrownBy(() -> AvroHeader.metadata(new Header().magic().count(1).count(-1).in()))
                .isInstanceOf(IOException.class)
                .hasMessage("an Avro header value of -1 bytes is out of bounds");
    }

    @Test
    void aValueCutShortIsAnEarlyEnd() {
        Header h = new Header().magic().count(1).count(5).raw("ab".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> AvroHeader.metadata(h.in()))
                .isInstanceOf(EOFException.class)
                .hasMessage("the Avro header ended early");
    }

    @Test
    void aHeaderEndingBeforeTheClosingCountIsAnEarlyEnd() {
        Header h = new Header().magic().count(1).entry("a", "1");

        assertThatThrownBy(() -> AvroHeader.metadata(h.in()))
                .isInstanceOf(EOFException.class)
                .hasMessage("the Avro header ended early");
    }

    @Test
    void aVarintThatNeverTerminatesIsRefused() {
        byte[] continuation = new byte[10];
        Arrays.fill(continuation, (byte) 0x80);

        assertThatThrownBy(() -> AvroHeader.metadata(new Header().magic().raw(continuation).in()))
                .isInstanceOf(IOException.class)
                .hasMessage("an Avro variable-length integer never terminates");
    }

    /** Writes header bytes with Avro's zig-zag varint encoding. */
    private static final class Header {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        Header magic() {
            return raw(MAGIC);
        }

        Header count(long value) {
            long zigzag = (value << 1) ^ (value >> 63);
            while ((zigzag & ~0x7FL) != 0) {
                out.write((int) ((zigzag & 0x7F) | 0x80));
                zigzag >>>= 7;
            }
            out.write((int) zigzag);
            return this;
        }

        Header entry(String key, String value) {
            return text(key).text(value);
        }

        Header text(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            count(bytes.length);
            return raw(bytes);
        }

        Header raw(byte[] bytes) {
            out.writeBytes(bytes);
            return this;
        }

        ByteArrayInputStream in() {
            return new ByteArrayInputStream(out.toByteArray());
        }
    }
}
