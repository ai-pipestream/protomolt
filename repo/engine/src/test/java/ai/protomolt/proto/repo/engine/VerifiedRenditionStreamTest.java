package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/** The length and digest checks a streamed rendition read reports before end of stream. */
class VerifiedRenditionStreamTest {
    private static final byte[] BYTES = "rendition bytes for the verified stream".getBytes(StandardCharsets.UTF_8);

    @Test void aWholeObjectMatchingItsDigestEndsCleanly() throws Exception {
        try (var stream = new VerifiedRenditionStream(new ByteArrayInputStream(BYTES), BYTES.length, sha256(BYTES))) {
            assertThat(stream.readAllBytes()).isEqualTo(BYTES);
            assertThat(stream.read()).isEqualTo(-1);
        }
    }

    @Test void aWholeObjectWithOtherBytesFailsBeforeEndOfStream() throws Exception {
        var damaged = BYTES.clone();
        damaged[3] ^= 1;
        var stream = new VerifiedRenditionStream(new ByteArrayInputStream(damaged), BYTES.length, sha256(BYTES));
        var received = new byte[BYTES.length];
        assertThat(stream.readNBytes(received, 0, received.length)).isEqualTo(BYTES.length);
        assertDataLoss(stream::read);
    }

    @Test void aShortProviderStreamIsDataLoss() {
        var stream = new VerifiedRenditionStream(new ByteArrayInputStream(BYTES, 0, 10), BYTES.length, null);
        assertDataLoss(stream::readAllBytes);
    }

    @Test void aLongProviderStreamIsDataLoss() {
        var stream = new VerifiedRenditionStream(new ByteArrayInputStream(BYTES), BYTES.length - 1, null);
        assertDataLoss(stream::readAllBytes);
    }

    @Test void aRangeChecksLengthOnly() throws Exception {
        var range = "any bytes at all".getBytes(StandardCharsets.UTF_8);
        try (var stream = new VerifiedRenditionStream(new ByteArrayInputStream(range), range.length, null)) {
            assertThat(stream.readAllBytes()).isEqualTo(range);
        }
    }

    @Test void readsNeverRunPastTheRange() throws Exception {
        var stream = new VerifiedRenditionStream(new ByteArrayInputStream(BYTES), 5, null);
        var buffer = new byte[64];
        assertThat(stream.read(buffer, 0, buffer.length)).isEqualTo(5);
        assertDataLoss(() -> stream.read(buffer, 0, buffer.length));
    }

    @Test void closingClosesTheProviderStream() throws Exception {
        var closed = new boolean[1];
        var body = new ByteArrayInputStream(BYTES) {
            @Override public void close() throws IOException {
                closed[0] = true;
                super.close();
            }
        };
        new VerifiedRenditionStream(body, BYTES.length, null).close();
        assertThat(closed[0]).isTrue();
    }

    private interface Read { Object run() throws IOException; }

    private static void assertDataLoss(Read read) {
        assertThatThrownBy(read::run).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
