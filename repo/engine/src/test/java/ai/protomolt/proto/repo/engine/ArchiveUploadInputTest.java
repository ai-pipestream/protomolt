package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import ai.protomolt.proto.repo.spi.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The upload stream is the only place a declared archive length meets the bytes actually sent.
 * A short body, a long body and a finish before the body is drained must each refuse, and a
 * recorded length failure must stay sticky so a retried read cannot slip past it.
 */
class ArchiveUploadInputTest {

    private static final Duration LONG_LEASE = Duration.ofHours(1);
    private static final Runnable NO_RENEW = () -> { };

    private static ArchiveUploadInput input(byte[] body, long declared) {
        return new ArchiveUploadInput(new ByteArrayInputStream(body), declared, LONG_LEASE, NO_RENEW);
    }

    private static void assertInvalid(Throwable thrown, String message) {
        assertThat(thrown).isInstanceOf(RepositoryException.class).hasMessage(message);
        assertThat(((RepositoryException) thrown).code()).isEqualTo(RepositoryException.Code.INVALID_ARGUMENT);
    }

    @Test
    void anExactBodyFinishesWithItsSha256() throws Exception {
        byte[] body = {1, 2, (byte) 0xFF};
        var input = input(body, 3);
        assertThat(input.readAllBytes()).isEqualTo(body);
        String expected = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        assertThat(input.finish()).isEqualTo(expected);
        assertThat(input.lengthFailure()).isNull();
    }

    @Test
    void anEmptyDeclaredBodyFinishesWithTheEmptyDigest() throws Exception {
        assertThat(input(new byte[0], 0).finish())
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void singleByteReadsReturnUnsignedValues() throws Exception {
        var input = input(new byte[] {(byte) 0xFF}, 1);
        assertThat(input.read()).isEqualTo(255);
        assertThat(input.read()).isEqualTo(-1);
    }

    @Test
    void aShortBodyIsRefusedAtEndOfStreamAndTheFailureIsSticky() {
        var input = input(new byte[] {1, 2}, 3);
        Throwable first = catchThrowable(input::readAllBytes);
        assertInvalid(first, "Archive upload is shorter than declared size");
        assertThat(input.lengthFailure()).isSameAs(first);
        assertThatThrownBy(() -> input.read(new byte[4], 0, 4)).isSameAs(first);
    }

    @Test
    void aLongBodyIsRefusedOnTheReadThatCrossesTheDeclaredSize() {
        var input = input(new byte[] {1, 2, 3, 4}, 3);
        assertInvalid(catchThrowable(() -> input.read(new byte[8], 0, 8)),
                "Archive upload exceeds declared size");
    }

    @Test
    void finishBeforeTheDeclaredLengthIsConsumedIsRefused() {
        var input = input(new byte[] {1, 2, 3}, 3);
        assertInvalid(catchThrowable(input::finish),
                "Archive upload length differs from declared size");
    }

    @Test
    void finishProbesForTrailingBytesAfterTheDeclaredLength() throws Exception {
        var input = input(new byte[] {1, 2, 3, 4}, 3);
        assertThat(input.read(new byte[3], 0, 3)).isEqualTo(3);
        assertInvalid(catchThrowable(input::finish),
                "Archive upload exceeds declared size");
    }

    @Test
    void aZeroSizeReadNeverTouchesTheBody() throws Exception {
        var input = new ArchiveUploadInput(new InputStream() {
            @Override public int read() {
                throw new AssertionError("body read");
            }
        }, 1, LONG_LEASE, NO_RENEW);
        assertThat(input.read(new byte[1], 0, 0)).isZero();
        assertThatThrownBy(() -> input.read(new byte[1], 1, 1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void anInterruptedReaderIsCancelledWithoutReadingTheBody() {
        var input = input(new byte[] {1}, 1);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> input.read(new byte[1], 0, 1))
                    .isInstanceOf(RepositoryException.class).hasMessage("Archive upload interrupted")
                    .satisfies(e -> assertThat(((RepositoryException) e).code())
                            .isEqualTo(RepositoryException.Code.CANCELLED));
        } finally {
            Thread.interrupted();
        }
        assertThat(input.lengthFailure()).isNull();
    }

    @Test
    void anElapsedHalfLeaseRenewsBeforeReading() throws IOException {
        var renewals = new AtomicInteger();
        var input = new ArchiveUploadInput(new ByteArrayInputStream(new byte[] {1, 2}), 2,
                Duration.ZERO, renewals::incrementAndGet);
        input.read();
        input.read();
        assertThat(renewals).hasValue(2);

        var lease = new AtomicInteger();
        var held = new ArchiveUploadInput(new ByteArrayInputStream(new byte[] {1}), 1, LONG_LEASE,
                lease::incrementAndGet);
        held.read();
        assertThat(lease).hasValue(0);
    }

    @Test
    void aNullBodyIsRefused() {
        assertThatThrownBy(() -> new ArchiveUploadInput(null, 0, LONG_LEASE, NO_RENEW))
                .isInstanceOf(NullPointerException.class);
    }
}
