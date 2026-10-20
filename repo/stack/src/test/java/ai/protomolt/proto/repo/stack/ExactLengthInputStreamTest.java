package ai.protomolt.proto.repo.stack;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A range read that ends short or runs long is data loss, never a clean end of stream;
 * skipped bytes count toward the length.
 */
class ExactLengthInputStreamTest {

    @Test
    void aStreamOfTheExpectedLengthEndsCleanly() throws IOException {
        try (ExactLengthInputStream in = new ExactLengthInputStream(provider(4), 4)) {
            assertThat(in.readAllBytes()).hasSize(4);
            assertThat(in.read()).isEqualTo(-1);
        }
    }

    @Test
    void anEmptyRangeEndsCleanly() throws IOException {
        try (ExactLengthInputStream in = new ExactLengthInputStream(provider(0), 0)) {
            assertThat(in.read()).isEqualTo(-1);
        }
    }

    @Test
    void aShortProviderStreamFailsAtItsEnd() {
        ExactLengthInputStream in = new ExactLengthInputStream(provider(3), 5);

        assertThatThrownBy(in::readAllBytes)
                .isInstanceOf(StackDataLoss.class)
                .hasMessage("Range read ended at 3 of 5 bytes");
    }

    @Test
    void aShortStreamFailsOnSingleByteReadsToo() throws IOException {
        ExactLengthInputStream in = new ExactLengthInputStream(provider(1), 2);
        assertThat(in.read()).isZero();

        assertThatThrownBy(in::read)
                .isInstanceOf(StackDataLoss.class)
                .hasMessage("Range read ended at 1 of 2 bytes");
    }

    @Test
    void aLongProviderStreamFailsOnTheFirstExtraByte() throws IOException {
        ExactLengthInputStream bulk = new ExactLengthInputStream(provider(6), 4);
        assertThatThrownBy(() -> bulk.read(new byte[8], 0, 8))
                .isInstanceOf(StackDataLoss.class)
                .hasMessage("Range read returned more than 4 bytes");

        ExactLengthInputStream single = new ExactLengthInputStream(provider(2), 1);
        assertThat(single.read()).isZero();
        assertThatThrownBy(single::read)
                .isInstanceOf(StackDataLoss.class)
                .hasMessage("Range read returned more than 1 bytes");
    }

    @Test
    void skippedBytesCountTowardTheLength() throws IOException {
        ExactLengthInputStream exact = new ExactLengthInputStream(provider(10), 10);
        assertThat(exact.skip(10)).isEqualTo(10);
        assertThat(exact.read()).isEqualTo(-1);

        ExactLengthInputStream longer = new ExactLengthInputStream(provider(10), 4);
        assertThatThrownBy(() -> longer.skip(10))
                .isInstanceOf(StackDataLoss.class)
                .hasMessage("Range read returned more than 4 bytes");
    }

    @Test
    void skippingPastAShortEndIsDataLoss() {
        ExactLengthInputStream in = new ExactLengthInputStream(provider(3), 5);

        assertThatThrownBy(() -> in.skip(5))
                .isInstanceOf(StackDataLoss.class)
                .hasMessage("Range read ended at 3 of 5 bytes");
    }

    @Test
    void markIsNotSupported() {
        assertThat(new ExactLengthInputStream(provider(1), 1).markSupported()).isFalse();
    }

    private static ByteArrayInputStream provider(int length) {
        return new ByteArrayInputStream(new byte[length]);
    }
}
