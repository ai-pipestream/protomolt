package ai.protomolt.proto.asset.characterize;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Capturing both windows off the pass the writer already makes. */
class WindowCaptureTest {

    private static byte[] ramp(int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) i;
        }
        return out;
    }

    /** Drains a stream in fixed-size bulk reads. */
    private static void drain(InputStream in, int chunk) throws IOException {
        byte[] buffer = new byte[chunk];
        while (in.read(buffer, 0, chunk) >= 0) {
            // The capture is the point; the bytes go nowhere here.
        }
    }

    @ParameterizedTest(name = "chunked {0} bytes at a time")
    @ValueSource(ints = {1, 3, 7, 16, 64, 1000})
    @DisplayName("the windows are the same however the stream is chunked")
    void chunkingDoesNotMatter(int chunk) throws IOException {
        byte[] content = ramp(500);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 8, 8);
        drain(capture, chunk);
        ByteWindows windows = capture.windows();
        assertThat(windows.sizeBytes()).isEqualTo(500);
        assertThat(windows.head()).isEqualTo(Arrays.copyOfRange(content, 0, 8));
        assertThat(windows.tail()).isEqualTo(Arrays.copyOfRange(content, 492, 500));
    }

    @Test
    @DisplayName("single-byte reads capture the same windows as bulk reads")
    void singleByteReads() throws IOException {
        byte[] content = ramp(300);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 8, 8);
        while (capture.read() >= 0) {
            // Byte at a time: the slow path has to agree with the fast one.
        }
        ByteWindows windows = capture.windows();
        assertThat(windows.head()).isEqualTo(Arrays.copyOfRange(content, 0, 8));
        assertThat(windows.tail()).isEqualTo(Arrays.copyOfRange(content, 292, 300));
        assertThat(windows.sizeBytes()).isEqualTo(300);
    }

    @Test
    @DisplayName("mixed single-byte and bulk reads still agree")
    void mixedReads() throws IOException {
        byte[] content = ramp(97);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 5, 5);
        capture.read();
        capture.read();
        byte[] buffer = new byte[13];
        capture.read(buffer, 0, 13);
        capture.read();
        drain(capture, 9);
        ByteWindows windows = capture.windows();
        assertThat(windows.head()).isEqualTo(Arrays.copyOfRange(content, 0, 5));
        assertThat(windows.tail()).isEqualTo(Arrays.copyOfRange(content, 92, 97));
    }

    @Test
    @DisplayName("content shorter than the windows is wholly resident in both")
    void shorterThanWindows() throws IOException {
        byte[] content = ramp(3);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 8, 8);
        drain(capture, 8);
        ByteWindows windows = capture.windows();
        assertThat(windows.head()).isEqualTo(content);
        assertThat(windows.tail()).isEqualTo(content);
        assertThat(windows.sizeBytes()).isEqualTo(3);
        assertThat(windows.observable(0, 3)).isTrue();
    }

    @Test
    @DisplayName("content exactly the window size fills the ring without rotating it")
    void exactlyWindowSized() throws IOException {
        byte[] content = ramp(8);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 8, 8);
        drain(capture, 4);
        assertThat(capture.windows().tail()).isEqualTo(content);
    }

    @Test
    @DisplayName("one bulk read larger than the ring keeps only its last bytes")
    void singleOversizedRead() throws IOException {
        byte[] content = ramp(200);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 4, 4);
        byte[] buffer = new byte[200];
        assertThat(capture.read(buffer, 0, 200)).isEqualTo(200);
        assertThat(capture.read(buffer, 0, 200)).isEqualTo(-1);
        assertThat(capture.windows().tail()).isEqualTo(Arrays.copyOfRange(content, 196, 200));
    }

    @Test
    @DisplayName("a read into the middle of a buffer captures the right bytes")
    void offsetReads() throws IOException {
        byte[] content = ramp(40);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 6, 6);
        byte[] buffer = new byte[64];
        int at = 0;
        int n;
        while ((n = capture.read(buffer, 17, 9)) >= 0) {
            at += n;
        }
        assertThat(at).isEqualTo(40);
        assertThat(capture.windows().head()).isEqualTo(Arrays.copyOfRange(content, 0, 6));
        assertThat(capture.windows().tail()).isEqualTo(Arrays.copyOfRange(content, 34, 40));
    }

    @Test
    @DisplayName("an empty stream captures nothing and knows the content is empty")
    void empty() throws IOException {
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(new byte[0]), 8, 8);
        drain(capture, 8);
        ByteWindows windows = capture.windows();
        assertThat(windows.isEmpty()).isTrue();
        assertThat(windows.sizeBytes()).isZero();
    }

    @Test
    @DisplayName("a capture read partway reports only what it has seen")
    void partialRead() throws IOException {
        byte[] content = ramp(100);
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(content), 4, 4);
        byte[] buffer = new byte[10];
        capture.read(buffer, 0, 10);
        ByteWindows windows = capture.windows();
        assertThat(windows.sizeBytes()).isEqualTo(10);
        assertThat(windows.tail()).isEqualTo(Arrays.copyOfRange(content, 6, 10));
    }

    @Test
    @DisplayName("a zero-sized window captures nothing without failing")
    void zeroSizedWindows() throws IOException {
        WindowCapture capture = new WindowCapture(
                new ByteArrayInputStream(ramp(50)), 0, 0);
        drain(capture, 8);
        ByteWindows windows = capture.windows();
        assertThat(windows.head()).isEmpty();
        assertThat(windows.tail()).isEmpty();
        assertThat(windows.sizeBytes()).isEqualTo(50);
    }
}
