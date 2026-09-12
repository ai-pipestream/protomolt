package ai.protomolt.proto.asset.characterize;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Captures the leading and trailing windows of a stream as it passes,
 * without holding the stream or reading it twice.
 *
 * <p>Content already flows through the writer once — a digest is computed
 * over it on the way to the store — so identification's windows come off
 * that same pass. The leading window fills and stops; the trailing window
 * is a ring the size of the window itself, so the memory a capture costs
 * is fixed no matter how much content passes through it.
 *
 * <p>Reading the stream to its end is what makes {@link #windows()}
 * complete: the trailing window is only correct once the last byte has
 * been seen, and the content length is only known then. A capture read
 * partway reports the bytes it saw and the length it reached, which is why
 * the caller wraps the stream the store consumes rather than a copy of it.
 */
public final class WindowCapture extends FilterInputStream {

    private final byte[] head;
    private final byte[] ring;
    private int headCaptured;
    private int ringCursor;
    private long total;

    /**
     * Captures windows of the default sizes.
     *
     * @param in the stream to pass through
     */
    public WindowCapture(InputStream in) {
        this(in, ByteWindows.DEFAULT_HEAD_BYTES, ByteWindows.DEFAULT_TAIL_BYTES);
    }

    /**
     * Captures windows of stated sizes.
     *
     * @param in the stream to pass through
     * @param headBytes how many leading bytes to keep; may be zero
     * @param tailBytes how many trailing bytes to keep; may be zero
     */
    public WindowCapture(InputStream in, int headBytes, int tailBytes) {
        super(in);
        if (headBytes < 0 || tailBytes < 0) {
            throw new IllegalArgumentException("window sizes must not be negative");
        }
        this.head = new byte[headBytes];
        this.ring = new byte[tailBytes];
    }

    /**
     * The windows as captured so far.
     *
     * @return the windows; the content length is the byte count read to
     *         this point, so this is the asset's true length only once the
     *         stream has been read to its end
     */
    public ByteWindows windows() {
        return ByteWindows.of(Arrays.copyOf(head, headCaptured), trailing(), total);
    }

    /** The ring unrolled into content order. */
    private byte[] trailing() {
        if (ring.length == 0 || total == 0) {
            return new byte[0];
        }
        if (total <= ring.length) {
            return Arrays.copyOf(ring, (int) total);
        }
        byte[] out = new byte[ring.length];
        int oldest = ringCursor;
        System.arraycopy(ring, oldest, out, 0, ring.length - oldest);
        System.arraycopy(ring, 0, out, ring.length - oldest, oldest);
        return out;
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        if (b >= 0) {
            capture((byte) b);
        }
        return b;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        int n = super.read(buffer, offset, length);
        if (n > 0) {
            capture(buffer, offset, n);
        }
        return n;
    }

    private void capture(byte value) {
        if (headCaptured < head.length) {
            head[headCaptured++] = value;
        }
        if (ring.length > 0) {
            ring[ringCursor] = value;
            ringCursor = (ringCursor + 1) % ring.length;
        }
        total++;
    }

    private void capture(byte[] buffer, int offset, int length) {
        int toHead = Math.min(length, head.length - headCaptured);
        if (toHead > 0) {
            System.arraycopy(buffer, offset, head, headCaptured, toHead);
            headCaptured += toHead;
        }
        if (ring.length > 0) {
            // Only the last ring-length bytes of this read can survive in
            // the ring; copying the earlier ones would be overwritten work.
            int keep = Math.min(length, ring.length);
            int from = offset + length - keep;
            for (int i = 0; i < keep; i++) {
                ring[ringCursor] = buffer[from + i];
                ringCursor = (ringCursor + 1) % ring.length;
            }
        }
        total += length;
    }
}
