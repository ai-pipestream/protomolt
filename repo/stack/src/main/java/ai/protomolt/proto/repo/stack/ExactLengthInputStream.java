package ai.protomolt.proto.repo.stack;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A range read: counts bytes and fails rather than signal a clean end when the
 * provider returns fewer or more than the requested length. It makes no digest
 * claim. Closing it closes the provider stream, which abandons the transfer.
 */
final class ExactLengthInputStream extends FilterInputStream {
    private final long expected;
    private long count;

    ExactLengthInputStream(InputStream in, long expected) {
        super(in);
        this.expected = expected;
    }

    @Override public int read() throws IOException {
        int value = super.read();
        if (value < 0) { requireComplete(); return -1; }
        advance(1);
        return value;
    }

    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        int read = super.read(buffer, offset, length);
        if (read < 0) { requireComplete(); return -1; }
        advance(read);
        return read;
    }

    @Override public long skip(long n) throws IOException {
        // Skipped bytes must still be counted; read them instead.
        byte[] buffer = new byte[(int) Math.min(8192, Math.max(n, 1))];
        long skipped = 0;
        while (skipped < n) {
            int read = read(buffer, 0, (int) Math.min(buffer.length, n - skipped));
            if (read < 0) break;
            skipped += read;
        }
        return skipped;
    }

    @Override public boolean markSupported() { return false; }

    private void advance(int read) throws StackDataLoss {
        count += read;
        if (count > expected) throw new StackDataLoss("Range read returned more than " + expected + " bytes");
    }

    private void requireComplete() throws StackDataLoss {
        if (count != expected) throw new StackDataLoss("Range read ended at " + count + " of " + expected + " bytes");
    }
}
