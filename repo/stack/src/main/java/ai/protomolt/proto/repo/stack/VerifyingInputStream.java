package ai.protomolt.proto.repo.stack;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Hashes and counts bytes as they pass. Reading checks the expected length and
 * SHA-256 at the end of the stream and fails rather than signal a clean end
 * for bytes that differ. Reading past the expected length fails at once.
 */
final class VerifyingInputStream extends FilterInputStream {
    private final MessageDigest digest = sha256();
    private final long expectedLength;
    private final String expectedSha256;
    private long count;
    private boolean verified;

    VerifyingInputStream(InputStream in, long expectedLength, String expectedSha256) {
        super(in);
        this.expectedLength = expectedLength;
        this.expectedSha256 = expectedSha256;
    }

    @Override public int read() throws IOException {
        int value = super.read();
        if (value < 0) { verify(); return -1; }
        digest.update((byte) value);
        advance(1);
        return value;
    }

    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        int read = super.read(buffer, offset, length);
        if (read < 0) { verify(); return -1; }
        digest.update(buffer, offset, read);
        advance(read);
        return read;
    }

    @Override public long skip(long n) throws IOException {
        // Skipped bytes would escape the digest; read them through it instead.
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
        if (count > expectedLength)
            throw new StackDataLoss("Stored object is longer than its reference: " + expectedLength + " bytes");
    }

    private void verify() throws StackDataLoss {
        if (verified) return;
        if (count != expectedLength)
            throw new StackDataLoss("Stored object is truncated: " + count + " of " + expectedLength + " bytes");
        var actual = HexFormat.of().formatHex(digest.digest());
        if (!actual.equals(expectedSha256))
            throw new StackDataLoss("Stored object SHA-256 differs from its reference");
        verified = true;
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
