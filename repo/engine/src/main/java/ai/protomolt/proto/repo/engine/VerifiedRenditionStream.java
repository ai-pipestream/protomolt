package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * A rendition range as the manifest describes it. The provider stream must yield
 * exactly {@code length} bytes and then end; a whole-object read must also hash to
 * the manifest's SHA-256. Each check runs when the reader reaches the end, before
 * end of stream is reported, so a caller that sees end of stream has the verified
 * bytes and a caller that sees {@code DATA_LOSS} must discard what it received.
 * A range read proves its length only.
 */
final class VerifiedRenditionStream extends InputStream {
    private final InputStream body;
    private final long length;
    private final MessageDigest digest;
    private final String sha256;
    private long delivered;
    private boolean verified;

    /**
     * @param body the provider stream for the range; closed by {@link #close()}
     * @param length the exact byte count of the range
     * @param sha256 the whole object's digest, or null for a range that proves length only
     */
    VerifiedRenditionStream(InputStream body, long length, String sha256) {
        this.body = Objects.requireNonNull(body, "body");
        if (length < 0) throw new IllegalArgumentException("Negative rendition range length");
        this.length = length;
        this.sha256 = sha256;
        this.digest = sha256 == null ? null : sha256Digest();
    }

    @Override public int read() throws IOException {
        byte[] one = new byte[1];
        int read = read(one, 0, 1);
        return read < 0 ? -1 : one[0] & 0xFF;
    }

    @Override public int read(byte[] buffer, int offset, int count) throws IOException {
        Objects.checkFromIndexSize(offset, count, buffer.length);
        if (count == 0) return 0;
        long remaining = length - delivered;
        if (remaining == 0) {
            finish();
            return -1;
        }
        int read = body.read(buffer, offset, (int) Math.min(count, remaining));
        if (read < 0)
            throw lost("Archive object ended after " + delivered + " of " + length + " bytes");
        if (digest != null) digest.update(buffer, offset, read);
        delivered += read;
        return read;
    }

    private void finish() throws IOException {
        if (verified) return;
        if (body.read() >= 0) throw lost("Archive object runs past its manifest size of " + length + " bytes");
        if (digest != null && !HexFormat.of().formatHex(digest.digest()).equals(sha256))
            throw lost("Archive object bytes disagree with the manifest's SHA-256");
        verified = true;
    }

    @Override public void close() throws IOException {
        body.close();
    }

    private static RepositoryException lost(String message) {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, message);
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException missing) {
            throw new IllegalStateException("SHA-256 is unavailable", missing);
        }
    }
}
