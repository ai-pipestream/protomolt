package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;

/** Borrowed stream with exact length, digest and lease checks on consumption. */
final class ArchiveUploadInput extends InputStream {
    private final InputStream body;
    private final long length;
    private final Runnable renew;
    private final long renewalNanos;
    private long nextRenewal;
    private final java.security.MessageDigest digest = ArchiveManifests.sha256();
    private long count;
    private RepositoryException lengthFailure;

    ArchiveUploadInput(InputStream body, long length, Duration lease, Runnable renew) {
        this.body = Objects.requireNonNull(body);
        this.length = length;
        this.renew = renew;
        renewalNanos = lease.toNanos() / 2;
        nextRenewal = System.nanoTime() + renewalNanos;
    }

    @Override public int read() throws IOException {
        byte[] single = new byte[1];
        return read(single, 0, 1) < 0 ? -1 : single[0] & 0xff;
    }

    @Override public int read(byte[] bytes, int offset, int size) throws IOException {
        Objects.checkFromIndexSize(offset, size, bytes.length);
        if (size == 0) return 0;
        if (lengthFailure != null) throw lengthFailure;
        if (Thread.currentThread().isInterrupted())
            throw new RepositoryException(RepositoryException.Code.CANCELLED, "Archive upload interrupted");
        if (System.nanoTime() - nextRenewal >= 0) {
            renew.run();
            nextRenewal = System.nanoTime() + renewalNanos;
        }
        int got = body.read(bytes, offset, size);
        if (got < 0 && count < length) throw invalidLength("Archive upload is shorter than declared size");
        if (got > 0) {
            count = Math.addExact(count, got);
            if (count > length) throw invalidLength("Archive upload exceeds declared size");
            digest.update(bytes, offset, got);
        }
        return got;
    }

    String finish() throws IOException {
        if (count != length) throw invalidLength("Archive upload length differs from declared size");
        if (read() != -1) throw invalidLength("Archive upload exceeds declared size");
        return HexFormat.of().formatHex(digest.digest());
    }

    RepositoryException lengthFailure() { return lengthFailure; }

    private RepositoryException invalidLength(String message) {
        lengthFailure = RepositoryErrors.invalidArgument(message);
        return lengthFailure;
    }
}
