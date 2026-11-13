package ai.protomolt.proto.repo.blob.cache;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Disposable bounded-disk staging; never an authoritative object or a heap-sized payload. */
final class CacheSpool implements AutoCloseable {
    private final Path path;
    private final long limit;
    private final MessageDigest digest;
    private final FileChannel channel;
    private long size;
    private String sha;
    private boolean opened;

    CacheSpool(long limit) throws IOException {
        this.limit = limit;
        try { digest = MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException(failure); }
        // Unsupported owner-only file creation disables this optional cache operation.
        path = Files.createTempFile("protomolt-cache-", ".bytes",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        try {
            // On POSIX, DELETE_ON_CLOSE unlinks the file when opened. Even a process
            // crash cannot leave payload bytes behind under a temporary pathname.
            channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.DELETE_ON_CLOSE);
        } catch (IOException | RuntimeException failure) {
            try { Files.deleteIfExists(path); } catch (IOException close) { failure.addSuppressed(close); }
            throw failure;
        }
    }

    void write(byte[] bytes, int offset, int count) throws IOException {
        if (sha != null) throw new IllegalStateException("Cache spool is finished");
        if (count > limit - size) throw new IOException("Cache spool exceeds configured ceiling");
        var buffer = ByteBuffer.wrap(bytes, offset, count);
        while (buffer.hasRemaining()) channel.write(buffer);
        digest.update(bytes, offset, count);
        size += count;
    }

    void finish() {
        if (sha == null) sha = HexFormat.of().formatHex(digest.digest());
    }

    long size() { return size; }
    String sha() { return sha; }

    /** One reader owns the channel from this point onward. */
    InputStream open() throws IOException {
        if (sha == null || opened) throw new IllegalStateException("Cache spool is unfinished or already opened");
        opened = true;
        channel.position(0);
        return Channels.newInputStream(channel);
    }

    InputStream ownedStream() throws IOException {
        return new java.io.FilterInputStream(open()) {
            @Override public void close() throws IOException { CacheSpool.this.close(); }
        };
    }

    @Override public void close() throws IOException {
        IOException failure = null;
        try { channel.close(); } catch (IOException close) { failure = close; }
        try { Files.deleteIfExists(path); }
        catch (IOException delete) {
            if (failure == null) failure = delete; else failure.addSuppressed(delete);
        }
        if (failure != null) throw failure;
    }
}
