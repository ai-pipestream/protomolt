package ai.protomolt.proto.mesh.runtime;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * One-writer append-only protobuf log with a format header and per-frame CRC32C.
 * Only an incomplete final frame is repairable; invalid lengths, protobuf bytes,
 * checksums, and state transitions remain corruption for the owner to refuse.
 */
final class FramedProtobufWal<T extends MessageLite> implements AutoCloseable {

    private final Path path;
    private final byte[] magic;
    private final int maxRecordBytes;
    private final Parser<T> parser;
    private final FileChannel file;
    private final FileLock lock;
    private final List<T> records = new ArrayList<>();
    private boolean closed;

    FramedProtobufWal(Path path, byte[] magic, int maxRecordBytes, Parser<T> parser)
            throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        this.magic = Objects.requireNonNull(magic, "magic").clone();
        this.parser = Objects.requireNonNull(parser, "parser");
        if (magic.length < 4 || magic.length > 64) {
            throw new IllegalArgumentException("WAL magic must contain 4 to 64 bytes");
        }
        if (maxRecordBytes < 1) {
            throw new IllegalArgumentException("maxRecordBytes must be positive");
        }
        this.maxRecordBytes = maxRecordBytes;
        Path parent = this.path.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("WAL path requires a parent directory");
        }
        Files.createDirectories(parent);

        FileChannel opened = null;
        FileLock acquired = null;
        try {
            opened = FileChannel.open(this.path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            try {
                acquired = opened.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IOException("WAL already has a writer: " + this.path, e);
            }
            if (acquired == null) {
                throw new IOException("WAL already has a writer: " + this.path);
            }
            this.file = opened;
            this.lock = acquired;
            initializeOrRecover();
        } catch (Throwable failure) {
            closeAfterFailedOpen(acquired, opened);
            throw failure;
        }
    }

    List<T> records() {
        return List.copyOf(records);
    }

    void append(T record) throws IOException {
        Objects.requireNonNull(record, "record");
        requireOpen();
        byte[] bytes = record.toByteArray();
        if (bytes.length < 1 || bytes.length > maxRecordBytes) {
            throw new IOException("WAL record is outside the 1 to " + maxRecordBytes
                    + " byte bound: " + bytes.length);
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        ByteBuffer frame = ByteBuffer.allocate(
                Integer.BYTES + bytes.length + Integer.BYTES);
        frame.putInt(bytes.length).put(bytes).putInt((int) crc.getValue()).flip();
        file.position(file.size());
        writeFully(frame);
        file.force(true);
        records.add(record);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            if (lock.isValid()) {
                lock.release();
            }
        } catch (IOException e) {
            failure = e;
        }
        try {
            file.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void initializeOrRecover() throws IOException {
        if (file.size() == 0) {
            writeFully(ByteBuffer.wrap(magic));
            file.force(true);
            return;
        }
        if (file.size() < magic.length) {
            throw new IOException("WAL header is truncated: " + path);
        }
        ByteBuffer header = ByteBuffer.allocate(magic.length);
        readFully(header, 0);
        if (!java.util.Arrays.equals(header.array(), magic)) {
            throw new IOException("WAL format header does not match: " + path);
        }

        long position = magic.length;
        while (position < file.size()) {
            long frameStart = position;
            if (file.size() - position < Integer.BYTES) {
                truncateTail(frameStart);
                break;
            }
            ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
            readFully(lengthBuffer, position);
            int length = ByteBuffer.wrap(lengthBuffer.array()).getInt();
            if (length < 1 || length > maxRecordBytes) {
                throw new IOException("invalid WAL frame length " + length
                        + " at byte " + position + " in " + path);
            }
            position += Integer.BYTES;
            if (file.size() - position < (long) length + Integer.BYTES) {
                truncateTail(frameStart);
                break;
            }
            ByteBuffer bytes = ByteBuffer.allocate(length);
            readFully(bytes, position);
            position += length;
            ByteBuffer crcBuffer = ByteBuffer.allocate(Integer.BYTES);
            readFully(crcBuffer, position);
            position += Integer.BYTES;

            CRC32C crc = new CRC32C();
            crc.update(bytes.array(), 0, length);
            int expected = ByteBuffer.wrap(crcBuffer.array()).getInt();
            if ((int) crc.getValue() != expected) {
                throw new IOException("WAL CRC mismatch at byte " + frameStart
                        + " in " + path);
            }
            try {
                records.add(parser.parseFrom(bytes.array()));
            } catch (InvalidProtocolBufferException e) {
                throw new IOException("invalid WAL protobuf at byte " + frameStart
                        + " in " + path, e);
            }
        }
        file.position(file.size());
    }

    private void truncateTail(long position) throws IOException {
        file.truncate(position);
        file.force(true);
    }

    private void writeFully(ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            file.write(source);
        }
    }

    private void readFully(ByteBuffer destination, long position) throws IOException {
        while (destination.hasRemaining()) {
            int read = file.read(destination, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in WAL " + path);
            }
            position += read;
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("WAL is closed: " + path);
        }
    }

    private static void closeAfterFailedOpen(FileLock lock, FileChannel file) {
        try {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } catch (IOException ignored) {
            // Preserve the failure that prevented the WAL from opening.
        }
        try {
            if (file != null && file.isOpen()) {
                file.close();
            }
        } catch (IOException ignored) {
            // Preserve the failure that prevented the WAL from opening.
        }
    }
}
