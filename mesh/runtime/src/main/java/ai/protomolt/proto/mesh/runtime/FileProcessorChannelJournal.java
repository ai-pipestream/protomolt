package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

/** One-writer, CRC32C-framed protobuf journal for the processor channel. */
final class FileProcessorChannelJournal implements ProcessorChannelJournal {

    static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;
    private static final byte[] MAGIC = {'P', 'M', 'C', 'H', '0', '0', '0', '2'};

    private final Path path;
    private final FileChannel file;
    private final FileLock lock;
    private boolean closed;

    FileProcessorChannelJournal(Path path) {
        this.path = path.toAbsolutePath().normalize();
        FileChannel opened = null;
        FileLock acquired = null;
        try {
            Path parent = this.path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            opened = FileChannel.open(this.path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            try {
                acquired = opened.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IllegalStateException(
                        "processor channel is already open: " + this.path, e);
            }
            if (acquired == null) {
                throw new IllegalStateException(
                        "processor channel is locked by another process: " + this.path);
            }
            file = opened;
            lock = acquired;
            initialize();
        } catch (IOException | RuntimeException e) {
            closeAfterFailedOpen(acquired, opened);
            if (e instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("cannot open processor channel " + this.path, e);
        }
    }

    @Override
    public synchronized List<ChannelRecord> load() {
        requireOpen();
        List<ChannelRecord> records = new ArrayList<>();
        try {
            long position = MAGIC.length;
            while (position < file.size()) {
                long frameStart = position;
                if (file.size() - position < Integer.BYTES) {
                    truncateTail(frameStart);
                    break;
                }
                ByteBuffer lengthBytes = ByteBuffer.allocate(Integer.BYTES);
                readFully(lengthBytes, position);
                int length = ByteBuffer.wrap(lengthBytes.array()).getInt();
                if (length < 1 || length > MAX_RECORD_BYTES) {
                    throw new IllegalArgumentException(
                            "invalid processor channel frame length " + length
                                    + " at byte " + position);
                }
                position += Integer.BYTES;
                if (file.size() - position < (long) length + Integer.BYTES) {
                    truncateTail(frameStart);
                    break;
                }
                ByteBuffer bytes = ByteBuffer.allocate(length);
                readFully(bytes, position);
                position += length;
                ByteBuffer checksumBytes = ByteBuffer.allocate(Integer.BYTES);
                readFully(checksumBytes, position);
                position += Integer.BYTES;
                CRC32C checksum = new CRC32C();
                checksum.update(bytes.array(), 0, length);
                if ((int) checksum.getValue()
                        != ByteBuffer.wrap(checksumBytes.array()).getInt()) {
                    throw new IllegalArgumentException(
                            "processor channel CRC mismatch at byte " + frameStart);
                }
                try {
                    records.add(ChannelRecord.parseFrom(bytes.array()));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException(
                            "invalid processor channel protobuf at byte " + frameStart, e);
                }
            }
            file.position(file.size());
            return List.copyOf(records);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read processor channel " + path, e);
        }
    }

    @Override
    public synchronized void append(ChannelRecord record) {
        requireOpen();
        byte[] bytes = record.toByteArray();
        if (bytes.length > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException(
                    "channel record exceeds " + MAX_RECORD_BYTES + " bytes");
        }
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + bytes.length + Integer.BYTES)
                .putInt(bytes.length).put(bytes).putInt((int) checksum.getValue());
        frame.flip();
        try {
            file.position(file.size());
            while (frame.hasRemaining()) {
                file.write(frame);
            }
            file.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("failed to append processor channel record", e);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            lock.release();
            file.close();
        } catch (IOException e) {
            throw new IllegalStateException("failed to close processor channel " + path, e);
        }
    }

    private void initialize() throws IOException {
        if (file.size() == 0) {
            ByteBuffer header = ByteBuffer.wrap(MAGIC);
            while (header.hasRemaining()) {
                file.write(header);
            }
            file.force(true);
            return;
        }
        if (file.size() < MAGIC.length) {
            throw new IllegalArgumentException(
                    "processor channel has an incomplete header: " + path);
        }
        ByteBuffer header = ByteBuffer.allocate(MAGIC.length);
        readFully(header, 0);
        if (!Arrays.equals(header.array(), MAGIC)) {
            throw new IllegalArgumentException(
                    "processor-channel-format-unsupported: expected PMCH0002 at " + path);
        }
    }

    private void truncateTail(long position) throws IOException {
        file.truncate(position);
        file.force(true);
    }

    private void readFully(ByteBuffer destination, long position) throws IOException {
        while (destination.hasRemaining()) {
            int read = file.read(destination, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in processor channel");
            }
            position += read;
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("processor channel journal is closed");
        }
    }

    private static void closeAfterFailedOpen(FileLock lock, FileChannel file) {
        try {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } catch (IOException ignored) {
            // Preserve the original open failure.
        }
        try {
            if (file != null && file.isOpen()) {
                file.close();
            }
        } catch (IOException ignored) {
            // Preserve the original open failure.
        }
    }
}
