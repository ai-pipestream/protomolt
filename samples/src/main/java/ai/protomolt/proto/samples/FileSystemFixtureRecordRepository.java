package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.FixtureStoredRecord;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/** A write-once, fsynced sample ledger keyed by the caller's operation UUID. */
public final class FileSystemFixtureRecordRepository {
    private static final ConcurrentMap<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    @FunctionalInterface
    interface BeforePublish {
        void run() throws IOException;
    }

    private final Path root;
    private final Path lockPath;
    private final ReentrantLock jvmLock;
    private final BeforePublish beforePublish;
    private final BeforePublish afterPublish;

    public FileSystemFixtureRecordRepository(Path directory) throws IOException {
        this(directory, () -> {}, () -> {});
    }

    // Test seam for a storage failure after the temporary file has been synced.
    FileSystemFixtureRecordRepository(Path directory, BeforePublish beforePublish) throws IOException {
        this(directory, beforePublish, () -> {});
    }

    // The second hook models an interrupted reply after atomic publish but before directory fsync.
    FileSystemFixtureRecordRepository(Path directory, BeforePublish beforePublish,
            BeforePublish afterPublish) throws IOException {
        if (directory == null || beforePublish == null || afterPublish == null) {
            throw new IllegalArgumentException("directory and hooks are required");
        }
        Files.createDirectories(directory);
        root = directory.toRealPath();
        lockPath = root.resolve(".fixture-record.lock");
        jvmLock = JVM_LOCKS.computeIfAbsent(lockPath, ignored -> new ReentrantLock());
        this.beforePublish = beforePublish;
        this.afterPublish = afterPublish;
    }

    public Optional<FixtureStoredRecord> find(String operationId) throws IOException {
        String key = canonicalId(operationId);
        return locked(() -> read(path(key), key));
    }

    public WriteRecordResponse writeOrMatch(WriteRecordRequest request) throws IOException {
        if (request == null) throw new IllegalArgumentException("request is required");
        FixtureValidation.validate(request);
        String key = canonicalId(request.getOperationId());
        byte[] content = request.getContentBytes().toByteArray();
        WriteRecordResponse response = WriteRecordResponse.newBuilder()
                .setOperationId(key)
                .setContentSha256(sha256(content))
                .build();
        FixtureValidation.validate(response);
        FixtureStoredRecord proposed = FixtureStoredRecord.newBuilder()
                .setFormatVersion(1)
                .setRequest(request)
                .setResponse(response)
                .build();
        FixtureValidation.validate(proposed);
        byte[] bytes = proposed.toByteArray();
        if (bytes.length > FixtureValidation.MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("fixture record exceeds storage limit");
        }
        Path target = path(key);
        return locked(() -> {
            Optional<FixtureStoredRecord> stored = read(target, key);
            if (stored.isPresent()) {
                if (!MessageDigest.isEqual(content,
                        stored.get().getRequest().getContentBytes().toByteArray())) {
                    throw new FixtureRecordConflictException();
                }
                return stored.get().getResponse();
            }
            Path temporary = Files.createTempFile(root, ".fixture-record-", ".tmp");
            try {
                try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    while (buffer.hasRemaining()) output.write(buffer);
                    output.force(true);
                }
                beforePublish.run();
                // No non-atomic fallback: a temporary file is never a committed record.
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                afterPublish.run();
                forceDirectory();
                return response;
            } finally {
                Files.deleteIfExists(temporary);
            }
        });
    }

    private Optional<FixtureStoredRecord> read(Path target, String key) throws IOException {
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("fixture record is not a regular file");
        }
        if (Files.size(target) > FixtureValidation.MAX_RECORD_BYTES) {
            throw new IOException("fixture record exceeds storage limit");
        }
        try {
            byte[] bytes = Files.readAllBytes(target);
            if (bytes.length > FixtureValidation.MAX_RECORD_BYTES) {
                throw new IOException("fixture record exceeds storage limit");
            }
            FixtureStoredRecord stored = FixtureStoredRecord.parseFrom(bytes);
            FixtureValidation.validate(stored);
            if (stored.getFormatVersion() != 1
                    || !canonicalId(stored.getRequest().getOperationId()).equals(key)
                    || !stored.getRequest().getOperationId().equals(key)
                    || !stored.getResponse().getOperationId().equals(key)
                    || !stored.getResponse().getContentSha256().equals(
                            sha256(stored.getRequest().getContentBytes().toByteArray()))
                    || !MessageDigest.isEqual(bytes, stored.toByteArray())) {
                throw new IOException("fixture record failed integrity check");
            }
            // Finish the durability barrier if a prior process died after rename.
            try (FileChannel file = FileChannel.open(target, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                file.force(true);
            }
            forceDirectory();
            return Optional.of(stored);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new IOException("invalid fixture record", e);
        }
    }

    private Path path(String key) {
        return root.resolve(key + ".pb");
    }

    private static String canonicalId(String id) {
        if (id == null) throw new IllegalArgumentException("operation ID is required");
        try {
            String canonical = UUID.fromString(id).toString();
            if (!canonical.equals(id)) throw new IllegalArgumentException("noncanonical UUID");
            return canonical;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("operation ID must be a canonical lowercase UUID", e);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private void forceDirectory() throws IOException {
        try (FileChannel directory = FileChannel.open(root, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private <T> T locked(IoOperation<T> operation) throws IOException {
        jvmLock.lock();
        try (FileChannel channel = FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS);
                FileLock ignored = channel.lock()) {
            return operation.run();
        } finally {
            jvmLock.unlock();
        }
    }

    @FunctionalInterface
    private interface IoOperation<T> {
        T run() throws IOException;
    }
}
