package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailure;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/** Single-coordinator durable preparation ledger with tuple execution ownership. */
public final class FileSystemWorkflowPreparationRepository implements WorkflowPreparationRepository {
    private static final ConcurrentMap<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();
    private final Path root;
    private final Path records;
    private final Path locks;
    private final Path globalLock;
    private final BeforePublish beforePublish;
    private final AfterPublish afterPublish;

    public FileSystemWorkflowPreparationRepository(Path directory) throws IOException {
        this(directory, (temporary, target) -> {}, target -> {});
    }

    FileSystemWorkflowPreparationRepository(Path directory, BeforePublish beforePublish) throws IOException {
        this(directory, beforePublish, target -> {});
    }

    FileSystemWorkflowPreparationRepository(Path directory, BeforePublish beforePublish,
            AfterPublish afterPublish) throws IOException {
        if (directory == null || beforePublish == null || afterPublish == null) {
            throw new IllegalArgumentException("directory and hooks required");
        }
        Files.createDirectories(directory);
        root = directory.toRealPath();
        records = root.resolve("records");
        locks = root.resolve("locks");
        Files.createDirectories(records);
        Files.createDirectories(locks);
        if (Files.isSymbolicLink(records) || Files.isSymbolicLink(locks)) {
            throw new IOException("preparation ledger directory is a symbolic link");
        }
        globalLock = root.resolve(".preparation-global.lock");
        this.beforePublish = beforePublish;
        this.afterPublish = afterPublish;
    }

    @Override
    public Optional<WorkflowPreparationRecord> find(String taskId, int attempt, int revision)
            throws IOException {
        String key = key(taskId, attempt, revision);
        return global(() -> read(recordPath(key), key));
    }

    @Override
    public <T> T withExclusiveIntent(String taskId, int attempt, int revision, Work<T> work)
            throws Exception {
        if (work == null) throw new IllegalArgumentException("work required");
        String key = key(taskId, attempt, revision);
        Path lockPath = locks.resolve(key + ".lock");
        ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(lockPath, ignored -> new ReentrantLock());
        jvmLock.lock();
        try (FileChannel channel = lockChannel(lockPath); FileLock ignored = channel.lock()) {
            SessionImpl session = new SessionImpl(key);
            try {
                return work.execute(session);
            } finally {
                session.active = false;
            }
        } finally {
            jvmLock.unlock();
        }
    }

    private final class SessionImpl implements Session {
        private final String key;
        private final Thread owner = Thread.currentThread();
        private boolean active = true;

        SessionImpl(String key) { this.key = key; }

        @Override
        public Optional<WorkflowPreparationRecord> current() throws IOException {
            checkActive();
            return global(() -> read(recordPath(key), key));
        }

        @Override
        public WorkflowPreparationRecord reserveOrMatch(WorkflowPreparationIntent intent) throws IOException {
            checkActive();
            WorkflowPreparationValidation.validateIntent(intent);
            if (!key.equals(key(intent.getRequest().getTaskId(), intent.getRequest().getAttempt(),
                    intent.getRequest().getRevision()))) {
                throw new WorkflowPreparationConflictException("preparation tuple differs from session");
            }
            return global(() -> {
                Optional<WorkflowPreparationRecord> old = read(recordPath(key), key);
                // Scan every record under the global lock. Malformed records fail closed;
                // no stale or rebuildable UUID index can silently permit reuse.
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(records, "*.pb")) {
                    for (Path path : stream) {
                        WorkflowPreparationRecord other = read(path, fileKey(path)).orElseThrow();
                        if (other.getIntent().getBinding().getPreparationId()
                                .equals(intent.getBinding().getPreparationId())
                                && !fileKey(path).equals(key)) {
                            throw new WorkflowPreparationConflictException("preparation UUID already reserved");
                        }
                    }
                }
                if (old.isPresent()) {
                    if (!Arrays.equals(bytes(old.get().getIntent()), bytes(intent))) {
                        throw new WorkflowPreparationConflictException("preparation intent differs");
                    }
                    return old.get();
                }
                WorkflowPreparationRecord pending = WorkflowPreparationRecord.newBuilder()
                        .setIntent(intent).setPending(true).build();
                write(recordPath(key), pending);
                return pending;
            });
        }

        @Override
        public WorkflowPreparationRecord complete(PrepareWorkflowCandidateResponse response)
                throws IOException {
            checkActive();
            WorkflowPreparationValidation.validateResponse(response);
            return transition(WorkflowPreparationRecord.newBuilder().setCompleted(response));
        }

        @Override
        public WorkflowPreparationRecord fail(WorkflowPreparationFailure failure) throws IOException {
            checkActive();
            WorkflowPreparationValidation.validate(failure, WorkflowPreparationValidation.RESPONSE_MAX);
            return transition(WorkflowPreparationRecord.newBuilder().setFailed(failure));
        }

        private WorkflowPreparationRecord transition(WorkflowPreparationRecord.Builder builder)
                throws IOException {
            return global(() -> {
                WorkflowPreparationRecord old = read(recordPath(key), key)
                        .orElseThrow(() -> new WorkflowPreparationConflictException("preparation not reserved"));
                WorkflowPreparationRecord proposed = builder.setIntent(old.getIntent()).build();
                if (!old.hasPending()) {
                    if (Arrays.equals(bytes(old), bytes(proposed))) return old;
                    throw new WorkflowPreparationConflictException("preparation already terminal");
                }
                WorkflowPreparationValidation.validateRecord(proposed);
                write(recordPath(key), proposed);
                return proposed;
            });
        }

        private void checkActive() {
            if (!active || Thread.currentThread() != owner) {
                throw new IllegalStateException("preparation session is not owned by this thread");
            }
        }
    }

    private Optional<WorkflowPreparationRecord> read(Path path, String expectedKey) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("preparation record is not a regular file");
        }
        if (Files.size(path) > WorkflowPreparationValidation.RECORD_MAX) {
            throw new IOException("preparation record exceeds storage bound");
        }
        try {
            byte[] data = Files.readAllBytes(path);
            if (data.length > WorkflowPreparationValidation.RECORD_MAX) {
                throw new IOException("preparation record exceeds storage bound");
            }
            WorkflowPreparationRecord record = WorkflowPreparationRecord.parseFrom(data);
            WorkflowPreparationValidation.validateRecord(record);
            if (!expectedKey.equals(key(record.getIntent().getRequest().getTaskId(),
                    record.getIntent().getRequest().getAttempt(), record.getIntent().getRequest().getRevision()))) {
                throw new IOException("preparation record tuple differs from path");
            }
            if (!Arrays.equals(data, bytes(record))) {
                throw new IOException("preparation record is noncanonical");
            }
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                channel.force(true);
            }
            forceDirectory(records);
            return Optional.of(record);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new IOException("invalid preparation record", e);
        }
    }

    private void write(Path target, WorkflowPreparationRecord record) throws IOException {
        WorkflowPreparationValidation.validateRecord(record);
        byte[] data = bytes(record);
        Path temporary = Files.createTempFile(records, ".preparation-", ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            beforePublish.run(temporary, target);
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            afterPublish.run(target);
            forceDirectory(records);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private <T> T global(IoOperation<T> operation) throws IOException {
        ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(globalLock, ignored -> new ReentrantLock());
        jvmLock.lock();
        try (FileChannel channel = lockChannel(globalLock); FileLock ignored = channel.lock()) {
            return operation.run();
        } finally {
            jvmLock.unlock();
        }
    }

    private static FileChannel lockChannel(Path path) throws IOException {
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(path)) {
            throw new IOException("preparation lock is a symbolic link");
        }
        return FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS);
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private Path recordPath(String key) { return records.resolve(key + ".pb"); }

    private static String fileKey(Path path) throws IOException {
        String name = path.getFileName().toString();
        if (!name.matches("[0-9a-f]{64}\\.pb")) throw new IOException("invalid preparation record name");
        return name.substring(0, 64);
    }

    private static String key(String taskId, int attempt, int revision) {
        if (taskId == null || !taskId.matches(
                "(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                || attempt < 1 || attempt > 1024 || revision < 1 || revision > 1024) {
            throw new IllegalArgumentException("valid preparation tuple required");
        }
        byte[] task = taskId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer tuple = ByteBuffer.allocate(Integer.BYTES + task.length + Integer.BYTES * 2);
        tuple.putInt(task.length).put(task).putInt(attempt).putInt(revision);
        return WorkRecords.sha256Hex(tuple.array());
    }

    private static byte[] bytes(WorkflowPreparationRecord record) {
        return WorkflowPreparationValidation.deterministicBytes(record,
                WorkflowPreparationValidation.RECORD_MAX);
    }

    private static byte[] bytes(WorkflowPreparationIntent intent) {
        return WorkflowPreparationValidation.deterministicBytes(intent,
                WorkflowPreparationValidation.INTENT_MAX);
    }

    @FunctionalInterface
    interface BeforePublish {
        void run(Path temporary, Path target) throws IOException;
    }

    @FunctionalInterface
    interface AfterPublish {
        void run(Path target) throws IOException;
    }

    @FunctionalInterface
    private interface IoOperation<T> { T run() throws IOException; }
}
