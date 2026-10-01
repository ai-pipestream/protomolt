package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerPending;
import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerState;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorAssignment;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** One-process, crash-durable recovery state for the demonstration author. */
public final class AuthoringWorkerStateStore implements AutoCloseable {
    static final int MAX_BYTES = 16 * 1024 * 1024;
    static final String PROBE_CONTENT = "Hello\nworld";
    private static final String STATE_FILE = "authoring-worker-state.pb";
    private static final String LOCK_FILE = ".authoring-worker-state.lock";
    private static final String DELIVERABLE_TYPE_URL = "type.googleapis.com/"
            + WorkflowAuthoringDeliverable.getDescriptor().getFullName();

    private final Path root;
    private final Path file;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final String coordinator;
    private final String fixture;
    private final String workerId;
    private final AtomicMove atomicMove;
    private final AfterMove afterMove;
    private AuthoringWorkerState state;
    private boolean closed;
    private boolean poisoned;

    private AuthoringWorkerStateStore(Path root, FileChannel lockChannel, FileLock lock,
            String coordinator, String fixture, String workerId,
            AtomicMove atomicMove, AfterMove afterMove) {
        this.root = root;
        this.file = root.resolve(STATE_FILE);
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.coordinator = coordinator;
        this.fixture = fixture;
        this.workerId = workerId;
        this.atomicMove = atomicMove;
        this.afterMove = afterMove;
    }

    /** Locks the directory for this worker's entire run and validates its saved identity. */
    public static AuthoringWorkerStateStore open(Path directory, String coordinator,
            String fixture, String workerId) throws IOException {
        return open(directory, coordinator, fixture, workerId,
                (source, target) -> Files.move(source, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING), () -> {});
    }

    // Test seams for an unsupported atomic move and an uncertain exit after rename.
    static AuthoringWorkerStateStore open(Path directory, String coordinator,
            String fixture, String workerId, AtomicMove atomicMove, AfterMove afterMove) throws IOException {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(atomicMove, "atomicMove");
        Objects.requireNonNull(afterMove, "afterMove");
        requireIdentity(coordinator, fixture, workerId);
        Files.createDirectories(directory);
        Path root = directory.toRealPath();
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("author state path is not a directory");
        }
        FileChannel channel = FileChannel.open(root.resolve(LOCK_FILE),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        FileLock lock = null;
        try {
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException alreadyHeld) {
                throw new IOException("author state is already owned", alreadyHeld);
            }
            if (lock == null) throw new IOException("author state is already owned");
            var store = new AuthoringWorkerStateStore(root, channel, lock, coordinator, fixture, workerId,
                    atomicMove, afterMove);
            if (Files.exists(store.file, LinkOption.NOFOLLOW_LINKS)) {
                store.state = store.read();
            } else {
                store.state = AuthoringWorkerState.newBuilder().setFormatVersion(1)
                        .setCoordinator(coordinator).setFixture(fixture).setWorkerId(workerId).build();
                // Bind this directory before the caller can register or discover work.
                store.commit(store.state);
            }
            return store;
        } catch (IOException | RuntimeException | Error failure) {
            if (lock != null) lock.release();
            channel.close();
            throw failure;
        }
    }

    /** Returns the last fully committed snapshot. A failed commit poisons this instance. */
    public synchronized AuthoringWorkerState state() {
        requireOpen();
        return state;
    }

    /** Publishes the entire validated snapshot before its dependent network effect. */
    public synchronized void commit(AuthoringWorkerState proposed) throws IOException {
        requireOpen();
        validate(proposed, coordinator, fixture, workerId);
        if (state != null) requireTransition(state, proposed);
        byte[] bytes = proposed.toByteArray();
        Path temporary = null;
        try {
            temporary = Files.createTempFile(root, ".authoring-worker-state-", ".tmp");
            try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) output.write(buffer);
                output.force(true);
            }
            atomicMove.move(temporary, file);
            afterMove.run();
            forceDirectory();
            state = proposed;
        } catch (IOException | RuntimeException | Error failure) {
            // A failure after rename has an ambiguous durable outcome. No call using
            // this instance may proceed from stale in-memory state.
            poisoned = true;
            throw failure;
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException cleanup) {
                    poisoned = true;
                    throw cleanup;
                }
            }
        }
    }

    /** Stable fixture key for a particular original offer; use before WriteRecord. */
    static String probeOperationId(WorkflowAuthorAssignment assignment, String workerId) {
        Objects.requireNonNull(assignment, "assignment");
        Objects.requireNonNull(workerId, "workerId");
        return UUID.nameUUIDFromBytes(("authoring-probe-v1|" + workerId + "|"
                + assignment.getTaskId() + "|" + assignment.getAttempt() + "|"
                + assignment.getOfferEntrySha256()).getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** Preserve the sample's existing preparation key across its legacy and idle modes. */
    static String preparationId(WorkflowAuthorAssignment assignment, String workerId) {
        Objects.requireNonNull(assignment, "assignment");
        Objects.requireNonNull(workerId, "workerId");
        return UUID.nameUUIDFromBytes(("authoring-preparation-v1|" + assignment.getTaskId()
                + "|" + assignment.getAttempt() + "|1|" + workerId)
                .getBytes(StandardCharsets.UTF_8)).toString();
    }

    private AuthoringWorkerState read() throws IOException {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("author state is not a regular file");
        }
        if (Files.size(file) > MAX_BYTES) throw new IOException("author state exceeds size limit");
        try {
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length > MAX_BYTES) throw new IOException("author state exceeds size limit");
            AuthoringWorkerState loaded = AuthoringWorkerState.parseFrom(bytes);
            validate(loaded, coordinator, fixture, workerId);
            if (!Arrays.equals(bytes, loaded.toByteArray())) {
                throw new IOException("author state has noncanonical protobuf encoding");
            }
            // Complete a prior process's durability barrier after an interrupted
            // acknowledgement between atomic rename and directory fsync.
            try (FileChannel existing = FileChannel.open(file, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                existing.force(true);
            }
            forceDirectory();
            return loaded;
        } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) {
            throw new IOException("author state is corrupt or belongs to another worker", malformed);
        }
    }

    private static void validate(AuthoringWorkerState candidate, String coordinator,
            String fixture, String workerId) {
        Objects.requireNonNull(candidate, "candidate");
        if (candidate.getSerializedSize() > MAX_BYTES) {
            throw new IllegalArgumentException("author state exceeds size limit");
        }
        validateMessage(candidate);
        if (candidate.getFormatVersion() != 1 || !candidate.getCoordinator().equals(coordinator)
                || !candidate.getFixture().equals(fixture) || !candidate.getWorkerId().equals(workerId)) {
            throw new IllegalArgumentException("author state identity or format differs");
        }
        long priorCursor = 0;
        Set<String> identities = new HashSet<>();
        for (AuthoringWorkerPending pending : candidate.getPendingList()) {
            WorkflowAuthorAssignment assignment = pending.getAssignment();
            if (assignment.getCursor() <= priorCursor
                    || assignment.getCursor() > candidate.getDiscoveryCursor()
                    || !identities.add(assignment.getTaskId() + "|" + assignment.getAttempt())) {
                throw new IllegalArgumentException("author assignments are unordered or duplicated");
            }
            priorCursor = assignment.getCursor();
            if (pending.hasProbe() && (!pending.getProbe().getOperationId().equals(
                    probeOperationId(assignment, workerId))
                    || !pending.getProbe().getContent().equals(PROBE_CONTENT))) {
                throw new IllegalArgumentException("author probe intent differs");
            }
            if (pending.hasPreparation() && !pending.getPreparation().getPreparationId().equals(
                    preparationId(assignment, workerId))) {
                throw new IllegalArgumentException("author preparation intent differs");
            }
            if (pending.hasSubmission()) {
                Any result = pending.getSubmission().getCandidate().getResult();
                if (!result.getTypeUrl().equals(DELIVERABLE_TYPE_URL)) {
                    throw new IllegalArgumentException("author submission deliverable type differs");
                }
            }
        }
    }

    private static void requireTransition(AuthoringWorkerState previous,
            AuthoringWorkerState next) {
        if (next.getDiscoveryCursor() < previous.getDiscoveryCursor()) {
            throw new IllegalArgumentException("author discovery cursor cannot regress");
        }
        for (AuthoringWorkerPending before : previous.getPendingList()) {
            for (AuthoringWorkerPending after : next.getPendingList()) {
                if (before.getAssignment().getTaskId().equals(after.getAssignment().getTaskId())
                        && before.getAssignment().getAttempt() == after.getAssignment().getAttempt()
                        && !before.getAssignment().equals(after.getAssignment())) {
                    throw new IllegalArgumentException("author assignment identity cannot change");
                }
                if (before.getAssignment().getCursor() != after.getAssignment().getCursor()) continue;
                if (!before.getAssignment().equals(after.getAssignment())
                        || (before.hasProbe() && !before.getProbe().equals(after.getProbe()))
                        || (before.hasPreparation() && !before.getPreparation().equals(after.getPreparation()))
                        || (before.hasSubmission() && !before.getSubmission().equals(after.getSubmission()))
                        || after.getReviewCursor() < before.getReviewCursor()
                        || after.getSubmissionCursor() < before.getSubmissionCursor()) {
                    throw new IllegalArgumentException("author intent or event cursor cannot change");
                }
            }
        }
        for (AuthoringWorkerPending after : next.getPendingList()) {
            if (after.getAssignment().getCursor() <= previous.getDiscoveryCursor()
                    && previous.getPendingList().stream().noneMatch(before ->
                            before.getAssignment().getCursor() == after.getAssignment().getCursor())) {
                throw new IllegalArgumentException("retired author assignment cannot be reintroduced");
            }
        }
    }

    private static void requireIdentity(String coordinator, String fixture, String workerId) {
        if (coordinator == null || coordinator.isBlank() || coordinator.length() > 2048
                || fixture == null || fixture.isBlank() || fixture.length() > 2048
                || workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("author state identity is required");
        }
        AuthoringWorkerState initial = AuthoringWorkerState.newBuilder().setFormatVersion(1)
                .setCoordinator(coordinator).setFixture(fixture).setWorkerId(workerId).build();
        validateMessage(initial);
    }

    private static void validateMessage(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unknown author state fields");
        }
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw new IllegalArgumentException("invalid author state message: " + result.violations());
        if (message instanceof Any any) {
            if (!any.getTypeUrl().equals(DELIVERABLE_TYPE_URL)) {
                throw new IllegalArgumentException("unsupported author state Any type");
            }
            try {
                validateMessage(any.unpack(WorkflowAuthoringDeliverable.class));
            } catch (InvalidProtocolBufferException malformed) {
                throw new IllegalArgumentException("invalid author state deliverable", malformed);
            }
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) validateMessage((Message) nested);
            } else {
                validateMessage((Message) field.getValue());
            }
        }
    }

    private void forceDirectory() throws IOException {
        try (FileChannel directory = FileChannel.open(root, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private void requireOpen() {
        if (closed || poisoned) throw new IllegalStateException("author state store is closed or uncertain");
    }

    @Override public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            lock.release();
        } finally {
            lockChannel.close();
        }
    }

    @FunctionalInterface interface AtomicMove {
        void move(Path source, Path target) throws IOException;
    }

    @FunctionalInterface interface AfterMove {
        void run() throws IOException;
    }
}
