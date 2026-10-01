package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchAuthorization;
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
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single-coordinator filesystem ledger. A same-directory atomic rename publishes each
 * validated protobuf record; a cross-process lock serializes creation and comparison.
 */
public final class FileSystemWorkflowLaunchAuthorizationRepository
        implements WorkflowLaunchAuthorizationRepository {

    private static final ConcurrentMap<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();
    private final Path root;
    private final Path lockPath;
    private final ReentrantLock jvmLock;

    public FileSystemWorkflowLaunchAuthorizationRepository(Path directory) throws IOException {
        if (directory == null) throw new IllegalArgumentException("directory must not be null");
        Files.createDirectories(directory);
        root = directory.toRealPath();
        lockPath = root.resolve(".workflow-launch.lock");
        jvmLock = JVM_LOCKS.computeIfAbsent(lockPath, ignored -> new ReentrantLock());
    }

    @Override
    public Optional<WorkflowAuthoringLaunchAuthorization> find(String launchId) throws IOException {
        String key = canonicalId(launchId);
        Path target = path(key);
        return locked(() -> read(target, key));
    }

    @Override
    public WorkflowAuthoringLaunchAuthorization createOrMatch(
            WorkflowAuthoringLaunchAuthorization authorization) throws IOException {
        if (authorization == null) throw new IllegalArgumentException("authorization must not be null");
        WorkflowLaunchValidation.validate(authorization);
        String launchId = authorization.getRequest().getLaunchId();
        String key = canonicalId(launchId);
        Path target = path(key);
        byte[] proposed = WorkflowLaunchValidation.deterministicBytes(authorization);
        if (proposed.length > WorkflowLaunchValidation.MAX_BYTES) {
            throw new IllegalArgumentException("launch authorization exceeds 4 MiB");
        }
        return locked(() -> {
            Optional<WorkflowAuthoringLaunchAuthorization> existing = read(target, key);
            if (existing.isPresent()) {
                if (!Arrays.equals(proposed,
                        WorkflowLaunchValidation.deterministicBytes(existing.get()))) {
                    throw new WorkflowLaunchConflictException(
                            "launch authorization conflict for " + launchId);
                }
                return existing.get();
            }
            Path temporary = Files.createTempFile(root, ".workflow-launch-", ".tmp");
            try {
                try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    ByteBuffer buffer = ByteBuffer.wrap(proposed);
                    while (buffer.hasRemaining()) output.write(buffer);
                    output.force(true);
                }
                // No non-atomic fallback: interrupted writes must never become visible.
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                forceDirectory();
                return authorization;
            } finally {
                Files.deleteIfExists(temporary);
            }
        });
    }

    private Optional<WorkflowAuthoringLaunchAuthorization> read(Path target, String launchId)
            throws IOException {
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new WorkflowLaunchAuthorizationCorruptException(
                    "launch authorization is not a regular file: " + target);
        }
        long size = Files.size(target);
        if (size > WorkflowLaunchValidation.MAX_BYTES) {
            throw new WorkflowLaunchAuthorizationCorruptException(
                    "stored launch authorization exceeds 4 MiB: " + launchId);
        }
        try {
            byte[] bytes = Files.readAllBytes(target);
            if (bytes.length > WorkflowLaunchValidation.MAX_BYTES) {
                throw new WorkflowLaunchAuthorizationCorruptException(
                        "stored launch authorization exceeds 4 MiB: " + launchId);
            }
            WorkflowAuthoringLaunchAuthorization stored =
                    WorkflowAuthoringLaunchAuthorization.parseFrom(bytes);
            WorkflowLaunchValidation.validate(stored);
            if (!canonicalId(stored.getRequest().getLaunchId()).equals(launchId)) {
                throw new WorkflowLaunchAuthorizationCorruptException(
                        "launch authorization identity differs from path: " + launchId);
            }
            if (!Arrays.equals(bytes, WorkflowLaunchValidation.deterministicBytes(stored))) {
                throw new WorkflowLaunchAuthorizationCorruptException(
                        "launch authorization has noncanonical storage bytes: " + launchId);
            }
            // A prior writer may have crashed after rename but before directory fsync.
            // Complete that durability barrier before allowing recovery effects.
            try (FileChannel file = FileChannel.open(target,
                    StandardOpenOption.READ, StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS)) {
                file.force(true);
            }
            forceDirectory();
            return Optional.of(stored);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new WorkflowLaunchAuthorizationCorruptException(
                    "invalid stored launch authorization: " + launchId, e);
        }
    }

    private void forceDirectory() throws IOException {
        try (FileChannel directory = FileChannel.open(root, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private Path path(String launchId) {
        return root.resolve(launchId + ".pb");
    }

    private static String canonicalId(String launchId) {
        if (launchId == null) throw new IllegalArgumentException("launchId must not be null");
        try {
            String canonical = UUID.fromString(launchId).toString();
            if (!canonical.equalsIgnoreCase(launchId)) {
                throw new IllegalArgumentException("launchId must be a full UUID");
            }
            return canonical;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("launchId must be a full UUID", e);
        }
    }

    private <T> T locked(IoOperation<T> operation) throws IOException {
        jvmLock.lock();
        try (FileChannel channel = FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
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
