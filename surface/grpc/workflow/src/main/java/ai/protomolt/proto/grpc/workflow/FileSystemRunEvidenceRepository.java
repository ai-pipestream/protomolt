package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Immutable binary-protobuf run-evidence storage. Run identities are validated before they
 * become filenames, writes land atomically, and saving different bytes under an existing
 * identity fails instead of rewriting execution history.
 */
public final class FileSystemRunEvidenceRepository implements RunEvidenceRepository {

    private static final String SUFFIX = ".pb";
    private static final int MAX_STORED_RUNS = 10_000;

    private final Path root;

    /** Creates a repository under {@code root}, creating it when needed. */
    public FileSystemRunEvidenceRepository(Path root) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("root must not be null");
        }
        this.root = root.toAbsolutePath().normalize();
        Files.createDirectories(this.root);
    }

    @Override
    public Optional<RunEvidence> find(String runId) throws IOException {
        WorkflowValidation.validateName(runId, "run_id");
        Path path = path(runId);
        if (!Files.isRegularFile(path)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = readBounded(path);
            RunEvidence evidence = RunEvidence.parseFrom(bytes);
            WorkflowValidation.validate(evidence);
            if (!evidence.getRunId().equals(runId)) {
                throw new IOException("run identity does not match its storage path: " + runId);
            }
            return Optional.of(evidence);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new IOException("invalid run evidence stored at " + path, e);
        }
    }

    @Override
    public List<RunEvidence> list(String workflowName, int limit) throws IOException {
        WorkflowValidation.validateName(workflowName, "workflow_name");
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException("limit must be between 1 and 1000");
        }
        List<Path> paths = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root, "*" + SUFFIX)) {
            for (Path path : stream) {
                if (Files.isRegularFile(path) && path.getParent().equals(root)) {
                    if (paths.size() == MAX_STORED_RUNS) {
                        throw new IOException("run-evidence repository exceeds the maximum of "
                                + MAX_STORED_RUNS + " stored runs");
                    }
                    paths.add(path);
                }
            }
        }
        paths.sort(Comparator.comparing(path -> path.getFileName().toString()));
        List<RunEvidence> result = new ArrayList<>();
        for (Path candidate : paths) {
            String filename = candidate.getFileName().toString();
            RunEvidence evidence = find(filename.substring(0, filename.length() - SUFFIX.length()))
                    .orElseThrow(() -> new IOException(
                            "run evidence disappeared while listing: " + candidate));
            if (evidence.getWorkflowName().equals(workflowName)) {
                result.add(evidence);
                if (result.size() == limit) {
                    break;
                }
            }
        }
        return List.copyOf(result);
    }

    @Override
    public void save(RunEvidence evidence) throws IOException {
        WorkflowValidation.validate(evidence);
        Path target = path(evidence.getRunId());
        byte[] bytes = evidence.toByteArray();
        if (Files.exists(target)) {
            if (!java.util.Arrays.equals(readBounded(target), bytes)) {
                throw new IOException("run evidence is immutable: " + evidence.getRunId());
            }
            return;
        }
        Path temporary = Files.createTempFile(root, ".run-", ".tmp");
        try {
            Files.write(temporary, bytes);
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, target);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private Path path(String runId) {
        Path path = root.resolve(runId + SUFFIX);
        if (!path.getParent().equals(root)) {
            throw new IllegalArgumentException("run identity escapes repository root");
        }
        return path;
    }

    private static byte[] readBounded(Path path) throws IOException {
        long size = Files.size(path);
        if (size > WorkflowValidation.MAX_RUN_EVIDENCE_BYTES) {
            throw new IOException("run evidence at " + path + " exceeds the maximum size of "
                    + WorkflowValidation.MAX_RUN_EVIDENCE_BYTES + " bytes");
        }
        return Files.readAllBytes(path);
    }
}
