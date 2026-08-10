package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.profile.ServiceProfileValidation;
import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Filesystem-backed {@link ArtifactRepository}: content-addressed, deduplicating, and
 * tamper-evident. One artifact is two files under the root: the content bytes at
 * {@code <sha256>} and the serialized {@link ArtifactReference} at {@code <sha256>.ref}.
 * The content name <em>is</em> the integrity check, so a lookup re-hashes the bytes (via
 * {@link StoredArtifact}) and any tampering, truncation, or bit rot fails loudly instead of
 * returning corrupt fixtures.
 *
 * <p>Writes are crash-safe: content lands in a temporary sibling and is moved into place
 * atomically, then the reference sidecar the same way. A process killed between the two
 * moves leaves content without a reference, which {@link #find} reports as corruption rather
 * than serving unverifiable bytes. Saving the same content twice is a no-op that returns the
 * first stored reference: one content, one identity, and no clock or metadata races on
 * rewrite.</p>
 *
 * <p>Redaction is the caller's act, per the interface contract: the repository persists
 * exactly the bytes it is given and records the {@code redacted} flag in the reference, so a
 * sensitive fixture can prove the redaction pass ran before persistence.</p>
 */
public final class FileSystemArtifactRepository implements ArtifactRepository {

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final String REFERENCE_SUFFIX = ".ref";

    private final Path root;

    /** A repository storing under {@code root}, creating the directory when absent. */
    public FileSystemArtifactRepository(Path root) {
        if (root == null) {
            throw new IllegalArgumentException("root must not be null");
        }
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create artifact root " + root, e);
        }
        this.root = root;
    }

    @Override
    public ArtifactReference save(byte[] content, String mediaType, boolean redacted)
            throws IOException {
        if (content == null) {
            throw new IllegalArgumentException("content must not be null");
        }
        if (content.length > RecipeValidation.MAX_ARTIFACT_BYTES) {
            throw new IllegalArgumentException("artifact exceeds the maximum of "
                    + RecipeValidation.MAX_ARTIFACT_BYTES + " bytes: " + content.length);
        }
        ArtifactReference reference = ArtifactReference.newBuilder()
                .setSha256(ServiceProfileValidation.sha256(content))
                .setMediaType(mediaType == null ? "" : mediaType)
                .setSizeBytes(content.length)
                .setRedacted(redacted)
                .build();
        // Fail fast on a malformed media type before anything touches disk.
        RecipeValidation.validate(reference);

        Path contentPath = contentPath(reference.getSha256());
        if (Files.exists(contentPath)) {
            // One content, one identity: the first stored reference stands, whatever media
            // type or redaction flag a duplicate save arrives with.
            return readReference(referencePath(reference.getSha256()));
        }
        writeAtomically(contentPath, content);
        writeAtomically(referencePath(reference.getSha256()), reference.toByteArray());
        return reference;
    }

    @Override
    public Optional<StoredArtifact> find(String sha256) throws IOException {
        if (sha256 == null || !SHA256.matcher(sha256).matches()) {
            // Also the path-traversal guard: only a well-formed identity ever reaches the
            // filesystem, so a caller-controlled sha can never escape the root.
            throw new IllegalArgumentException(
                    "sha256 must be 64 lowercase hex characters: " + sha256);
        }
        Path contentPath = contentPath(sha256);
        Path referencePath = referencePath(sha256);
        if (!Files.exists(contentPath) && !Files.exists(referencePath)) {
            return Optional.empty();
        }
        if (!Files.exists(contentPath) || !Files.exists(referencePath)) {
            throw new IOException("corrupt artifact " + sha256
                    + ": content and reference must both be present");
        }
        ArtifactReference reference = readReference(referencePath);
        if (!reference.getSha256().equals(sha256)) {
            throw new IOException("corrupt artifact " + sha256
                    + ": reference names a different identity");
        }
        // StoredArtifact re-hashes the content and checks the size, so tampered bytes throw
        // here rather than reaching a replay.
        return Optional.of(new StoredArtifact(reference, Files.readAllBytes(contentPath)));
    }

    private Path contentPath(String sha256) {
        return root.resolve(sha256);
    }

    private Path referencePath(String sha256) {
        return root.resolve(sha256 + REFERENCE_SUFFIX);
    }

    private static ArtifactReference readReference(Path path) throws IOException {
        return ArtifactReference.parseFrom(Files.readAllBytes(path));
    }

    /** Temp sibling plus atomic rename: readers never observe a partially written file. */
    private static void writeAtomically(Path target, byte[] bytes) throws IOException {
        Path temp = Files.createTempFile(target.getParent(), target.getFileName().toString(),
                ".tmp");
        try {
            Files.write(temp, bytes);
            try {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }
}
