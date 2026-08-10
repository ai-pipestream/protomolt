package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The durable artifact store behind run evidence: content-addressed identity, deduplication,
 * enforced bounds, and tamper detection. Redaction stays the caller's act (the interface
 * contract), so the redaction test proves the repository persists exactly the redacted bytes
 * and records the flag.
 */
class FileSystemArtifactRepositoryTest {

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void saveThenFindRoundTripsBytesAndReference(@TempDir Path root) throws Exception {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);

        ArtifactReference reference = repository.save(
                bytes("{\"text\":\"hello\"}"), "application/json", true);

        assertThat(reference.getSha256()).matches("[0-9a-f]{64}");
        assertThat(reference.getMediaType()).isEqualTo("application/json");
        assertThat(reference.getSizeBytes()).isEqualTo(16);
        assertThat(reference.getRedacted()).isTrue();

        Optional<ArtifactRepository.StoredArtifact> found =
                repository.find(reference.getSha256());
        assertThat(found).isPresent();
        assertThat(found.get().reference()).isEqualTo(reference);
        assertThat(found.get().content()).isEqualTo(bytes("{\"text\":\"hello\"}"));
    }

    @Test
    void duplicateContentHasOneIdentity(@TempDir Path root) throws Exception {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);

        ArtifactReference first = repository.save(bytes("same"), "application/json", true);
        // The duplicate arrives with different metadata; content identity still wins.
        ArtifactReference second = repository.save(bytes("same"), "text/plain", false);

        assertThat(second).isEqualTo(first);
        assertThat(root.toFile().listFiles()).hasSize(2); // one content, one reference
    }

    @Test
    void oversizedAndMalformedSavesFailBeforeTouchingDisk(@TempDir Path root) {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);

        assertThatThrownBy(() -> repository.save(
                new byte[RecipeValidation.MAX_ARTIFACT_BYTES + 1], "application/json", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
        assertThatThrownBy(() -> repository.save(bytes("x"), "not a media type", true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.save(null, "application/json", true))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(root.toFile().listFiles()).isEmpty();
    }

    @Test
    void findRejectsMalformedIdentitiesWithoutTouchingDisk(@TempDir Path root) {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);

        for (String bad : new String[]{"ABC", "../etc/passwd", "g".repeat(64), null}) {
            assertThatThrownBy(() -> repository.find(bad))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void unknownIdentityIsAbsent(@TempDir Path root) throws Exception {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);

        assertThat(repository.find("a".repeat(64))).isEmpty();
    }

    @Test
    void tamperedContentFailsVerification(@TempDir Path root) throws Exception {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);
        ArtifactReference reference = repository.save(bytes("original"), "text/plain", false);

        Files.write(root.resolve(reference.getSha256()), bytes("tampered"));

        assertThatThrownBy(() -> repository.find(reference.getSha256()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sha256");
    }

    @Test
    void contentWithoutReferenceIsCorruptionNotAnArtifact(@TempDir Path root) throws Exception {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);
        ArtifactReference reference = repository.save(bytes("orphan"), "text/plain", false);

        // Simulate a crash between the content and reference moves.
        Files.delete(root.resolve(reference.getSha256() + ".ref"));

        assertThatThrownBy(() -> repository.find(reference.getSha256()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("corrupt");
    }

    @Test
    void redactedFixturePersistsOnlyTheRedactedBytes(@TempDir Path root) throws Exception {
        FileSystemArtifactRepository repository = new FileSystemArtifactRepository(root);
        // The caller's redaction pass runs first; the store sees only its output.
        String redacted = "{\"ssn\":\"***\",\"name\":\"ada\"}";

        ArtifactReference reference = repository.save(
                bytes(redacted), "application/json", true);

        assertThat(reference.getRedacted()).isTrue();
        byte[] onDisk = Files.readAllBytes(root.resolve(reference.getSha256()));
        assertThat(new String(onDisk, StandardCharsets.UTF_8))
                .doesNotContain("123-45-6789")
                .isEqualTo(redacted);
    }

    @Test
    void restartRecoversEveryArtifact(@TempDir Path root) throws Exception {
        ArtifactReference first = new FileSystemArtifactRepository(root)
                .save(bytes("one"), "text/plain", false);
        ArtifactReference second = new FileSystemArtifactRepository(root)
                .save(bytes("two"), "text/plain", true);

        FileSystemArtifactRepository reopened = new FileSystemArtifactRepository(root);
        assertThat(reopened.find(first.getSha256())).isPresent();
        assertThat(reopened.find(second.getSha256())).isPresent();
        assertThat(reopened.find(second.getSha256()).orElseThrow().reference().getRedacted())
                .isTrue();
    }
}
