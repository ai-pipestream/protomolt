package ai.protomolt.proto.grpc.profile;

import ai.protomolt.proto.grpc.profile.v1.DescriptorArtifact;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemServiceProfileRepositoryTest {

    @TempDir
    Path tempDir;

    @Test
    void savesAtomicallyAndRecoversProfilesAfterRepositoryRestart() throws Exception {
        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(tempDir);
        ServiceProfile original = TestProfiles.profile("zeta");
        repository.save(original);

        ServiceProfile replacement = original.toBuilder().setDescription("replaced").build();
        repository.save(replacement);
        FileSystemServiceProfileRepository restarted =
                new FileSystemServiceProfileRepository(tempDir);

        assertThat(restarted.find("zeta")).contains(replacement);
        assertThat(restarted.find("missing")).isEmpty();
        assertThat(restarted.list()).extracting(ServiceProfile::getName).containsExactly("zeta");
        try (var files = Files.list(tempDir.resolve("profiles"))) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .containsExactly("zeta.pb");
        }
    }

    @Test
    void listsProfilesInStableOrder() throws Exception {
        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(tempDir);
        repository.save(TestProfiles.profile("zulu"));
        repository.save(TestProfiles.profile("alpha"));
        repository.save(TestProfiles.profile("middle"));

        assertThat(repository.list()).extracting(ServiceProfile::getName)
                .containsExactly("alpha", "middle", "zulu");
    }

    @Test
    void storesDescriptorArtifactsByFingerprintAndRecoversThemAfterRestart() throws Exception {
        DescriptorArtifact artifact = TestProfiles.artifact();
        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(tempDir);
        repository.saveDescriptorArtifact(artifact);

        FileSystemServiceProfileRepository restarted =
                new FileSystemServiceProfileRepository(tempDir);
        assertThat(restarted.findDescriptorArtifact(artifact.getFingerprint()))
                .contains(artifact);
        assertThat(restarted.findDescriptorArtifact(
                "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"))
                .isEmpty();
        try (var files = Files.list(tempDir.resolve("descriptor-artifacts"))) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .containsExactly(artifact.getFingerprint() + ".pb");
        }
    }

    @Test
    void rejectsNamesThatCouldEscapeTheRepository() throws Exception {
        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(tempDir);
        assertThatThrownBy(() -> repository.save(TestProfiles.profile("../outside")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.find("nested/name"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Files.exists(tempDir.resolve("outside.pb"))).isFalse();
    }

    @Test
    void reportsCorruptStoredMessages() throws Exception {
        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(tempDir);
        repository.save(TestProfiles.profile("corrupt"));
        Files.write(tempDir.resolve("profiles/corrupt.pb"), new byte[] {1, 2, 3});

        assertThatThrownBy(() -> repository.find("corrupt"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid service profile");
    }

    @Test
    void rejectsArtifactWithAChangedContentFingerprint() throws Exception {
        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(tempDir);
        DescriptorArtifact artifact = TestProfiles.artifact().toBuilder()
                .setFingerprint("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .build();

        assertThatThrownBy(() -> repository.saveDescriptorArtifact(artifact))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }
}
