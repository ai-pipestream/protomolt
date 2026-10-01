package ai.protomolt.proto.samples;

import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.HashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthoringPolicySeedTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TARGET = "fixture.example:9778";
    private static final String MANIFEST = ".authoring-policy-seed-v1.json";
    private static final String OPERATION_ID = "00000000-0000-4000-8000-000000000442";

    @TempDir Path temp;

    @Test
    void sameConfigurationReusesCanonicalManifestAndProducesValidatedImportClosure() throws Exception {
        Path workspace = temp.resolve("workspace");
        Path artifactsPath = workspace.resolve("artifacts");
        Path shaFile = workspace.resolve("policy.sha256");

        String first = AuthoringPolicySeed.seed(workspace, artifactsPath, TARGET, shaFile);
        byte[] manifest = Files.readAllBytes(workspace.resolve(MANIFEST));
        byte[] ready = Files.readAllBytes(shaFile);
        String replayed = AuthoringPolicySeed.seed(workspace, artifactsPath, TARGET, shaFile);

        assertThat(replayed).isEqualTo(first);
        assertThat(ready).isEqualTo((first + "\n").getBytes(StandardCharsets.US_ASCII));
        assertThat(Files.readAllBytes(workspace.resolve(MANIFEST))).isEqualTo(manifest);
        JsonNode intent = JSON.readTree(manifest);
        assertThat(intent.path("version").asInt()).isEqualTo(1);
        assertThat(intent.path("fixtureTarget").asText()).isEqualTo(TARGET);
        assertThat(intent.path("policySha256").asText()).isEqualTo(first);

        FileSystemArtifactRepository artifacts = new FileSystemArtifactRepository(artifactsPath);
        var descriptorBytes = artifacts.find(intent.path("descriptorsSha256").asText()).orElseThrow().content();
        FileDescriptorSet descriptors = FileDescriptorSet.parseFrom(descriptorBytes);
        HashSet<String> fileNames = new HashSet<>();
        descriptors.getFileList().forEach(file -> fileNames.add(file.getName()));
        assertThat(fileNames).contains(WriteRecordRequest.getDescriptor().getFile().getName());
        descriptors.getFileList().forEach(file -> assertThat(fileNames).containsAll(file.getDependencyList()));
        assertThat(ai.protomolt.proto.descriptors.GoogleDescriptorLoader.fromDescriptorSet(descriptors))
                .isNotEmpty();

        WriteRecordRequest input = WriteRecordRequest.parseFrom(
                artifacts.find(intent.path("inputSha256").asText()).orElseThrow().content());
        WriteRecordResponse output = WriteRecordResponse.parseFrom(
                artifacts.find(intent.path("expectedOutputSha256").asText()).orElseThrow().content());
        WorkflowAuthoringPolicy policy = WorkflowAuthoringPolicy.parseFrom(
                artifacts.find(first).orElseThrow().content());
        assertThat(ProtoValidator.forMessageType(input.getDescriptorForType()).validate(input).valid()).isTrue();
        assertThat(ProtoValidator.forMessageType(output.getDescriptorForType()).validate(output).valid()).isTrue();
        assertThat(ProtoValidator.forMessageType(policy.getDescriptorForType()).validate(policy).valid()).isTrue();
        assertThat(input.getOperationId()).isEqualTo(OPERATION_ID);
        assertThat(input.getContent()).isEqualTo(" \tfixture\r\n record\t ");
        assertThat(output.getOperationId()).isEqualTo(OPERATION_ID);
        assertThat(output.getContentSha256()).isEqualTo(sha256("fixture\n record".getBytes(StandardCharsets.UTF_8)));
        assertThat(policy.getDescriptors().getSha256()).isEqualTo(intent.path("descriptorsSha256").asText());
        assertThat(policy.getFixturesCount()).isEqualTo(1);
        assertThat(policy.getPermittedCallsList()).allSatisfy(call -> assertThat(call.getTarget()).isEqualTo(TARGET));
    }

    @Test
    void rejectsReservedOutputPathsAndOversizedPersistedMetadata() throws Exception {
        for (String reserved : new String[]{MANIFEST, ".authoring-policy-seed.lock"}) {
            Path workspace = temp.resolve(reserved + "-workspace");
            assertThatThrownBy(() -> AuthoringPolicySeed.seed(workspace,
                    workspace.resolve("artifacts"), TARGET, workspace.resolve(reserved)))
                    .hasMessageContaining("reserved");
            assertThat(workspace.resolve(MANIFEST)).doesNotExist();
        }
        for (String file : new String[]{MANIFEST, "policy.sha256"}) {
            Path workspace = temp.resolve(file + "-oversized");
            Path ready = workspace.resolve("policy.sha256");
            AuthoringPolicySeed.seed(workspace, workspace.resolve("artifacts"), TARGET, ready);
            Files.writeString(workspace.resolve(file), "x".repeat(100_000));
            assertThatThrownBy(() -> AuthoringPolicySeed.seed(workspace,
                    workspace.resolve("artifacts"), TARGET, ready))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(Files.size(workspace.resolve(file))).isEqualTo(100_000);
        }
    }

    @Test
    void changedFixtureTargetCannotReuseAnExistingManifestOrReadyHash() throws Exception {
        Path workspace = temp.resolve("workspace");
        Path artifacts = workspace.resolve("artifacts");
        Path shaFile = workspace.resolve("policy.sha256");
        String original = AuthoringPolicySeed.seed(workspace, artifacts, TARGET, shaFile);
        byte[] originalReady = Files.readAllBytes(shaFile);

        assertThatThrownBy(() -> AuthoringPolicySeed.seed(workspace, artifacts,
                "fixture.example:9779", shaFile)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("intent differs");
        assertThat(Files.readAllBytes(shaFile)).isEqualTo(originalReady);
        assertThat(Files.readString(shaFile)).isEqualTo(original + "\n");
    }

    @Test
    void readyManifestWithMissingOrCorruptArtifactFailsClosedWithoutRepair() throws Exception {
        assertReadyArtifactFailure("missing", (artifacts, hash) -> Files.delete(artifacts.resolve(hash)));
        assertReadyArtifactFailure("corrupt", (artifacts, hash) -> {
            Path content = artifacts.resolve(hash);
            byte[] bytes = Files.readAllBytes(content);
            bytes[0] ^= 1;
            Files.write(content, bytes);
        });
    }

    @Test
    void malformedTargetAndPartialManifestNeverPublishReadyHash() throws Exception {
        Path malformedWorkspace = temp.resolve("malformed-target");
        Path malformedSha = malformedWorkspace.resolve("policy.sha256");
        assertThatThrownBy(() -> AuthoringPolicySeed.seed(malformedWorkspace,
                malformedWorkspace.resolve("artifacts"), "https://fixture.example:9778", malformedSha))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Files.exists(malformedSha)).isFalse();
        assertThat(Files.exists(malformedWorkspace.resolve(MANIFEST))).isFalse();

        Path partialWorkspace = temp.resolve("partial-manifest");
        Files.createDirectories(partialWorkspace);
        Files.writeString(partialWorkspace.resolve(MANIFEST), "{\"version\":1}\n");
        Path partialSha = partialWorkspace.resolve("policy.sha256");
        assertThatThrownBy(() -> AuthoringPolicySeed.seed(partialWorkspace,
                partialWorkspace.resolve("artifacts"), TARGET, partialSha))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("intent differs");
        assertThat(Files.exists(partialSha)).isFalse();
    }

    @Test
    void atomicReadyPublicationFailureLeavesNoShaAndMatchingIncompleteSeedCanRecover() throws Exception {
        Path workspace = temp.resolve("publication-failure");
        Path artifacts = workspace.resolve("artifacts");
        Path blockedParent = workspace.resolve("not-a-directory");
        Files.createDirectories(workspace);
        Files.writeString(blockedParent, "block SHA temp-file creation");
        Path shaFile = blockedParent.resolve("policy.sha256");

        assertThatThrownBy(() -> AuthoringPolicySeed.seed(workspace, artifacts, TARGET, shaFile))
                .isInstanceOf(Exception.class);
        assertThat(Files.exists(shaFile)).isFalse();
        assertThat(Files.isRegularFile(workspace.resolve(MANIFEST))).isTrue();

        byte[] manifest = Files.readAllBytes(workspace.resolve(MANIFEST));
        JsonNode intent = JSON.readTree(manifest);
        String inputHash = intent.path("inputSha256").asText();
        var originalInput = new FileSystemArtifactRepository(artifacts).find(inputHash).orElseThrow();
        Files.delete(artifacts.resolve(inputHash));
        Files.delete(artifacts.resolve(inputHash + ".ref"));

        Files.delete(blockedParent);
        Files.createDirectory(blockedParent);
        String recovered = AuthoringPolicySeed.seed(workspace, artifacts, TARGET, shaFile);
        assertThat(Files.readString(shaFile)).isEqualTo(recovered + "\n");
        assertThat(new FileSystemArtifactRepository(artifacts).find(recovered)).isPresent();
        assertThat(recovered).isEqualTo(intent.path("policySha256").asText());
        assertThat(Files.readAllBytes(workspace.resolve(MANIFEST))).isEqualTo(manifest);
        var restoredInput = new FileSystemArtifactRepository(artifacts).find(inputHash).orElseThrow();
        assertThat(restoredInput.reference()).isEqualTo(originalInput.reference());
        assertThat(restoredInput.content()).isEqualTo(originalInput.content());
    }

    private void assertReadyArtifactFailure(String name, ArtifactMutation mutation) throws Exception {
        Path workspace = temp.resolve(name);
        Path artifacts = workspace.resolve("artifacts");
        Path shaFile = workspace.resolve("policy.sha256");
        String hash = AuthoringPolicySeed.seed(workspace, artifacts, TARGET, shaFile);
        JsonNode intent = JSON.readTree(Files.readAllBytes(workspace.resolve(MANIFEST)));
        String targetHash = intent.path("inputSha256").asText();
        mutation.apply(artifacts, targetHash);

        assertThatThrownBy(() -> AuthoringPolicySeed.seed(workspace, artifacts, TARGET, shaFile))
                .isInstanceOf(Exception.class);
        assertThat(Files.readString(shaFile)).isEqualTo(hash + "\n");
        assertThatThrownBy(() -> new FileSystemArtifactRepository(artifacts).find(targetHash))
                .isInstanceOf(Exception.class);
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    @FunctionalInterface
    private interface ArtifactMutation {
        void apply(Path artifacts, String sha) throws Exception;
    }
}
