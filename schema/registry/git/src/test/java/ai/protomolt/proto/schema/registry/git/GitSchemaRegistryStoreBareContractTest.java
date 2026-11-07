package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaRegistryStoreContractTest;

import com.google.protobuf.ByteString;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The full store contract and the git-specific document surfaces against bare repositories:
 * initialization, auto-detection, object-only writes (no working tree at all), federation
 * into a bare registry, and restart.
 */
class GitSchemaRegistryStoreBareContractTest extends SchemaRegistryStoreContractTest {

    @TempDir
    Path tempDir;

    private int repos;

    @Override
    protected SchemaRegistryStore create(SchemaRegistryStore.WriteGate gate) {
        return GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("bare-repo-" + repos++))
                .bareInit(true)
                .writeGate(gate)
                .build();
    }

    @Test
    void bareInitializationHasNoWorkingTree() throws Exception {
        Path dir = tempDir.resolve("bare-shape");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).bareInit(true).build()) {
            store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            store.setCompatibilityMode(CORE_SUBJECT, "FULL");

            assertThat(Files.isDirectory(dir.resolve(".git"))).isFalse();
            assertThat(Files.isRegularFile(dir.resolve("HEAD"))).isTrue();
            // Object-only writes: no working-tree files ever appear.
            try (var files = Files.list(dir)) {
                assertThat(files.map(path -> path.getFileName().toString()))
                        .contains("HEAD", "objects", "refs", RegistryLayout.LOCK_FILE)
                        .doesNotContain("subjects", "registry.json", "workflows", "configs");
            }
            // The writes landed as real commits, readable through the store and git itself.
            assertThat(store.subjects()).containsExactly(CORE_SUBJECT);
            assertThat(RegistryGitTestSupport.commitMessages(dir))
                    .containsExactly("Set compatibility " + CORE_SUBJECT + " FULL",
                            "Register " + CORE_SUBJECT + " v1");
        }
    }

    @Test
    void existingBareRepositoryIsOpenedWithoutAnyFlag() throws Exception {
        Path dir = tempDir.resolve("auto-detect");
        try (Git ignored = Git.init().setDirectory(dir.toFile()).setBare(true).call()) {
            // an outside actor created a bare registry repository
        }
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            assertThat(store.subjects()).containsExactly(CORE_SUBJECT);
        }
        // Reopening auto-detects the bare repository again.
        try (GitSchemaRegistryStore reopened = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            assertThat(reopened.latest(CORE_SUBJECT)).isPresent();
            ai.protomolt.proto.registry.StoredSchema next =
                    reopened.register("other.proto", CORE_PROTO, List.of());
            assertThat(next.version()).isEqualTo(1);
            assertThat(next.globalId()).isGreaterThan(1);
        }
    }

    @Test
    void bareRegistryServesDocumentsDescriptorsAndWorkflows() throws Exception {
        Path dir = tempDir.resolve("bare-surfaces");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).bareInit(true).build()) {
            store.putConfig("limits", "{\"max\":1}");
            assertThat(store.config("limits")).contains("{\"max\":1}");
            assertThat(store.configVersion("limits")).isPresent();
            assertThat(store.configs()).containsExactly("limits");

            store.putWorkflow("ingest.v1", "{\"steps\":[]}");
            assertThat(store.workflow("ingest.v1")).contains("{\"steps\":[]}");
            assertThat(store.workflows()).containsExactly("ingest.v1");

            ByteString descriptorSet = descriptorSet();
            String fingerprint = HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(descriptorSet.toByteArray()));
            store.putDescriptorSet(fingerprint, descriptorSet);
            assertThat(store.descriptorSet(fingerprint)).contains(descriptorSet);
        }
    }

    @Test
    void stateSurvivesReopeningABareRegistry() throws Exception {
        Path dir = tempDir.resolve("bare-restart");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).bareInit(true).build()) {
            store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            store.register(USER_SUBJECT, USER_PROTO,
                    List.of(new ai.protomolt.proto.registry.SchemaReference(
                            CORE_SUBJECT, CORE_SUBJECT, 1)));
        }
        try (GitSchemaRegistryStore reopened = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) { // auto-detected, no flag
            assertThat(reopened.subjects()).containsExactly(CORE_SUBJECT, USER_SUBJECT);
            assertThat(reopened.latest(USER_SUBJECT)).isPresent();
        }
    }

    @Test
    void federationSyncsIntoABareLocalRegistry() throws Exception {
        Path remoteDir = tempDir.resolve("remote");
        try (GitSchemaRegistryStore remote = GitSchemaRegistryStore.builder()
                .repositoryDir(remoteDir).build()) {
            remote.register(CORE_SUBJECT, CORE_PROTO, List.of());
        }
        Path localDir = tempDir.resolve("bare-local");
        try (GitSchemaRegistryStore local = GitSchemaRegistryStore.builder()
                .repositoryDir(localDir).bareInit(true).build()) {
            RegistryFederation federation = RegistryFederation.over(local);
            federation.addRemote("upstream", remoteDir.toUri().toString());

            RegistryFederation.SyncReport report = federation.sync("upstream");

            assertThat(report.errors()).isEmpty();
            assertThat(local.subjects()).containsExactly("upstream:" + CORE_SUBJECT);
            assertThat(local.latest("upstream:" + CORE_SUBJECT))
                    .hasValueSatisfying(schema -> assertThat(schema.version()).isEqualTo(1));
            // The import is one ordinary commit on the bare registry's history.
            assertThat(RegistryGitTestSupport.commitMessages(localDir))
                    .anySatisfy(message -> assertThat(message)
                            .isEqualTo("Register upstream:" + CORE_SUBJECT + " v1"));
        }
    }
}
