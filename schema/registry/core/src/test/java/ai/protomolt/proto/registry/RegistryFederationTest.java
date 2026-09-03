package ai.protomolt.proto.registry;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Federation behavior over two real git-backed stores: remote management, namespaced import
 * with reference rewriting, idempotent re-sync, the always-on compatibility gate, divergence
 * refusal and descriptor-artifact import.
 */
class RegistryFederationTest {

    private static final String CORE_SUBJECT = "common/v1/core.proto";
    private static final String USER_SUBJECT = "common/v1/user.proto";

    private static final String CORE_PROTO = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    private static final String CORE_PROTO_V2 = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
              string name = 2;
            }
            """;

    /** Backward-incompatible with v2: {@code name} changes type on the same tag. */
    private static final String CORE_PROTO_BROKEN = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
              int64 name = 2;
            }
            """;

    private static final String USER_PROTO = """
            syntax = "proto3";
            package common.v1;
            import "common/v1/core.proto";
            message User {
              Core core = 1;
            }
            """;

    @TempDir
    Path tempDir;

    private GitSchemaRegistryStore local;
    private GitSchemaRegistryStore remote;
    private RegistryFederation federation;

    @BeforeEach
    void setUp() {
        local = GitSchemaRegistryStore.builder().repositoryDir(tempDir.resolve("local")).build();
        remote = GitSchemaRegistryStore.builder().repositoryDir(tempDir.resolve("remote")).build();
        federation = RegistryFederation.over(local);
    }

    @AfterEach
    void tearDown() {
        local.close();
        remote.close();
    }

    private String remoteUrl() {
        return tempDir.resolve("remote").toUri().toString();
    }

    // ---------------------------------------------------------------- remotes

    @Test
    void remotesListAddRemoveRoundTrip() {
        assertThat(federation.remotes()).isEmpty();

        federation.addRemote("upstream", remoteUrl());
        assertThat(federation.remotes())
                .containsExactly(new RegistryFederation.RemoteInfo("upstream", remoteUrl()));

        federation.removeRemote("upstream");
        assertThat(federation.remotes()).isEmpty();
    }

    @Test
    void remoteNamesAreValidatedByName() {
        assertThatThrownBy(() -> federation.addRemote("Bad Name", remoteUrl()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remote names");
        assertThatThrownBy(() -> federation.addRemote("upstream", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("url is required");
    }

    @Test
    void duplicateAddAndUnknownRemoveAreRefused() {
        federation.addRemote("upstream", remoteUrl());
        assertThatThrownBy(() -> federation.addRemote("upstream", remoteUrl()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already exists");
        assertThatThrownBy(() -> federation.removeRemote("elsewhere"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown remote 'elsewhere'");
    }

    @Test
    void syncOfAnUnconfiguredRemoteIsRefusedByName() {
        assertThatThrownBy(() -> federation.sync("upstream"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown remote 'upstream'");
    }

    // ---------------------------------------------------------------- sync

    @Test
    void syncImportsSubjectsUnderTheOriginPrefixWithLocalGlobalIds() {
        remote.register(CORE_SUBJECT, CORE_PROTO, List.of());
        remote.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
        // Claim global ids 1 and 2 locally first: imports must renumber, not copy.
        local.register("local.proto", CORE_PROTO, List.of());
        local.register("local.proto", CORE_PROTO_V2, List.of());

        federation.addRemote("upstream", remoteUrl());
        RegistryFederation.SyncReport report = federation.sync("upstream");

        assertThat(report.errors()).isEmpty();
        assertThat(report.subjects()).hasSize(1);
        RegistryFederation.SubjectSync subject = report.subjects().getFirst();
        assertThat(subject.localSubject()).isEqualTo("upstream:" + CORE_SUBJECT);
        assertThat(subject.imported()).isEqualTo(2);
        assertThat(subject.alreadyPresent()).isZero();
        assertThat(subject.rejections()).isEmpty();

        assertThat(local.subjects()).contains("upstream:" + CORE_SUBJECT);
        StoredSchema imported = local.latest("upstream:" + CORE_SUBJECT).orElseThrow();
        assertThat(imported.version()).isEqualTo(2);
        assertThat(imported.schemaText()).isEqualTo(CORE_PROTO_V2);
        assertThat(imported.globalId())
                .isNotEqualTo(remote.latest(CORE_SUBJECT).orElseThrow().globalId());
    }

    @Test
    void syncRewritesReferencesToTheNamespacedSubjects() {
        StoredSchema core = remote.register(CORE_SUBJECT, CORE_PROTO, List.of());
        remote.register(USER_SUBJECT, USER_PROTO,
                List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, core.version())));

        federation.addRemote("upstream", remoteUrl());
        RegistryFederation.SyncReport report = federation.sync("upstream");

        assertThat(report.subjects()).allSatisfy(subject ->
                assertThat(subject.rejections()).isEmpty());
        StoredSchema user = local.latest("upstream:" + USER_SUBJECT).orElseThrow();
        assertThat(user.references()).containsExactly(
                new SchemaReference(CORE_SUBJECT, "upstream:" + CORE_SUBJECT, 1));
        // The reference name (the import path) is untouched, so the text still compiles.
        assertThat(user.schemaText()).contains("import \"common/v1/core.proto\";");
    }

    @Test
    void reSyncIsIdempotentAndPicksUpNewVersions() {
        remote.register(CORE_SUBJECT, CORE_PROTO, List.of());
        federation.addRemote("upstream", remoteUrl());
        federation.sync("upstream");

        RegistryFederation.SyncReport again = federation.sync("upstream");
        assertThat(again.subjects().getFirst().imported()).isZero();
        assertThat(again.subjects().getFirst().alreadyPresent()).isEqualTo(1);

        remote.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
        RegistryFederation.SyncReport third = federation.sync("upstream");
        assertThat(third.subjects().getFirst().imported()).isEqualTo(1);
        assertThat(third.subjects().getFirst().alreadyPresent()).isEqualTo(1);
        assertThat(local.versions("upstream:" + CORE_SUBJECT)).containsExactly(1, 2);
    }

    @Test
    void incompatibleRemoteEvolutionIsRejectedWithViolationsButEarlierVersionsImport() {
        // The remote store has no write gate, so it accepts the break; the sync gate must not.
        remote.register(CORE_SUBJECT, CORE_PROTO, List.of());
        remote.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
        remote.register(CORE_SUBJECT, CORE_PROTO_BROKEN, List.of());

        federation.addRemote("upstream", remoteUrl());
        RegistryFederation.SyncReport report = federation.sync("upstream");

        RegistryFederation.SubjectSync subject = report.subjects().getFirst();
        assertThat(subject.imported()).isEqualTo(2);
        assertThat(subject.rejections()).hasSize(1);
        assertThat(subject.rejections().getFirst())
                .contains("v3").contains("incompatible");
        assertThat(local.versions("upstream:" + CORE_SUBJECT)).containsExactly(1, 2);
    }

    @Test
    void divergedRemoteHistoryIsRefusedAtTheDivergencePoint() {
        remote.register(CORE_SUBJECT, CORE_PROTO, List.of());
        federation.addRemote("upstream", remoteUrl());
        federation.sync("upstream");

        // Same remote name, different repository claiming a different v1: divergence.
        try (GitSchemaRegistryStore other = GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("other")).build()) {
            other.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
        }
        federation.removeRemote("upstream");
        federation.addRemote("upstream", tempDir.resolve("other").toUri().toString());

        RegistryFederation.SyncReport report = federation.sync("upstream");
        RegistryFederation.SubjectSync subject = report.subjects().getFirst();
        assertThat(subject.imported()).isZero();
        assertThat(subject.rejections()).hasSize(1);
        assertThat(subject.rejections().getFirst()).contains("diverged");
        // The local import is untouched.
        assertThat(local.latest("upstream:" + CORE_SUBJECT).orElseThrow().schemaText())
                .isEqualTo(CORE_PROTO);
    }

    @Test
    void descriptorArtifactsFederateContentAddressed() throws Exception {
        ByteString descriptorSet = SchemaRegistryStoreContractTest.descriptorSet();
        String fingerprint = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(descriptorSet.toByteArray()));
        remote.putDescriptorSet(fingerprint, descriptorSet);

        federation.addRemote("upstream", remoteUrl());
        RegistryFederation.SyncReport report = federation.sync("upstream");
        assertThat(report.descriptorsImported()).isEqualTo(1);
        assertThat(local.descriptorSet(fingerprint)).contains(descriptorSet);

        assertThat(federation.sync("upstream").descriptorsImported()).isZero();
    }

    @Test
    void syncOfAnEmptyRemoteReportsNoBranchToSyncFrom() {
        try (GitSchemaRegistryStore ignored = GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("empty")).build()) {
            // opened and closed: an initialized repository with no commits has no branches
        }
        federation.addRemote("empty", tempDir.resolve("empty").toUri().toString());
        assertThatThrownBy(() -> federation.sync("empty"))
                .isInstanceOf(RegistryStoreException.class)
                .hasMessageContaining("no main or master branch");
    }
}
