package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the ingress additions to {@link RepoServiceConfig}: the
 * HTTP port convention and the blob-store selection validation. Only the
 * logic that can actually break is tested — no ceremonial default echoing of
 * the pre-existing fields.
 */
class RepoServiceConfigTest {

    private static final LedgerConfig LEDGER =
            new LedgerConfig("jdbc:postgresql://localhost:5432/x", "u", "p");

    private static RepoServiceConfig config(int httpPort, String blobStore,
            String repoTarget, String repoDrive) {
        return new RepoServiceConfig(0, LEDGER, null, null, null, null, null,
                httpPort, blobStore, repoTarget, repoDrive);
    }

    @Test
    void defaultsKeepTodaysBehavior() {
        RepoServiceConfig config = config(-1, null, null, null);
        assertThat(config.httpPort()).isEqualTo(8080);
        assertThat(config.blobStore()).isEqualTo("s3");
        assertThat(config.repoDrive()).isEqualTo("default");
        assertThat(config.repoTarget()).isNull();
    }

    @Test
    void httpPortZeroMeansDisabled() {
        assertThat(config(0, null, null, null).httpPort()).isZero();
    }

    @Test
    void unknownBlobStoreIsRejected() {
        assertThatThrownBy(() -> config(0, "gcs", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_BLOB_STORE");
    }

    @Test
    void repoModesRequireATarget() {
        assertThatThrownBy(() -> config(0, "repo", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_REPO_TARGET");
        assertThatThrownBy(() -> config(0, "repo-inprocess", "  ", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOCUMENT_PLATFORM_REPO_TARGET");

        RepoServiceConfig repo = config(0, "repo", "repo-backend:9090", null);
        assertThat(repo.repoTarget()).isEqualTo("repo-backend:9090");
        assertThat(repo.blobStore()).isEqualTo("repo");
        RepoServiceConfig inProcess = config(0, "repo-inprocess", "backend-inproc", "blobs");
        assertThat(inProcess.repoDrive()).isEqualTo("blobs");
    }
}
