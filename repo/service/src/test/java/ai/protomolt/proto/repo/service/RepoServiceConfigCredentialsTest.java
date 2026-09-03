package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the {@link RepoServiceConfig} validation and normalization
 * arms {@link RepoServiceConfigTest} does not already pin: the static S3
 * credential pairing rule, blob-store spelling normalization, and numeric
 * defaulting. Complements — does not repeat — the ingress/selection tests.
 */
class RepoServiceConfigCredentialsTest {

    private static final LedgerConfig LEDGER =
            new LedgerConfig("jdbc:postgresql://localhost:5432/x", "u", "p");

    /** The 14-component compatibility constructor with credentials as the variable. */
    private static RepoServiceConfig withCredentials(String accessKey, String secretKey) {
        return new RepoServiceConfig(0, LEDGER, null, null, accessKey, secretKey, null,
                0, null, null, null, null, -1, -1L);
    }

    // ------------------------------------------------------------------ S3 credentials

    @Test
    void accessAndSecretKeyMustBeConfiguredTogetherOrNeither() {
        assertThatThrownBy(() -> withCredentials("AKID", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("together");
        assertThatThrownBy(() -> withCredentials(null, "secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("together");

        RepoServiceConfig pair = withCredentials("AKID", "secret");
        assertThat(pair.hasStaticCredentials()).isTrue();
        assertThat(pair.s3AccessKey()).isEqualTo("AKID");
        assertThat(pair.s3SecretKey()).isEqualTo("secret");
    }

    @Test
    void blankCredentialsNormalizeToTheSdkDefaultChain() {
        RepoServiceConfig blank = withCredentials("  ", "");
        assertThat(blank.hasStaticCredentials()).isFalse();
        assertThat(blank.s3AccessKey()).isNull();
        assertThat(blank.s3SecretKey()).isNull();

        // Surrounding whitespace is trimmed from a real pair.
        RepoServiceConfig padded = withCredentials(" AKID ", " secret ");
        assertThat(padded.s3AccessKey()).isEqualTo("AKID");
        assertThat(padded.s3SecretKey()).isEqualTo("secret");
    }

    // ------------------------------------------------------------------ normalization

    @Test
    void blobStoreSelectionIsTrimmedAndCaseInsensitive() {
        RepoServiceConfig loud = new RepoServiceConfig(0, LEDGER, null, null, null, null, null,
                0, " S3 ", null, null, null, -1, -1L);
        assertThat(loud.blobStore()).isEqualTo("s3");

        RepoServiceConfig inProcess = new RepoServiceConfig(0, LEDGER, null, null, null, null, null,
                0, "Repo-InProcess", "backend-inproc", null, null, -1, -1L);
        assertThat(inProcess.blobStore()).isEqualTo("repo-inprocess");
    }

    @Test
    void negativePortsFallBackToTheDefaults() {
        RepoServiceConfig config = new RepoServiceConfig(-5, LEDGER, null, null, null, null, null,
                -1, null, null, null, null, -1, -1L);
        assertThat(config.grpcPort()).isEqualTo(9090);
        assertThat(config.httpPort()).isEqualTo(8080);
    }

    @Test
    void nonPositiveLifecycleIntervalsFallBackToTheDefaults() {
        RepoServiceConfig config = new RepoServiceConfig(0, LEDGER, null, null, null, null, null,
                0, null, null, null, null, -1, -1L,
                true, 0L, -1L, false, true, -1L);
        assertThat(config.purgeIntervalMs()).isEqualTo(5000L);
        assertThat(config.sweepIntervalMs()).isEqualTo(60000L);
        assertThat(config.reconcileMinAgeMs()).isEqualTo(3600000L);
    }

    @Test
    void blankOptionalStringsNormalizeToNull() {
        RepoServiceConfig config = new RepoServiceConfig(0, LEDGER, "  ", null, null, null, null,
                0, null, null, null, null, -1, -1L);
        assertThat(config.s3Endpoint()).isNull();
        // A blank bucket base falls back to the default rather than staying blank.
        assertThat(config.defaultBucketBase()).isEqualTo("documents");
    }
}
