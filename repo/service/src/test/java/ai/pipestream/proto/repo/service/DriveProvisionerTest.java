package ai.pipestream.proto.repo.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DriveProvisioner#sanitizeBucketName}: the S3
 * bucket-name rules (lowercase letters/digits/dots/dashes, 3–63 chars) applied
 * to the {@code <base>-<accountId>-<name>} provisioning default. The
 * bucket-creating half of the provisioner is covered by the container-backed
 * ITs; this pins the pure name-mapping logic.
 */
class DriveProvisionerTest {

    @Test
    void wellFormedNamesPassThroughLowercased() {
        assertThat(DriveProvisioner.sanitizeBucketName("documents-acct-1-intake"))
                .isEqualTo("documents-acct-1-intake");
        assertThat(DriveProvisioner.sanitizeBucketName("Documents-ACCT-Intake"))
                .isEqualTo("documents-acct-intake");
    }

    @Test
    void invalidCharactersCollapseToDashes() {
        // Underscores, spaces and punctuation are not S3 bucket characters.
        assertThat(DriveProvisioner.sanitizeBucketName("my_bucket name!")).isEqualTo("my-bucket-name");
        // Runs of dashes/dots (introduced or pre-existing) collapse to one dash.
        assertThat(DriveProvisioner.sanitizeBucketName("a__b..--c")).isEqualTo("a-b-c");
    }

    @Test
    void edgesAreTrimmedOfDashesAndDots() {
        assertThat(DriveProvisioner.sanitizeBucketName("..--abc--..")).isEqualTo("abc");
        assertThat(DriveProvisioner.sanitizeBucketName("-docs-")).isEqualTo("docs");
    }

    @Test
    void dotsInsideTheNameAreKept() {
        assertThat(DriveProvisioner.sanitizeBucketName("my.docs.tar.gz")).isEqualTo("my.docs.tar.gz");
    }

    @Test
    void namesLongerThan63CharactersAreTruncatedAndReTrimmed() {
        String longName = "a".repeat(70);
        String sanitized = DriveProvisioner.sanitizeBucketName(longName);
        assertThat(sanitized).hasSize(63);
        assertThat(sanitized).isEqualTo("a".repeat(63));

        // Truncation must not leave a dangling separator at the end.
        String withTailDash = "a".repeat(62) + "--bbbb";
        String trimmed = DriveProvisioner.sanitizeBucketName(withTailDash);
        assertThat(trimmed).isEqualTo("a".repeat(62));
        assertThat(trimmed).doesNotEndWith("-").doesNotEndWith(".");
    }

    @Test
    void namesShorterThan3CharactersGainASuffix() {
        assertThat(DriveProvisioner.sanitizeBucketName("ab")).isEqualTo("ab-bucket");
        assertThat(DriveProvisioner.sanitizeBucketName("a")).isEqualTo("a-bucket");
        // Everything invalid collapses away; the suffix still yields a legal name.
        assertThat(DriveProvisioner.sanitizeBucketName("__")).isEqualTo("bucket");
    }

    @Test
    void everyResultObeysTheS3Shape() {
        for (String raw : new String[]{"Documents-acct_1-Intake", "..x..", "a",
                "b".repeat(100) + "-tail", "sp ace", "UPPER.lower"}) {
            String sanitized = DriveProvisioner.sanitizeBucketName(raw);
            assertThat(sanitized).as(raw).matches("[a-z0-9.-]{3,63}");
            assertThat(sanitized).doesNotStartWith("-").doesNotStartWith(".");
            assertThat(sanitized).doesNotEndWith("-").doesNotEndWith(".");
        }
    }
}
