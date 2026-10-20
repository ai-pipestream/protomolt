package ai.protomolt.proto.repo.catalog;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the catalog's Flyway baseline (decision 44). Flyway records the checksum of every applied
 * migration, so editing {@code V1__catalog_baseline.sql} after a catalog has applied it fails
 * validation on that catalog. Schema changes ship as new {@code V2__*.sql} (and later) migrations
 * instead. The digest covers the script with line endings normalized to LF, so a checkout's line
 * ending conversion does not change it.
 */
class CatalogBaselineChecksumPinTest {
    private static final String BASELINE = "db/repo-catalog/V1__catalog_baseline.sql";
    private static final String PINNED_SHA256 = "d15a2acfb1781343215907d7494ff1603ea0987c87fbc8dab25adf5e5709f5a9";

    @Test void baselineScriptMatchesItsPinnedChecksum() throws IOException, NoSuchAlgorithmException {
        assertThat(lfSha256(BASELINE))
                .as(BASELINE + " is the released catalog baseline and must not change. Add the change as a new "
                        + "db/repo-catalog/V2__<description>.sql migration (or the next free version) and restore V1.")
                .isEqualTo(PINNED_SHA256);
    }

    private static String lfSha256(String resource) throws IOException, NoSuchAlgorithmException {
        String script;
        try (InputStream in = CatalogBaselineChecksumPinTest.class.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in).as("catalog baseline resource " + resource).isNotNull();
            script = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        var normalized = script.replace("\r\n", "\n").replace('\r', '\n');
        var digest = MessageDigest.getInstance("SHA-256").digest(normalized.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }
}
