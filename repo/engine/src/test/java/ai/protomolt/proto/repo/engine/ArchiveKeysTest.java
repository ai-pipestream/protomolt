package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Archive object keys are write identities: every physical write gets a fresh key under the
 * drive prefix, the free-form entry id never appears, and the account id is the only segment
 * sanitized here because the contract does not already restrict it.
 */
class ArchiveKeysTest {

    private static final UUID ENTRY = UUID.fromString("00000000-0000-0000-0000-00000000000e");
    private static final String SHA = "ab".repeat(32);

    private static DriveRecord drive(String prefix) {
        DriveRecord drive = new DriveRecord();
        drive.prefix = prefix;
        return drive;
    }

    @Test
    void sanitizeKeepsTheSafeAlphabetAndReplacesEveryOtherCharacterOneForOne() {
        assertThat(ArchiveKeys.sanitize("Acct-1.a_b")).isEqualTo("Acct-1.a_b");
        assertThat(ArchiveKeys.sanitize("a/b c\\d:e")).isEqualTo("a_b_c_d_e");
        assertThat(ArchiveKeys.sanitize("café")).isEqualTo("caf_");
        assertThat(ArchiveKeys.sanitize("😀")).isEqualTo("__");
        assertThat(ArchiveKeys.sanitize("")).isEmpty();
    }

    @Test
    void renditionKeyCarriesEveryIdentitySegmentAndAFreshWriteId() {
        String prefix = "tenant/archive/a_b/docs/" + ENTRY + "/original/writes/";
        String first = ArchiveKeys.rendition(drive("tenant/"), "a/b", "docs", ENTRY, "original", null, SHA);
        String second = ArchiveKeys.rendition(drive("tenant/"), "a/b", "docs", ENTRY, "original", "", SHA);

        assertThat(first).startsWith(prefix).endsWith("/" + SHA);
        String writeId = first.substring(prefix.length(), first.length() - SHA.length() - 1);
        assertThat(UUID.fromString(writeId).toString()).isEqualTo(writeId);
        assertThat(second).startsWith(prefix).isNotEqualTo(first);
    }

    @Test
    void aNonBlankSubKeyAddsOneSegmentAndABlankOneAddsNone() {
        assertThat(ArchiveKeys.rendition(drive(""), "acct", "docs", ENTRY, "pages", "p-1", SHA))
                .startsWith("archive/acct/docs/" + ENTRY + "/pages/p-1/writes/");
        assertThat(ArchiveKeys.rendition(drive(null), "acct", "docs", ENTRY, "pages", "  ", SHA))
                .startsWith("archive/acct/docs/" + ENTRY + "/pages/writes/");
    }

    @Test
    void stagingKeyIsDeterministicForItsUploadIdAndStreamedKeysAreFresh() {
        UUID upload = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
        assertThat(ArchiveKeys.staging(drive("p"), "acct", "docs", ENTRY, upload))
                .isEqualTo("p/archive/acct/docs/" + ENTRY + "/staging/" + upload);

        String streamed = ArchiveKeys.streamed(drive("p"), "x y", "docs", ENTRY);
        String base = "p/archive/x_y/docs/" + ENTRY + "/writes/";
        assertThat(streamed).startsWith(base);
        UUID.fromString(streamed.substring(base.length()));
        assertThat(ArchiveKeys.streamed(drive("p"), "x y", "docs", ENTRY)).isNotEqualTo(streamed);
    }
}
