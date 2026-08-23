package ai.pipestream.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A drive's prefix is an operator-typed setting, so it arrives in every shape a hand-edited
 * value does. Two callers disagreeing about how to join it to a key would put objects
 * somewhere the reader does not look, and nothing detects that until something is missing.
 */
class DriveKeysTest {

    private static DriveRecord drive(String prefix) {
        DriveRecord drive = new DriveRecord();
        drive.name = "primary";
        drive.bucket = "bucket";
        drive.prefix = prefix;
        return drive;
    }

    @Test
    void aPlainPrefixIsJoinedWithASingleSlash() {
        assertThat(DriveKeys.under(drive("tenant"), "documents/a/b"))
                .isEqualTo("tenant/documents/a/b");
    }

    @Test
    void aTrailingSlashDoesNotDoubleUp() {
        assertThat(DriveKeys.under(drive("tenant/"), "documents/a/b"))
                .isEqualTo("tenant/documents/a/b");
    }

    @Test
    void anAbsentPrefixLeavesTheKeyAtTheRoot() {
        assertThat(DriveKeys.under(drive(null), "documents/a/b")).isEqualTo("documents/a/b");
        assertThat(DriveKeys.under(drive(""), "documents/a/b")).isEqualTo("documents/a/b");
        assertThat(DriveKeys.under(drive("   "), "documents/a/b")).isEqualTo("documents/a/b");
        assertThat(DriveKeys.under(drive("/"), "documents/a/b")).isEqualTo("documents/a/b");
    }

    @Test
    void aNestedPrefixIsKept() {
        assertThat(DriveKeys.under(drive("a/b/c"), "blobs/x")).isEqualTo("a/b/c/blobs/x");
    }

    @Test
    void theDocumentKeyRootCarriesTheAccountAndNode() {
        UUID node = UUID.fromString("00000000-0000-0000-0000-00000000beef");
        assertThat(SaveResolution.basePrefix(drive("tenant"), "acct-1", node))
                .isEqualTo("tenant/documents/acct-1/00000000-0000-0000-0000-00000000beef");
    }

    /**
     * The default blob key is derived from the content digest, so the same bytes put twice
     * overwrite one object instead of accumulating randomly-keyed copies.
     */
    @Test
    void theDefaultBlobKeyIsContentAddressedAndStable() {
        String digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        String first = DriveKeys.blob(drive("tenant"), digest);

        assertThat(first).isEqualTo(DriveKeys.blob(drive("tenant"), digest));
        assertThat(first).startsWith("tenant/blobs/");
        assertThat(DriveKeys.blob(drive("tenant"), digest.replace('e', 'f'))).isNotEqualTo(first);
    }

    @Test
    void blobsAndDocumentsDoNotShareAKeyspace() {
        UUID node = UUID.fromString("00000000-0000-0000-0000-00000000beef");
        assertThat(DriveKeys.blob(drive("t"), "abc"))
                .isNotEqualTo(SaveResolution.basePrefix(drive("t"), "acct-1", node));
    }
}
