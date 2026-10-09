package ai.protomolt.proto.repo.engine;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.archive.v1.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
class ArchiveAuthorityTest {
    @Test void scopedCallerCannotReachArchiveStorage() {
        var operations = new ArchiveOperations(null, null, null);
        var caller = new RepositoryCaller("scoped", false);
        for (Runnable operation : java.util.List.<Runnable>of(
                () -> operations.createArchive(caller, CreateArchiveRequest.getDefaultInstance()),
                () -> operations.getArchive(caller, GetArchiveRequest.getDefaultInstance()),
                () -> operations.listArchives(caller, ListArchivesRequest.getDefaultInstance()),
                () -> operations.stats(caller, GetArchiveStatsRequest.getDefaultInstance()),
                () -> operations.putEntry(caller, PutEntryRequest.getDefaultInstance()),
                () -> operations.getEntry(caller, GetEntryRequest.getDefaultInstance()),
                () -> operations.getManifest(caller, GetEntryManifestRequest.getDefaultInstance()),
                () -> operations.listEntries(caller, ListEntriesRequest.getDefaultInstance()),
                () -> operations.listVersions(caller, ListVersionsRequest.getDefaultInstance()),
                () -> operations.classifyEntry(caller, ClassifyEntryRequest.getDefaultInstance()),
                () -> operations.bridgeEntry(caller, BridgeEntryRequest.getDefaultInstance()))) {
            assertThatThrownBy(operation::run).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
        }
        assertThatThrownBy(() -> operations.uploadStream(caller, null, null, 1, "", null,
                null, null, null, null)).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
    }
}
