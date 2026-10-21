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

    @Test void boundCallerReadsAreRefusedOutsideItsAccountsBeforeTheLedger() {
        var operations = new ArchiveOperations(null, null, null);
        var caller = new RepositoryCaller("alice", false, java.util.Set.of("acct-a"), java.util.Set.of());
        var other = EntryAddress.newBuilder().setAccountId("acct-b").setArchive("docs").setEntryId("e1").build();
        for (Runnable operation : java.util.List.<Runnable>of(
                () -> operations.getArchive(caller, GetArchiveRequest.newBuilder().setAccountId("acct-b").setArchive("docs").build()),
                () -> operations.listArchives(caller, ListArchivesRequest.newBuilder().setAccountId("acct-b").build()),
                () -> operations.stats(caller, GetArchiveStatsRequest.newBuilder().setAccountId("acct-b").setArchive("docs").build()),
                () -> operations.getEntry(caller, GetEntryRequest.newBuilder().setAddress(other).build()),
                () -> operations.getManifest(caller, GetEntryManifestRequest.newBuilder().setAddress(other).build()),
                () -> operations.listEntries(caller, ListEntriesRequest.newBuilder().setAccountId("acct-b").setArchive("docs").build()),
                () -> operations.listVersions(caller, ListVersionsRequest.newBuilder().setAddress(other).build()))) {
            assertThatThrownBy(operation::run).isInstanceOfSatisfying(RepositoryException.class, error -> {
                assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED);
                assertThat(error.getMessage()).isEqualTo("The caller is not bound to account 'acct-b'");
            });
        }
    }

    @Test void boundCallerWritesStayProcessAuthority() {
        var operations = new ArchiveOperations(null, null, null);
        var caller = new RepositoryCaller("alice", false, java.util.Set.of("acct-a"), java.util.Set.of());
        var own = EntryAddress.newBuilder().setAccountId("acct-a").setArchive("docs").setEntryId("e1").build();
        for (Runnable operation : java.util.List.<Runnable>of(
                () -> operations.createArchive(caller, CreateArchiveRequest.getDefaultInstance()),
                () -> operations.putEntry(caller, PutEntryRequest.newBuilder().setAddress(own).build()),
                () -> operations.classifyEntry(caller, ClassifyEntryRequest.getDefaultInstance()),
                () -> operations.bridgeEntry(caller, BridgeEntryRequest.getDefaultInstance()))) {
            assertThatThrownBy(operation::run).isInstanceOfSatisfying(RepositoryException.class, error -> {
                assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED);
                assertThat(error.getMessage()).isEqualTo("Repository account policy is not configured for this principal");
            });
        }
    }

    @Test void unboundScopedCallerReadsAreRefusedAsUnbound() {
        var operations = new ArchiveOperations(null, null, null);
        var caller = new RepositoryCaller("carol", false);
        assertThatThrownBy(() -> operations.getArchive(caller, GetArchiveRequest.newBuilder().setAccountId("acct-a").setArchive("docs").build()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.getMessage()).isEqualTo("The caller is bound to no account"));
    }
}
