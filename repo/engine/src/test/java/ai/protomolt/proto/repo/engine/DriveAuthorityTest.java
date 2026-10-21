package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DriveAuthorityTest {
    @Test void scopedCallerCannotReachDriveStorage() {
        var operations = new DriveOperations(null, null);
        var caller = new RepositoryCaller("scoped", false);
        for (Runnable call : java.util.List.<Runnable>of(
                () -> operations.createDrive(caller, CreateDriveRequest.getDefaultInstance()),
                () -> operations.getDrive(caller, GetDriveRequest.getDefaultInstance()),
                () -> operations.listDrives(caller, ListDrivesRequest.getDefaultInstance()))) {
            assertThatThrownBy(call::run).isInstanceOfSatisfying(RepositoryException.class,
                    error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
        }
    }

    @Test void boundCallerDriveReadsAreRefusedOutsideItsAccountsBeforeTheLedger() {
        var operations = new DriveOperations(null, null);
        var caller = new RepositoryCaller("alice", false, java.util.Set.of("acct-a"), java.util.Set.of());
        for (Runnable call : java.util.List.<Runnable>of(
                () -> operations.getDrive(caller, GetDriveRequest.newBuilder().setAccountId("acct-b").setName("intake").build()),
                () -> operations.listDrives(caller, ListDrivesRequest.newBuilder().setAccountId("acct-b").build()))) {
            assertThatThrownBy(call::run).isInstanceOfSatisfying(RepositoryException.class, error -> {
                assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED);
                assertThat(error.getMessage()).isEqualTo("The caller is not bound to account 'acct-b'");
            });
        }
        assertThatThrownBy(() -> operations.createDrive(caller, CreateDriveRequest.newBuilder()
                .setAccountId("acct-a").setName("mine").build()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
    }

    @Test void unboundScopedCallerIsRefusedAsUnbound() {
        var operations = new DriveOperations(null, null);
        assertThatThrownBy(() -> operations.listDrives(new RepositoryCaller("carol", false),
                ListDrivesRequest.newBuilder().setAccountId("acct-a").build()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.getMessage()).isEqualTo("The caller is bound to no account"));
    }
}
