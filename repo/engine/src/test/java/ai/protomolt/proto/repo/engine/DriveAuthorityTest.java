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
}
