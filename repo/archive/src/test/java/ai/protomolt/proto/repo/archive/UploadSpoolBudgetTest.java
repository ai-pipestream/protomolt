package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class UploadSpoolBudgetTest {

    @Test
    void aReservationTheRemainingBudgetDoesNotCoverIsRefusedAndNothingIsHeld() {
        var budget = new UploadSpoolBudget(100);
        var held = budget.reserve(60);

        assertThatThrownBy(() -> budget.reserve(41))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                    assertThat(failure.getStatus().getDescription())
                            .isEqualTo("Upload spool budget exhausted: 41 bytes requested, 40 of 100 available");
                });
        assertThat(budget.reservedBytes()).isEqualTo(60);

        budget.reserve(40).close();
        held.close();
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test
    void aLeaseGivesItsBytesBackOnce() {
        var budget = new UploadSpoolBudget(100);
        var first = budget.reserve(70);
        first.close();
        var second = budget.reserve(70);
        first.close();

        assertThat(budget.reservedBytes()).isEqualTo(70);
        second.close();
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test
    void capacityAndReservationsMustBePositive() {
        assertThatThrownBy(() -> new UploadSpoolBudget(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new UploadSpoolBudget(1).reserve(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
