package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryException;
import org.junit.jupiter.api.Test;

/** The production read-side reservation: fail fast, never queue, never allocate past the budget. */
class ReadByteBudgetTest {

    @Test
    void reservationsWithinTheBudgetSucceedAndReleaseRestoresTheBytes() {
        var budget = new ReadByteBudget(100);
        var first = budget.reserve(60);
        var second = budget.reserve(40);
        assertThat(budget.remainingBytes()).isZero();
        first.close();
        assertThat(budget.remainingBytes()).isEqualTo(60);
        second.close();
        assertThat(budget.remainingBytes()).isEqualTo(100);
    }

    @Test
    void aReservationPastTheRemainingBudgetIsRefusedAtOnceWithResourceExhausted() {
        var budget = new ReadByteBudget(100);
        var held = budget.reserve(70);
        assertThatThrownBy(() -> budget.reserve(31))
                .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED);
                    assertThat(failure.getMessage()).contains("31").contains("30").contains("100");
                });
        assertThat(budget.remainingBytes()).as("a refused reservation takes nothing").isEqualTo(30);
        held.close();
        assertThat(budget.reserve(100)).isNotNull();
    }

    @Test
    void aCorruptOversizedLengthIsRefusedWithoutAllocation() {
        var budget = new ReadByteBudget(64L * 1024 * 1024);
        assertThatThrownBy(() -> budget.reserve(Long.MAX_VALUE))
                .isInstanceOfSatisfying(RepositoryException.class, failure ->
                        assertThat(failure.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
        assertThat(budget.remainingBytes()).isEqualTo(64L * 1024 * 1024);
    }

    @Test
    void closeIsIdempotentAndNeverReturnsBytesTwice() {
        var budget = new ReadByteBudget(10);
        var lease = budget.reserve(10);
        lease.close();
        lease.close();
        assertThat(budget.remainingBytes()).isEqualTo(10);
    }

    @Test
    void zeroByteReservationsSucceedAndNegativeOnesAreInvalid() {
        var budget = new ReadByteBudget(1);
        assertThat(budget.reserve(0)).isNotNull();
        assertThatThrownBy(() -> budget.reserve(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReadByteBudget(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
