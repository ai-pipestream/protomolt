package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * The ledger's size bounds are checked before an operation is sent, so nothing runs at the
 * provider that the ledger then has no room to record.
 */
class ExternalLedgerCapacityTest {

    private static ExternalPublicationLedger ledger(int operations, int historicalLinks) {
        ExternalPublicationLedger.Builder ledger = ExternalPublicationLedger.newBuilder();
        for (int i = 0; i < operations; i++) {
            ledger.addOperations(ExternalPublicationOperationRecord.newBuilder()
                    .setOperationId("op-" + i));
        }
        for (int i = 0; i < historicalLinks; i++) {
            ledger.addHistoricalLinks(ExternalLink.newBuilder().setExternalObjectId("o-" + i));
        }
        return ledger.build();
    }

    private static ExternalPublicationOperationRecord operation(int attempts) {
        ExternalPublicationOperationRecord.Builder op =
                ExternalPublicationOperationRecord.newBuilder().setOperationId("op-many");
        for (int i = 1; i <= attempts; i++) {
            op.addAttempts(ExternalPublicationAttempt.newBuilder().setAttemptNumber(i));
        }
        return op.build();
    }

    private static void assertExhausted(Runnable check, String says) {
        assertThatThrownBy(check::run)
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                    assertThat(refused.getStatus().getCode())
                            .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                    assertThat(refused.getStatus().getDescription()).contains(says);
                });
    }

    @Test
    void aNewOperationIsRefusedWhenItOrItsOutcomeCouldNotBeRecorded() {
        assertThatCode(() -> ExternalLedgerStore.requireRoomForNewOperation(ledger(1023, 511)))
                .doesNotThrowAnyException();
        assertExhausted(() -> ExternalLedgerStore.requireRoomForNewOperation(ledger(1024, 0)),
                "1024 operations");
        // Settling a publication can add one historical link; at 512 there is nowhere to put it.
        assertExhausted(() -> ExternalLedgerStore.requireRoomForNewOperation(ledger(1, 512)),
                "512 historical links");
    }

    @Test
    void aRetryIsRefusedWhenOnlyTheSettlingAttemptSlotIsLeft() {
        assertThatCode(() -> ExternalLedgerStore.requireRoomForRetry(operation(254)))
                .doesNotThrowAnyException();
        assertThat(ExternalLedgerStore.hasRoomForUnsettledAttempt(operation(254))).isTrue();
        // 255 attempts: the 256th is kept for the receipt that settles the operation.
        assertThat(ExternalLedgerStore.hasRoomForUnsettledAttempt(operation(255))).isFalse();
        assertExhausted(() -> ExternalLedgerStore.requireRoomForRetry(operation(255)),
                "can only be reconciled");
    }
}
