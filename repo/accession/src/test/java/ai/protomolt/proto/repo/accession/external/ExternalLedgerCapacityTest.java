package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import com.google.protobuf.Timestamp;
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
                    .setOperationId("op-" + i)
                    .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED));
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

    @Test
    void roomIsReservedForThePublicationsStillOutstanding() {
        // 510 historical links and two publications in flight: each may add one when it
        // settles, which fills the ledger. A third would have nowhere to go.
        ExternalPublicationLedger.Builder nearFull = ledger(0, 510).toBuilder();
        for (int i = 0; i < 2; i++) {
            nearFull.addOperations(ExternalPublicationOperationRecord.newBuilder()
                    .setOperationId("pending-" + i)
                    .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING));
        }
        assertExhausted(() -> ExternalLedgerStore.requireRoomForNewOperation(nearFull.build()),
                "512 historical links");
        // With one in flight there is room for exactly one more.
        assertThatCode(() -> ExternalLedgerStore.requireRoomForNewOperation(
                nearFull.clone().removeOperations(1).build()))
                .doesNotThrowAnyException();
    }

    @Test
    void aRefusedRetryDoesNotSettleAnOperationWhoseEarlierAttemptIsUnknown() {
        Timestamp t0 = Timestamp.newBuilder().setSeconds(1_770_000_000L).build();
        ExternalPublicationOperationRecord lostThenRetried = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId("op-lost")
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DISPATCHING)
                .addAttempts(ExternalPublicationAttempt.newBuilder().setAttemptNumber(1)
                        .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                        .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                        .setPendingPhase(
                                ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY))
                .addAttempts(ExternalPublicationAttempt.newBuilder().setAttemptNumber(2)
                        .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RETRY)
                        .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                        .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DISPATCHING))
                .build();

        ExternalPublicationOperationRecord afterRefusal = ExternalLedgerStore.refusedByProvider(
                lostThenRetried, 2, t0, t0, "ENDPOINT_PERMISSION_DENIED", "token revoked");
        // Attempt 1 may have committed at the provider: the operation is still unknown, and
        // reconciliation must still be able to ask.
        assertThat(afterRefusal.getOutcome())
                .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
        assertThat(afterRefusal.getPendingPhase())
                .isEqualTo(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY);
        assertThat(afterRefusal.hasFailureCode()).isFalse();
        // The refusal itself is on record against the attempt that got it.
        assertThat(afterRefusal.getAttempts(1).getOutcome())
                .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
        assertThat(afterRefusal.getAttempts(1).getDiagnosticCode())
                .isEqualTo("ENDPOINT_PERMISSION_DENIED");

        // A first attempt that is refused has nothing unknown behind it: the operation failed.
        ExternalPublicationOperationRecord firstAttempt = lostThenRetried.toBuilder()
                .clearAttempts().addAttempts(lostThenRetried.getAttempts(1).toBuilder()
                        .setAttemptNumber(1)).build();
        assertThat(ExternalLedgerStore.refusedByProvider(
                        firstAttempt, 1, t0, t0, "ENDPOINT_PERMISSION_DENIED", "token revoked")
                .getOutcome())
                .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
    }
}
