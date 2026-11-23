package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

/**
 * A result is recorded against the attempt it belongs to, and an operation stays open only while
 * a send whose outcome is unknown remains.
 */
class ExternalAttemptSettlementTest {

    private static final Timestamp T1 = Timestamp.newBuilder().setSeconds(1_770_000_001L).build();
    private static final Timestamp T2 = Timestamp.newBuilder().setSeconds(1_770_000_002L).build();
    private static final Timestamp T9 = Timestamp.newBuilder().setSeconds(1_770_000_009L).build();

    private static ExternalPublicationAttempt attempt(int number, ExternalAttemptTrigger trigger,
            ExternalPublicationOutcome outcome, Timestamp startedAt) {
        return ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(number).setTrigger(trigger).setOutcome(outcome).setStartedAt(startedAt)
                .build();
    }

    private static ExternalPublicationOperationRecord publish(ExternalPublicationAttempt... attempts) {
        ExternalPublicationOperationRecord.Builder op = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId("op-publish")
                .setIntent(ExternalPublicationIntent.newBuilder()
                        .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH))
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY);
        for (ExternalPublicationAttempt a : attempts) {
            op.addAttempts(a);
        }
        return op.build();
    }

    @Test
    void aLateResultForTheFirstSendKeepsThatSendsTriggerAndStartAfterAStatusQuery() {
        // The initial send is still out when reconciliation appends a status query.
        ExternalPublicationOperationRecord op = publish(
                attempt(1, ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL,
                        ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING, T1),
                attempt(2, ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RECONCILIATION,
                        ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING, T2));
        ExternalPublicationOperationRecord refused = ExternalLedgerStore.withRefusedAttempt(
                op, 1, T9, "PERMISSION_DENIED", "refused");
        ExternalPublicationAttempt first = refused.getAttemptsList().stream()
                .filter(a -> a.getAttemptNumber() == 1).findFirst().orElseThrow();
        assertThat(first.getTrigger()).isEqualTo(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL);
        assertThat(first.getStartedAt()).isEqualTo(T1);
        assertThat(first.getOutcome()).isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
        // A status query is not a send, so nothing else is out and the operation is settled.
        assertThat(refused.getOutcome()).isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
    }

    @Test
    void twoOverlappingSendsBothRefusedSettleTheOperationInEitherOrder() {
        ExternalPublicationAttempt initial = attempt(1, ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL,
                ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING, T1);
        ExternalPublicationAttempt retry = attempt(2, ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RETRY,
                ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING, T2);

        // The retry is refused first: the initial send is still out, so the operation stays open.
        ExternalPublicationOperationRecord retryRefused = ExternalLedgerStore.withRefusedAttempt(
                publish(initial, retry), 2, T9, "PERMISSION_DENIED", "refused");
        assertThat(retryRefused.getOutcome()).isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
        // Then the initial send is refused: nothing is out any more.
        assertThat(ExternalLedgerStore.withRefusedAttempt(retryRefused, 1, T9, "PERMISSION_DENIED", "refused")
                .getOutcome()).isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);

        // The other order ends the same way.
        ExternalPublicationOperationRecord initialRefused = ExternalLedgerStore.withRefusedAttempt(
                publish(initial, retry), 1, T9, "PERMISSION_DENIED", "refused");
        assertThat(initialRefused.getOutcome()).isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
        assertThat(ExternalLedgerStore.withRefusedAttempt(initialRefused, 2, T9, "PERMISSION_DENIED", "refused")
                .getOutcome()).isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
    }

    @Test
    void aResultForAnAttemptTheOperationNeverRecordedIsAnError() {
        ExternalPublicationOperationRecord op = publish(
                attempt(1, ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL,
                        ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING, T1));
        assertThatThrownBy(() -> ExternalLedgerStore.attemptTrigger(op, 7))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no attempt 7");
    }
}
