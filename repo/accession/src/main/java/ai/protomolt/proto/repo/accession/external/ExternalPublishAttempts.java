package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.attemptStartedAt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.attemptTrigger;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isSourceRevisionFenced;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isTerminalOutcome;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.withRefusedAttempt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceAttempt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.timestamp;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.Set;
import java.util.Optional;
import java.util.UUID;

/**
 * Runs one publish attempt against the provider: the first dispatch, a retry of a pending
 * operation, or the recorded failure of a publication refused by the source-revision fence.
 */
final class ExternalPublishAttempts {

    private final ExternalLedgerStore ledgers;
    private final ExternalReceiptSettlement settlement;
    private final ExternalBindingStore bindingStore;
    private final ExternalProviderGateway gateway;
    private final Clock clock;

    ExternalPublishAttempts(ExternalLedgerStore ledgers, ExternalReceiptSettlement settlement,
            ExternalBindingStore bindingStore, ExternalProviderGateway gateway, Clock clock) {
        this.ledgers = ledgers;
        this.settlement = settlement;
        this.bindingStore = bindingStore;
        this.gateway = gateway;
        this.clock = clock;
    }

    ExternalPublicationCoordinator.OperationResult retryPendingPublish(
            NodeAddress ledgerAddr,
            LedgerAccess retainedSecurity,
            ExternalPublicationOperationRecord priorOp,
            ExternalPublicationIntent intent,
            String intentSha256) {
        String operationId = priorOp.getOperationId();
        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(priorOp.getBindingSnapshot());
        ExternalDeletionLifecycle.requireEndpointUnmoved(priorOp.getBindingSnapshot(), outboundAuth);
        // The attempt number is taken inside the ledger update, from the operation as it is
        // then, so two retries that overlap never share one.
        final int[] attemptHolder = new int[] {priorOp.getAttemptsCount() + 1};
        Timestamp startedAt = timestamp(clock.instant());

        final boolean[] fenced = new boolean[] {false};
        ExternalPublicationLedger preUpdated = ledgers.updateLedgerWithCas(
                ledgerAddr,
                intent.getSourceAddress(),
                intent.getBindingId(),
                retainedSecurity,
                "ext-retry-pre:" + operationId + ":" + UUID.randomUUID(),
                current -> {
                    ExternalPublicationOperationRecord latestOp =
                            findOperation(current, operationId).orElseThrow();
                    if (isTerminalOutcome(latestOp.getOutcome())) {
                        return current;
                    }
                    ExternalLedgerStore.requireRoomForRetry(latestOp);
                    int attemptNumber = latestOp.getAttemptsCount() + 1;
                    attemptHolder[0] = attemptNumber;
                    if (isSourceRevisionFenced(current, intent.getSourceRevision())
                            || ExternalLedgerStore.heldByNewerBinding(current, latestOp.getBindingSnapshot(),
                                    intent.getExternalObjectId(), intent.getSourceRevision())) {
                        // A newer revision, tombstone or purge fences this one, or a newer
                        // binding revision already holds its object at the same place, so it is not
                        // sent again. It is not declared failed either: an earlier attempt
                        // was dispatched and nobody knows what the provider did with it.
                        // The operation stays as it is for reconciliation to settle.
                        fenced[0] = true;
                        return current;
                    }
                    ExternalPublicationAttempt retryDispatch = ExternalPublicationAttempt.newBuilder()
                            .setAttemptNumber(attemptNumber)
                            .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RETRY)
                            .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                            .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DISPATCHING)
                            .setStartedAt(startedAt)
                            .build();
                    ExternalPublicationOperationRecord dispatchingOp = latestOp.toBuilder()
                            .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DISPATCHING)
                            .addAttempts(retryDispatch)
                            .setUpdatedAt(startedAt)
                            .build();
                    return replaceOperation(current, ExternalLedgerStore.withDispatchIdentity(
                            dispatchingOp, outboundAuth.credentialBindingSha256()));
                });

        ExternalPublicationOperationRecord latestAfterPre =
                findOperation(preUpdated, operationId).orElseThrow();
        if (isTerminalOutcome(latestAfterPre.getOutcome()) || fenced[0]) {
            return new ExternalPublicationCoordinator.OperationResult(
                    latestAfterPre, optionalCurrentLink(preUpdated), false, false);
        }

        return executeAndSettlePublishAttempt(
                ledgerAddr,
                retainedSecurity,
                operationId,
                attemptHolder[0],
                intent,
                intentSha256,
                latestAfterPre.getBindingSnapshot(),
                outboundAuth);
    }

    ExternalPublicationCoordinator.OperationResult recordFencedPublishFailure(
            NodeAddress ledgerAddr,
            LedgerAccess retainedSecurity,
            String operationId,
            ExternalPublicationIntent intent,
            String intentSha256,
            ExternalSystemBinding bindingSnapshot,
            ExternalAttemptTrigger trigger) {
        Timestamp now = timestamp(clock.instant());
        ExternalPublicationAttempt fencedAttempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(1)
                .setTrigger(trigger)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .setStartedAt(now)
                .setFinishedAt(now)
                .setDiagnosticCode("FENCED_BY_NEWER_STATE")
                .setDiagnosticMessage(
                        "Publication of revision " + intent.getSourceRevision()
                                + " refused by monotonic source revision/deletion fence")
                .build();
        ExternalPublicationOperationRecord failedRecord = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId(operationId)
                .setIntentSha256(intentSha256)
                .setIntent(intent)
                .setBindingSnapshot(bindingSnapshot)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .addAttempts(fencedAttempt)
                .setFailureCode("FENCED_BY_NEWER_STATE")
                .setFailureMessage(fencedAttempt.getDiagnosticMessage())
                .setCreatedAt(now)
                .setUpdatedAt(now)
                .build();

        ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                ledgerAddr,
                intent.getSourceAddress(),
                intent.getBindingId(),
                retainedSecurity,
                "ext-fenced:" + operationId + ":" + UUID.randomUUID(),
                current -> {
                    // Another request may have registered this operation ID since the caller
                    // looked. The same intent is a replay; a different one is a conflict.
                    Optional<ExternalPublicationOperationRecord> registered =
                            findOperation(current, operationId);
                    if (registered.isPresent()) {
                        if (!registered.get().getIntentSha256().equals(intentSha256)) {
                            throw Status.ALREADY_EXISTS
                                    .withDescription("Operation '" + operationId
                                            + "' is already bound to a different publication intent")
                                    .asRuntimeException();
                        }
                        return current;
                    }
                    ExternalLedgerStore.requireRoomForLocalFailure(current);
                    return current.toBuilder().addOperations(failedRecord).build();
                });
        return new ExternalPublicationCoordinator.OperationResult(
                findOperation(updated, operationId).orElseThrow(),
                optionalCurrentLink(updated),
                false,
                false);
    }

    /**
     * Fails a registered publication whose first attempt was never sent. The attempt is
     * recorded as failed with {@code code}; nothing reached the provider, so nothing is owed.
     *
     * @throws io.grpc.StatusRuntimeException FAILED_PRECONDITION when the ledger no longer
     *         records the operation
     */
    ExternalPublicationCoordinator.OperationResult failBeforeDispatch(
            NodeAddress ledgerAddr,
            LedgerAccess retainedSecurity,
            String operationId,
            ExternalPublicationIntent intent,
            String code,
            String message) {
        Timestamp now = timestamp(clock.instant());
        ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                ledgerAddr,
                intent.getSourceAddress(),
                intent.getBindingId(),
                retainedSecurity,
                "ext-not-sent:" + operationId + ":" + UUID.randomUUID(),
                current -> findOperation(current, operationId)
                        .map(op -> replaceOperation(current, withRefusedAttempt(op, 1, now, code, message)))
                        .orElse(current));
        ExternalPublicationOperationRecord op = findOperation(updated, operationId)
                .orElseThrow(() -> Status.FAILED_PRECONDITION
                        .withDescription("Operation '" + operationId + "' is no longer recorded: "
                                + "the source and its ledger were removed before it was sent")
                        .asRuntimeException());
        return new ExternalPublicationCoordinator.OperationResult(
                op, optionalCurrentLink(updated), false, false);
    }

    ExternalPublicationCoordinator.OperationResult executeAndSettlePublishAttempt(
            NodeAddress ledgerAddr,
            LedgerAccess retainedSecurity,
            String operationId,
            int attemptNumber,
            ExternalPublicationIntent intent,
            String intentSha256,
            ExternalSystemBinding bindingSnapshot,
            ExternalProviderGateway.OutboundAuthContext outboundAuth) {
        Timestamp callStart = timestamp(clock.instant());
        ExternalProviderGateway.ProviderCallResult callResult;
        try {
            callResult = gateway.execute(new ExternalProviderGateway.ProviderCallRequest(
                    operationId, intentSha256, intent, outboundAuth));
        } catch (StatusRuntimeException callFailure) {
            callResult = ExternalProviderGateway.ProviderCallResult.fromCallFailure(callFailure);
        }

        if (callResult instanceof ExternalProviderGateway.ProviderCallResult.Refused refused) {
            Timestamp finishedAt = timestamp(clock.instant());
            ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                    ledgerAddr,
                    intent.getSourceAddress(),
                    intent.getBindingId(),
                    retainedSecurity,
                    "ext-refused:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                    current -> replaceOperation(current, withRefusedAttempt(
                            findOperation(current, operationId).orElseThrow(),
                            attemptNumber, finishedAt,
                            refused.diagnosticCode(), refused.diagnosticMessage())));
            return new ExternalPublicationCoordinator.OperationResult(
                    findOperation(updated, operationId).orElseThrow(),
                    optionalCurrentLink(updated),
                    false,
                    false);
        }

        if (callResult instanceof ExternalProviderGateway.ProviderCallResult.Unresolved unresolved) {
            Timestamp finishedAt = timestamp(clock.instant());
            ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                    ledgerAddr,
                    intent.getSourceAddress(),
                    intent.getBindingId(),
                    retainedSecurity,
                    "ext-unresolved:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                    current -> {
                        ExternalPublicationOperationRecord op =
                                findOperation(current, operationId).orElseThrow();
                        if (isTerminalOutcome(op.getOutcome())) {
                            return current;
                        }
                        ExternalPublicationAttempt updatedAttempt = ExternalPublicationAttempt.newBuilder()
                                .setAttemptNumber(attemptNumber)
                                .setTrigger(attemptTrigger(op, attemptNumber))
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                                .setStartedAt(attemptStartedAt(op, attemptNumber))
                                .setFinishedAt(finishedAt)
                                .setDiagnosticCode(unresolved.diagnosticCode())
                                .setDiagnosticMessage(sanitizeMessage(unresolved.diagnosticMessage()))
                                .build();
                        ExternalPublicationOperationRecord nextOp = replaceAttempt(op, updatedAttempt)
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                                .setUpdatedAt(finishedAt)
                                .build();
                        return replaceOperation(current, nextOp);
                    });
            return new ExternalPublicationCoordinator.OperationResult(
                    findOperation(updated, operationId).orElseThrow(),
                    optionalCurrentLink(updated),
                    false,
                    false);
        }

        ExternalProviderEvidence evidence =
                ((ExternalProviderGateway.ProviderCallResult.Receipt) callResult).evidence();
        return settlement.applyProviderReceipt(
                ledgerAddr,
                intent.getSourceAddress(),
                intent.getBindingId(),
                retainedSecurity,
                operationId,
                attemptNumber,
                null,
                callStart,
                intent,
                intentSha256,
                bindingSnapshot,
                outboundAuth,
                evidence);
    }
}
