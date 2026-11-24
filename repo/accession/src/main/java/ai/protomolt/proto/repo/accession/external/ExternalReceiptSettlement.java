package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.attemptStartedAt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.attemptTrigger;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isTerminalOutcome;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceAttempt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sha256Hex;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.timestamp;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalEndpointSummary;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.PurgeDocumentRequest;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Verifies a provider receipt and settles it onto the ledger: accepted-only, published,
 * rejected, deletion-accepted and deleted outcomes, link activation and retirement, and the
 * {@code PURGE_WITH_SOURCE} ledger purge once nothing remains outstanding.
 */
final class ExternalReceiptSettlement {

    private static final String DEFAULT_REJECTION_CODE = "PROVIDER_REJECTED";
    private static final String REJECTION_MESSAGE = "Provider rejected external operation";
    private static final String PURGE_REASON =
            "PURGE_WITH_SOURCE retention policy after external deletion settled";
    private static final int MAX_PURGE_ID_LEN = 200;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final ExternalLedgerStore ledgers;
    private final ExternalProviderVerifier verifier;
    private final Clock clock;

    ExternalReceiptSettlement(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            ExternalLedgerStore ledgers, ExternalProviderVerifier verifier, Clock clock) {
        this.documents = documents;
        this.ledgers = ledgers;
        this.verifier = verifier;
        this.clock = clock;
    }

    ExternalPublicationCoordinator.OperationResult applyProviderReceipt(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            LedgerAccess retainedSecurity,
            String operationId,
            int attemptNumber,
            ExternalAttemptTrigger explicitTrigger,
            Timestamp fallbackStartedAt,
            String expectedIntentSha256,
            ExternalProviderGateway.OutboundAuthContext outboundAuth,
            ExternalProviderEvidence evidence) {
        Timestamp finishedAt = timestamp(clock.instant());
        ExternalPublicationOperationRecord recorded = requireRecordedOperation(
                ledgerAddr, sourceAddress, operationId, expectedIntentSha256);
        try {
            verifier.verifyRecordedEvidence(evidence, recorded, outboundAuth);
        } catch (ExternalProviderVerifier.ProviderVerificationException invalidEvidence) {
            return recordVerificationFailure(
                    ledgerAddr,
                    sourceAddress,
                    bindingId,
                    retainedSecurity,
                    operationId,
                    attemptNumber,
                    explicitTrigger,
                    fallbackStartedAt,
                    finishedAt,
                    invalidEvidence);
        }

        ExternalPublicationLedger settledLedger = ledgers.updateLedgerWithCas(
                ledgerAddr,
                sourceAddress,
                bindingId,
                retainedSecurity,
                "ext-settle:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                current -> settleVerifiedEvidence(
                        current,
                        operationId,
                        attemptNumber,
                        explicitTrigger,
                        fallbackStartedAt,
                        finishedAt,
                        evidence));

        ExternalPublicationOperationRecord finalOp =
                requireOperationInLedger(settledLedger, operationId);
        boolean purged = maybePurgeLedgerIfEligible(ledgerAddr, settledLedger);
        return new ExternalPublicationCoordinator.OperationResult(
                finalOp,
                purged ? Optional.empty() : optionalCurrentLink(settledLedger),
                false,
                purged);
    }

    private ExternalPublicationOperationRecord requireRecordedOperation(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String operationId,
            String expectedIntentSha256) {
        ExternalPublicationLedger ledger = ledgers.loadOptionalLedger(ledgerAddr, sourceAddress)
                .map(ExternalLedgerStore.LoadedLedger::ledger)
                .orElseThrow(() -> missingRecordedOperation(operationId));
        ExternalPublicationOperationRecord recorded =
                requireOperationInLedger(ledger, operationId);
        if (!recorded.getIntentSha256().equals(expectedIntentSha256)) {
            throw Status.DATA_LOSS
                    .withDescription("Recorded operation '" + operationId
                            + "' intent digest diverged from in-flight operation")
                    .asRuntimeException();
        }
        return recorded;
    }

    private static ExternalPublicationOperationRecord requireOperationInLedger(
            ExternalPublicationLedger ledger, String operationId) {
        return findOperation(ledger, operationId)
                .orElseThrow(() -> missingRecordedOperation(operationId));
    }

    private static StatusRuntimeException missingRecordedOperation(String operationId) {
        return Status.FAILED_PRECONDITION
                .withDescription("Operation '" + operationId
                        + "' is no longer recorded on the external publication ledger")
                .asRuntimeException();
    }

    private ExternalPublicationCoordinator.OperationResult recordVerificationFailure(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            LedgerAccess retainedSecurity,
            String operationId,
            int attemptNumber,
            ExternalAttemptTrigger explicitTrigger,
            Timestamp fallbackStartedAt,
            Timestamp finishedAt,
            ExternalProviderVerifier.ProviderVerificationException invalidEvidence) {
        ExternalPublicationLedger failedLedger = ledgers.updateLedgerWithCas(
                ledgerAddr,
                sourceAddress,
                bindingId,
                retainedSecurity,
                "ext-verify-fail:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                current -> {
                    ExternalPublicationOperationRecord op =
                            requireOperationInLedger(current, operationId);
                    if (isTerminalOutcome(op.getOutcome())) {
                        return current;
                    }
                    ExternalAttemptTrigger trigger = explicitTrigger != null
                            ? explicitTrigger
                            : attemptTrigger(op, attemptNumber);
                    Timestamp startedAt = explicitTrigger != null
                            ? fallbackStartedAt
                            : attemptStartedAt(op, attemptNumber);
                    ExternalPublicationAttempt failedAttempt = ExternalPublicationAttempt.newBuilder()
                            .setAttemptNumber(attemptNumber)
                            .setTrigger(trigger)
                            .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                            .setStartedAt(startedAt)
                            .setFinishedAt(finishedAt)
                            .setDiagnosticCode(invalidEvidence.diagnosticCode())
                            .setDiagnosticMessage(sanitizeMessage(invalidEvidence.getMessage()))
                            .build();
                    ExternalPublicationOperationRecord.Builder opBuilder =
                            explicitTrigger != null
                                    ? op.toBuilder().addAttempts(failedAttempt)
                                    : replaceAttempt(op, failedAttempt);
                    ExternalPublicationOperationRecord updatedOp = opBuilder
                            .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                            .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                            // The operation fails hard, but the provider answered: it may
                            // have committed before sending evidence that does not verify.
                            .setProviderMayHoldObject(op.getIntent().getAction()
                                    == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                            .setFailureCode(invalidEvidence.diagnosticCode())
                            .setFailureMessage(sanitizeMessage(invalidEvidence.getMessage()))
                            .setUpdatedAt(finishedAt)
                            .build();
                    return replaceOperation(current, updatedOp);
                });
        return new ExternalPublicationCoordinator.OperationResult(
                requireOperationInLedger(failedLedger, operationId),
                optionalCurrentLink(failedLedger),
                false,
                false);
    }

    private static ExternalPublicationLedger settleVerifiedEvidence(
            ExternalPublicationLedger current,
            String operationId,
            int attemptNumber,
            ExternalAttemptTrigger explicitTrigger,
            Timestamp fallbackStartedAt,
            Timestamp finishedAt,
            ExternalProviderEvidence evidence) {
        ExternalPublicationOperationRecord op = requireOperationInLedger(current, operationId);
        if (isTerminalOutcome(op.getOutcome())) {
            return current;
        }
        ExternalAttemptTrigger trigger = explicitTrigger != null
                ? explicitTrigger
                : attemptTrigger(op, attemptNumber);
        Timestamp startedAt = explicitTrigger != null
                ? fallbackStartedAt
                : attemptStartedAt(op, attemptNumber);
        ExternalProviderSettlement settlement = evidence.getSettlement();
        boolean settles = settlement == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED
                || settlement == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_REJECTED
                || settlement == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETED;
        if (explicitTrigger != null && !settles
                && !ExternalLedgerStore.hasRoomForUnsettledAttempt(op)) {
            // A receipt that settles nothing is not written into the last attempt slot:
            // that slot is reserved for the receipt that ends the operation.
            return current;
        }
        boolean appendNewAttempt = explicitTrigger != null;
        return switch (settlement) {
            case EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY -> settleAcceptedOnly(
                    current, op, attemptNumber, trigger, startedAt, finishedAt,
                    appendNewAttempt, evidence);
            case EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED -> settleConfirmedPublication(
                    current, op, attemptNumber, trigger, startedAt, finishedAt,
                    appendNewAttempt, evidence);
            case EXTERNAL_PROVIDER_SETTLEMENT_REJECTED -> settleRejected(
                    current, op, attemptNumber, trigger, startedAt, finishedAt,
                    appendNewAttempt, evidence);
            case EXTERNAL_PROVIDER_SETTLEMENT_DELETION_ACCEPTED -> settleDeletionAccepted(
                    current, op, attemptNumber, trigger, startedAt, finishedAt,
                    appendNewAttempt, evidence);
            case EXTERNAL_PROVIDER_SETTLEMENT_DELETED -> settleConfirmedDeletion(
                    current, op, attemptNumber, trigger, startedAt, finishedAt,
                    appendNewAttempt, evidence);
            case EXTERNAL_PROVIDER_SETTLEMENT_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalStateException("Unexpected settlement: " + settlement);
        };
    }

    private static ExternalPublicationLedger settleAcceptedOnly(
            ExternalPublicationLedger current,
            ExternalPublicationOperationRecord op,
            int attemptNumber,
            ExternalAttemptTrigger trigger,
            Timestamp startedAt,
            Timestamp finishedAt,
            boolean appendNewAttempt,
            ExternalProviderEvidence evidence) {
        ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(attemptNumber)
                .setTrigger(trigger)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(
                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_ACCEPTED_AWAITING_PUBLICATION)
                .setStartedAt(startedAt)
                .setFinishedAt(finishedAt)
                .setEvidence(evidence)
                .build();
        ExternalPublicationOperationRecord.Builder opBuilder = appendNewAttempt
                ? op.toBuilder().addAttempts(attempt)
                : replaceAttempt(op, attempt);
        ExternalPublicationOperationRecord updatedOp = opBuilder
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(
                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_ACCEPTED_AWAITING_PUBLICATION)
                .setUpdatedAt(finishedAt)
                .build();
        return replaceOperation(current, updatedOp);
    }

    private static ExternalPublicationLedger settleRejected(
            ExternalPublicationLedger current,
            ExternalPublicationOperationRecord op,
            int attemptNumber,
            ExternalAttemptTrigger trigger,
            Timestamp startedAt,
            Timestamp finishedAt,
            boolean appendNewAttempt,
            ExternalProviderEvidence evidence) {
        String code = evidence.hasDiagnosticCode()
                ? evidence.getDiagnosticCode()
                : DEFAULT_REJECTION_CODE;
        ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(attemptNumber)
                .setTrigger(trigger)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .setStartedAt(startedAt)
                .setFinishedAt(finishedAt)
                .setDiagnosticCode(code)
                .setDiagnosticMessage(REJECTION_MESSAGE)
                .setEvidence(evidence)
                .build();
        ExternalPublicationOperationRecord.Builder opBuilder = appendNewAttempt
                ? op.toBuilder().addAttempts(attempt)
                : replaceAttempt(op, attempt);
        ExternalPublicationOperationRecord updatedOp = opBuilder
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                // A rejection of a send settles that send. An earlier send whose outcome was
                // never learned may still have committed; a status query answers for the
                // operation as a whole.
                .setProviderMayHoldObject(op.getIntent().getAction()
                                == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH
                        && trigger != ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RECONCILIATION
                        && ExternalLedgerStore.anotherSendIsUnresolved(op, attemptNumber))
                .setFailureCode(code)
                .setFailureMessage(REJECTION_MESSAGE)
                .setUpdatedAt(finishedAt)
                .build();
        return replaceOperation(current, updatedOp);
    }

    private static ExternalPublicationLedger settleDeletionAccepted(
            ExternalPublicationLedger current,
            ExternalPublicationOperationRecord op,
            int attemptNumber,
            ExternalAttemptTrigger trigger,
            Timestamp startedAt,
            Timestamp finishedAt,
            boolean appendNewAttempt,
            ExternalProviderEvidence evidence) {
        ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(attemptNumber)
                .setTrigger(trigger)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                .setStartedAt(startedAt)
                .setFinishedAt(finishedAt)
                .setEvidence(evidence)
                .build();
        ExternalPublicationOperationRecord.Builder opBuilder = appendNewAttempt
                ? op.toBuilder().addAttempts(attempt)
                : replaceAttempt(op, attempt);
        ExternalPublicationOperationRecord updatedOp = opBuilder
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                .setUpdatedAt(finishedAt)
                .build();
        return replaceOperation(current, updatedOp);
    }

    static ExternalPublicationLedger settleConfirmedPublication(
            ExternalPublicationLedger current,
            ExternalPublicationOperationRecord op,
            int attemptNumber,
            ExternalAttemptTrigger trigger,
            Timestamp startedAt,
            Timestamp finishedAt,
            boolean appendNewAttempt,
            ExternalProviderEvidence evidence) {
        ExternalPublicationIntent intent = op.getIntent();
        ExternalSystemBinding bindingSnapshot = op.getBindingSnapshot();
        ExternalPublicationAttempt confirmedAttempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(attemptNumber)
                .setTrigger(trigger)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED)
                .setStartedAt(startedAt)
                .setFinishedAt(finishedAt)
                .setEvidence(evidence)
                .build();
        ExternalPublicationOperationRecord.Builder opBuilder = appendNewAttempt
                ? op.toBuilder().addAttempts(confirmedAttempt)
                : replaceAttempt(op, confirmedAttempt);
        ExternalPublicationOperationRecord confirmedOp = opBuilder
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                .setTerminalEvidence(evidence)
                .setUpdatedAt(finishedAt)
                .build();

        ExternalLink.Builder linkBuilder = ExternalLink.newBuilder()
                .setBindingId(bindingSnapshot.getBindingId())
                .setBindingRevision(bindingSnapshot.getBindingRevision())
                .setBindingSnapshot(bindingSnapshot)
                .setExternalObjectId(intent.getExternalObjectId())
                .setSourceAddress(intent.getSourceAddress())
                .setSourceRevision(intent.getSourceRevision())
                .setConfirmedByOperationId(op.getOperationId())
                .setConfirmationEvidence(evidence)
                .setConfirmedAt(finishedAt);
        if (evidence.hasExternalVersion()) {
            linkBuilder.setExternalVersion(evidence.getExternalVersion());
        }
        if (intent.hasProcessing()) {
            linkBuilder.setProcessing(intent.getProcessing());
        }
        if (evidence.hasManagedSearch()) {
            linkBuilder.setManagedSearch(evidence.getManagedSearch());
        }
        if (intent.hasDisplayUrl()) {
            linkBuilder.setDisplayUrl(intent.getDisplayUrl());
        }

        boolean sourceDeleted = current.getSourcePurged()
                || (current.hasTombstonedAtSourceRevision()
                        && intent.getSourceRevision() <= current.getTombstonedAtSourceRevision());
        // Only a confirmed newer projection supersedes this one. A newer publication that is
        // merely registered raises the fence but may still fail, and then this projection is
        // the one the provider serves.
        boolean superseded = current.hasCurrentLink()
                && (current.getCurrentLink().getSourceRevision() > intent.getSourceRevision()
                        // The same source revision, published since under a newer binding
                        // revision: the older binding's projection arrives late and does not
                        // take its place.
                        || (current.getCurrentLink().getSourceRevision() == intent.getSourceRevision()
                                && current.getCurrentLink().getBindingRevision()
                                        > intent.getBindingRevision()));

        ExternalPublicationLedger withOp = replaceOperation(current, confirmedOp);
        ExternalPublicationLedger.Builder ledgerBuilder = withOp.toBuilder();
        if (sourceDeleted) {
            // The provider published after the source was tombstoned or purged. The object is
            // live there and nothing has deleted it, so it is recorded as owed a deletion and
            // never as deleted: only verified deletion evidence for it does that.
            ExternalLink owedDeletion = linkBuilder
                    .setProjectionLifecycle(
                            ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING)
                    .build();
            if (current.hasCurrentLink()) {
                ledgerBuilder.addHistoricalLinks(owedDeletion);
            } else {
                ledgerBuilder.setCurrentLink(owedDeletion);
            }
            return ledgerBuilder.build();
        }
        if (superseded) {
            ledgerBuilder.addHistoricalLinks(linkBuilder
                    .setProjectionLifecycle(
                            ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED)
                    .setRetiredByOperationId(current.getCurrentLink().getConfirmedByOperationId())
                    .build());
            return ledgerBuilder.build();
        }

        if (current.hasCurrentLink()) {
            ExternalLink existingCurrent = current.getCurrentLink();
            ExternalProjectionLifecycle displacedLifecycle =
                    remainsPendingDeletionWhenDisplaced(
                            current, existingCurrent, bindingSnapshot, intent.getExternalObjectId())
                            ? ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING
                            : ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED;
            ExternalLink displaced = existingCurrent.toBuilder()
                    .setProjectionLifecycle(displacedLifecycle)
                    .setRetiredByOperationId(op.getOperationId())
                    .build();
            ledgerBuilder.addHistoricalLinks(displaced);
        }
        ledgerBuilder.setCurrentLink(linkBuilder
                .setProjectionLifecycle(
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE)
                .build());
        ledgerBuilder.setHighestFencedSourceRevision(
                Math.max(current.getHighestFencedSourceRevision(), intent.getSourceRevision()));
        return ledgerBuilder.build();
    }

    private static boolean remainsPendingDeletionWhenDisplaced(
            ExternalPublicationLedger ledger,
            ExternalLink displacedCandidate,
            ExternalSystemBinding newBinding,
            String newExternalObjectId) {
        if (displacedCandidate.getProjectionLifecycle()
                != ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING) {
            return false;
        }
        boolean replacedInPlace = displacedCandidate.getExternalObjectId().equals(newExternalObjectId)
                && ExternalDeleteTargets.sameExternalTarget(
                        displacedCandidate.getBindingSnapshot(), newBinding);
        if (!replacedInPlace) {
            return true;
        }
        for (String deleteOpId : displacedCandidate.getDeletionClaimedByOperationIdsList()) {
            if (findOperation(ledger, deleteOpId)
                    .filter(claiming -> !isTerminalOutcome(claiming.getOutcome()))
                    .isPresent()) {
                return true;
            }
        }
        return false;
    }

    static ExternalPublicationLedger settleConfirmedDeletion(
            ExternalPublicationLedger current,
            ExternalPublicationOperationRecord op,
            int attemptNumber,
            ExternalAttemptTrigger trigger,
            Timestamp startedAt,
            Timestamp finishedAt,
            boolean appendNewAttempt,
            ExternalProviderEvidence evidence) {
        ExternalPublicationAttempt deletedAttempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(attemptNumber)
                .setTrigger(trigger)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED)
                .setStartedAt(startedAt)
                .setFinishedAt(finishedAt)
                .setEvidence(evidence)
                .build();
        ExternalPublicationOperationRecord.Builder opBuilder = appendNewAttempt
                ? op.toBuilder().addAttempts(deletedAttempt)
                : replaceAttempt(op, deletedAttempt);
        ExternalPublicationOperationRecord deletedOp = opBuilder
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                .setTerminalEvidence(evidence)
                .setUpdatedAt(finishedAt)
                .build();

        ExternalPublicationLedger withOp = replaceOperation(current, deletedOp);
        return retireDeletedProjection(withOp, deletedOp, evidence);
    }

    /**
     * Marks as deleted every projection the deletion claimed and the evidence covers: the
     * current link and any historical link of the deleted object at the same place in the
     * external system. Late publications can leave several such links owed a deletion, and one
     * verified deletion of the object settles them all. Links the deletion did not claim stay
     * as they were, because its evidence says nothing about them.
     */
    private static ExternalPublicationLedger retireDeletedProjection(
            ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord deletedOp,
            ExternalProviderEvidence evidence) {
        ExternalPublicationLedger.Builder next = ledger.toBuilder();
        for (int i = 0; i < ledger.getHistoricalLinksCount(); i++) {
            ExternalLink historical = ledger.getHistoricalLinks(i);
            if (historical.getProjectionLifecycle()
                            != ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED
                    && isDeletionTarget(historical, deletedOp)) {
                next.setHistoricalLinks(i, retired(historical, deletedOp, evidence));
            }
        }
        if (ledger.hasCurrentLink() && isDeletionTarget(ledger.getCurrentLink(), deletedOp)) {
            next.clearCurrentLink();
            next.addHistoricalLinks(retired(ledger.getCurrentLink(), deletedOp, evidence));
        }
        return next.build();
    }

    private static boolean isDeletionTarget(
            ExternalLink link, ExternalPublicationOperationRecord deletedOp) {
        ExternalPublicationIntent deletion = deletedOp.getIntent();
        // A tombstone deletion removes what was published up to that tombstone. Anything
        // published for a later revision came after it and is not what it deleted. A purge
        // deletion (source revision 0) is for every revision.
        boolean publishedBeforeDeletion = deletion.getSourceRevision() == 0L
                || link.getSourceRevision() <= deletion.getSourceRevision();
        // Source revisions and clocks do not order what the provider did. The deletion named
        // itself on the links that were on record when it was sent; a link without that name
        // was recorded later and may be a publication the provider ran after the deletion.
        boolean recordedBeforeDeletionWasSent =
                link.getDeletionClaimedByOperationIdsList().contains(deletedOp.getOperationId());
        return publishedBeforeDeletion
                && recordedBeforeDeletionWasSent
                && link.getExternalObjectId().equals(deletion.getExternalObjectId())
                // The same place in the external system, whatever the binding revision number.
                && ExternalDeleteTargets.sameExternalTarget(
                        link.getBindingSnapshot(), deletedOp.getBindingSnapshot());
    }

    private static ExternalLink retired(ExternalLink link,
            ExternalPublicationOperationRecord deletedOp, ExternalProviderEvidence evidence) {
        return link.toBuilder()
                .setProjectionLifecycle(
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED)
                .setRetiredByOperationId(deletedOp.getOperationId())
                .setDeletionEvidence(evidence)
                .build();
    }


    /**
     * True when the ledger records nothing still owed: no operation is pending, and every
     * external object any link names has verified deletion evidence. The ledger is the only
     * record of an object's identity and of what is owed for it, so it is purged with its
     * source only once this holds.
     */
    static boolean nothingOutstanding(ExternalPublicationLedger ledger) {
        for (int i = 0; i < ledger.getOperationsCount(); i++) {
            if (!isTerminalOutcome(ledger.getOperations(i).getOutcome())
                    || ExternalLedgerStore.publicationMayHoldObject(ledger, i)) {
                return false;
            }
        }
        if (ledger.hasCurrentLink()) {
            return false;
        }
        // The newest source revision each object has deletion evidence for.
        Map<ObjectKey, Long> deletedUpTo = new HashMap<>();
        for (ExternalLink link : ledger.getHistoricalLinksList()) {
            if (link.getProjectionLifecycle()
                    == ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED) {
                deletedUpTo.merge(objectKey(link), link.getSourceRevision(), Math::max);
            }
        }
        for (ExternalLink link : ledger.getHistoricalLinksList()) {
            switch (link.getProjectionLifecycle()) {
                case EXTERNAL_PROJECTION_LIFECYCLE_DELETED -> {
                    // Settled.
                }
                case EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED -> {
                    // An earlier projection of an object is gone once a later projection of
                    // that same object has been deleted. Evidence from before it proves nothing.
                    Long covered = deletedUpTo.get(objectKey(link));
                    if (covered == null || covered < link.getSourceRevision()) {
                        return false;
                    }
                }
                default -> {
                    // Owed a deletion, or in a state that should not be in history at all.
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * One object in the external system: its ID at one place there. Two binding revisions that
     * point at the same provider, dataset, namespace and endpoint name the same object, as
     * {@link ExternalDeleteTargets#sameExternalTarget} has it.
     */
    private record ObjectKey(String provider, String dataset, String namespace,
            ExternalEndpointSummary endpoint, String externalObjectId) {
    }

    private static ObjectKey objectKey(ExternalLink link) {
        ExternalSystemBinding target = link.getBindingSnapshot();
        return new ObjectKey(target.getProvider(), target.getDataset(), target.getNamespace(),
                target.getEndpointSummary(), link.getExternalObjectId());
    }

    /**
     * Purges the ledger with its source when the source is purged, every operation on it was
     * recorded under {@code PURGE_WITH_SOURCE}, and nothing is outstanding.
     *
     * <p>The decision is made again inside a ledger update against the ledger as it stands then,
     * and that update marks {@code purge_with_source_started_at}. A ledger so marked takes no
     * further save, so an operation registered after the check either lands before the mark (and
     * the check sees it) or is refused. {@code PurgeDocument} runs only once the mark is saved; a
     * non-admin writer who settles the last external operation leaves the mark in place so a
     * purge-administrator replay completes the repository purge.
     *
     * @return true when the ledger document is purged
     */
    boolean maybePurgeLedgerIfEligible(
            NodeAddress ledgerAddr, ExternalPublicationLedger ledger) {
        if (!ledger.getSourcePurged()) {
            return false;
        }
        ExternalPublicationLedger marked = ledger;
        if (!ledger.hasPurgeWithSourceStartedAt()) {
            if (!eligibleForPurgeWithSource(ledger)) {
                return false;
            }
            Timestamp startedAt = timestamp(clock.instant());
            try {
                marked = ledgers.updateLedgerWithCas(
                        ledgerAddr,
                        ledger.getSourceAddress(),
                        ledger.getBindingId(),
                        LedgerAccess.keep(),
                        "ext-ledger-purge-mark:" + ledger.getLastPurgeId() + ":" + UUID.randomUUID(),
                        current -> current.hasPurgeWithSourceStartedAt() || !eligibleForPurgeWithSource(current)
                                ? current
                                : current.toBuilder().setPurgeWithSourceStartedAt(startedAt).build());
            } catch (StatusRuntimeException markRefused) {
                if (markRefused.getStatus().getCode() == Status.Code.PERMISSION_DENIED) {
                    return false;
                }
                throw markRefused;
            }
            if (!marked.getSourcePurged()) {
                return true;
            }
            if (!marked.hasPurgeWithSourceStartedAt()) {
                return false;
            }
        }
        String purgeKey = "ext-ledger-purge:" + marked.getBindingId() + ":" + marked.getLastPurgeId();
        if (purgeKey.length() > MAX_PURGE_ID_LEN) {
            purgeKey = "ext-ledger-purge:" + sha256Hex(purgeKey.getBytes(StandardCharsets.UTF_8));
        }
        try {
            documents.purgeDocument(PurgeDocumentRequest.newBuilder()
                    .setPurgeId(purgeKey)
                    .setAddress(ledgerAddr)
                    .setReason(PURGE_REASON)
                    .build());
            return true;
        } catch (StatusRuntimeException purgeFailure) {
            Status.Code code = purgeFailure.getStatus().getCode();
            if (code == Status.Code.NOT_FOUND) {
                return true;
            }
            if (code == Status.Code.PERMISSION_DENIED) {
                return false;
            }
            throw purgeFailure;
        }
    }

    /**
     * Whether the ledger may be purged with its source: the source is purged, nothing is
     * outstanding, and every operation on the ledger was recorded under a binding revision
     * with {@code PURGE_WITH_SOURCE}. One operation recorded under {@code RETAIN_HISTORY}
     * keeps the ledger, whichever deletion settles last.
     */
    static boolean eligibleForPurgeWithSource(ExternalPublicationLedger ledger) {
        if (!ledger.getSourcePurged() || ledger.getOperationsCount() == 0) {
            return false;
        }
        for (ExternalPublicationOperationRecord operation : ledger.getOperationsList()) {
            if (operation.getBindingSnapshot().getRetentionPolicy()
                    != ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE) {
                return false;
            }
        }
        return nothingOutstanding(ledger);
    }
}
