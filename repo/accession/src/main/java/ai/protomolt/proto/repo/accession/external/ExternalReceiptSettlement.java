package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isTerminalOutcome;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.lastAttemptStartedAt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.lastAttemptTrigger;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceAttempt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sha256Hex;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.timestamp;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.PurgeDocumentRequest;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Verifies a provider receipt and settles it onto the ledger: accepted-only, published,
 * rejected, deletion-accepted and deleted outcomes, link activation and retirement, and the
 * {@code PURGE_WITH_SOURCE} ledger purge once a purge deletion settles.
 */
final class ExternalReceiptSettlement {

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
            DocumentSecurity retainedSecurity,
            String operationId,
            int attemptNumber,
            ExternalAttemptTrigger explicitTrigger,
            Timestamp fallbackStartedAt,
            ExternalPublicationIntent intent,
            String intentSha256,
            ExternalSystemBinding bindingSnapshot,
            ExternalProviderGateway.OutboundAuthContext outboundAuth,
            ExternalProviderEvidence evidence) {
        Timestamp finishedAt = timestamp(clock.instant());
        try {
            verifier.verifyEvidence(
                    evidence, bindingSnapshot, outboundAuth, intent, intentSha256);
        } catch (ExternalProviderVerifier.ProviderVerificationException invalidEvidence) {
            ExternalPublicationLedger failedLedger = ledgers.updateLedgerWithCas(
                    ledgerAddr,
                    sourceAddress,
                    bindingId,
                    retainedSecurity,
                    "ext-verify-fail:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                    current -> {
                        ExternalPublicationOperationRecord op =
                                findOperation(current, operationId).orElseThrow();
                        if (isTerminalOutcome(op.getOutcome())) {
                            return current;
                        }
                        ExternalAttemptTrigger trigger = explicitTrigger != null
                                ? explicitTrigger
                                : lastAttemptTrigger(op);
                        Timestamp startedAt = explicitTrigger != null
                                ? fallbackStartedAt
                                : lastAttemptStartedAt(op, fallbackStartedAt);
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
                                .setFailureCode(invalidEvidence.diagnosticCode())
                                .setFailureMessage(sanitizeMessage(invalidEvidence.getMessage()))
                                .setUpdatedAt(finishedAt)
                                .build();
                        return replaceOperation(current, updatedOp);
                    });
            return new ExternalPublicationCoordinator.OperationResult(
                    findOperation(failedLedger, operationId).orElseThrow(),
                    optionalCurrentLink(failedLedger),
                    false,
                    false);
        }

        ExternalProviderSettlement settlement = evidence.getSettlement();
        ExternalPublicationLedger settledLedger = ledgers.updateLedgerWithCas(
                ledgerAddr,
                sourceAddress,
                bindingId,
                retainedSecurity,
                "ext-settle:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                current -> {
                    ExternalPublicationOperationRecord op =
                            findOperation(current, operationId).orElseThrow();
                    if (isTerminalOutcome(op.getOutcome())) {
                        return current;
                    }
                    ExternalAttemptTrigger trigger = explicitTrigger != null
                            ? explicitTrigger
                            : lastAttemptTrigger(op);
                    Timestamp startedAt = explicitTrigger != null
                            ? fallbackStartedAt
                            : lastAttemptStartedAt(op, fallbackStartedAt);

                    boolean settles = settlement
                                    == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED
                            || settlement
                                    == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_REJECTED
                            || settlement
                                    == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETED;
                    if (explicitTrigger != null && !settles
                            && !ExternalLedgerStore.hasRoomForUnsettledAttempt(op)) {
                        // A receipt that settles nothing is not written into the last attempt
                        // slot: that slot is for the receipt that ends the operation.
                        return current;
                    }

                    return switch (settlement) {
                        case EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY -> {
                            ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                                    .setAttemptNumber(attemptNumber)
                                    .setTrigger(trigger)
                                    .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                                    .setPendingPhase(
                                            ExternalPendingPhase
                                                    .EXTERNAL_PENDING_PHASE_ACCEPTED_AWAITING_PUBLICATION)
                                    .setStartedAt(startedAt)
                                    .setFinishedAt(finishedAt)
                                    .setEvidence(evidence)
                                    .build();
                            ExternalPublicationOperationRecord.Builder opBuilder =
                                    explicitTrigger != null
                                            ? op.toBuilder().addAttempts(attempt)
                                            : replaceAttempt(op, attempt);
                            ExternalPublicationOperationRecord updatedOp = opBuilder
                                    .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                                    .setPendingPhase(
                                            ExternalPendingPhase
                                                    .EXTERNAL_PENDING_PHASE_ACCEPTED_AWAITING_PUBLICATION)
                                    .setUpdatedAt(finishedAt)
                                    .build();
                            yield replaceOperation(current, updatedOp);
                        }
                        case EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED -> settleConfirmedPublication(
                                current,
                                op,
                                attemptNumber,
                                trigger,
                                startedAt,
                                finishedAt,
                                explicitTrigger != null,
                                intent,
                                bindingSnapshot,
                                evidence);
                        case EXTERNAL_PROVIDER_SETTLEMENT_REJECTED -> {
                            String code = evidence.hasDiagnosticCode()
                                    ? evidence.getDiagnosticCode()
                                    : "PROVIDER_REJECTED";
                            ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                                    .setAttemptNumber(attemptNumber)
                                    .setTrigger(trigger)
                                    .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                    .setStartedAt(startedAt)
                                    .setFinishedAt(finishedAt)
                                    .setDiagnosticCode(code)
                                    .setDiagnosticMessage("Provider rejected external operation")
                                    .setEvidence(evidence)
                                    .build();
                            ExternalPublicationOperationRecord.Builder opBuilder =
                                    explicitTrigger != null
                                            ? op.toBuilder().addAttempts(attempt)
                                            : replaceAttempt(op, attempt);
                            ExternalPublicationOperationRecord updatedOp = opBuilder
                                    .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                    .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                                    .setFailureCode(code)
                                    .setFailureMessage("Provider rejected external operation")
                                    .setUpdatedAt(finishedAt)
                                    .build();
                            yield replaceOperation(current, updatedOp);
                        }
                        case EXTERNAL_PROVIDER_SETTLEMENT_DELETION_ACCEPTED -> {
                            ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                                    .setAttemptNumber(attemptNumber)
                                    .setTrigger(trigger)
                                    .setOutcome(
                                            ExternalPublicationOutcome
                                                    .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                                    .setPendingPhase(
                                            ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                                    .setStartedAt(startedAt)
                                    .setFinishedAt(finishedAt)
                                    .setEvidence(evidence)
                                    .build();
                            ExternalPublicationOperationRecord.Builder opBuilder =
                                    explicitTrigger != null
                                            ? op.toBuilder().addAttempts(attempt)
                                            : replaceAttempt(op, attempt);
                            ExternalPublicationOperationRecord updatedOp = opBuilder
                                    .setOutcome(
                                            ExternalPublicationOutcome
                                                    .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                                    .setPendingPhase(
                                            ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                                    .setUpdatedAt(finishedAt)
                                    .build();
                            yield replaceOperation(current, updatedOp);
                        }
                        case EXTERNAL_PROVIDER_SETTLEMENT_DELETED -> settleConfirmedDeletion(
                                current,
                                op,
                                attemptNumber,
                                trigger,
                                startedAt,
                                finishedAt,
                                explicitTrigger != null,
                                evidence);
                        case EXTERNAL_PROVIDER_SETTLEMENT_UNSPECIFIED, UNRECOGNIZED ->
                                throw new IllegalStateException("Unexpected settlement: " + settlement);
                    };
                });

        ExternalPublicationOperationRecord finalOp =
                findOperation(settledLedger, operationId).orElseThrow();
        boolean purged = maybePurgeLedgerIfEligible(ledgerAddr, settledLedger, finalOp);
        return new ExternalPublicationCoordinator.OperationResult(
                finalOp,
                purged ? Optional.empty() : optionalCurrentLink(settledLedger),
                false,
                purged);
    }

    static ExternalPublicationLedger settleConfirmedPublication(
            ExternalPublicationLedger current,
            ExternalPublicationOperationRecord op,
            int attemptNumber,
            ExternalAttemptTrigger trigger,
            Timestamp startedAt,
            Timestamp finishedAt,
            boolean appendNewAttempt,
            ExternalPublicationIntent intent,
            ExternalSystemBinding bindingSnapshot,
            ExternalProviderEvidence evidence) {
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
        boolean superseded = current.getHighestFencedSourceRevision() > intent.getSourceRevision()
                || (current.hasCurrentLink()
                        && current.getCurrentLink().getSourceRevision() > intent.getSourceRevision());

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
            String retiringOpId = current.hasCurrentLink()
                    ? current.getCurrentLink().getConfirmedByOperationId()
                    : op.getOperationId();
            ledgerBuilder.addHistoricalLinks(linkBuilder
                    .setProjectionLifecycle(
                            ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED)
                    .setRetiredByOperationId(retiringOpId)
                    .build());
            return ledgerBuilder.build();
        }

        if (current.hasCurrentLink()) {
            ExternalLink displaced = current.getCurrentLink().toBuilder()
                    .setProjectionLifecycle(
                            ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED)
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
     * Marks as deleted the one projection the evidence is for: the link published under the
     * deleted object's ID and the binding revision the deletion ran against. A deletion of
     * anything else leaves every link as it was, because that evidence says nothing about them.
     */
    private static ExternalPublicationLedger retireDeletedProjection(
            ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord deletedOp,
            ExternalProviderEvidence evidence) {
        ExternalPublicationLedger.Builder next = ledger.toBuilder();
        if (ledger.hasCurrentLink() && isDeletionTarget(ledger.getCurrentLink(), deletedOp)) {
            next.clearCurrentLink();
            next.addHistoricalLinks(retired(ledger.getCurrentLink(), deletedOp, evidence));
            return next.build();
        }
        for (int i = ledger.getHistoricalLinksCount() - 1; i >= 0; i--) {
            ExternalLink historical = ledger.getHistoricalLinks(i);
            if (historical.getProjectionLifecycle()
                            != ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED
                    && isDeletionTarget(historical, deletedOp)) {
                next.setHistoricalLinks(i, retired(historical, deletedOp, evidence));
                break;
            }
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
        return publishedBeforeDeletion
                && link.getExternalObjectId().equals(deletion.getExternalObjectId())
                && link.getBindingRevision() == deletion.getBindingRevision();
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
        for (ExternalPublicationOperationRecord operation : ledger.getOperationsList()) {
            if (!isTerminalOutcome(operation.getOutcome())) {
                return false;
            }
        }
        if (ledger.hasCurrentLink()) {
            return false;
        }
        // The newest source revision each object has deletion evidence for.
        Map<String, Long> deletedUpTo = new HashMap<>();
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

    /** One external object: an ID within the dataset and namespace of a binding revision. */
    private static String objectKey(ExternalLink link) {
        return link.getBindingRevision() + ":" + link.getExternalObjectId();
    }

    boolean maybePurgeLedgerIfEligible(
            NodeAddress ledgerAddr,
            ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord settledOp) {
        if (!ledger.getSourcePurged()
                || settledOp.getOutcome() != ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED
                || settledOp.getBindingSnapshot().getRetentionPolicy()
                        != ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE
                || !nothingOutstanding(ledger)) {
            return false;
        }
        String purgeKey = "ext-ledger-purge:" + ledger.getBindingId() + ":" + ledger.getLastPurgeId();
        if (purgeKey.length() > 200) {
            purgeKey = "ext-ledger-purge:" + sha256Hex(
                    purgeKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        documents.purgeDocument(PurgeDocumentRequest.newBuilder()
                .setPurgeId(purgeKey)
                .setAddress(ledgerAddr)
                .setReason("PURGE_WITH_SOURCE retention policy after external deletion settled")
                .build());
        return true;
    }
}
