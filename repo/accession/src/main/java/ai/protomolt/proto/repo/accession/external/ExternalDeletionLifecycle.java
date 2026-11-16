package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isTerminalOutcome;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.lastAttemptStartedAt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.lastAttemptTrigger;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceAttempt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.timestamp;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Deletes the external projection of a tombstoned or purged source: resolves the delete
 * target, fences delayed publications, records the deletion attempt, calls the provider,
 * and hands the receipt to {@link ExternalReceiptSettlement}.
 */
final class ExternalDeletionLifecycle {

    private final ExternalLedgerStore ledgers;
    private final ExternalSourceAccess sources;
    private final ExternalReceiptSettlement settlement;
    private final ExternalBindingStore bindingStore;
    private final ExternalProviderGateway gateway;
    private final Clock clock;

    ExternalDeletionLifecycle(ExternalLedgerStore ledgers, ExternalSourceAccess sources,
            ExternalReceiptSettlement settlement, ExternalBindingStore bindingStore,
            ExternalProviderGateway gateway, Clock clock) {
        this.ledgers = ledgers;
        this.sources = sources;
        this.settlement = settlement;
        this.bindingStore = bindingStore;
        this.gateway = gateway;
        this.clock = clock;
    }

    ExternalPublicationCoordinator.OperationResult deleteForTombstone(ExternalPublicationCoordinator.DeleteRequest request) {
        Objects.requireNonNull(request, "request");
        ExternalPublicationCoordinates.requireSourceAddress(request.sourceAddress());
        if (request.tombstoneRevision() <= 0L) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("tombstoneRevision must be > 0")
                    .asRuntimeException();
        }
        String accountId = request.sourceAddress().getAccountId();
        DocumentSecurity retainedSecurity = sources.verifyTombstoneRevision(
                request.sourceAddress(), request.tombstoneRevision());

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());
        Optional<ExternalLedgerStore.LoadedLedger> ledgerOpt = ledgers.loadOptionalLedger(ledgerAddr, request.sourceAddress());

        ResolvedDeleteTarget target = resolveDeleteTarget(
                accountId,
                request.bindingId(),
                request.bindingRevision(),
                request.externalObjectId(),
                ledgerOpt.map(ExternalLedgerStore.LoadedLedger::ledger));

        ExternalPublicationIntent intent = ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1)
                .setAccountId(accountId)
                .setSourceAddress(request.sourceAddress())
                .setSourceRevision(request.tombstoneRevision())
                .setBindingId(request.bindingId())
                .setBindingRevision(target.binding().getBindingRevision())
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE)
                .setExternalObjectId(target.externalObjectId())
                .build();
        String intentSha256 = ExternalOperationKeys.intentSha256(intent);
        String operationId = (request.operationId() == null || request.operationId().isEmpty())
                ? ExternalOperationKeys.deriveOperationId(intent)
                : request.operationId();

        return executeDeleteLifecycle(
                ledgerAddr,
                request.sourceAddress(),
                request.bindingId(),
                retainedSecurity,
                operationId,
                intent,
                intentSha256,
                target.binding(),
                Optional.of(request.tombstoneRevision()),
                Optional.empty());
    }

    ExternalPublicationCoordinator.OperationResult handlePurge(ExternalPublicationCoordinator.PurgeRequest request) {
        Objects.requireNonNull(request, "request");
        ExternalPublicationCoordinates.requireSourceAddress(request.sourceAddress());
        ExternalPublicationCoordinates.requireNonBlankCoordinate(request.purgeId(), "purgeId");

        String accountId = request.sourceAddress().getAccountId();
        sources.verifyAccountCallerAccess(request.sourceAddress());

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());
        Optional<ExternalLedgerStore.LoadedLedger> ledgerOpt = ledgers.loadOptionalLedger(ledgerAddr, request.sourceAddress());
        DocumentSecurity retainedSecurity = ledgerOpt
                .map(ExternalLedgerStore.LoadedLedger::security)
                .orElse(DocumentSecurity.getDefaultInstance());

        ResolvedDeleteTarget target = resolveDeleteTarget(
                accountId,
                request.bindingId(),
                0L,
                request.externalObjectId(),
                ledgerOpt.map(ExternalLedgerStore.LoadedLedger::ledger));

        ExternalPublicationIntent intent = ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1)
                .setAccountId(accountId)
                .setSourceAddress(request.sourceAddress())
                .setSourceRevision(0L)
                .setPurgeId(request.purgeId())
                .setBindingId(request.bindingId())
                .setBindingRevision(target.binding().getBindingRevision())
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE)
                .setExternalObjectId(target.externalObjectId())
                .build();
        String intentSha256 = ExternalOperationKeys.intentSha256(intent);
        String operationId = (request.operationId() == null || request.operationId().isEmpty())
                ? ExternalOperationKeys.deriveOperationId(intent)
                : request.operationId();

        return executeDeleteLifecycle(
                ledgerAddr,
                request.sourceAddress(),
                request.bindingId(),
                retainedSecurity,
                operationId,
                intent,
                intentSha256,
                target.binding(),
                Optional.empty(),
                Optional.of(request.purgeId()));
    }

    record ResolvedDeleteTarget(ExternalSystemBinding binding, String externalObjectId) {
    }

    ExternalPublicationCoordinator.OperationResult executeDeleteLifecycle(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            DocumentSecurity retainedSecurity,
            String operationId,
            ExternalPublicationIntent intent,
            String intentSha256,
            ExternalSystemBinding bindingSnapshot,
            Optional<Long> tombstoneRevision,
            Optional<String> purgeId) {
        Optional<ExternalLedgerStore.LoadedLedger> existingOpt = ledgers.loadOptionalLedger(ledgerAddr, sourceAddress);
        if (existingOpt.isPresent()) {
            ExternalPublicationLedger existingLedger = existingOpt.get().ledger();
            Optional<ExternalPublicationOperationRecord> priorOpOpt =
                    findOperation(existingLedger, operationId);
            if (priorOpOpt.isPresent()) {
                ExternalPublicationOperationRecord priorOp = priorOpOpt.get();
                if (!priorOp.getIntentSha256().equals(intentSha256)) {
                    throw Status.ALREADY_EXISTS
                            .withDescription("Operation '" + operationId
                                    + "' is already bound to a different intent")
                            .asRuntimeException();
                }
                if (isTerminalOutcome(priorOp.getOutcome())) {
                    boolean purged = settlement.maybePurgeLedgerIfEligible(ledgerAddr, existingLedger, priorOp);
                    return new ExternalPublicationCoordinator.OperationResult(
                            priorOp,
                            purged ? Optional.empty() : optionalCurrentLink(existingLedger),
                            true,
                            purged);
                }
            }
        }

        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(bindingSnapshot);
        Timestamp startedAt = timestamp(clock.instant());

        final int[] attemptHolder = new int[]{1};
        ExternalPublicationLedger preDeleteLedger = ledgers.updateLedgerWithCas(
                ledgerAddr,
                sourceAddress,
                bindingId,
                retainedSecurity,
                "ext-del-pre:" + operationId + ":" + UUID.randomUUID(),
                current -> {
                    ExternalPublicationLedger.Builder next = current.toBuilder();
                    if (tombstoneRevision.isPresent()) {
                        long tr = tombstoneRevision.get();
                        long maxTombstone = current.hasTombstonedAtSourceRevision()
                                ? Math.max(current.getTombstonedAtSourceRevision(), tr)
                                : tr;
                        next.setTombstonedAtSourceRevision(maxTombstone);
                        next.setHighestFencedSourceRevision(
                                Math.max(current.getHighestFencedSourceRevision(), tr));
                    }
                    if (purgeId.isPresent()) {
                        next.setSourcePurged(true);
                        next.setLastPurgeId(purgeId.get());
                    }
                    if (current.hasCurrentLink()
                            && current.getCurrentLink().getProjectionLifecycle()
                                    == ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE) {
                        next.setCurrentLink(current.getCurrentLink().toBuilder()
                                .setProjectionLifecycle(
                                        ExternalProjectionLifecycle
                                                .EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING));
                    }

                    Optional<ExternalPublicationOperationRecord> existingOp =
                            findOperation(current, operationId);
                    if (existingOp.isPresent()) {
                        ExternalPublicationOperationRecord op = existingOp.get();
                        if (!op.getIntentSha256().equals(intentSha256)) {
                            throw Status.ALREADY_EXISTS
                                    .withDescription("Operation '" + operationId
                                            + "' bound to different intent")
                                    .asRuntimeException();
                        }
                        if (isTerminalOutcome(op.getOutcome())) {
                            return next.build();
                        }
                        int retryNum = op.getAttemptsCount() + 1;
                        attemptHolder[0] = retryNum;
                        ExternalPublicationAttempt retryAttempt = ExternalPublicationAttempt.newBuilder()
                                .setAttemptNumber(retryNum)
                                .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RETRY)
                                .setOutcome(
                                        ExternalPublicationOutcome
                                                .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                                .setPendingPhase(
                                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                                .setStartedAt(startedAt)
                                .build();
                        ExternalPublicationOperationRecord updatedOp = op.toBuilder()
                                .setOutcome(
                                        ExternalPublicationOutcome
                                                .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                                .setPendingPhase(
                                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                                .addAttempts(retryAttempt)
                                .setUpdatedAt(startedAt)
                                .build();
                        return replaceOperation(next.build(), updatedOp);
                    }

                    attemptHolder[0] = 1;
                    ExternalPublicationAttempt initialAttempt = ExternalPublicationAttempt.newBuilder()
                            .setAttemptNumber(1)
                            .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                            .setOutcome(
                                    ExternalPublicationOutcome
                                            .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                            .setPendingPhase(
                                    ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                            .setStartedAt(startedAt)
                            .build();
                    ExternalPublicationOperationRecord newOp = ExternalPublicationOperationRecord.newBuilder()
                            .setOperationId(operationId)
                            .setIntentSha256(intentSha256)
                            .setIntent(intent)
                            .setBindingSnapshot(bindingSnapshot)
                            .setOutcome(
                                    ExternalPublicationOutcome
                                            .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                            .setPendingPhase(
                                    ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                            .addAttempts(initialAttempt)
                            .setCreatedAt(startedAt)
                            .setUpdatedAt(startedAt)
                            .build();
                    return next.addOperations(newOp).build();
                });

        ExternalPublicationOperationRecord preOp =
                findOperation(preDeleteLedger, operationId).orElseThrow();
        if (isTerminalOutcome(preOp.getOutcome())) {
            boolean purged = settlement.maybePurgeLedgerIfEligible(ledgerAddr, preDeleteLedger, preOp);
            return new ExternalPublicationCoordinator.OperationResult(
                    preOp,
                    purged ? Optional.empty() : optionalCurrentLink(preDeleteLedger),
                    true,
                    purged);
        }

        int attemptNumber = attemptHolder[0];
        ExternalProviderGateway.ProviderCallResult callResult;
        try {
            callResult = gateway.execute(new ExternalProviderGateway.ProviderCallRequest(
                    operationId, intentSha256, intent, outboundAuth));
        } catch (StatusRuntimeException transportFailure) {
            callResult = new ExternalProviderGateway.ProviderCallResult.Unresolved(
                    "ENDPOINT_" + transportFailure.getStatus().getCode().name(),
                    sanitizeMessage(transportFailure.getMessage()));
        }

        if (callResult instanceof ExternalProviderGateway.ProviderCallResult.Unresolved unresolved) {
            Timestamp finishedAt = timestamp(clock.instant());
            ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                    ledgerAddr,
                    sourceAddress,
                    bindingId,
                    retainedSecurity,
                    "ext-del-unresolved:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                    current -> {
                        ExternalPublicationOperationRecord op =
                                findOperation(current, operationId).orElseThrow();
                        if (isTerminalOutcome(op.getOutcome())) {
                            return current;
                        }
                        ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                                .setAttemptNumber(attemptNumber)
                                .setTrigger(lastAttemptTrigger(op))
                                .setOutcome(
                                        ExternalPublicationOutcome
                                                .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                                .setPendingPhase(
                                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                                .setStartedAt(lastAttemptStartedAt(op, startedAt))
                                .setFinishedAt(finishedAt)
                                .setDiagnosticCode(unresolved.diagnosticCode())
                                .setDiagnosticMessage(sanitizeMessage(unresolved.diagnosticMessage()))
                                .build();
                        ExternalPublicationOperationRecord updatedOp = replaceAttempt(op, attempt)
                                .setOutcome(
                                        ExternalPublicationOutcome
                                                .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                                .setPendingPhase(
                                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                                .setUpdatedAt(finishedAt)
                                .build();
                        return replaceOperation(current, updatedOp);
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
                sourceAddress,
                bindingId,
                retainedSecurity,
                operationId,
                attemptNumber,
                null,
                startedAt,
                intent,
                intentSha256,
                bindingSnapshot,
                outboundAuth,
                evidence);
    }

    ResolvedDeleteTarget resolveDeleteTarget(
            String accountId,
            String bindingId,
            long explicitBindingRevision,
            String explicitExternalObjectId,
            Optional<ExternalPublicationLedger> ledgerOpt) {
        Optional<ResolvedDeleteTarget> recorded = recordedTarget(
                ledgerOpt, explicitBindingRevision, explicitExternalObjectId);
        if (recorded.isPresent()) {
            return recorded.get();
        }
        if (explicitExternalObjectId == null || explicitExternalObjectId.isEmpty()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("No externalObjectId supplied and no prior ExternalLink recorded")
                    .asRuntimeException();
        }
        // The ledger holds no link for this object (for example a first publication that was
        // only accepted), so the caller's coordinates are all there is to go on.
        ExternalSystemBinding binding = explicitBindingRevision > 0L
                ? bindingStore.getBindingRevision(accountId, bindingId, explicitBindingRevision)
                        .orElseThrow(() -> Status.NOT_FOUND
                                .withDescription("ExternalSystemBinding '" + bindingId
                                        + "' revision " + explicitBindingRevision + " not found")
                                .asRuntimeException())
                : bindingStore.getCurrentBinding(accountId, bindingId)
                        .orElseThrow(() -> Status.NOT_FOUND
                                .withDescription("ExternalSystemBinding '" + bindingId + "' not found")
                                .asRuntimeException());
        return new ResolvedDeleteTarget(binding, explicitExternalObjectId);
    }

    /**
     * The deletion target the ledger itself records: an object together with the binding
     * snapshot it was published under, taken from one link so the two can never be mixed. An
     * object ID alone means nothing outside the dataset and namespace of its binding revision.
     *
     * <p>With no object named, the target is the current link, else the newest historical link
     * that is not yet deleted, else the newest historical link. With an object named, it is
     * that object's link, restricted to {@code explicitBindingRevision} when one is given.
     *
     * @return the recorded target, or empty when the named object has no link in the ledger
     * @throws io.grpc.StatusRuntimeException FAILED_PRECONDITION when no object is named and
     *         {@code explicitBindingRevision} is not the revision the default link used
     */
    static Optional<ResolvedDeleteTarget> recordedTarget(
            Optional<ExternalPublicationLedger> ledgerOpt,
            long explicitBindingRevision,
            String explicitExternalObjectId) {
        if (ledgerOpt.isEmpty()) {
            return Optional.empty();
        }
        List<ExternalLink> candidates = linksInDeletionOrder(ledgerOpt.get());
        if (explicitExternalObjectId != null && !explicitExternalObjectId.isEmpty()) {
            return candidates.stream()
                    .filter(link -> link.getExternalObjectId().equals(explicitExternalObjectId))
                    .filter(link -> explicitBindingRevision <= 0L
                            || link.getBindingRevision() == explicitBindingRevision)
                    .findFirst()
                    .map(link -> new ResolvedDeleteTarget(
                            link.getBindingSnapshot(), link.getExternalObjectId()));
        }
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        ExternalLink chosen = candidates.getFirst();
        if (explicitBindingRevision > 0L && chosen.getBindingRevision() != explicitBindingRevision) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("No externalObjectId supplied, and the recorded object '"
                            + chosen.getExternalObjectId() + "' was published under binding revision "
                            + chosen.getBindingRevision() + ", not " + explicitBindingRevision)
                    .asRuntimeException();
        }
        return Optional.of(new ResolvedDeleteTarget(
                chosen.getBindingSnapshot(), chosen.getExternalObjectId()));
    }

    /** Current link first, then live historical links newest first, then deleted ones. */
    private static List<ExternalLink> linksInDeletionOrder(ExternalPublicationLedger ledger) {
        List<ExternalLink> ordered = new ArrayList<>(ledger.getHistoricalLinksCount() + 1);
        if (ledger.hasCurrentLink()) {
            ordered.add(ledger.getCurrentLink());
        }
        List<ExternalLink> deleted = new ArrayList<>();
        for (int i = ledger.getHistoricalLinksCount() - 1; i >= 0; i--) {
            ExternalLink historical = ledger.getHistoricalLinks(i);
            if (historical.getProjectionLifecycle()
                    == ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED) {
                deleted.add(historical);
            } else {
                ordered.add(historical);
            }
        }
        ordered.addAll(deleted);
        return ordered;
    }
}
