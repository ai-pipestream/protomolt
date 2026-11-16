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
        ExternalSystemBinding binding;
        if (explicitBindingRevision > 0L) {
            binding = bindingStore.getBindingRevision(accountId, bindingId, explicitBindingRevision)
                    .orElseThrow(() -> Status.NOT_FOUND
                            .withDescription("ExternalSystemBinding '" + bindingId
                                    + "' revision " + explicitBindingRevision + " not found")
                            .asRuntimeException());
        } else if (ledgerOpt.isPresent() && ledgerOpt.get().hasCurrentLink()) {
            binding = ledgerOpt.get().getCurrentLink().getBindingSnapshot();
        } else {
            binding = bindingStore.getCurrentBinding(accountId, bindingId)
                    .orElseThrow(() -> Status.NOT_FOUND
                            .withDescription("ExternalSystemBinding '" + bindingId + "' not found")
                            .asRuntimeException());
        }

        String objectId = explicitExternalObjectId;
        if (objectId == null || objectId.isEmpty()) {
            if (ledgerOpt.isPresent() && ledgerOpt.get().hasCurrentLink()) {
                objectId = ledgerOpt.get().getCurrentLink().getExternalObjectId();
            } else if (ledgerOpt.isPresent() && ledgerOpt.get().getHistoricalLinksCount() > 0) {
                objectId = ledgerOpt.get()
                        .getHistoricalLinks(ledgerOpt.get().getHistoricalLinksCount() - 1)
                        .getExternalObjectId();
            } else {
                throw Status.FAILED_PRECONDITION
                        .withDescription("No externalObjectId supplied and no prior ExternalLink recorded")
                        .asRuntimeException();
            }
        }
        return new ResolvedDeleteTarget(binding, objectId);
    }
}
