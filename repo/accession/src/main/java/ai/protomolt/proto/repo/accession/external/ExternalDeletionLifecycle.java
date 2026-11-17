package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isTerminalOutcome;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.lastAttemptStartedAt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.lastAttemptTrigger;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.refusedByProvider;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

        ResolvedDeleteTarget target = replayedTarget(
                        ledgerOpt.map(ExternalLedgerStore.LoadedLedger::ledger),
                        request.operationId(), request.externalObjectId(), request.bindingRevision())
                .orElseGet(() -> resolveDeleteTarget(
                        accountId,
                        request.bindingId(),
                        request.bindingRevision(),
                        request.externalObjectId(),
                        request.tombstoneRevision(),
                        ledgerOpt.map(ExternalLedgerStore.LoadedLedger::ledger)));

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
        sources.requireSourcePurged(request.sourceAddress());

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());
        Optional<ExternalLedgerStore.LoadedLedger> ledgerOpt = ledgers.loadOptionalLedger(ledgerAddr, request.sourceAddress());
        if (ledgerOpt.isEmpty()) {
            // The source is gone and nothing was ever recorded for it under this binding:
            // there are no access rules to check the caller against and no object the
            // coordinator knows it published. Deleting whatever ID the caller names would be
            // an external deletion on their word alone.
            throw Status.FAILED_PRECONDITION
                    .withDescription("No external publication is recorded for this source under "
                            + "binding '" + request.bindingId() + "'; there is nothing to purge")
                    .asRuntimeException();
        }
        DocumentSecurity retainedSecurity = ledgerOpt.get().security();

        ResolvedDeleteTarget target = replayedTarget(
                        ledgerOpt.map(ExternalLedgerStore.LoadedLedger::ledger),
                        request.operationId(), request.externalObjectId(), 0L)
                .orElseGet(() -> resolveDeleteTarget(
                        accountId,
                        request.bindingId(),
                        0L,
                        request.externalObjectId(),
                        ALL_REVISIONS,
                        ledgerOpt.map(ExternalLedgerStore.LoadedLedger::ledger)));

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
                    boolean settledAlready = findOperation(current, operationId)
                            .map(known -> isTerminalOutcome(known.getOutcome()))
                            .orElse(false);
                    if (!settledAlready && tombstoneRevision.isPresent()) {
                        // Checked again here, against the ledger as it is when the deletion
                        // is about to be sent: the target may have been republished since it
                        // was resolved, or since an earlier attempt of this same operation.
                        requireNotRepublished(
                                current,
                                new ResolvedDeleteTarget(bindingSnapshot, intent.getExternalObjectId()),
                                tombstoneRevision.get());
                    }
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
                    // A tombstone covers what was published up to it. A publication made
                    // after the source was restored is not owed this deletion.
                    boolean coversCurrent = current.hasCurrentLink()
                            && (tombstoneRevision.isEmpty()
                                    || current.getCurrentLink().getSourceRevision()
                                            <= tombstoneRevision.get());
                    if (coversCurrent
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
                        ExternalLedgerStore.requireRoomForRetry(op);
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

                    ExternalLedgerStore.requireRoomForNewOperation(current);
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
        } catch (StatusRuntimeException callFailure) {
            callResult = ExternalProviderGateway.ProviderCallResult.fromCallFailure(callFailure);
        }

        if (callResult instanceof ExternalProviderGateway.ProviderCallResult.Refused refused) {
            // The provider turned the deletion down, so the object is still there: the
            // operation fails and the link stays owed a deletion.
            Timestamp finishedAt = timestamp(clock.instant());
            ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                    ledgerAddr,
                    sourceAddress,
                    bindingId,
                    retainedSecurity,
                    "ext-del-refused:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                    current -> replaceOperation(current, refusedByProvider(
                            findOperation(current, operationId).orElseThrow(),
                            attemptNumber, startedAt, finishedAt,
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

    /** For a purge: every source revision is covered. */
    static final long ALL_REVISIONS = 0L;

    /**
     * When a request carries an operation ID the ledger already knows and names no target of
     * its own, the target is the one that operation recorded. Resolving it afresh could pick a
     * different object once the first one is gone, and turn a replay into a conflict.
     */
    static Optional<ResolvedDeleteTarget> replayedTarget(
            Optional<ExternalPublicationLedger> ledgerOpt,
            String operationId,
            String explicitExternalObjectId,
            long explicitBindingRevision) {
        boolean namesObject = explicitExternalObjectId != null && !explicitExternalObjectId.isEmpty();
        if (ledgerOpt.isEmpty() || operationId == null || operationId.isEmpty() || namesObject) {
            return Optional.empty();
        }
        Optional<ExternalPublicationOperationRecord> recorded =
                findOperation(ledgerOpt.get(), operationId)
                        .filter(operation -> operation.getIntent().getAction()
                                == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE);
        if (recorded.isPresent() && explicitBindingRevision > 0L
                && recorded.get().getIntent().getBindingRevision() != explicitBindingRevision) {
            throw Status.ALREADY_EXISTS
                    .withDescription("Operation '" + operationId + "' deleted under binding revision "
                            + recorded.get().getIntent().getBindingRevision() + ", not "
                            + explicitBindingRevision)
                    .asRuntimeException();
        }
        return recorded.map(operation -> new ResolvedDeleteTarget(
                operation.getBindingSnapshot(),
                operation.getIntent().getExternalObjectId()));
    }

    ResolvedDeleteTarget resolveDeleteTarget(
            String accountId,
            String bindingId,
            long explicitBindingRevision,
            String explicitExternalObjectId,
            long coveredUpToSourceRevision,
            Optional<ExternalPublicationLedger> ledgerOpt) {
        Optional<ResolvedDeleteTarget> recorded = recordedTarget(
                ledgerOpt, explicitBindingRevision, explicitExternalObjectId,
                coveredUpToSourceRevision);
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
     * that is not yet deleted, else the newest historical link, else (with no link at all) the
     * newest publication still pending. With an object named, it is
     * that object's link, restricted to {@code explicitBindingRevision} when one is given.
     *
     * <p>Only projections published up to {@code coveredUpToSourceRevision} are considered, so
     * a tombstone handled late never reaches a publication made after the source was restored.
     *
     * @return the recorded target, or empty when the named object has no link in the ledger
     * @throws io.grpc.StatusRuntimeException FAILED_PRECONDITION when no object is named and
     *         {@code explicitBindingRevision} is not the revision the default link used
     */
    static Optional<ResolvedDeleteTarget> recordedTarget(
            Optional<ExternalPublicationLedger> ledgerOpt,
            long explicitBindingRevision,
            String explicitExternalObjectId,
            long coveredUpToSourceRevision) {
        if (ledgerOpt.isEmpty()) {
            return Optional.empty();
        }
        List<ExternalLink> candidates =
                linksInDeletionOrder(ledgerOpt.get(), coveredUpToSourceRevision);
        if (explicitExternalObjectId != null && !explicitExternalObjectId.isEmpty()) {
            Optional<ResolvedDeleteTarget> covered = recordedSnapshotFor(
                    ledgerOpt.get(), candidates, explicitBindingRevision, explicitExternalObjectId,
                    coveredUpToSourceRevision);
            if (covered.isEmpty() && coveredUpToSourceRevision != ALL_REVISIONS
                    && recordedSnapshotFor(ledgerOpt.get(),
                            linksInDeletionOrder(ledgerOpt.get(), ALL_REVISIONS),
                            explicitBindingRevision, explicitExternalObjectId, ALL_REVISIONS)
                    .isPresent()) {
                // The ledger knows this object, and it was published after the tombstone:
                // that is not an unrecorded object to delete on the caller's word.
                throw Status.FAILED_PRECONDITION
                        .withDescription("External object '" + explicitExternalObjectId
                                + "' was published after source revision "
                                + coveredUpToSourceRevision + "; this tombstone does not cover it")
                        .asRuntimeException();
            }
            covered.ifPresent(target -> requireNotRepublished(
                    ledgerOpt.get(), target, coveredUpToSourceRevision));
            return covered;
        }
        boolean onlyDeletedHistory = candidates.stream().allMatch(link ->
                link.getProjectionLifecycle()
                        == ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);
        if (onlyDeletedHistory) {
            // Nothing recorded is still live. A publication still pending may be, and it is
            // reached before going back over what has already been deleted.
            Optional<ResolvedDeleteTarget> pending = unsettledPublication(
                    ledgerOpt.get(), explicitBindingRevision, coveredUpToSourceRevision);
            if (pending.isPresent() || candidates.isEmpty()) {
                return pending;
            }
        }
        ExternalLink chosen = candidates.getFirst();
        if (explicitBindingRevision > 0L && chosen.getBindingRevision() != explicitBindingRevision) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("No externalObjectId supplied, and the recorded object '"
                            + chosen.getExternalObjectId() + "' was published under binding revision "
                            + chosen.getBindingRevision() + ", not " + explicitBindingRevision)
                    .asRuntimeException();
        }
        ResolvedDeleteTarget target = new ResolvedDeleteTarget(
                chosen.getBindingSnapshot(), chosen.getExternalObjectId());
        requireNotRepublished(ledgerOpt.get(), target, coveredUpToSourceRevision);
        return Optional.of(target);
    }

    /**
     * Refuses a tombstone deletion whose object has been published again, under the same ID
     * in the same external target, for a source revision after the tombstone. The provider knows the
     * object by ID alone, so deleting it now would delete the republished one.
     */
    /**
     * Whether two binding revisions address the same place in the external system. A binding
     * can be revised without moving (new labels, rotated credentials), so the revision number
     * does not say. An object ID means the same object exactly when provider, dataset,
     * namespace and endpoint are the same.
     */
    static boolean sameExternalTarget(ExternalSystemBinding left, ExternalSystemBinding right) {
        return left.getProvider().equals(right.getProvider())
                && left.getDataset().equals(right.getDataset())
                && left.getNamespace().equals(right.getNamespace())
                && left.getEndpointSummary().equals(right.getEndpointSummary());
    }

    private static void requireNotRepublished(
            ExternalPublicationLedger ledger, ResolvedDeleteTarget target,
            long coveredUpToSourceRevision) {
        if (coveredUpToSourceRevision == ALL_REVISIONS) {
            return;
        }
        List<ExternalLink> all = new ArrayList<>(ledger.getHistoricalLinksList());
        if (ledger.hasCurrentLink()) {
            all.add(ledger.getCurrentLink());
        }
        long republishedAt = 0L;
        for (ExternalLink link : all) {
            if (link.getExternalObjectId().equals(target.externalObjectId())
                    && sameExternalTarget(link.getBindingSnapshot(), target.binding())
                    && link.getSourceRevision() > coveredUpToSourceRevision) {
                republishedAt = Math.max(republishedAt, link.getSourceRevision());
            }
        }
        for (ExternalPublicationOperationRecord operation : ledger.getOperationsList()) {
            ExternalPublicationIntent intent = operation.getIntent();
            if (intent.getAction() == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH
                    && operation.getOutcome()
                            == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING
                    && intent.getExternalObjectId().equals(target.externalObjectId())
                    && sameExternalTarget(operation.getBindingSnapshot(), target.binding())
                    && intent.getSourceRevision() > coveredUpToSourceRevision) {
                republishedAt = Math.max(republishedAt, intent.getSourceRevision());
            }
        }
        if (republishedAt > 0L) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("External object '" + target.externalObjectId()
                            + "' was republished for source revision " + republishedAt
                            + ", after tombstone revision " + coveredUpToSourceRevision
                            + "; deleting it would delete that publication")
                    .asRuntimeException();
        }
    }

    /**
     * The binding snapshot a named object was recorded under: from its link, or from the
     * publication still pending for it (sent, with no confirmed link yet). The same ID under
     * two binding revisions is two objects in two datasets, so without a revision to choose
     * by the request is refused instead of guessed.
     */
    private static Optional<ResolvedDeleteTarget> recordedSnapshotFor(
            ExternalPublicationLedger ledger,
            List<ExternalLink> linksInOrder,
            long explicitBindingRevision,
            String externalObjectId,
            long coveredUpToSourceRevision) {
        Map<Long, ExternalSystemBinding> byRevision = new LinkedHashMap<>();
        for (ExternalLink link : linksInOrder) {
            if (link.getExternalObjectId().equals(externalObjectId)) {
                byRevision.putIfAbsent(link.getBindingRevision(), link.getBindingSnapshot());
            }
        }
        for (int i = ledger.getOperationsCount() - 1; i >= 0; i--) {
            ExternalPublicationOperationRecord operation = ledger.getOperations(i);
            if (operation.getIntent().getAction()
                            == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH
                    && operation.getOutcome()
                            == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING
                    && covered(operation.getIntent().getSourceRevision(), coveredUpToSourceRevision)
                    && operation.getIntent().getExternalObjectId().equals(externalObjectId)) {
                byRevision.putIfAbsent(
                        operation.getIntent().getBindingRevision(), operation.getBindingSnapshot());
            }
        }
        if (explicitBindingRevision > 0L) {
            return Optional.ofNullable(byRevision.get(explicitBindingRevision))
                    .map(binding -> new ResolvedDeleteTarget(binding, externalObjectId));
        }
        if (byRevision.size() > 1) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("External object '" + externalObjectId
                            + "' is recorded under binding revisions " + byRevision.keySet()
                            + "; name the binding revision to delete it from")
                    .asRuntimeException();
        }
        return byRevision.values().stream().findFirst()
                .map(binding -> new ResolvedDeleteTarget(binding, externalObjectId));
    }

    /**
     * With no link recorded, the newest publication still pending: accepted but not yet
     * confirmed, or sent with the reply lost. The provider may hold that object, so it is what
     * a deletion that names no object must reach. A publication that failed left nothing.
     */
    private static Optional<ResolvedDeleteTarget> unsettledPublication(
            ExternalPublicationLedger ledger, long explicitBindingRevision,
            long coveredUpToSourceRevision) {
        for (int i = ledger.getOperationsCount() - 1; i >= 0; i--) {
            ExternalPublicationOperationRecord operation = ledger.getOperations(i);
            if (operation.getIntent().getAction()
                            != ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH
                    || operation.getOutcome()
                            != ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING) {
                continue;
            }
            if (explicitBindingRevision > 0L
                    && operation.getIntent().getBindingRevision() != explicitBindingRevision) {
                continue;
            }
            if (!covered(operation.getIntent().getSourceRevision(), coveredUpToSourceRevision)) {
                continue;
            }
            return Optional.of(new ResolvedDeleteTarget(
                    operation.getBindingSnapshot(), operation.getIntent().getExternalObjectId()));
        }
        return Optional.empty();
    }

    /** Current link first, then live historical links newest first, then deleted ones. */
    private static List<ExternalLink> linksInDeletionOrder(
            ExternalPublicationLedger ledger, long coveredUpToSourceRevision) {
        List<ExternalLink> ordered = new ArrayList<>(ledger.getHistoricalLinksCount() + 1);
        if (ledger.hasCurrentLink()
                && covered(ledger.getCurrentLink().getSourceRevision(), coveredUpToSourceRevision)) {
            ordered.add(ledger.getCurrentLink());
        }
        List<ExternalLink> deleted = new ArrayList<>();
        for (int i = ledger.getHistoricalLinksCount() - 1; i >= 0; i--) {
            ExternalLink historical = ledger.getHistoricalLinks(i);
            if (!covered(historical.getSourceRevision(), coveredUpToSourceRevision)) {
                continue;
            }
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

    /**
     * Whether a deletion bounded at {@code coveredUpToSourceRevision} reaches something
     * published for {@code sourceRevision}. A tombstone at revision N covers revisions up to
     * N; {@link #ALL_REVISIONS} (a purge) covers everything.
     */
    private static boolean covered(long sourceRevision, long coveredUpToSourceRevision) {
        return coveredUpToSourceRevision == ALL_REVISIONS
                || sourceRevision <= coveredUpToSourceRevision;
    }
}
