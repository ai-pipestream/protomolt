package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalDeleteTargets.ALL_REVISIONS;
import static ai.protomolt.proto.repo.accession.external.ExternalDeleteTargets.covered;
import static ai.protomolt.proto.repo.accession.external.ExternalDeleteTargets.replayedTarget;
import static ai.protomolt.proto.repo.accession.external.ExternalDeleteTargets.requireNotRepublished;
import static ai.protomolt.proto.repo.accession.external.ExternalDeleteTargets.resolveDeleteTarget;
import static ai.protomolt.proto.repo.accession.external.ExternalDeleteTargets.sameExternalTarget;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.attemptStartedAt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.attemptTrigger;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isTerminalOutcome;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.withRefusedAttempt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceAttempt;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.replaceOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.timestamp;

import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalEndpointSummary;
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
import ai.protomolt.proto.repo.external.v1.ExternalPurgePreparation;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
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
        LedgerAccess retainedSecurity = sources.verifyTombstoneRevision(
                request.sourceAddress(), request.tombstoneRevision());

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());
        Optional<ExternalLedgerStore.LoadedLedger> ledgerOpt = ledgers.loadOptionalLedger(ledgerAddr, request.sourceAddress());

        ExternalDeleteTargets.ResolvedDeleteTarget target = replayedTarget(
                        ledgerOpt.map(ExternalLedgerStore.LoadedLedger::ledger),
                        request.operationId(), request.externalObjectId(), request.bindingRevision())
                .orElseGet(() -> resolveDeleteTarget(
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
        String intentSha256 = intentDigest(intent);
        String operationId = (request.operationId() == null || request.operationId().isEmpty())
                ? ExternalOperationKeys.deriveOperationId(intent)
                : ExternalPublicationCoordinates.requireCallerKey(request.operationId(), "operationId");

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

    /**
     * First step of a purge, made while the source still exists: copies the source's current
     * access rules onto the ledger and records the purge ID. Without it, a ledger kept after the
     * purge would answer to the rules of the last publication, including grants the source has
     * since revoked. Preparing again for the same purge ID copies the rules again.
     */
    ExternalPurgePreparation preparePurge(ExternalPublicationCoordinator.PreparePurgeRequest request) {
        Objects.requireNonNull(request, "request");
        ExternalPublicationCoordinates.requireSourceAddress(request.sourceAddress());
        ExternalPublicationCoordinates.requireBindingId(request.bindingId());
        ExternalPublicationCoordinates.requireCallerKey(request.purgeId(), "purgeId");

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                request.sourceAddress().getAccountId(), request.bindingId(), request.sourceAddress());
        ExternalSourceAccess.SourceAccessRules rules =
                sources.accessRulesBeforePurge(request.sourceAddress());
        ExternalPublicationLedger existing = ledgers.loadOptionalLedger(ledgerAddr, request.sourceAddress())
                .orElseThrow(() -> Status.FAILED_PRECONDITION
                        .withDescription("No external publication is recorded for this source under "
                                + "binding '" + request.bindingId() + "'; there is nothing to prepare")
                        .asRuntimeException())
                .ledger();
        requireNotPurgedUnderAnotherId(existing, request.purgeId());
        requireNoOtherPreparation(existing, request.purgeId());

        ExternalPurgePreparation preparation = ExternalPurgePreparation.newBuilder()
                .setPurgeId(request.purgeId())
                .setRulesFromSourceRevision(rules.sourceRevision())
                .setPreparedAt(timestamp(clock.instant()))
                .build();
        ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                ledgerAddr,
                request.sourceAddress(),
                request.bindingId(),
                LedgerAccess.purgePreparation(rules.security()),
                "ext-purge-prep:" + request.purgeId() + ":" + UUID.randomUUID(),
                current -> {
                    requireNotPurgedUnderAnotherId(current, request.purgeId());
                    requireNoOtherPreparation(current, request.purgeId());
                    return current.toBuilder().setPurgePreparation(preparation).build();
                });
        return updated.getPurgePreparation();
    }

    /** The intent's digest; an intent that fails validation is the caller's error. */
    private static String intentDigest(ExternalPublicationIntent intent) {
        try {
            return ExternalOperationKeys.intentSha256(intent);
        } catch (IllegalArgumentException badIntent) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(badIntent.getMessage())
                    .withCause(badIntent)
                    .asRuntimeException();
        }
    }

    /**
     * A ledger holds one purge preparation. Preparing again under the same purge ID refreshes
     * it; preparing under another ID while one is recorded is refused, since the repository
     * purge already announced under the first ID would then find nothing prepared for it.
     */
    private static void requireNoOtherPreparation(ExternalPublicationLedger ledger, String purgeId) {
        if (ledger.hasPurgePreparation() && !ledger.getPurgePreparation().getPurgeId().equals(purgeId)) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Purge '" + ledger.getPurgePreparation().getPurgeId()
                            + "' is already prepared on this ledger; handle that purge, or prepare "
                            + "again under the same purge ID")
                    .asRuntimeException();
        }
    }

    private static void requireNotPurgedUnderAnotherId(ExternalPublicationLedger ledger, String purgeId) {
        if (ledger.getSourcePurged() && !ledger.getLastPurgeId().equals(purgeId)) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("The source was already purged under purge ID '"
                            + ledger.getLastPurgeId() + "'")
                    .asRuntimeException();
        }
    }

    private static void requirePrepared(ExternalPublicationLedger ledger, String purgeId) {
        if (!ledger.hasPurgePreparation() || !ledger.getPurgePreparation().getPurgeId().equals(purgeId)) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Purge '" + purgeId + "' was not prepared on this ledger; "
                            + "call preparePurge while the source still exists, before PurgeDocument")
                    .asRuntimeException();
        }
    }

    ExternalPublicationCoordinator.OperationResult handlePurge(
            ExternalPublicationCoordinator.PurgeRequest request) {
        Objects.requireNonNull(request, "request");
        ExternalPublicationCoordinates.requireSourceAddress(request.sourceAddress());
        ExternalPublicationCoordinates.requireCallerKey(request.purgeId(), "purgeId");

        String accountId = request.sourceAddress().getAccountId();
        sources.requireSourcePurged(request.sourceAddress());

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());
        Optional<ExternalLedgerStore.LoadedLedger> ledgerOpt =
                ledgers.loadOptionalLedger(ledgerAddr, request.sourceAddress());
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
        ExternalPublicationLedger existingLedger = ledgerOpt.get().ledger();
        requirePrepared(existingLedger, request.purgeId());
        Optional<ExternalPublicationCoordinator.OperationResult> readyForPurge =
                finishPurgeIfSettled(ledgerAddr, existingLedger, request);
        if (readyForPurge.isPresent()) {
            return readyForPurge.get();
        }
        LedgerAccess retainedSecurity = LedgerAccess.keep();

        ExternalDeleteTargets.ResolvedDeleteTarget target = replayedTarget(
                        Optional.of(existingLedger),
                        request.operationId(), request.externalObjectId(), 0L)
                .orElseGet(() -> resolveDeleteTarget(
                        request.bindingId(),
                        0L,
                        request.externalObjectId(),
                        ALL_REVISIONS,
                        Optional.of(existingLedger)));

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
        String intentSha256 = intentDigest(intent);
        String operationId = (request.operationId() == null || request.operationId().isEmpty())
                ? ExternalOperationKeys.deriveOperationId(intent)
                : ExternalPublicationCoordinates.requireCallerKey(request.operationId(), "operationId");

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

    private Optional<ExternalPublicationCoordinator.OperationResult> finishPurgeIfSettled(
            NodeAddress ledgerAddr,
            ExternalPublicationLedger existingLedger,
            ExternalPublicationCoordinator.PurgeRequest request) {
        if (!existingLedger.hasPurgeWithSourceStartedAt()
                && !(existingLedger.getSourcePurged()
                        && ExternalReceiptSettlement.nothingOutstanding(existingLedger))) {
            return Optional.empty();
        }
        String requestedOpId = request.operationId();
        String requestedObjectId = request.externalObjectId();
        boolean hasOpId = requestedOpId != null && !requestedOpId.isEmpty();
        boolean hasObjectId = requestedObjectId != null && !requestedObjectId.isEmpty();
        Optional<ExternalPublicationOperationRecord> matched;
        if (hasOpId) {
            matched = findOperation(existingLedger, requestedOpId)
                    .filter(op -> op.getIntent().getAction()
                                    == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE
                            && request.purgeId().equals(op.getIntent().getPurgeId()));
            if (matched.isPresent() && hasObjectId
                    && !matched.get().getIntent().getExternalObjectId().equals(requestedObjectId)) {
                throw Status.ALREADY_EXISTS
                        .withDescription("Operation '" + requestedOpId
                                + "' deleted another external object")
                        .asRuntimeException();
            }
        } else {
            matched = latestPurgeDeletion(
                    existingLedger, request.purgeId(), hasObjectId ? requestedObjectId : null);
        }
        return matched.filter(op -> isTerminalOutcome(op.getOutcome())).map(op -> {
            boolean purged = settlement.maybePurgeLedgerIfEligible(ledgerAddr, existingLedger);
            return new ExternalPublicationCoordinator.OperationResult(
                    op,
                    purged ? Optional.empty() : optionalCurrentLink(existingLedger),
                    true,
                    purged);
        });
    }

    private static Optional<ExternalPublicationOperationRecord> latestPurgeDeletion(
            ExternalPublicationLedger ledger, String purgeId, String objectIdOrNull) {
        for (int i = ledger.getOperationsCount() - 1; i >= 0; i--) {
            ExternalPublicationOperationRecord op = ledger.getOperations(i);
            ExternalPublicationIntent intent = op.getIntent();
            if (intent.getAction() == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE
                    && purgeId.equals(intent.getPurgeId())
                    && (objectIdOrNull == null
                            || objectIdOrNull.equals(intent.getExternalObjectId()))) {
                return Optional.of(op);
            }
        }
        return Optional.empty();
    }

    /**
     * Names this deletion on every link it is about to delete: the links for its object, in
     * its external target, that are on record now and that it covers. Settlement retires only
     * links carrying that name. A publication recorded after this point does not carry it,
     * however the clocks read, so this deletion's evidence can never be taken to cover it.
     */
    private static void claimLinksForDeletion(
            ExternalPublicationLedger.Builder next,
            ExternalPublicationLedger current,
            String operationId,
            ExternalSystemBinding bindingSnapshot,
            String externalObjectId,
            long coveredUpToSourceRevision) {
        if (next.hasCurrentLink()
                && claimable(next.getCurrentLink(), bindingSnapshot, externalObjectId,
                        coveredUpToSourceRevision)) {
            next.setCurrentLink(withDeletionClaim(next.getCurrentLink(), operationId));
        }
        for (int i = 0; i < current.getHistoricalLinksCount(); i++) {
            ExternalLink historical = current.getHistoricalLinks(i);
            if (historical.getProjectionLifecycle()
                            != ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED
                    && claimable(historical, bindingSnapshot, externalObjectId,
                            coveredUpToSourceRevision)) {
                next.setHistoricalLinks(i, withDeletionClaim(historical, operationId));
            }
        }
    }

    /**
     * Adds {@code operationId} to the deletions that claim {@code link}. Claims of other
     * deletions stay: one still in flight may settle later, and its evidence must still find
     * the link it named.
     */
    private static ExternalLink withDeletionClaim(ExternalLink link, String operationId) {
        if (link.getDeletionClaimedByOperationIdsList().contains(operationId)) {
            return link;
        }
        return link.toBuilder().addDeletionClaimedByOperationIds(operationId).build();
    }

    private static boolean claimable(ExternalLink link, ExternalSystemBinding bindingSnapshot,
            String externalObjectId, long coveredUpToSourceRevision) {
        return link.getExternalObjectId().equals(externalObjectId)
                && sameExternalTarget(link.getBindingSnapshot(), bindingSnapshot)
                && covered(link.getSourceRevision(), coveredUpToSourceRevision);
    }

    /**
     * Refuses to send a publication or deletion when the binding revision's service profile
     * no longer resolves to the endpoint that revision recorded. An operation is recorded, and
     * later compared, by what the snapshot says, but it is sent where the live profile points;
     * if the two differ it would act on some other place than the one on record.
     */
    static void requireEndpointUnmoved(
            ExternalSystemBinding bindingSnapshot,
            ExternalProviderGateway.OutboundAuthContext outboundAuth) {
        ExternalEndpointSummary recorded = bindingSnapshot.getEndpointSummary();
        ServiceEndpoint live = outboundAuth.serviceEndpoint();
        boolean liveTls = live.getTransport() == Transport.TRANSPORT_TLS;
        if (!recorded.getHost().equals(live.getHost())
                || recorded.getPort() != live.getPort()
                || recorded.getTlsEnabled() != liveTls) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Binding '" + bindingSnapshot.getBindingId() + "' revision "
                            + bindingSnapshot.getBindingRevision() + " recorded endpoint "
                            + recorded.getHost() + ":" + recorded.getPort()
                            + ", but its service profile now resolves to " + live.getHost() + ":"
                            + live.getPort() + "; nothing is sent to a different endpoint")
                    .asRuntimeException();
        }
    }

    /** The ledger with a deletion attempt registered, and that attempt's number. */
    record DeletionRegistration(ExternalPublicationLedger ledger, int attemptNumber) {
    }

    /**
     * Registers an attempt of deletion {@code operationId} on the ledger as it stands before the
     * deletion is sent. The first registration names, on each link on record that the deletion
     * covers, this operation as the one retiring it; that coverage is fixed then. A retry adds
     * an attempt and claims nothing new.
     */
    static DeletionRegistration registerDeletionAttempt(
            ExternalPublicationLedger current,
            String operationId,
            String intentSha256,
            ExternalPublicationIntent intent,
            ExternalSystemBinding bindingSnapshot,
            Optional<Long> tombstoneRevision,
            Optional<String> purgeId,
            Timestamp startedAt) {
        boolean settledAlready = findOperation(current, operationId)
                .map(known -> isTerminalOutcome(known.getOutcome()))
                .orElse(false);
        if (!settledAlready && tombstoneRevision.isPresent()) {
            // Checked again here, against the ledger as it is when the deletion
            // is about to be sent: the target may have been republished since it
            // was resolved, or since an earlier attempt of this same operation.
            requireNotRepublished(
                    current,
                    new ExternalDeleteTargets.ResolvedDeleteTarget(
                            bindingSnapshot, intent.getExternalObjectId()),
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
                && claimable(current.getCurrentLink(), bindingSnapshot, intent.getExternalObjectId(),
                        tombstoneRevision.orElse(ALL_REVISIONS));
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
                return new DeletionRegistration(next.build(), op.getAttemptsCount());
            }
            ExternalLedgerStore.requireRoomForRetry(op);
            int retryNum = op.getAttemptsCount() + 1;
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
            // A retry keeps the coverage its first registration fixed. A link recorded since
            // is not this deletion's to retire, even if the provider answers the retry with
            // the original deletion's receipt; it stays owed a deletion of its own.
            return new DeletionRegistration(replaceOperation(next.build(), updatedOp), retryNum);
        }

        ExternalLedgerStore.requireRoomForDeletion(current);
        claimLinksForDeletion(next, current, operationId, bindingSnapshot,
                intent.getExternalObjectId(),
                tombstoneRevision.orElse(ALL_REVISIONS));
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
                // Fixed at first registration, like the link claims: publications whose doubt
                // is recorded after this point are not what this deletion was sent for.
                .addAllCoversUncertainPublicationIds(ExternalLedgerStore.uncertainPublicationsCoveredBy(
                        current, bindingSnapshot, intent.getExternalObjectId()))
                .setOutcome(
                        ExternalPublicationOutcome
                                .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                .setPendingPhase(
                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                .addAttempts(initialAttempt)
                .setCreatedAt(startedAt)
                .setUpdatedAt(startedAt)
                .build();
        return new DeletionRegistration(next.addOperations(newOp).build(), 1);
    }

    ExternalPublicationCoordinator.OperationResult executeDeleteLifecycle(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            LedgerAccess retainedSecurity,
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
                    boolean purged = settlement.maybePurgeLedgerIfEligible(ledgerAddr, existingLedger);
                    return new ExternalPublicationCoordinator.OperationResult(
                            priorOp,
                            purged ? Optional.empty() : optionalCurrentLink(existingLedger),
                            true,
                            purged);
                }
            }
            if (retainedSecurity.kind() == LedgerAccess.Kind.KEEP) {
                sources.requireLedgerWrite(
                        sourceAddress.getAccountId(),
                        existingOpt.get().security(),
                        "Purging external publications requires WRITE access to the source "
                                + "document's retained rules");
            }
        }

        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(bindingSnapshot);
        requireEndpointUnmoved(bindingSnapshot, outboundAuth);
        Timestamp startedAt = timestamp(clock.instant());

        final int[] attemptHolder = new int[]{1};
        ExternalPublicationLedger preDeleteLedger = ledgers.updateLedgerWithCas(
                ledgerAddr,
                sourceAddress,
                bindingId,
                retainedSecurity,
                "ext-del-pre:" + operationId + ":" + UUID.randomUUID(),
                current -> {
                    DeletionRegistration registration = registerDeletionAttempt(current,
                            operationId, intentSha256, intent, bindingSnapshot,
                            tombstoneRevision, purgeId, startedAt);
                    attemptHolder[0] = registration.attemptNumber();
                    ExternalPublicationOperationRecord registered =
                            findOperation(registration.ledger(), operationId).orElseThrow();
                    return isTerminalOutcome(registered.getOutcome()) ? registration.ledger()
                            : replaceOperation(registration.ledger(), ExternalLedgerStore.withDispatchIdentity(
                                    registered, outboundAuth.credentialBindingSha256()));
                });

        ExternalPublicationOperationRecord preOp =
                findOperation(preDeleteLedger, operationId).orElseThrow();
        if (isTerminalOutcome(preOp.getOutcome())) {
            boolean purged = settlement.maybePurgeLedgerIfEligible(ledgerAddr, preDeleteLedger);
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
                                .setTrigger(attemptTrigger(op, attemptNumber))
                                .setOutcome(
                                        ExternalPublicationOutcome
                                                .EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING)
                                .setPendingPhase(
                                        ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT)
                                .setStartedAt(attemptStartedAt(op, attemptNumber))
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
                intentSha256,
                outboundAuth,
                evidence);
    }
}
