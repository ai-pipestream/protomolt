package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isSourceRevisionFenced;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.isTerminalOutcome;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.timestamp;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalBindingLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPartDigest;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProcessingIdentity;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalPurgePreparation;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Stateless coordinator for external-system publication, provider receipt verification,
 * status reconciliation, monotonic replacement fencing, tombstone deletion, and
 * revision-zero purge settlement over the canonical selected-part {@code DocumentService}.
 *
 * <p>This type owns publication and reconciliation. Ledger storage
 * ({@link ExternalLedgerStore}), source reads and access checks ({@link ExternalSourceAccess}),
 * publish attempts ({@link ExternalPublishAttempts}), receipt settlement
 * ({@link ExternalReceiptSettlement}), deletion
 * ({@link ExternalDeletionLifecycle}) and serving observation
 * ({@link ExternalServingObservation}) live in their own package-private types.
 *
 * <p>All durable state resides in {@code external-publications:<account_id>} and
 * {@code external-bindings:<account_id>} through {@code DocumentService}, so coordinator
 * restarts reload in-flight operations without in-memory caches and source documents
 * are never rewritten by publication activity.
 */
public final class ExternalPublicationCoordinator {

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final ExternalBindingStore bindingStore;
    private final ExternalProviderGateway gateway;
    private final Clock clock;
    private final ExternalLedgerStore ledgers;
    private final ExternalSourceAccess sources;
    private final ExternalReceiptSettlement settlement;
    private final ExternalServingObservation serving;
    private final ExternalDeletionLifecycle deletion;
    private final ExternalPublishAttempts attempts;

    /**
     * Constructs a coordinator with default intake datasource ownership for ledger saves.
     *
     * @param documents gRPC channel to {@code DocumentService}
     * @param bindingStore versioned external-system binding store
     * @param gateway outbound provider execution and status SPI
     * @param verifier provider receipt verifier
     * @param drive named repository drive for ledger saves
     * @param caller the caller each call acts for, as forwarded to {@code DocumentService}
     * @param clock clock stamping operation and link transitions
     */
    public ExternalPublicationCoordinator(
            Channel documents,
            ExternalBindingStore bindingStore,
            ExternalProviderGateway gateway,
            ExternalProviderVerifier verifier,
            String drive,
            ExternalPublicationCaller caller,
            Clock clock) {
        this(
                documents,
                bindingStore,
                gateway,
                verifier,
                drive,
                caller,
                SaveOwner.newBuilder()
                        .setIntake(
                                IntakeOrigin.newBuilder()
                                        .setDatasourceId(
                                                ExternalPublicationCoordinates
                                                        .DEFAULT_PUBLICATIONS_DATASOURCE_ID))
                        .build(),
                clock);
    }

    /**
     * Constructs a coordinator with an explicit {@link SaveOwner} for ledger saves.
     *
     * @param documents gRPC channel to {@code DocumentService}
     * @param bindingStore versioned external-system binding store
     * @param gateway outbound provider execution and status SPI
     * @param verifier provider receipt verifier
     * @param drive named repository drive for ledger saves
     * @param caller the caller each call acts for, as forwarded to {@code DocumentService}
     * @param saveOwner explicit repository save owner for ledger writes
     * @param clock clock stamping operation and link transitions
     */
    public ExternalPublicationCoordinator(
            Channel documents,
            ExternalBindingStore bindingStore,
            ExternalProviderGateway gateway,
            ExternalProviderVerifier verifier,
            String drive,
            ExternalPublicationCaller caller,
            SaveOwner saveOwner,
            Clock clock) {
        this.documents = DocumentServiceGrpc.newBlockingStub(
                Objects.requireNonNull(documents, "documents"));
        this.bindingStore = Objects.requireNonNull(bindingStore, "bindingStore");
        this.gateway = Objects.requireNonNull(gateway, "gateway");
        Objects.requireNonNull(verifier, "verifier");
        ExternalPublicationCoordinates.requireNonBlankCoordinate(drive, "drive");
        Objects.requireNonNull(saveOwner, "saveOwner");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.ledgers = new ExternalLedgerStore(this.documents, drive, saveOwner, clock);
        this.sources = new ExternalSourceAccess(
                this.documents, ledgers, Objects.requireNonNull(caller, "caller"));
        this.settlement = new ExternalReceiptSettlement(this.documents, ledgers, verifier, clock);
        this.serving = new ExternalServingObservation(ledgers, bindingStore, gateway, verifier);
        this.deletion = new ExternalDeletionLifecycle(
                ledgers, sources, settlement, bindingStore, gateway, clock);
        this.attempts = new ExternalPublishAttempts(ledgers, settlement, bindingStore, gateway, clock);
    }

    /**
     * Verified snapshot of an exact repository source revision read through
     * {@code ReadDocument} and {@code GetSchemaDescriptor}.
     *
     * @param sourceAddress canonical source document address
     * @param sourceRevision exact positive source revision
     * @param partDigests SHA-256 verified present part digests
     * @param retainedSchemaSha256s SHA-256 verified retained descriptor-set digests
     * @param documentSecurity access rules recorded on {@code sourceRevision} itself
     */
    public record VerifiedSourceSnapshot(
            NodeAddress sourceAddress,
            long sourceRevision,
            List<ExternalPartDigest> partDigests,
            List<String> retainedSchemaSha256s,
            DocumentSecurity documentSecurity) {

        /** Validates non-null verified source snapshot components. */
        public VerifiedSourceSnapshot {
            Objects.requireNonNull(sourceAddress, "sourceAddress");
            partDigests = List.copyOf(partDigests);
            retainedSchemaSha256s = List.copyOf(retainedSchemaSha256s);
            Objects.requireNonNull(documentSecurity, "documentSecurity");
        }
    }

    /**
     * Request to publish an exact positive repository source revision to an external binding.
     *
     * @param operationId optional caller-supplied operation key (derived deterministically if null)
     * @param sourceAddress canonical source document address
     * @param sourceRevision exact positive source revision ({@code > 0})
     * @param bindingId target external-system binding ID
     * @param bindingRevision exact 1-based binding configuration revision ({@code > 0})
     * @param externalObjectId canonical target external object identity
     * @param processing optional processing/mapping/schema/NLP receipt identities
     * @param displayUrl optional guarded {@code http:}/{@code https:} display URL
     */
    public record PublishRequest(
            String operationId,
            NodeAddress sourceAddress,
            long sourceRevision,
            String bindingId,
            long bindingRevision,
            String externalObjectId,
            ExternalProcessingIdentity processing,
            String displayUrl) {

        /** Validates required publish request fields. */
        public PublishRequest {
            Objects.requireNonNull(sourceAddress, "sourceAddress");
            Objects.requireNonNull(bindingId, "bindingId");
            Objects.requireNonNull(externalObjectId, "externalObjectId");
        }
    }

    /**
     * Request to delete/retire an external projection following a source tombstone revision.
     *
     * @param operationId optional caller-supplied operation key (derived deterministically if null)
     * @param sourceAddress canonical source document address
     * @param tombstoneRevision exact positive tombstone revision on the source document
     * @param bindingId target external-system binding ID
     * @param bindingRevision optional binding revision (if {@code <= 0}, uses active/current link's binding revision)
     * @param externalObjectId optional external object ID (if null, uses active/current link's object ID)
     */
    public record DeleteRequest(
            String operationId,
            NodeAddress sourceAddress,
            long tombstoneRevision,
            String bindingId,
            long bindingRevision,
            String externalObjectId) {

        /** Validates required delete request fields. */
        public DeleteRequest {
            Objects.requireNonNull(sourceAddress, "sourceAddress");
            Objects.requireNonNull(bindingId, "bindingId");
        }
    }

    /**
     * Request to process a revision-zero repository purge event ({@code PurgeDocument} /
     * {@code PurgeMarked} / {@code PurgeCompleted}) without fabricating a source revision.
     *
     * @param operationId optional caller-supplied operation key (derived deterministically if null)
     * @param sourceAddress canonical purged source document address
     * @param purgeId repository purge correlation ID
     * @param bindingId target external-system binding ID
     * @param externalObjectId optional external object ID override
     */
    public record PurgeRequest(
            String operationId,
            NodeAddress sourceAddress,
            String purgeId,
            String bindingId,
            String externalObjectId) {

        /** Validates required purge request fields. */
        public PurgeRequest {
            Objects.requireNonNull(sourceAddress, "sourceAddress");
            Objects.requireNonNull(purgeId, "purgeId");
            Objects.requireNonNull(bindingId, "bindingId");
        }
    }

    /**
     * Request to prepare a purge while the source document still exists.
     *
     * @param sourceAddress canonical source document address
     * @param purgeId repository purge correlation ID the purge will carry
     * @param bindingId target external-system binding ID
     */
    public record PreparePurgeRequest(NodeAddress sourceAddress, String purgeId, String bindingId) {

        /** Validates required fields. */
        public PreparePurgeRequest {
            Objects.requireNonNull(sourceAddress, "sourceAddress");
            Objects.requireNonNull(purgeId, "purgeId");
            Objects.requireNonNull(bindingId, "bindingId");
        }
    }

    /**
     * Outcome of a publication, deletion, purge, or reconciliation call.
     *
     * @param operation the durable operation record
     * @param currentLink the active or deletion-pending link on the ledger, if any
     * @param replayed {@code true} when an already-settled operation was replayed idempotently
     * @param ledgerPurged {@code true} when {@code PURGE_WITH_SOURCE} purged the ledger document
     */
    public record OperationResult(
            ExternalPublicationOperationRecord operation,
            Optional<ExternalLink> currentLink,
            boolean replayed,
            boolean ledgerPurged) {

        /** Validates non-null operation result components. */
        public OperationResult {
            Objects.requireNonNull(operation, "operation");
            Objects.requireNonNull(currentLink, "currentLink");
        }
    }

    /**
     * Authorized read view of the external publication ledger for one {@code (sourceAddress, bindingId)}.
     *
     * @param sourceAddress canonical source document address
     * @param bindingId external-system binding ID
     * @param currentLink current {@code ACTIVE} or {@code DELETION_PENDING} link, if present
     * @param historicalLinks retained {@code SUPERSEDED} or {@code DELETED} links
     * @param operations recorded publication and deletion operations
     * @param highestFencedSourceRevision monotonic source revision fence
     * @param tombstonedAtSourceRevision tombstone revision fence, if tombstoned
     * @param sourcePurged {@code true} if the source document was purged
     * @param lastPurgeId correlation ID of the last processed purge, if purged
     */
    public record ExternalLedgerView(
            NodeAddress sourceAddress,
            String bindingId,
            Optional<ExternalLink> currentLink,
            List<ExternalLink> historicalLinks,
            List<ExternalPublicationOperationRecord> operations,
            long highestFencedSourceRevision,
            Optional<Long> tombstonedAtSourceRevision,
            boolean sourcePurged,
            Optional<String> lastPurgeId) {

        /** Validates non-null ledger view collections. */
        public ExternalLedgerView {
            Objects.requireNonNull(sourceAddress, "sourceAddress");
            Objects.requireNonNull(bindingId, "bindingId");
            Objects.requireNonNull(currentLink, "currentLink");
            historicalLinks = List.copyOf(historicalLinks);
            operations = List.copyOf(operations);
            Objects.requireNonNull(tombstonedAtSourceRevision, "tombstonedAtSourceRevision");
            Objects.requireNonNull(lastPurgeId, "lastPurgeId");
        }
    }

    /**
     * Streams and SHA-256 verifies an exact positive source revision through
     * {@code ReadDocument} and {@code GetSchemaDescriptor}, enforcing caller READ access
     * and refusing missing, tombstoned, incomplete, or digest-mismatched revisions.
     *
     * @param sourceAddress canonical source document address
     * @param sourceRevision exact positive source revision
     * @return verified part digests, retained schema digests, and the revision's access rules
     */
    public VerifiedSourceSnapshot inspectSourceRevision(
            NodeAddress sourceAddress, long sourceRevision) {
        return sources.inspectSourceRevision(sourceAddress, sourceRevision);
    }

    /**
     * Publishes or retries publication of an exact positive source revision to an external
     * binding, enforcing altered-intent conflict refusal, binding retirement checks,
     * source revision/deletion fencing, and verified provider evidence confirmation.
     *
     * @param request typed publish request
     * @return durable operation outcome and active link projection
     */
    public OperationResult publish(PublishRequest request) {
        Objects.requireNonNull(request, "request");
        ExternalPublicationCoordinates.requireSourceAddress(request.sourceAddress());
        if (request.sourceRevision() <= 0L) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("PUBLISH requires sourceRevision > 0")
                    .asRuntimeException();
        }
        if (request.bindingRevision() <= 0L) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("PUBLISH requires bindingRevision > 0")
                    .asRuntimeException();
        }
        if (request.displayUrl() != null) {
            try {
                ExternalPublicationCoordinates.validateDisplayUrl(request.displayUrl());
            } catch (IllegalArgumentException invalidUrl) {
                throw Status.INVALID_ARGUMENT
                        .withDescription(invalidUrl.getMessage())
                        .withCause(invalidUrl)
                        .asRuntimeException();
            }
        }

        String accountId = request.sourceAddress().getAccountId();
        LedgerAccess retainedSecurity = sources.verifyCallerSourceAccessForMutation(request.sourceAddress());

        ExternalPublicationIntent.Builder intentBuilder = ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1)
                .setAccountId(accountId)
                .setSourceAddress(request.sourceAddress())
                .setSourceRevision(request.sourceRevision())
                .setBindingId(request.bindingId())
                .setBindingRevision(request.bindingRevision())
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                .setExternalObjectId(request.externalObjectId());
        if (request.processing() != null) {
            intentBuilder.setProcessing(request.processing());
        }
        if (request.displayUrl() != null) {
            intentBuilder.setDisplayUrl(request.displayUrl());
        }
        ExternalPublicationIntent intent;
        String intentSha256;
        try {
            intent = intentBuilder.build();
            intentSha256 = ExternalOperationKeys.intentSha256(intent);
        } catch (IllegalArgumentException badIntent) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(badIntent.getMessage())
                    .withCause(badIntent)
                    .asRuntimeException();
        }
        String operationId = (request.operationId() == null || request.operationId().isEmpty())
                ? ExternalOperationKeys.deriveOperationId(intent)
                : ExternalPublicationCoordinates.requireCallerKey(request.operationId(), "operationId");

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());

        Optional<ExternalLedgerStore.LoadedLedger> existingLedgerOpt = ledgers.loadOptionalLedger(ledgerAddr, request.sourceAddress());
        if (existingLedgerOpt.isPresent()) {
            ExternalPublicationLedger existingLedger = existingLedgerOpt.get().ledger();
            Optional<ExternalPublicationOperationRecord> priorOpOpt =
                    findOperation(existingLedger, operationId);
            if (priorOpOpt.isPresent()) {
                ExternalPublicationOperationRecord priorOp = priorOpOpt.get();
                if (!priorOp.getIntentSha256().equals(intentSha256)) {
                    throw Status.ALREADY_EXISTS
                            .withDescription("Operation '" + operationId
                                    + "' is already bound to a different publication intent")
                            .asRuntimeException();
                }
                if (isTerminalOutcome(priorOp.getOutcome())) {
                    return new OperationResult(
                            priorOp,
                            optionalCurrentLink(existingLedger),
                            true,
                            false);
                }
                if (sources.isCurrentlyTombstoned(request.sourceAddress())) {
                    // The source has been deleted since this operation was first sent. It
                    // is not sent again; what the first attempt did is still for
                    // reconciliation to find out, so the operation is left as it is.
                    return new OperationResult(
                            priorOp, optionalCurrentLink(existingLedger), true, false);
                }
                return attempts.retryPendingPublish(
                        ledgerAddr,
                        retainedSecurity,
                        priorOp,
                        intent,
                        intentSha256);
            }
        }

        ExternalSystemBinding requestedBinding = bindingStore
                .getBindingRevision(accountId, request.bindingId(), request.bindingRevision())
                .orElseThrow(() -> Status.NOT_FOUND
                        .withDescription("ExternalSystemBinding '" + request.bindingId()
                                + "' revision " + request.bindingRevision() + " not found")
                        .asRuntimeException());
        ExternalSystemBinding latestBinding = bindingStore
                .getCurrentBinding(accountId, request.bindingId())
                .orElseThrow(() -> Status.NOT_FOUND
                        .withDescription("ExternalSystemBinding '" + request.bindingId() + "' not found")
                        .asRuntimeException());
        if (requestedBinding.getLifecycle() == ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_RETIRED
                || latestBinding.getLifecycle() == ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_RETIRED) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("ExternalSystemBinding '" + request.bindingId()
                            + "' is RETIRED and refuses new publications")
                    .asRuntimeException();
        }

        // A source that is deleted now fences every revision of it, whether or not a tombstone
        // deletion has reached this ledger yet to say so.
        if (sources.isCurrentlyTombstoned(request.sourceAddress())
                || (existingLedgerOpt.isPresent()
                && isSourceRevisionFenced(existingLedgerOpt.get().ledger(), request.sourceRevision()))) {
            return attempts.recordFencedPublishFailure(
                    ledgerAddr,
                    retainedSecurity,
                    operationId,
                    intent,
                    intentSha256,
                    requestedBinding,
                    ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL);
        }

        // The ledger keeps the source's access as it is now. The published revision's own
        // security is history: taking it would hand access back to anyone a later revision
        // revoked.
        VerifiedSourceSnapshot verifiedSource = sources.inspectSourceRevision(
                request.sourceAddress(), request.sourceRevision());
        ExternalSourceAccess.verifyDeclaredProcessingMatchesSource(request.processing(), verifiedSource);

        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(requestedBinding);
        // What is recorded as published to endpoint A must have been sent to endpoint A.
        ExternalDeletionLifecycle.requireEndpointUnmoved(requestedBinding, outboundAuth);

        Timestamp startedAt = timestamp(clock.instant());
        ExternalPublicationAttempt dispatchingAttempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(1)
                .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DISPATCHING)
                .setStartedAt(startedAt)
                .build();
        ExternalPublicationOperationRecord initialRecord = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId(operationId)
                .setIntentSha256(intentSha256)
                .setIntent(intent)
                .setBindingSnapshot(requestedBinding)
                .addDispatchCredentialBindingSha256S(outboundAuth.credentialBindingSha256())
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DISPATCHING)
                .addAttempts(dispatchingAttempt)
                .setCreatedAt(startedAt)
                .setUpdatedAt(startedAt)
                .build();

        final LedgerAccess securityForSave = retainedSecurity;
        // Set inside the ledger update: true only when this call registered the operation.
        final boolean[] registeredHere = new boolean[] {false};
        ExternalPublicationLedger afterPreSave = ledgers.updateLedgerWithCas(
                ledgerAddr,
                request.sourceAddress(),
                request.bindingId(),
                securityForSave,
                "ext-pre:" + operationId + ":" + UUID.randomUUID(),
                current -> {
                    Optional<ExternalPublicationOperationRecord> raceOp =
                            findOperation(current, operationId);
                    if (raceOp.isPresent()) {
                        if (!raceOp.get().getIntentSha256().equals(intentSha256)) {
                            throw Status.ALREADY_EXISTS
                                    .withDescription("Operation '" + operationId
                                            + "' concurrently bound to different intent")
                                    .asRuntimeException();
                        }
                        registeredHere[0] = false;
                        return current;
                    }
                    registeredHere[0] = true;
                    ExternalLedgerStore.requireRoomForPublication(
                            current, requestedBinding, intent.getExternalObjectId());
                    boolean fencedByState = isSourceRevisionFenced(current, request.sourceRevision());
                    boolean fencedByBinding = !fencedByState && ExternalLedgerStore.heldByNewerBinding(
                            current, requestedBinding, intent.getExternalObjectId(), request.sourceRevision());
                    if (fencedByState || fencedByBinding) {
                        Timestamp now = timestamp(clock.instant());
                        String fenceCode = fencedByState ? "FENCED_BY_NEWER_STATE" : "FENCED_BY_NEWER_BINDING";
                        String fenceMessage = fencedByState
                                ? "Source revision " + request.sourceRevision()
                                        + " is fenced by a newer revision, tombstone, or purge"
                                : "External object '" + intent.getExternalObjectId()
                                        + "' is already held for this source revision or a later one"
                                        + " under a newer binding revision at the same place";
                        ExternalPublicationAttempt fencedAttempt = dispatchingAttempt.toBuilder()
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                                .setFinishedAt(now)
                                .setDiagnosticCode(fenceCode)
                                .setDiagnosticMessage(fenceMessage)
                                .build();
                        ExternalPublicationOperationRecord fencedOp = initialRecord.toBuilder()
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                                .clearAttempts()
                                .addAttempts(fencedAttempt)
                                .setFailureCode(fenceCode)
                                .setFailureMessage(fencedAttempt.getDiagnosticMessage())
                                .setUpdatedAt(now)
                                .build();
                        return current.toBuilder().addOperations(fencedOp).build();
                    }
                    long nextFence = Math.max(
                            current.getHighestFencedSourceRevision(), request.sourceRevision());
                    return current.toBuilder()
                            .setHighestFencedSourceRevision(nextFence)
                            .addOperations(initialRecord)
                            .build();
                });

        ExternalPublicationOperationRecord savedPreOp =
                findOperation(afterPreSave, operationId).orElseThrow();
        if (isTerminalOutcome(savedPreOp.getOutcome())) {
            return new OperationResult(
                    savedPreOp,
                    optionalCurrentLink(afterPreSave),
                    false,
                    false);
        }

        if (!registeredHere[0]) {
            // Another call registered this operation first and owns its dispatch. Sending it
            // again from here would be a second "attempt 1" whose outcome could overwrite the
            // first's; this call reports the operation as it stands.
            return new OperationResult(savedPreOp, optionalCurrentLink(afterPreSave), true, false);
        }
        // The source was read before the operation was registered, and reading it can take a
        // while. If it was deleted or purged since, nothing is sent: the operation was only
        // registered, so it fails here without any provider call.
        String removedCode = null;
        try {
            sources.verifyCallerSourceAccessForMutation(request.sourceAddress());
            if (sources.isCurrentlyTombstoned(request.sourceAddress())) {
                removedCode = "FENCED_BY_NEWER_STATE";
            }
        } catch (StatusRuntimeException gone) {
            if (gone.getStatus().getCode() != Status.Code.NOT_FOUND) {
                throw gone;
            }
            removedCode = "SOURCE_REMOVED_BEFORE_DISPATCH";
        }
        if (removedCode != null) {
            return attempts.failBeforeDispatch(
                    ledgerAddr, securityForSave, operationId, intent, removedCode,
                    "The source document was deleted or purged before the publication was sent");
        }

        return attempts.executeAndSettlePublishAttempt(
                ledgerAddr,
                securityForSave,
                operationId,
                1,
                intent,
                intentSha256,
                requestedBinding,
                outboundAuth);
    }

    /**
     * Dispatches or retries external deletion for a source document tombstoned at
     * {@code tombstoneRevision}, fencing any delayed pre-delete publication attempts.
     *
     * @param request typed tombstone delete request
     * @return durable deletion operation outcome
     */
    public OperationResult deleteForTombstone(DeleteRequest request) {
        return deletion.deleteForTombstone(request);
    }

    /**
     * First step of a purge: while the source still exists, copies its current access rules
     * onto the ledger and records {@code purgeId}. {@link #handlePurge} accepts only a purge ID
     * prepared this way. Call it immediately before {@code PurgeDocument}: rules changed on the
     * source after it are not on the ledger.
     *
     * @param request typed preparation request
     * @return the recorded preparation
     */
    public ExternalPurgePreparation preparePurge(PreparePurgeRequest request) {
        return deletion.preparePurge(request);
    }

    /**
     * Processes a revision-zero source purge event ({@code PurgeDocument}) without
     * fabricating a source revision, settling external deletion and applying the
     * binding's {@link ExternalRetentionPolicy}.
     *
     * <p>The purge must have been prepared with {@link #preparePurge} for the same purge ID.
     *
     * @param request typed purge request
     * @return durable deletion outcome (with {@code ledgerPurged == true} under {@code PURGE_WITH_SOURCE})
     */
    public OperationResult handlePurge(PurgeRequest request) {
        return deletion.handlePurge(request);
    }

    /**
     * Reconciles an operation against the external provider's read-only status endpoint
     * without re-executing a mutating write. Also supports refreshing advisory
     * {@code ManagedSearchIdentity.serving_visibility} when {@code includeServingObservation} is true.
     *
     * @param sourceAddress canonical source document address
     * @param bindingId target external-system binding ID
     * @param operationId operation key to reconcile
     * @param includeServingObservation whether to request serving-layer visibility
     * @return updated or settled operation outcome
     */
    public OperationResult reconcileOperation(
            NodeAddress sourceAddress,
            String bindingId,
            String operationId,
            boolean includeServingObservation) {
        ExternalPublicationCoordinates.requireSourceAddress(sourceAddress);
        ExternalPublicationCoordinates.requireBindingId(bindingId);
        ExternalPublicationCoordinates.requireNonBlankCoordinate(operationId, "operationId");

        LedgerAccess liveOrRetainedSecurity = sources.authorizeReadOrReconcile(sourceAddress, bindingId);
        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                sourceAddress.getAccountId(), bindingId, sourceAddress);
        ExternalLedgerStore.LoadedLedger loaded = ledgers.loadOptionalLedger(ledgerAddr, sourceAddress)
                .orElseThrow(() -> Status.NOT_FOUND
                        .withDescription("No external publication ledger for source document")
                        .asRuntimeException());
        ExternalPublicationLedger ledger = loaded.ledger();
        ExternalPublicationOperationRecord op = findOperation(ledger, operationId)
                .orElseThrow(() -> Status.NOT_FOUND
                        .withDescription("External publication operation not found: " + operationId)
                        .asRuntimeException());

        if (isTerminalOutcome(op.getOutcome())) {
            if (includeServingObservation
                    && op.getOutcome() == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED
                    && ledger.hasCurrentLink()
                    && ledger.getCurrentLink().getConfirmedByOperationId().equals(operationId)) {
                return serving.refreshServingObservation(
                        ledgerAddr, liveOrRetainedSecurity, ledger, op);
            }
            return new OperationResult(op, optionalCurrentLink(ledger), true, false);
        }

        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(op.getBindingSnapshot());
        // A status query carries the operation's intent and credentials; it goes only to the
        // endpoint the operation's binding snapshot recorded.
        ExternalDeletionLifecycle.requireEndpointUnmoved(op.getBindingSnapshot(), outboundAuth);

        Timestamp startedAt = timestamp(clock.instant());
        int nextAttemptNumber = op.getAttemptsCount() + 1;
        ExternalProviderGateway.ProviderStatusResult statusResult;
        try {
            statusResult = gateway.queryStatus(new ExternalProviderGateway.ProviderStatusQuery(
                    operationId,
                    op.getIntentSha256(),
                    op.getIntent(),
                    outboundAuth,
                    includeServingObservation));
        } catch (StatusRuntimeException statusFailure) {
            Status.Code code = statusFailure.getStatus().getCode();
            if (code != Status.Code.UNAVAILABLE && code != Status.Code.DEADLINE_EXCEEDED) {
                // The provider answered the status query with a refusal. That is not an
                // outage to wait out: the caller is told, and the operation is left as it is.
                throw statusFailure;
            }
            statusResult = new ExternalProviderGateway.ProviderStatusResult.Unavailable(
                    "STATUS_ENDPOINT_" + code.name(),
                    sanitizeMessage(statusFailure.getMessage()));
        }

        Timestamp finishedAt = timestamp(clock.instant());
        if (statusResult instanceof ExternalProviderGateway.ProviderStatusResult.Unavailable unavailable) {
            ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                    .setAttemptNumber(nextAttemptNumber)
                    .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RECONCILIATION)
                    .setOutcome(op.getOutcome())
                    .setPendingPhase(
                            op.getOutcome() == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING
                                    ? ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT
                                    : ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                    .setStartedAt(startedAt)
                    .setFinishedAt(finishedAt)
                    .setDiagnosticCode(unavailable.diagnosticCode())
                    .setDiagnosticMessage(sanitizeMessage(unavailable.diagnosticMessage()))
                    .build();
            ExternalPublicationLedger updated = ledgers.appendReconciliationAttemptOnly(
                    ledgerAddr, sourceAddress, bindingId, liveOrRetainedSecurity, operationId, attempt);
            ExternalPublicationOperationRecord updatedOp =
                    findOperation(updated, operationId).orElseThrow();
            return new OperationResult(updatedOp, optionalCurrentLink(updated), false, false);
        }

        if (statusResult instanceof ExternalProviderGateway.ProviderStatusResult.NoRecordedOutcome none) {
            ExternalPendingPhase phase =
                    op.getOutcome() == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING
                            ? ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT
                            : ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY;
            ExternalPublicationAttempt attempt = ExternalPublicationAttempt.newBuilder()
                    .setAttemptNumber(nextAttemptNumber)
                    .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RECONCILIATION)
                    .setOutcome(op.getOutcome())
                    .setPendingPhase(phase)
                    .setStartedAt(startedAt)
                    .setFinishedAt(finishedAt)
                    .setDiagnosticCode("NO_PROVIDER_OUTCOME_YET")
                    .setDiagnosticMessage(sanitizeMessage(none.diagnosticMessage()))
                    .build();
            ExternalPublicationLedger updated = ledgers.appendReconciliationAttemptOnly(
                    ledgerAddr, sourceAddress, bindingId, liveOrRetainedSecurity, operationId, attempt);
            ExternalPublicationOperationRecord updatedOp =
                    findOperation(updated, operationId).orElseThrow();
            return new OperationResult(updatedOp, optionalCurrentLink(updated), false, false);
        }

        ExternalProviderEvidence evidence =
                ((ExternalProviderGateway.ProviderStatusResult.Receipt) statusResult).evidence();
        return settlement.applyProviderReceipt(
                ledgerAddr,
                sourceAddress,
                bindingId,
                liveOrRetainedSecurity,
                operationId,
                nextAttemptNumber,
                ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RECONCILIATION,
                startedAt,
                op.getIntent(),
                op.getIntentSha256(),
                op.getBindingSnapshot(),
                outboundAuth,
                evidence);
    }

    /**
     * Reads the external publication ledger view for {@code (sourceAddress, bindingId)},
     * enforcing current repository account and document READ access on {@code sourceAddress}.
     *
     * @param sourceAddress canonical source document address
     * @param bindingId external-system binding ID
     * @return authorized ledger view, or empty if no external ledger exists for the document
     */
    public Optional<ExternalLedgerView> getLedgerView(
            NodeAddress sourceAddress, String bindingId) {
        ExternalPublicationCoordinates.requireSourceAddress(sourceAddress);
        ExternalPublicationCoordinates.requireBindingId(bindingId);
        try {
            sources.authorizeReadOrReconcile(sourceAddress, bindingId);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                sourceAddress.getAccountId(), bindingId, sourceAddress);
        return ledgers.loadOptionalLedger(ledgerAddr, sourceAddress).map(loaded -> {
            ExternalPublicationLedger l = loaded.ledger();
            return new ExternalLedgerView(
                    l.getSourceAddress(),
                    l.getBindingId(),
                    optionalCurrentLink(l),
                    l.getHistoricalLinksList(),
                    l.getOperationsList(),
                    l.getHighestFencedSourceRevision(),
                    l.hasTombstonedAtSourceRevision()
                            ? Optional.of(l.getTombstonedAtSourceRevision())
                            : Optional.empty(),
                    l.getSourcePurged(),
                    l.hasLastPurgeId() ? Optional.of(l.getLastPurgeId()) : Optional.empty());
        });
    }

    /**
     * Reads one recorded {@link ExternalPublicationOperationRecord} after enforcing
     * current caller READ access on {@code sourceAddress}.
     *
     * @param sourceAddress canonical source document address
     * @param bindingId external-system binding ID
     * @param operationId operation identifier
     * @return the durable operation record, or empty if absent
     */
    public Optional<ExternalPublicationOperationRecord> getOperationRecord(
            NodeAddress sourceAddress, String bindingId, String operationId) {
        ExternalPublicationCoordinates.requireNonBlankCoordinate(operationId, "operationId");
        return getLedgerView(sourceAddress, bindingId)
                .flatMap(view -> view.operations().stream()
                        .filter(op -> op.getOperationId().equals(operationId))
                        .findFirst());
    }

    /**
     * Lists all active {@link ExternalLink} projections in {@code accountId} for {@code bindingId}
     * whose source documents the current caller is authorized to read.
     *
     * @param accountId owning account ID
     * @param bindingId external-system binding ID
     * @return list of caller-readable active external links
     */
    public List<ExternalLink> listActiveExternalLinks(String accountId, String bindingId) {
        ExternalPublicationCoordinates.requireNonBlankCoordinate(accountId, "accountId");
        ExternalPublicationCoordinates.requireBindingId(bindingId);
        String graphId = ExternalPublicationCoordinates.PUBLICATIONS_GRAPH_PREFIX + accountId;
        List<ExternalLink> visibleLinks = new ArrayList<>();
        String pageToken = "";
        do {
            ListDocumentsResponse page = documents.listDocuments(
                    ListDocumentsRequest.newBuilder()
                            .setAccountId(accountId)
                            .setGraphId(graphId)
                            .setGraphAddressId(bindingId)
                            .setPageSize(100)
                            .setPageToken(pageToken)
                            .build());
            for (DocumentSummary summary : page.getDocumentsList()) {
                Optional<ExternalLedgerStore.LoadedLedger> loaded = ledgers.loadOptionalLedger(summary.getAddress(), null);
                if (loaded.isEmpty() || !loaded.get().ledger().hasCurrentLink()) {
                    continue;
                }
                ExternalLink link = loaded.get().ledger().getCurrentLink();
                if (link.getProjectionLifecycle()
                        != ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE) {
                    continue;
                }
                if (sources.canCallerReadSource(link.getSourceAddress())) {
                    visibleLinks.add(link);
                }
            }
            pageToken = page.getNextPageToken();
        } while (!pageToken.isEmpty());
        return List.copyOf(visibleLinks);
    }

}
