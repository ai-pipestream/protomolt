package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalBindingLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPartDigest;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProcessingIdentity;
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
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.GetManifestRequest;
import ai.protomolt.proto.repo.v2.GetManifestResponse;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorRequest;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsResponse;
import ai.protomolt.proto.repo.v2.PartData;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.PurgeDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationMode;
import ai.protomolt.proto.repo.v2.WriteAction;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * Stateless coordinator for external-system publication, provider receipt verification,
 * status reconciliation, monotonic replacement fencing, tombstone deletion, and
 * revision-zero purge settlement over the canonical selected-part {@code DocumentService}.
 *
 * <p>All durable state resides in {@code external-publications:<account_id>} and
 * {@code external-bindings:<account_id>} through {@code DocumentService}, so coordinator
 * restarts reload in-flight operations without in-memory caches and source documents
 * are never rewritten by publication activity.
 */
public final class ExternalPublicationCoordinator {

    private static final int MAX_CAS_RETRIES = 8;

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource()));

    private static final PartSelector CORE_SELECTOR =
            PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();

    private static final PartSelection CORE_SELECTION =
            PartSelection.newBuilder().addSelectors(CORE_SELECTOR).build();

    private static final PartWrite CORE_PUT = PartWrite.newBuilder()
            .setSelector(CORE_SELECTOR)
            .setAction(WriteAction.WRITE_ACTION_PUT)
            .build();

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final ExternalBindingStore bindingStore;
    private final ExternalProviderGateway gateway;
    private final ExternalProviderVerifier verifier;
    private final String drive;
    private final SaveOwner saveOwner;
    private final Clock clock;

    /**
     * Constructs a coordinator with default intake datasource ownership for ledger saves.
     *
     * @param documents gRPC channel to {@code DocumentService}
     * @param bindingStore versioned external-system binding store
     * @param gateway outbound provider execution and status SPI
     * @param verifier provider receipt verifier
     * @param drive named repository drive for ledger saves
     * @param clock clock stamping operation and link transitions
     */
    public ExternalPublicationCoordinator(
            Channel documents,
            ExternalBindingStore bindingStore,
            ExternalProviderGateway gateway,
            ExternalProviderVerifier verifier,
            String drive,
            Clock clock) {
        this(
                documents,
                bindingStore,
                gateway,
                verifier,
                drive,
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
     * @param saveOwner explicit repository save owner for ledger writes
     * @param clock clock stamping operation and link transitions
     */
    public ExternalPublicationCoordinator(
            Channel documents,
            ExternalBindingStore bindingStore,
            ExternalProviderGateway gateway,
            ExternalProviderVerifier verifier,
            String drive,
            SaveOwner saveOwner,
            Clock clock) {
        this.documents = DocumentServiceGrpc.newBlockingStub(
                Objects.requireNonNull(documents, "documents"));
        this.bindingStore = Objects.requireNonNull(bindingStore, "bindingStore");
        this.gateway = Objects.requireNonNull(gateway, "gateway");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        ExternalPublicationCoordinates.requireNonBlankCoordinate(drive, "drive");
        this.drive = drive;
        this.saveOwner = Objects.requireNonNull(saveOwner, "saveOwner");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Verified snapshot of an exact repository source revision read through
     * {@code ReadDocument} and {@code GetSchemaDescriptor}.
     *
     * @param sourceAddress canonical source document address
     * @param sourceRevision exact positive source revision
     * @param partDigests SHA-256 verified present part digests
     * @param retainedSchemaSha256s SHA-256 verified retained descriptor-set digests
     * @param documentSecurity current ACL security policy on the source document
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
     * @return verified part digests, retained schema digests, and current ACL policy
     */
    public VerifiedSourceSnapshot inspectSourceRevision(
            NodeAddress sourceAddress, long sourceRevision) {
        ExternalPublicationCoordinates.requireSourceAddress(sourceAddress);
        if (sourceRevision <= 0L) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("sourceRevision must be > 0")
                    .asRuntimeException();
        }
        RevisionSelector exact = RevisionSelector.newBuilder()
                .setRevision(sourceRevision)
                .build();

        GetManifestResponse manifestResp = documents.getManifest(
                GetManifestRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(exact)
                        .build());
        DocumentManifest manifest = manifestResp.getManifest();
        if (manifest.getTombstone()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Source revision " + sourceRevision + " is a tombstone")
                    .asRuntimeException();
        }

        Iterator<ReadDocumentResponse> stream = documents.readDocument(
                ReadDocumentRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(exact)
                        .build());

        DocumentManifest streamedManifest = null;
        boolean sawComplete = false;
        Map<PartSelector, ByteArrayOutputStream> buffers = new HashMap<>();
        while (stream.hasNext()) {
            ReadDocumentResponse frame = stream.next();
            if (frame.hasManifest()) {
                streamedManifest = frame.getManifest();
            } else if (frame.hasPartData()) {
                PartData data = frame.getPartData();
                ByteArrayOutputStream buf = buffers.computeIfAbsent(
                        data.getSelector(), ignored -> new ByteArrayOutputStream());
                if (buf.size() != data.getOffset()) {
                    throw Status.DATA_LOSS
                            .withDescription("Non-contiguous part stream offset")
                            .asRuntimeException();
                }
                byte[] chunk = data.getData().toByteArray();
                buf.writeBytes(chunk);
            } else if (frame.hasComplete()) {
                sawComplete = true;
            }
        }
        if (streamedManifest == null || !sawComplete) {
            throw Status.DATA_LOSS
                    .withDescription("ReadDocument ended without manifest and ReadComplete")
                    .asRuntimeException();
        }

        List<ExternalPartDigest> verifiedParts = new ArrayList<>();
        for (PartManifestEntry entry : streamedManifest.getPartsList()) {
            if (entry.getState() != PartState.PART_STATE_PRESENT) {
                continue;
            }
            ByteArrayOutputStream buf = buffers.get(entry.getSelector());
            byte[] actualBytes = buf == null ? new byte[0] : buf.toByteArray();
            if (actualBytes.length != entry.getSizeBytes()) {
                throw Status.DATA_LOSS
                        .withDescription("Streamed part byte length mismatch on " + entry.getSelector())
                        .asRuntimeException();
            }
            String actualSha = sha256Hex(actualBytes);
            if (!actualSha.equals(entry.getSha256())) {
                throw Status.DATA_LOSS
                        .withDescription("Streamed part SHA-256 mismatch on " + entry.getSelector())
                        .asRuntimeException();
            }
            verifiedParts.add(toExternalPartDigest(entry.getSelector(), actualSha, actualBytes.length));
        }

        Set<String> retainedSchemas = collectRetainedSchemaDigests(manifestResp.getValidation());
        List<String> sortedSchemas = new ArrayList<>(retainedSchemas);
        sortedSchemas.sort(Comparator.naturalOrder());
        for (String schemaDigest : sortedSchemas) {
            GetSchemaDescriptorResponse schemaResp = documents.getSchemaDescriptor(
                    GetSchemaDescriptorRequest.newBuilder()
                            .setAddress(sourceAddress)
                            .setRevision(exact)
                            .setDescriptorSha256(schemaDigest)
                            .build());
            String verifiedDigest = sha256Hex(schemaResp.getDescriptorSet().toByteArray());
            if (!verifiedDigest.equals(schemaDigest)) {
                throw Status.DATA_LOSS
                        .withDescription("Retained schema descriptor digest mismatch: " + schemaDigest)
                        .asRuntimeException();
            }
        }

        GetDocumentResponse coreAtRevision = documents.getDocument(
                GetDocumentRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(exact)
                        .setSelection(CORE_SELECTION)
                        .build());
        DocumentSecurity security = coreAtRevision.getDocument()
                .getOwnership()
                .getSecurity();

        return new VerifiedSourceSnapshot(
                sourceAddress,
                sourceRevision,
                verifiedParts,
                sortedSchemas,
                security);
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
        DocumentSecurity retainedSecurity = verifyCallerSourceAccessForMutation(request.sourceAddress());

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
                : request.operationId();

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());

        Optional<LoadedLedger> existingLedgerOpt = loadOptionalLedger(ledgerAddr, request.sourceAddress());
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
                return retryPendingPublish(
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

        if (existingLedgerOpt.isPresent()
                && isSourceRevisionFenced(existingLedgerOpt.get().ledger(), request.sourceRevision())) {
            return recordFencedPublishFailure(
                    ledgerAddr,
                    retainedSecurity,
                    operationId,
                    intent,
                    intentSha256,
                    requestedBinding,
                    ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL);
        }

        VerifiedSourceSnapshot verifiedSource = inspectSourceRevision(
                request.sourceAddress(), request.sourceRevision());
        retainedSecurity = verifiedSource.documentSecurity();
        verifyDeclaredProcessingMatchesSource(request.processing(), verifiedSource);

        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(requestedBinding);

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
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DISPATCHING)
                .addAttempts(dispatchingAttempt)
                .setCreatedAt(startedAt)
                .setUpdatedAt(startedAt)
                .build();

        final DocumentSecurity securityForSave = retainedSecurity;
        ExternalPublicationLedger afterPreSave = updateLedgerWithCas(
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
                        return current;
                    }
                    if (isSourceRevisionFenced(current, request.sourceRevision())) {
                        Timestamp now = timestamp(clock.instant());
                        ExternalPublicationAttempt fencedAttempt = dispatchingAttempt.toBuilder()
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                                .setFinishedAt(now)
                                .setDiagnosticCode("FENCED_BY_NEWER_STATE")
                                .setDiagnosticMessage(
                                        "Source revision " + request.sourceRevision()
                                                + " is fenced by a newer revision, tombstone, or purge")
                                .build();
                        ExternalPublicationOperationRecord fencedOp = initialRecord.toBuilder()
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                                .clearAttempts()
                                .addAttempts(fencedAttempt)
                                .setFailureCode("FENCED_BY_NEWER_STATE")
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

        return executeAndSettlePublishAttempt(
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
        Objects.requireNonNull(request, "request");
        ExternalPublicationCoordinates.requireSourceAddress(request.sourceAddress());
        if (request.tombstoneRevision() <= 0L) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("tombstoneRevision must be > 0")
                    .asRuntimeException();
        }
        String accountId = request.sourceAddress().getAccountId();
        DocumentSecurity retainedSecurity = verifyTombstoneRevision(
                request.sourceAddress(), request.tombstoneRevision());

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());
        Optional<LoadedLedger> ledgerOpt = loadOptionalLedger(ledgerAddr, request.sourceAddress());

        ResolvedDeleteTarget target = resolveDeleteTarget(
                accountId,
                request.bindingId(),
                request.bindingRevision(),
                request.externalObjectId(),
                ledgerOpt.map(LoadedLedger::ledger));

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

    /**
     * Processes a revision-zero source purge event ({@code PurgeDocument}) without
     * fabricating a source revision, settling external deletion and applying the
     * binding's {@link ExternalRetentionPolicy}.
     *
     * @param request typed purge request
     * @return durable deletion outcome (with {@code ledgerPurged == true} under {@code PURGE_WITH_SOURCE})
     */
    public OperationResult handlePurge(PurgeRequest request) {
        Objects.requireNonNull(request, "request");
        ExternalPublicationCoordinates.requireSourceAddress(request.sourceAddress());
        ExternalPublicationCoordinates.requireNonBlankCoordinate(request.purgeId(), "purgeId");

        String accountId = request.sourceAddress().getAccountId();
        verifyAccountCallerAccess(request.sourceAddress());

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                accountId, request.bindingId(), request.sourceAddress());
        Optional<LoadedLedger> ledgerOpt = loadOptionalLedger(ledgerAddr, request.sourceAddress());
        DocumentSecurity retainedSecurity = ledgerOpt
                .map(LoadedLedger::security)
                .orElse(DocumentSecurity.getDefaultInstance());

        ResolvedDeleteTarget target = resolveDeleteTarget(
                accountId,
                request.bindingId(),
                0L,
                request.externalObjectId(),
                ledgerOpt.map(LoadedLedger::ledger));

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

        DocumentSecurity liveOrRetainedSecurity = authorizeReadOrReconcile(sourceAddress, bindingId);
        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                sourceAddress.getAccountId(), bindingId, sourceAddress);
        LoadedLedger loaded = loadOptionalLedger(ledgerAddr, sourceAddress)
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
                return refreshServingObservation(
                        ledgerAddr, liveOrRetainedSecurity, ledger, op);
            }
            return new OperationResult(op, optionalCurrentLink(ledger), true, false);
        }

        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(op.getBindingSnapshot());

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
        } catch (StatusRuntimeException transportFailure) {
            statusResult = new ExternalProviderGateway.ProviderStatusResult.Unavailable(
                    "STATUS_ENDPOINT_" + transportFailure.getStatus().getCode().name(),
                    sanitizeMessage(transportFailure.getMessage()));
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
            ExternalPublicationLedger updated = appendReconciliationAttemptOnly(
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
            ExternalPublicationLedger updated = appendReconciliationAttemptOnly(
                    ledgerAddr, sourceAddress, bindingId, liveOrRetainedSecurity, operationId, attempt);
            ExternalPublicationOperationRecord updatedOp =
                    findOperation(updated, operationId).orElseThrow();
            return new OperationResult(updatedOp, optionalCurrentLink(updated), false, false);
        }

        ExternalProviderEvidence evidence =
                ((ExternalProviderGateway.ProviderStatusResult.Receipt) statusResult).evidence();
        return applyProviderReceipt(
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
            authorizeReadOrReconcile(sourceAddress, bindingId);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }

        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                sourceAddress.getAccountId(), bindingId, sourceAddress);
        return loadOptionalLedger(ledgerAddr, sourceAddress).map(loaded -> {
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
                Optional<LoadedLedger> loaded = loadOptionalLedger(summary.getAddress(), null);
                if (loaded.isEmpty() || !loaded.get().ledger().hasCurrentLink()) {
                    continue;
                }
                ExternalLink link = loaded.get().ledger().getCurrentLink();
                if (link.getProjectionLifecycle()
                        != ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE) {
                    continue;
                }
                if (canCallerReadSource(link.getSourceAddress())) {
                    visibleLinks.add(link);
                }
            }
            pageToken = page.getNextPageToken();
        } while (!pageToken.isEmpty());
        return List.copyOf(visibleLinks);
    }

    private record LoadedLedger(
            ExternalPublicationLedger ledger,
            long documentRevision,
            DocumentSecurity security) {
    }

    private record ResolvedDeleteTarget(ExternalSystemBinding binding, String externalObjectId) {
    }

    private OperationResult retryPendingPublish(
            NodeAddress ledgerAddr,
            DocumentSecurity retainedSecurity,
            ExternalPublicationOperationRecord priorOp,
            ExternalPublicationIntent intent,
            String intentSha256) {
        String operationId = priorOp.getOperationId();
        int attemptNumber = priorOp.getAttemptsCount() + 1;
        Timestamp startedAt = timestamp(clock.instant());

        ExternalPublicationLedger preUpdated = updateLedgerWithCas(
                ledgerAddr,
                intent.getSourceAddress(),
                intent.getBindingId(),
                retainedSecurity,
                "ext-retry-pre:" + operationId + ":" + attemptNumber + ":" + UUID.randomUUID(),
                current -> {
                    ExternalPublicationOperationRecord latestOp =
                            findOperation(current, operationId).orElseThrow();
                    if (isTerminalOutcome(latestOp.getOutcome())) {
                        return current;
                    }
                    if (isSourceRevisionFenced(current, intent.getSourceRevision())) {
                        ExternalPublicationAttempt fencedAttempt = ExternalPublicationAttempt.newBuilder()
                                .setAttemptNumber(attemptNumber)
                                .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_RETRY)
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                .setStartedAt(startedAt)
                                .setFinishedAt(startedAt)
                                .setDiagnosticCode("FENCED_BY_NEWER_STATE")
                                .setDiagnosticMessage(
                                        "Retry fenced because a newer source revision, tombstone, or purge superseded revision "
                                                + intent.getSourceRevision())
                                .build();
                        ExternalPublicationOperationRecord failedOp = latestOp.toBuilder()
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                                .addAttempts(fencedAttempt)
                                .setFailureCode("FENCED_BY_NEWER_STATE")
                                .setFailureMessage(fencedAttempt.getDiagnosticMessage())
                                .setUpdatedAt(startedAt)
                                .build();
                        return replaceOperation(current, failedOp);
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
                    return replaceOperation(current, dispatchingOp);
                });

        ExternalPublicationOperationRecord latestAfterPre =
                findOperation(preUpdated, operationId).orElseThrow();
        if (isTerminalOutcome(latestAfterPre.getOutcome())) {
            return new OperationResult(
                    latestAfterPre, optionalCurrentLink(preUpdated), false, false);
        }

        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(latestAfterPre.getBindingSnapshot());
        return executeAndSettlePublishAttempt(
                ledgerAddr,
                retainedSecurity,
                operationId,
                attemptNumber,
                intent,
                intentSha256,
                latestAfterPre.getBindingSnapshot(),
                outboundAuth);
    }

    private OperationResult recordFencedPublishFailure(
            NodeAddress ledgerAddr,
            DocumentSecurity retainedSecurity,
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

        ExternalPublicationLedger updated = updateLedgerWithCas(
                ledgerAddr,
                intent.getSourceAddress(),
                intent.getBindingId(),
                retainedSecurity,
                "ext-fenced:" + operationId + ":" + UUID.randomUUID(),
                current -> current.toBuilder().addOperations(failedRecord).build());
        return new OperationResult(
                findOperation(updated, operationId).orElseThrow(),
                optionalCurrentLink(updated),
                false,
                false);
    }

    private OperationResult executeAndSettlePublishAttempt(
            NodeAddress ledgerAddr,
            DocumentSecurity retainedSecurity,
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
        } catch (StatusRuntimeException transportFailure) {
            callResult = new ExternalProviderGateway.ProviderCallResult.Unresolved(
                    "ENDPOINT_" + transportFailure.getStatus().getCode().name(),
                    sanitizeMessage(transportFailure.getMessage()));
        }

        if (callResult instanceof ExternalProviderGateway.ProviderCallResult.Unresolved unresolved) {
            Timestamp finishedAt = timestamp(clock.instant());
            ExternalPublicationLedger updated = updateLedgerWithCas(
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
                                .setTrigger(lastAttemptTrigger(op))
                                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                                .setStartedAt(lastAttemptStartedAt(op, callStart))
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
            return new OperationResult(
                    findOperation(updated, operationId).orElseThrow(),
                    optionalCurrentLink(updated),
                    false,
                    false);
        }

        ExternalProviderEvidence evidence =
                ((ExternalProviderGateway.ProviderCallResult.Receipt) callResult).evidence();
        return applyProviderReceipt(
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

    private OperationResult executeDeleteLifecycle(
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
        Optional<LoadedLedger> existingOpt = loadOptionalLedger(ledgerAddr, sourceAddress);
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
                    boolean purged = maybePurgeLedgerIfEligible(ledgerAddr, existingLedger, priorOp);
                    return new OperationResult(
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
        ExternalPublicationLedger preDeleteLedger = updateLedgerWithCas(
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
            boolean purged = maybePurgeLedgerIfEligible(ledgerAddr, preDeleteLedger, preOp);
            return new OperationResult(
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
            ExternalPublicationLedger updated = updateLedgerWithCas(
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
            return new OperationResult(
                    findOperation(updated, operationId).orElseThrow(),
                    optionalCurrentLink(updated),
                    false,
                    false);
        }

        ExternalProviderEvidence evidence =
                ((ExternalProviderGateway.ProviderCallResult.Receipt) callResult).evidence();
        return applyProviderReceipt(
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

    private OperationResult applyProviderReceipt(
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
            ExternalPublicationLedger failedLedger = updateLedgerWithCas(
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
            return new OperationResult(
                    findOperation(failedLedger, operationId).orElseThrow(),
                    optionalCurrentLink(failedLedger),
                    false,
                    false);
        }

        ExternalProviderSettlement settlement = evidence.getSettlement();
        ExternalPublicationLedger settledLedger = updateLedgerWithCas(
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
        return new OperationResult(
                finalOp,
                purged ? Optional.empty() : optionalCurrentLink(settledLedger),
                false,
                purged);
    }

    private static ExternalPublicationLedger settleConfirmedPublication(
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

        boolean supersededOrDeleted = current.getSourcePurged()
                || (current.hasTombstonedAtSourceRevision()
                        && intent.getSourceRevision() <= current.getTombstonedAtSourceRevision())
                || current.getHighestFencedSourceRevision() > intent.getSourceRevision()
                || (current.hasCurrentLink()
                        && current.getCurrentLink().getSourceRevision() > intent.getSourceRevision());

        ExternalPublicationLedger withOp = replaceOperation(current, confirmedOp);
        ExternalPublicationLedger.Builder ledgerBuilder = withOp.toBuilder();
        if (supersededOrDeleted) {
            ExternalProjectionLifecycle historicalState =
                    (current.getSourcePurged() || current.hasTombstonedAtSourceRevision())
                            ? ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED
                            : ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED;
            String retiringOpId = current.hasCurrentLink()
                    ? current.getCurrentLink().getConfirmedByOperationId()
                    : op.getOperationId();
            ledgerBuilder.addHistoricalLinks(linkBuilder
                    .setProjectionLifecycle(historicalState)
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

    private static ExternalPublicationLedger settleConfirmedDeletion(
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
        ExternalPublicationLedger.Builder next = withOp.toBuilder();
        if (current.hasCurrentLink()) {
            ExternalLink retiredLink = current.getCurrentLink().toBuilder()
                    .setProjectionLifecycle(
                            ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED)
                    .setRetiredByOperationId(op.getOperationId())
                    .setDeletionEvidence(evidence)
                    .build();
            next.clearCurrentLink();
            next.addHistoricalLinks(retiredLink);
        }
        return next.build();
    }

    private OperationResult refreshServingObservation(
            NodeAddress ledgerAddr,
            DocumentSecurity security,
            ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord op) {
        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(op.getBindingSnapshot());
        ExternalProviderGateway.ProviderStatusResult statusResult;
        try {
            statusResult = gateway.queryStatus(new ExternalProviderGateway.ProviderStatusQuery(
                    op.getOperationId(),
                    op.getIntentSha256(),
                    op.getIntent(),
                    outboundAuth,
                    true));
        } catch (StatusRuntimeException ignoredUnavailable) {
            return new OperationResult(op, optionalCurrentLink(ledger), true, false);
        }
        if (!(statusResult instanceof ExternalProviderGateway.ProviderStatusResult.Receipt receipt)
                || !receipt.evidence().hasManagedSearch()) {
            return new OperationResult(op, optionalCurrentLink(ledger), true, false);
        }
        verifier.verifyEvidence(
                receipt.evidence(),
                op.getBindingSnapshot(),
                outboundAuth,
                op.getIntent(),
                op.getIntentSha256());

        ExternalPublicationLedger updated = updateLedgerWithCas(
                ledgerAddr,
                ledger.getSourceAddress(),
                ledger.getBindingId(),
                security,
                "ext-serving-obs:" + op.getOperationId() + ":" + UUID.randomUUID(),
                current -> {
                    if (!current.hasCurrentLink()
                            || !current.getCurrentLink()
                                    .getConfirmedByOperationId()
                                    .equals(op.getOperationId())) {
                        return current;
                    }
                    ExternalLink updatedLink = current.getCurrentLink().toBuilder()
                            .setManagedSearch(receipt.evidence().getManagedSearch())
                            .build();
                    return current.toBuilder().setCurrentLink(updatedLink).build();
                });
        return new OperationResult(
                findOperation(updated, op.getOperationId()).orElse(op),
                optionalCurrentLink(updated),
                true,
                false);
    }

    private ExternalPublicationLedger appendReconciliationAttemptOnly(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            DocumentSecurity security,
            String operationId,
            ExternalPublicationAttempt attempt) {
        return updateLedgerWithCas(
                ledgerAddr,
                sourceAddress,
                bindingId,
                security,
                "ext-reconcile:" + operationId + ":" + attempt.getAttemptNumber() + ":" + UUID.randomUUID(),
                current -> {
                    ExternalPublicationOperationRecord op =
                            findOperation(current, operationId).orElseThrow();
                    if (isTerminalOutcome(op.getOutcome())) {
                        return current;
                    }
                    ExternalPublicationOperationRecord updatedOp = op.toBuilder()
                            .setOutcome(attempt.getOutcome())
                            .setPendingPhase(attempt.getPendingPhase())
                            .addAttempts(attempt)
                            .setUpdatedAt(attempt.hasFinishedAt()
                                    ? attempt.getFinishedAt()
                                    : attempt.getStartedAt())
                            .build();
                    return replaceOperation(current, updatedOp);
                });
    }

    private boolean maybePurgeLedgerIfEligible(
            NodeAddress ledgerAddr,
            ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord settledOp) {
        if (!ledger.getSourcePurged()
                || settledOp.getOutcome() != ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED
                || settledOp.getBindingSnapshot().getRetentionPolicy()
                        != ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE) {
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

    private ResolvedDeleteTarget resolveDeleteTarget(
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

    private DocumentSecurity verifyCallerSourceAccessForMutation(NodeAddress sourceAddress) {
        ListRevisionsResponse revisions = documents.listRevisions(
                ListRevisionsRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setPageSize(1)
                        .build());
        if (revisions.getRevisionsCount() == 0) {
            throw Status.NOT_FOUND
                    .withDescription("Source document has no revisions")
                    .asRuntimeException();
        }
        long latestRev = revisions.getRevisions(0).getRevision();
        if (revisions.getRevisions(0).getTombstone()) {
            return DocumentSecurity.getDefaultInstance();
        }
        GetDocumentResponse latestCore = documents.getDocument(
                GetDocumentRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(RevisionSelector.newBuilder().setRevision(latestRev))
                        .setSelection(CORE_SELECTION)
                        .build());
        return latestCore.getDocument().getOwnership().getSecurity();
    }

    private DocumentSecurity verifyTombstoneRevision(
            NodeAddress sourceAddress, long tombstoneRevision) {
        GetManifestResponse manifestResp = documents.getManifest(
                GetManifestRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(RevisionSelector.newBuilder()
                                .setRevision(tombstoneRevision))
                        .build());
        if (!manifestResp.getManifest().getTombstone()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Source revision " + tombstoneRevision
                            + " is not a tombstone")
                    .asRuntimeException();
        }
        NodeAddress ledgercheck = sourceAddress;
        ListRevisionsResponse revs = documents.listRevisions(
                ListRevisionsRequest.newBuilder()
                        .setAddress(ledgercheck)
                        .setPageSize(2)
                        .build());
        for (var summary : revs.getRevisionsList()) {
            if (!summary.getTombstone()) {
                GetDocumentResponse prior = documents.getDocument(
                        GetDocumentRequest.newBuilder()
                                .setAddress(sourceAddress)
                                .setRevision(RevisionSelector.newBuilder()
                                        .setRevision(summary.getRevision()))
                                .setSelection(CORE_SELECTION)
                                .build());
                return prior.getDocument().getOwnership().getSecurity();
            }
        }
        return DocumentSecurity.getDefaultInstance();
    }

    private void verifyAccountCallerAccess(NodeAddress sourceAddress) {
        try {
            documents.listRevisions(ListRevisionsRequest.newBuilder()
                    .setAddress(sourceAddress)
                    .setPageSize(1)
                    .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                throw e;
            }
        }
    }

    private DocumentSecurity authorizeReadOrReconcile(
            NodeAddress sourceAddress, String bindingId) {
        try {
            ListRevisionsResponse revs = documents.listRevisions(
                    ListRevisionsRequest.newBuilder()
                            .setAddress(sourceAddress)
                            .setPageSize(1)
                            .build());
            if (revs.getRevisionsCount() > 0 && !revs.getRevisions(0).getTombstone()) {
                GetDocumentResponse latest = documents.getDocument(
                        GetDocumentRequest.newBuilder()
                                .setAddress(sourceAddress)
                                .setRevision(RevisionSelector.newBuilder()
                                        .setRevision(revs.getRevisions(0).getRevision()))
                                .setSelection(CORE_SELECTION)
                                .build());
                return latest.getDocument().getOwnership().getSecurity();
            }
            return DocumentSecurity.getDefaultInstance();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                throw e;
            }
            NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                    sourceAddress.getAccountId(), bindingId, sourceAddress);
            Optional<LoadedLedger> retained = loadOptionalLedger(ledgerAddr, sourceAddress);
            if (retained.isPresent() && retained.get().ledger().getSourcePurged()) {
                return retained.get().security();
            }
            throw e;
        }
    }

    private boolean canCallerReadSource(NodeAddress sourceAddress) {
        try {
            documents.listRevisions(ListRevisionsRequest.newBuilder()
                    .setAddress(sourceAddress)
                    .setPageSize(1)
                    .build());
            return true;
        } catch (StatusRuntimeException deniedOrMissing) {
            return false;
        }
    }

    private static void verifyDeclaredProcessingMatchesSource(
            ExternalProcessingIdentity declared, VerifiedSourceSnapshot verifiedSource) {
        if (declared == null) {
            return;
        }
        if (declared.getSourcePartDigestsCount() > 0) {
            Map<String, ExternalPartDigest> actualByKey = new HashMap<>();
            for (ExternalPartDigest part : verifiedSource.partDigests()) {
                actualByKey.put(part.getPartValue() + ":" + part.getSubKey(), part);
            }
            for (ExternalPartDigest claimed : declared.getSourcePartDigestsList()) {
                ExternalPartDigest actual = actualByKey.get(
                        claimed.getPartValue() + ":" + claimed.getSubKey());
                if (actual == null
                        || !actual.getSha256().equals(claimed.getSha256())
                        || actual.getSizeBytes() != claimed.getSizeBytes()) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("Declared source_part_digests mismatch verified source revision "
                                    + verifiedSource.sourceRevision())
                            .asRuntimeException();
                }
            }
        }
        if (declared.getRetainedSchemaSha256SCount() > 0) {
            Set<String> actualSchemas = Set.copyOf(verifiedSource.retainedSchemaSha256s());
            for (String claimedSchema : declared.getRetainedSchemaSha256SList()) {
                if (!actualSchemas.contains(claimedSchema)) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("Declared retained_schema_sha256 '" + claimedSchema
                                    + "' is not retained on source revision "
                                    + verifiedSource.sourceRevision())
                            .asRuntimeException();
                }
            }
        }
    }

    private ExternalPublicationLedger updateLedgerWithCas(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            DocumentSecurity security,
            String saveOperationPrefix,
            UnaryOperator<ExternalPublicationLedger> mutator) {
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Optional<LoadedLedger> existing = loadOptionalLedger(ledgerAddr, sourceAddress);
            ExternalPublicationLedger base = existing.isPresent()
                    ? existing.get().ledger()
                    : ExternalPublicationLedger.newBuilder()
                            .setAccountId(sourceAddress.getAccountId())
                            .setBindingId(bindingId)
                            .setSourceAddress(sourceAddress)
                            .setLogicalSourceKey(ByteString.copyFrom(
                                    ExternalPublicationCoordinates.logicalSourceKey(sourceAddress)))
                            .setHighestFencedSourceRevision(0L)
                            .build();
            ExpectedRevision expected = existing.isPresent()
                    ? ExpectedRevision.newBuilder()
                            .setRevision(existing.get().documentRevision())
                            .build()
                    : ExpectedRevision.newBuilder().setNone(true).build();

            ExternalPublicationLedger next = mutator.apply(base);
            if (next.equals(base) && existing.isPresent()) {
                return next;
            }
            ValidationResult valid = VALIDATOR.validate(next);
            if (!valid.valid()) {
                throw Status.INTERNAL
                        .withDescription("Invalid ExternalPublicationLedger transition: "
                                + valid.violations())
                        .asRuntimeException();
            }

            String datasourceId = saveOwner.hasIntake()
                    ? saveOwner.getIntake().getDatasourceId()
                    : ExternalPublicationCoordinates.DEFAULT_PUBLICATIONS_DATASOURCE_ID;
            DocumentSecurity effectiveSecurity =
                    (security != null && security.getPermissionsCount() > 0)
                            ? security
                            : (existing.isPresent()
                                    ? existing.get().security()
                                    : DocumentSecurity.getDefaultInstance());

            Document doc = Document.newBuilder()
                    .setDocId(ledgerAddr.getDocId())
                    .setOwnership(OwnershipContext.newBuilder()
                            .setAccountId(sourceAddress.getAccountId())
                            .setDatasourceId(datasourceId)
                            .setSecurity(effectiveSecurity))
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle(bindingId + "->" + truncateTitle(sourceAddress.getDocId()))
                            .setDocumentType("external-publication-ledger")
                            .setCreationDate(timestamp(clock.instant())))
                    .setStructuredData(Any.pack(next))
                    .build();

            String repoSaveOpId = truncateOpId(saveOperationPrefix + "#" + attempt);
            try {
                documents.saveDocument(SaveDocumentRequest.newBuilder()
                        .setOperationId(repoSaveOpId)
                        .setTarget(DocumentTarget.newBuilder().setAddress(ledgerAddr))
                        .setExpected(expected)
                        .setDocument(doc)
                        .addWrites(CORE_PUT)
                        .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                        .setDrive(drive)
                        .setOwner(saveOwner)
                        .build());
                return next;
            } catch (StatusRuntimeException conflict) {
                Status.Code code = conflict.getStatus().getCode();
                if (code == Status.Code.FAILED_PRECONDITION
                        || code == Status.Code.ABORTED
                        || code == Status.Code.ALREADY_EXISTS) {
                    continue;
                }
                throw conflict;
            }
        }
        throw Status.ABORTED
                .withDescription("Exceeded optimistic concurrency retries updating ExternalPublicationLedger")
                .asRuntimeException();
    }

    private Optional<LoadedLedger> loadOptionalLedger(
            NodeAddress ledgerAddr, NodeAddress expectedSourceOrNull) {
        try {
            GetDocumentResponse resp = documents.getDocument(
                    GetDocumentRequest.newBuilder()
                            .setAddress(ledgerAddr)
                            .setSelection(CORE_SELECTION)
                            .build());
            Document doc = resp.getDocument();
            if (!doc.hasStructuredData()) {
                throw Status.DATA_LOSS
                        .withDescription("Ledger document missing structured_data")
                        .asRuntimeException();
            }
            ExternalPublicationLedger unpacked = doc.getStructuredData()
                    .unpack(ExternalPublicationLedger.class);
            if (expectedSourceOrNull != null
                    && !unpacked.getSourceAddress().equals(expectedSourceOrNull)) {
                throw Status.DATA_LOSS
                        .withDescription("Ledger source_address collision or corruption detected")
                        .asRuntimeException();
            }
            return Optional.of(new LoadedLedger(
                    unpacked,
                    resp.getManifest().getRevision(),
                    doc.getOwnership().getSecurity()));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        } catch (InvalidProtocolBufferException corrupt) {
            throw Status.DATA_LOSS
                    .withDescription("Failed unpacking ExternalPublicationLedger")
                    .withCause(corrupt)
                    .asRuntimeException();
        }
    }

    private static boolean isSourceRevisionFenced(
            ExternalPublicationLedger ledger, long candidateRevision) {
        if (ledger.getSourcePurged()) {
            return true;
        }
        if (ledger.hasTombstonedAtSourceRevision()
                && candidateRevision <= ledger.getTombstonedAtSourceRevision()) {
            return true;
        }
        return candidateRevision < ledger.getHighestFencedSourceRevision();
    }

    private static boolean isTerminalOutcome(ExternalPublicationOutcome outcome) {
        return outcome == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED
                || outcome == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED
                || outcome == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED;
    }

    private static Optional<ExternalPublicationOperationRecord> findOperation(
            ExternalPublicationLedger ledger, String operationId) {
        for (ExternalPublicationOperationRecord record : ledger.getOperationsList()) {
            if (record.getOperationId().equals(operationId)) {
                return Optional.of(record);
            }
        }
        return Optional.empty();
    }

    private static ExternalPublicationLedger replaceOperation(
            ExternalPublicationLedger ledger, ExternalPublicationOperationRecord updated) {
        ExternalPublicationLedger.Builder builder = ledger.toBuilder().clearOperations();
        boolean replaced = false;
        for (ExternalPublicationOperationRecord existing : ledger.getOperationsList()) {
            if (existing.getOperationId().equals(updated.getOperationId())) {
                builder.addOperations(updated);
                replaced = true;
            } else {
                builder.addOperations(existing);
            }
        }
        if (!replaced) {
            builder.addOperations(updated);
        }
        return builder.build();
    }

    private static ExternalPublicationOperationRecord.Builder replaceAttempt(
            ExternalPublicationOperationRecord op, ExternalPublicationAttempt updatedAttempt) {
        ExternalPublicationOperationRecord.Builder builder = op.toBuilder().clearAttempts();
        boolean replaced = false;
        for (ExternalPublicationAttempt existing : op.getAttemptsList()) {
            if (existing.getAttemptNumber() == updatedAttempt.getAttemptNumber()) {
                builder.addAttempts(updatedAttempt);
                replaced = true;
            } else {
                builder.addAttempts(existing);
            }
        }
        if (!replaced) {
            builder.addAttempts(updatedAttempt);
        }
        return builder;
    }

    private static ExternalAttemptTrigger lastAttemptTrigger(
            ExternalPublicationOperationRecord op) {
        if (op.getAttemptsCount() == 0) {
            return ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL;
        }
        return op.getAttempts(op.getAttemptsCount() - 1).getTrigger();
    }

    private static Timestamp lastAttemptStartedAt(
            ExternalPublicationOperationRecord op, Timestamp fallback) {
        if (op.getAttemptsCount() == 0) {
            return fallback;
        }
        return op.getAttempts(op.getAttemptsCount() - 1).getStartedAt();
    }

    private static Optional<ExternalLink> optionalCurrentLink(ExternalPublicationLedger ledger) {
        return ledger.hasCurrentLink() ? Optional.of(ledger.getCurrentLink()) : Optional.empty();
    }

    private static ExternalPartDigest toExternalPartDigest(
            PartSelector selector, String sha256, long sizeBytes) {
        if (selector.hasPart()) {
            ai.protomolt.proto.repo.v1.DocumentPart v1Part = switch (selector.getPart()) {
                case DOCUMENT_PART_CORE -> ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE;
                case DOCUMENT_PART_BLOBS -> ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_BLOBS;
                case DOCUMENT_PART_PARSED -> ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_PARSED;
                default -> throw new IllegalArgumentException("Unexpected direct part: " + selector);
            };
            return ExternalPartDigest.newBuilder()
                    .setPart(v1Part)
                    .setSubKey("")
                    .setSha256(sha256)
                    .setSizeBytes(sizeBytes)
                    .build();
        }
        if (selector.hasSegmentationId()) {
            return ExternalPartDigest.newBuilder()
                    .setPart(ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CHUNKS)
                    .setSubKey(selector.getSegmentationId())
                    .setSha256(sha256)
                    .setSizeBytes(sizeBytes)
                    .build();
        }
        if (selector.hasEmbeddingSet()) {
            EmbeddingSetKey key = selector.getEmbeddingSet();
            return ExternalPartDigest.newBuilder()
                    .setPart(ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_EMBEDDINGS)
                    .setSubKey(key.getSegmentationId() + ":" + key.getEmbeddingSetId())
                    .setSha256(sha256)
                    .setSizeBytes(sizeBytes)
                    .build();
        }
        throw new IllegalArgumentException("Unrecognized PartSelector: " + selector);
    }

    private static Set<String> collectRetainedSchemaDigests(RevisionValidation validation) {
        Set<String> digests = new LinkedHashSet<>();
        walkSchemaRefs(validation, digests);
        return digests;
    }

    private static void walkSchemaRefs(Message message, Set<String> digests) {
        if (message instanceof RepositorySchemaAssetReference ref) {
            digests.add(ref.getDescriptorSha256());
            return;
        }
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry :
                message.getAllFields().entrySet()) {
            if (entry.getKey().getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (entry.getKey().isRepeated()) {
                for (Object elem : (List<?>) entry.getValue()) {
                    walkSchemaRefs((Message) elem, digests);
                }
            } else {
                walkSchemaRefs((Message) entry.getValue(), digests);
            }
        }
    }

    private static String sanitizeMessage(String raw) {
        if (raw == null || raw.isBlank()) {
            return "External provider failure";
        }
        StringBuilder clean = new StringBuilder(Math.min(raw.length(), 512));
        for (int i = 0; i < raw.length() && clean.length() < 512; i++) {
            char ch = raw.charAt(i);
            if (ch == '\0' || ch == '\r' || ch == '\n') {
                clean.append(' ');
            } else {
                clean.append(ch);
            }
        }
        String result = clean.toString().trim();
        return result.isEmpty() ? "External provider failure" : result;
    }

    private static String truncateTitle(String docId) {
        return docId.length() <= 200 ? docId : docId.substring(0, 200);
    }

    private static String truncateOpId(String opId) {
        if (opId.length() <= 190) {
            return opId;
        }
        return opId.substring(0, 120) + ":" + sha256Hex(
                opId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
