package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAttempt;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * Loads and compare-and-swaps {@code ExternalPublicationLedger} documents in
 * {@code external-publications:<account_id>}, and holds the pure ledger and attempt
 * helpers the publication, deletion and serving-observation steps share.
 */
final class ExternalLedgerStore {

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
    private final String drive;
    private final SaveOwner saveOwner;
    private final Clock clock;

    ExternalLedgerStore(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String drive, SaveOwner saveOwner, Clock clock) {
        this.documents = documents;
        this.drive = drive;
        this.saveOwner = saveOwner;
        this.clock = clock;
    }

    record LoadedLedger(
            ExternalPublicationLedger ledger,
            long documentRevision,
            DocumentSecurity security) {
    }

    ExternalPublicationLedger appendReconciliationAttemptOnly(
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
                    if (!hasRoomForUnsettledAttempt(op)) {
                        // This lookup settled nothing, and the one attempt slot left is for
                        // the receipt that ends the operation, so it is not recorded.
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

    ExternalPublicationLedger updateLedgerWithCas(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            DocumentSecurity security,
            String saveOperationPrefix,
            UnaryOperator<ExternalPublicationLedger> mutator) {
        return updateLedger(ledgerAddr, sourceAddress, bindingId, security, false,
                saveOperationPrefix, mutator);
    }

    /**
     * As {@link #updateLedgerWithCas}, but the ledger takes {@code security} exactly as given,
     * even with no explicit rules, in place of the rules it held. A purge preparation uses this:
     * the source's rules as they are now replace whatever the ledger kept.
     */
    ExternalPublicationLedger updateLedgerReplacingSecurity(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            DocumentSecurity security,
            String saveOperationPrefix,
            UnaryOperator<ExternalPublicationLedger> mutator) {
        Objects.requireNonNull(security, "security");
        return updateLedger(ledgerAddr, sourceAddress, bindingId, security, true,
                saveOperationPrefix, mutator);
    }

    private ExternalPublicationLedger updateLedger(
            NodeAddress ledgerAddr,
            NodeAddress sourceAddress,
            String bindingId,
            DocumentSecurity security,
            boolean replaceSecurity,
            String saveOperationPrefix,
            UnaryOperator<ExternalPublicationLedger> mutator) {
        StatusRuntimeException lastConflict = null;
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
            DocumentSecurity effectiveSecurity = replaceSecurity
                    ? security
                    : (security != null && security.getPermissionsCount() > 0)
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
                    lastConflict = conflict;
                    continue;
                }
                throw conflict;
            }
        }
        throw Status.ABORTED
                .withDescription("Exceeded optimistic concurrency retries updating ExternalPublicationLedger")
                .withCause(lastConflict)
                .asRuntimeException();
    }

    Optional<LoadedLedger> loadOptionalLedger(
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

    static boolean isSourceRevisionFenced(
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

    /**
     * Whether the publication at {@code index} may have left an object at the provider that no
     * link records: it is still pending, or it was sent and its answer failed verification and
     * no verified deletion of the same object at the same place follows it on the ledger.
     * Operations are kept in registration order, so "follows" is that order, not a clock.
     */
    static boolean publicationMayHoldObject(ExternalPublicationLedger ledger, int index) {
        ExternalPublicationOperationRecord publication = ledger.getOperations(index);
        if (publication.getIntent().getAction()
                != ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH) {
            return false;
        }
        if (publication.getOutcome() == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING) {
            return true;
        }
        if (!publication.getProviderAnswerUnverified()) {
            return false;
        }
        for (int later = index + 1; later < ledger.getOperationsCount(); later++) {
            ExternalPublicationOperationRecord deletion = ledger.getOperations(later);
            if (deletion.getIntent().getAction() == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE
                    && deletion.getOutcome() == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED
                    && deletion.getIntent().getExternalObjectId()
                            .equals(publication.getIntent().getExternalObjectId())
                    && ExternalDeletionLifecycle.sameExternalTarget(
                            deletion.getBindingSnapshot(), publication.getBindingSnapshot())) {
                return false;
            }
        }
        return true;
    }

    static boolean isTerminalOutcome(ExternalPublicationOutcome outcome) {
        return outcome == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED
                || outcome == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED
                || outcome == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED;
    }

    static Optional<ExternalPublicationOperationRecord> findOperation(
            ExternalPublicationLedger ledger, String operationId) {
        for (ExternalPublicationOperationRecord record : ledger.getOperationsList()) {
            if (record.getOperationId().equals(operationId)) {
                return Optional.of(record);
            }
        }
        return Optional.empty();
    }

    static ExternalPublicationLedger replaceOperation(
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

    static ExternalPublicationOperationRecord.Builder replaceAttempt(
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

    static ExternalAttemptTrigger lastAttemptTrigger(
            ExternalPublicationOperationRecord op) {
        if (op.getAttemptsCount() == 0) {
            return ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL;
        }
        return op.getAttempts(op.getAttemptsCount() - 1).getTrigger();
    }

    static Timestamp lastAttemptStartedAt(
            ExternalPublicationOperationRecord op, Timestamp fallback) {
        if (op.getAttemptsCount() == 0) {
            return fallback;
        }
        return op.getAttempts(op.getAttemptsCount() - 1).getStartedAt();
    }

    /**
     * The operation after its provider refused attempt {@code attemptNumber}. A refused first
     * attempt fails the operation. A refused retry does not: see below. An operation already
     * settled is returned unchanged.
     */
    static ExternalPublicationOperationRecord refusedByProvider(
            ExternalPublicationOperationRecord op,
            int attemptNumber,
            Timestamp fallbackStartedAt,
            Timestamp finishedAt,
            String diagnosticCode,
            String diagnosticMessage) {
        if (isTerminalOutcome(op.getOutcome())) {
            return op;
        }
        String message = sanitizeMessage(diagnosticMessage);
        ExternalPublicationAttempt refusedAttempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(attemptNumber)
                .setTrigger(lastAttemptTrigger(op))
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .setStartedAt(lastAttemptStartedAt(op, fallbackStartedAt))
                .setFinishedAt(finishedAt)
                .setDiagnosticCode(diagnosticCode)
                .setDiagnosticMessage(message)
                .build();
        boolean earlierAttemptUnknown = false;
        for (ExternalPublicationAttempt attempt : op.getAttemptsList()) {
            earlierAttemptUnknown |= attempt.getAttemptNumber() != attemptNumber;
        }
        if (earlierAttemptUnknown) {
            // This attempt was refused, but the operation is only still open because an
            // earlier attempt was sent and never answered. That one may have gone through,
            // so the refusal is recorded against this attempt and the operation stays
            // unresolved for reconciliation to ask the provider.
            boolean deleting = op.getOutcome()
                    == ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETION_PENDING;
            return replaceAttempt(op, refusedAttempt)
                    .setPendingPhase(deleting
                            ? ExternalPendingPhase.EXTERNAL_PENDING_PHASE_DELETION_IN_FLIGHT
                            : ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                    .setUpdatedAt(finishedAt)
                    .build();
        }
        return replaceAttempt(op, refusedAttempt)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                .setFailureCode(diagnosticCode)
                .setFailureMessage(message)
                .setUpdatedAt(finishedAt)
                .build();
    }

    /** The ledger contract's bounds; see ExternalPublicationLedger and its operation record. */
    static final int MAX_OPERATIONS = 1024;
    static final int MAX_HISTORICAL_LINKS = 512;
    static final int MAX_ATTEMPTS = 256;

    /**
     * Refuses a new operation the ledger could not record to its end. Registering it takes one
     * operation slot, and settling it can add one historical link. Checked before anything is
     * sent to the provider: an operation that ran externally and then could not be written
     * down would be unrecoverable.
     *
     * @throws io.grpc.StatusRuntimeException RESOURCE_EXHAUSTED when either bound is reached
     */
    static void requireRoomForNewOperation(ExternalPublicationLedger ledger) {
        if (ledger.getOperationsCount() >= MAX_OPERATIONS) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription("External publication ledger holds " + MAX_OPERATIONS
                            + " operations and cannot record another")
                    .asRuntimeException();
        }
        // Every operation still outstanding may add a historical link when it settles, so
        // their room is spoken for.
        int outstanding = 0;
        for (ExternalPublicationOperationRecord operation : ledger.getOperationsList()) {
            if (!isTerminalOutcome(operation.getOutcome())) {
                outstanding++;
            }
        }
        if (ledger.getHistoricalLinksCount() + outstanding >= MAX_HISTORICAL_LINKS) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription("External publication ledger holds " + MAX_HISTORICAL_LINKS
                            + " historical links and cannot record the outcome of another operation")
                    .asRuntimeException();
        }
    }

    /**
     * Refuses to send an operation again when recording that attempt would leave no room for
     * the attempt that finally settles it. The last slot is kept for reconciliation.
     *
     * @throws io.grpc.StatusRuntimeException RESOURCE_EXHAUSTED when only that slot is left
     */
    static void requireRoomForRetry(ExternalPublicationOperationRecord op) {
        if (!hasRoomForUnsettledAttempt(op)) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription("Operation '" + op.getOperationId() + "' has "
                            + op.getAttemptsCount() + " attempts; it can only be reconciled now, "
                            + "not sent again")
                    .asRuntimeException();
        }
    }

    /** True while an attempt that settles nothing may still be appended to {@code op}. */
    static boolean hasRoomForUnsettledAttempt(ExternalPublicationOperationRecord op) {
        return op.getAttemptsCount() < MAX_ATTEMPTS - 1;
    }

    static Optional<ExternalLink> optionalCurrentLink(ExternalPublicationLedger ledger) {
        return ledger.hasCurrentLink() ? Optional.of(ledger.getCurrentLink()) : Optional.empty();
    }

    static String sanitizeMessage(String raw) {
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
        if (result.isEmpty()) {
            return "External provider failure";
        }
        if (ExternalPublicationCoordinates.containsCredentialMaterial(raw)) {
            // Providers and transports echo requests into their errors. The ledger is read by
            // everyone with access to the source, so a message that reads as carrying a
            // credential is not stored at all; the diagnostic code beside it still is.
            return WITHHELD_DIAGNOSTIC;
        }
        return result;
    }

    /** Stored in place of a diagnostic message that looked like it carried a credential. */
    static final String WITHHELD_DIAGNOSTIC =
            "Message withheld: it contained credential-like text";

    static String truncateTitle(String docId) {
        return docId.length() <= 200 ? docId : docId.substring(0, 200);
    }

    static String truncateOpId(String opId) {
        if (opId.length() <= 190) {
            return opId;
        }
        return opId.substring(0, 120) + ":" + sha256Hex(
                opId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
