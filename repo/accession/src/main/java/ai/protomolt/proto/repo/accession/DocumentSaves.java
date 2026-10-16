package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaRetention;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The v2 save (requirements §5, §7, §9; change map §3 steps S1-S15). One save
 * changes the parts it lists, carries every other part forward by reference,
 * validates the composed candidate, uploads only new bytes, and commits the
 * revision, outcome and outbox event in one catalog transaction.
 * <p>
 * This slice supports CORE, BLOBS and PARSED. Segmentations, embedding sets and
 * legacy semantic results are refused as unsupported rather than stored in CORE;
 * opaque mode is refused by host policy.
 */
public final class DocumentSaves {
    static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final Map<DocumentPart, ai.protomolt.proto.repo.v1.DocumentPart> PARTS = Map.of(
            DocumentPart.DOCUMENT_PART_CORE, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE,
            DocumentPart.DOCUMENT_PART_BLOBS, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_BLOBS,
            DocumentPart.DOCUMENT_PART_PARSED, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_PARSED);

    /** Named drives and the stack each currently resolves to. */
    public record Drives(String defaultDrive, Map<String, Stack> stacks) {
        public Drives {
            stacks = Map.copyOf(stacks);
            if (!stacks.containsKey(defaultDrive)) throw new IllegalArgumentException("Default drive has no stack");
        }
        Stack resolve(String drive) {
            var stack = stacks.get(drive.isEmpty() ? defaultDrive : drive);
            if (stack == null) throw malformedException("Unknown drive " + drive);
            return stack;
        }
        Stack byId(String stackId) {
            return stacks.values().stream().filter(stack -> stack.stackId().equals(stackId)).findFirst()
                    .orElseThrow(() -> new AccessionException(Status.Code.UNAVAILABLE,
                            "Stack " + stackId + " is not configured on this host", null));
        }
    }

    private final Catalog catalog;
    private final Drives drives;
    private final DocumentSchemaAdmission.Resolver resolver;
    private final DocumentSchemaAdmission.Limits limits;
    private final DocumentAdmissionReservations reservations;
    private final Clock clock;
    private final DocumentSchemas schemas;
    private final Map<String, Descriptors.Descriptor> types = new ConcurrentHashMap<>();
    private final WriterAuthority authority;

    /** A host without a schema registry: the compiled contract is the only Document version. */
    public DocumentSaves(Catalog catalog, Drives drives, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Clock clock) {
        this(catalog, drives, resolver, limits, reservations, clock, DocumentSchemas.compiled());
    }

    /**
     * @param schemas the latest registered {@code Document} version, asked on every save
     */
    public DocumentSaves(Catalog catalog, Drives drives, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Clock clock,
            DocumentSchemas schemas) {
        this.catalog = Objects.requireNonNull(catalog);
        this.drives = Objects.requireNonNull(drives);
        this.resolver = Objects.requireNonNull(resolver);
        this.limits = Objects.requireNonNull(limits);
        this.reservations = Objects.requireNonNull(reservations);
        this.clock = Objects.requireNonNull(clock);
        this.schemas = Objects.requireNonNull(schemas);
        this.authority = new WriterAuthority(catalog, DocumentSchemaRetention.retain(
                BuiltinDocumentSchema.definition(WriterGrant.getDescriptor())));
    }

    /** Seam for tests that need another container identity as the only registered version. */
    DocumentSaves(Catalog catalog, Drives drives, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Clock clock,
            DocumentSchemaAdmission.Definition containerDefinition) {
        this(catalog, drives, resolver, limits, reservations, clock, () -> containerDefinition);
    }

    public SaveDocumentResponse save(RepositoryCaller caller, SaveDocumentRequest request) {
        try {
            return saveChecked(caller, request);
        } catch (ai.protomolt.proto.repo.catalog.CatalogDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (CatalogException failure) {
            if (failure.unavailable()) throw new AccessionException(Status.Code.UNAVAILABLE, "Catalog unavailable", null, failure);
            throw failure;
        } catch (RepositoryException failure) {
            throw DocumentReads.translate(failure);
        }
    }

    private SaveDocumentResponse saveChecked(RepositoryCaller caller, SaveDocumentRequest request) {
        // S1: request structure, before anything is looked up or recorded.
        var structure = VALIDATOR.validate(request);
        if (!structure.valid()) throw malformedException("Invalid save request: " + structure.violations().getFirst());
        requireNoUnknownCommandFields(request);
        DocumentReads.requireBinding(caller);
        var writes = selectors(request);
        var document = normalizedDocument(request);

        // S2: the latest registered Document version decodes the content (decisions 34, 35).
        // An outage is UNAVAILABLE and records nothing; the save names no writer version.
        final DocumentSchemaAdmission.Definition container;
        final Descriptors.Descriptor documentType;
        try {
            container = schemas.latest();
            documentType = documentType(container);
        } catch (DocumentCandidateAdmission.SchemaUnavailable unavailable) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Schema registry unavailable", null, unavailable);
        } catch (DocumentCandidateAdmission.HostFailure host) {
            throw new AccessionException(Status.Code.INTERNAL, "Document schema host failure: " + host.getMessage(), null, host);
        }

        // S3-S4: call number and account scope.
        var address = resolveAddress(request, document);
        var account = address.getAccountId();
        DocumentReads.requireAccount(caller, account);

        // S6-S7: content identity, intent, and the early outcome lookup. The intent binds the
        // client's fragments, raw blob bytes included; the stored BLOBS fragment leaves them out.
        var fragments = split(document, documentType, writes);
        var storedFragments = new EnumMap<DocumentPart, ByteString>(fragments);
        List<RawBlobs.Raw> rawBlobs = List.of();
        if (fragments.containsKey(DocumentPart.DOCUMENT_PART_BLOBS)) {
            var extracted = RawBlobs.extract(fragments.get(DocumentPart.DOCUMENT_PART_BLOBS));
            storedFragments.put(DocumentPart.DOCUMENT_PART_BLOBS, extracted.stored());
            rawBlobs = extracted.blobs();
        }
        var contents = new HashMap<Integer, SaveIntents.Content>();
        for (int index = 0; index < request.getWritesCount(); index++) {
            var write = request.getWrites(index);
            if (write.getAction() != WriteAction.WRITE_ACTION_PUT) continue;
            var bytes = fragments.get(write.getSelector().getPart());
            contents.put(index, new SaveIntents.Content(sha256(bytes), bytes.size()));
        }
        var intentSha = SaveIntents.sha256(SaveIntents.intent(request, address, contents));
        var recorded = catalog.outcome(account, request.getOperationId());
        if (recorded.isPresent()) return replay(caller, request, intentSha, recorded.get());

        // S5, S8, S9: ownership, write policy and writer authority, against the current revision.
        var current = catalog.current(address);
        boolean creating = request.getExpected().hasNone();
        if (!creating && current.isEmpty()) return conflict(caller, request, intentSha, address);
        var base = current.map(state -> catalog.manifest(address, state.revision())
                .orElseThrow(() -> new IllegalStateException("Current revision has no manifest")));
        if (base.isPresent() && !DocumentAccessPolicy.allows(caller, account, base.get().getOwnership().getSecurity(),
                null, Access.ACCESS_WRITE))
            throw WriterAuthority.denied("The caller may not write this document");
        var ownership = ownership(document, address, base,
                writes.get(DocumentPart.DOCUMENT_PART_CORE) == WriteAction.WRITE_ACTION_PUT);
        requireOwnerMatches(request.getOwner(), ownership);
        var authorized = authority.authorize(caller, request.getOwner(), account,
                ownership.hasConnectorId() ? Optional.of(ownership.getConnectorId()) : Optional.empty(), creating);
        if (caller.processAuthority() && request.getOpaqueAttestationsCount() > 0)
            throw WriterAuthority.denied("Process authority may not attest opaque content");

        // The writer is authorized: from here on refusals are recorded outcomes.
        long currentRevision = current.map(CatalogRecords.Current::revision).orElse(0L);
        if (creating ? current.isPresent() : currentRevision != request.getExpected().getRevision())
            return conflict(caller, request, intentSha, address);
        if (request.getValidationMode() == ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
            return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_OPAQUE_NOT_ALLOWED,
                    "Host policy does not allow opaque content", List.of());

        // S10: compose the candidate from carried references, sources and writes.
        long revision = currentRevision + 1;
        var parts = new EnumMap<DocumentPart, StoredPartEntry>(DocumentPart.class);
        base.ifPresent(stored -> stored.getPartsList().forEach(part -> parts.put(part.getEntry().getSelector().getPart(), part)));
        var carried = new HashSet<>(parts.keySet());
        var changes = new ArrayList<PartChange>();
        var sourced = new LinkedHashMap<DocumentPart, StackRef>();
        for (var source : request.getSourcesList()) {
            var part = source.getSelector().getPart();
            var from = readableSource(caller, account, source.getFromAddress(), source.getFromRevision());
            if (from.isEmpty())
                return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND,
                        "Source revision " + source.getFromRevision() + " is not readable", List.of());
            var entry = from.get().getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart() == part).findFirst();
            if (entry.isEmpty())
                return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND,
                        "Source revision has no " + part + " part", List.of());
            var copied = entry.get().toBuilder();
            copied.getEntryBuilder().setWrittenInRevision(revision).setOwner(request.getOwner()).clearContentIncluded();
            parts.put(part, copied.build());
            carried.remove(part);
            if (entry.get().hasRef()) sourced.put(part, entry.get().getRef());
            changes.add(change(part, ChangeKind.CHANGE_KIND_SOURCED, entry.get().getEntry().getSha256()));
        }
        for (var lineage : request.getDerivedFromList()) {
            if (readableSource(caller, account, lineage.getAddress(), lineage.getRevision()).isEmpty())
                return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND,
                        "Lineage revision " + lineage.getRevision() + " is not readable", List.of());
        }
        for (var write : request.getWritesList()) {
            var part = write.getSelector().getPart();
            var previous = parts.get(part);
            carried.remove(part);
            var entry = PartManifestEntry.newBuilder().setSelector(write.getSelector()).setWrittenInRevision(revision)
                    .setOwner(request.getOwner());
            switch (write.getAction()) {
                case WRITE_ACTION_PUT -> {
                    var bytes = storedFragments.get(part);
                    entry.setState(PartState.PART_STATE_PRESENT).setSizeBytes(bytes.size()).setSha256(sha256(bytes));
                    changes.add(change(part, previous != null && previous.getEntry().getState() == PartState.PART_STATE_PRESENT
                            ? ChangeKind.CHANGE_KIND_REPLACED : ChangeKind.CHANGE_KIND_ADDED, entry.getSha256()));
                }
                case WRITE_ACTION_EMPTY -> {
                    entry.setState(PartState.PART_STATE_EMPTY);
                    changes.add(change(part, ChangeKind.CHANGE_KIND_EMPTIED, ""));
                }
                case WRITE_ACTION_DELETE -> {
                    entry.setState(PartState.PART_STATE_DELETED).setDeletedReason(write.getDeletedReason());
                    changes.add(change(part, ChangeKind.CHANGE_KIND_DELETED, ""));
                }
                default -> throw malformedException("Unsupported write action");
            }
            parts.put(part, StoredPartEntry.newBuilder().setEntry(entry).build());
        }
        if (!parts.containsKey(DocumentPart.DOCUMENT_PART_CORE)
                || parts.get(DocumentPart.DOCUMENT_PART_CORE).getEntry().getState() != PartState.PART_STATE_PRESENT)
            throw malformedException("A document revision needs a PRESENT CORE part");

        // S11: typed validation of written and sourced bytes. Carried parts are not read.
        var admitted = new ArrayList<DocumentCandidateAdmission.Fragment>();
        for (var write : request.getWritesList())
            if (write.getAction() == WriteAction.WRITE_ACTION_PUT)
                admitted.add(new DocumentCandidateAdmission.Fragment(PARTS.get(write.getSelector().getPart()),
                        storedFragments.get(write.getSelector().getPart())));
        ByteString sourcedCore = null;
        for (var entry : sourced.entrySet()) {
            var bytes = readVerified(entry.getValue());
            admitted.add(new DocumentCandidateAdmission.Fragment(PARTS.get(entry.getKey()), bytes));
            if (entry.getKey() == DocumentPart.DOCUMENT_PART_CORE) sourcedCore = bytes;
        }
        var validation = RevisionValidation.newBuilder().setStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                .setValidatorProfile(DocumentCandidateAdmission.PROFILE);
        Map<String, ByteString> artifacts = new HashMap<>(authority.grantSchema().artifacts());
        RepositorySchemaAssetReference fragmentSchema = null;
        if (!admitted.isEmpty()) {
            try (var result = DocumentCandidateAdmission.check(
                    new DocumentCandidateAdmission.Request(address.getDocId(), admitted, container),
                    resolver, limits, reservations, clock.instant(), () -> {})) {
                if (result instanceof DocumentCandidateAdmission.Rejected rejected) {
                    var violations = rejected.violations().stream().map(v -> FieldViolation.newBuilder()
                            .setPart(PartSelector.newBuilder().setPart(v2(v.part()))).setFieldPath(v.fieldPath())
                            .setRuleId(v.ruleId()).setMessage(v.message()).build()).toList();
                    return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
                            "The composed document failed validation", violations);
                }
                // Decision 47: use the same verified and admitted CORE bytes for ownership.
                // The current ACL and writer grant were checked above; the fixed-field check
                // keeps their account/datasource/connector scope valid. No second fetch or PUT.
                if (sourcedCore != null) {
                    var core = DocumentPartCodec.assemble(List.of(sourcedCore.toByteArray()),
                            Document.getDefaultInstance());
                    ownership = ownership(core, address, base, true);
                    requireOwnerMatches(request.getOwner(), ownership);
                }
                var accepted = (DocumentCandidateAdmission.Accepted) result;
                fragmentSchema = accepted.container();
                for (var occurrence : accepted.occurrences())
                    validation.addResolvedOccurrences(ResolvedOccurrence.newBuilder()
                            .setOccurrence(OccurrenceLocator.newBuilder()
                                    .setPart(PartSelector.newBuilder().setPart(v2(occurrence.part())))
                                    .addAllSteps(occurrence.steps()))
                            .setSchema(occurrence.schema()).setPayloadSha256(occurrence.payloadSha256())
                            .setPayloadLength(occurrence.payloadLength()));
                artifacts.putAll(accepted.artifacts());
            } catch (DocumentCandidateAdmission.SchemaMissing missing) {
                return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_SCHEMA_MISSING,
                        missing.getMessage(), List.of());
            } catch (DocumentCandidateAdmission.InvalidPayload invalid) {
                return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_INVALID_BYTES,
                        invalid.getMessage(), List.of());
            } catch (DocumentCandidateAdmission.SchemaUnavailable unavailable) {
                throw new AccessionException(Status.Code.UNAVAILABLE, "Schema registry unavailable", null, unavailable);
            } catch (DocumentCandidateAdmission.ContentRefused refused) {
                return reject(caller, request, address, intentSha, RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
                        "The composed document cannot be admitted: " + refused.getMessage(), List.of());
            } catch (DocumentCandidateAdmission.BudgetExhausted budget) {
                throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                        "Admission schema budget exhausted: " + budget.getMessage(), null, budget);
            } catch (DocumentCandidateAdmission.HostFailure | InvalidProtocolBufferException host) {
                // Not the caller's content: surface it, never as a recorded outcome against the operation ID.
                throw new AccessionException(Status.Code.INTERNAL, "Admission host failure: " + host.getMessage(), null, host);
            }
        }
        // Gap 9: carried parts keep the expected revision's schema entries; their bytes are unchanged.
        base.ifPresent(stored -> stored.getValidation().getResolvedOccurrencesList().stream()
                .filter(occurrence -> carried.contains(occurrence.getOccurrence().getPart().getPart()))
                .forEach(validation::addResolvedOccurrences));

        // S12: write new bytes to the drive's stack, before the commit transaction. Each raw
        // blob is its own object, bound to its blob ID on the BLOBS entry (decision 45).
        var stack = drives.resolve(request.getDrive());
        var newObjects = new ArrayList<StackRef>();
        var blobRefs = new ArrayList<BlobObjectRef>();
        for (var raw : rawBlobs) {
            var ref = put(stack, raw.data());
            newObjects.add(ref);
            blobRefs.add(BlobObjectRef.newBuilder().setBlobId(raw.blobId()).setRef(ref).build());
        }
        for (var write : request.getWritesList()) {
            if (write.getAction() != WriteAction.WRITE_ACTION_PUT) continue;
            var part = write.getSelector().getPart();
            var ref = put(stack, storedFragments.get(part));
            newObjects.add(ref);
            var entry = parts.get(part).toBuilder().setRef(ref);
            if (part == DocumentPart.DOCUMENT_PART_BLOBS) entry.addAllBlobRefs(blobRefs);
            parts.put(part, entry.build());
        }
        // Carried and sourced parts reference their objects, raw blobs included, without re-upload.
        var carriedObjects = new ArrayList<StackRef>();
        for (var part : parts.values()) {
            if (part.hasRef() && !newObjects.contains(part.getRef())) carriedObjects.add(part.getRef());
            for (var blob : part.getBlobRefsList())
                if (!newObjects.contains(blob.getRef())) carriedObjects.add(blob.getRef());
        }

        // S13-S15: build the records for this revision and commit them together.
        var ordered = parts.values().stream()
                .sorted((left, right) -> Integer.compare(left.getEntry().getSelector().getPartValue(),
                        right.getEntry().getSelector().getPartValue())).toList();
        var digest = CandidateDigestInput.newBuilder().setEncodingVersion(1);
        for (var part : ordered) {
            var entry = part.getEntry();
            digest.addParts(CandidatePart.newBuilder().setSelector(entry.getSelector()).setState(entry.getState())
                    .setSha256(entry.getSha256()).setLength(entry.getSizeBytes()));
            if (entry.getState() != PartState.PART_STATE_PRESENT) continue;
            // Gap 9: a carried part keeps the binding it was admitted under; written and sourced
            // parts were decoded with the latest registered Document version.
            var binding = carried.contains(entry.getSelector().getPart())
                    ? base.orElseThrow().getValidation().getPartSchemasList().stream()
                            .filter(schema -> schema.getPart().equals(entry.getSelector())).findFirst()
                            .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS,
                                    "Carried part has no retained schema binding", null))
                    : PartSchemaBinding.newBuilder().setPart(entry.getSelector())
                            .setFragmentSchema(Objects.requireNonNull(fragmentSchema, "admitted part schema")).build();
            validation.addPartSchemas(binding);
        }
        validation.setCandidateSha256(sha256(digest.build().toByteString()));
        authorized.grant().ifPresent(validation::setWriterGrant);
        authorized.process().ifPresent(validation::setProcessAuthority);
        var committedAt = timestamp(clock.instant());
        var stored = StoredManifest.newBuilder().setAddress(address).setRevision(revision)
                .setPreviousRevision(currentRevision).addAllParts(ordered).setValidation(validation)
                .setOperationId(request.getOperationId()).setIntentSha256(intentSha).setOwner(request.getOwner())
                .addAllDerivedFrom(request.getDerivedFromList()).setOwnership(ownership).setCommittedAt(committedAt)
                .putAllMetadata(request.getMetadataMap()).build();
        var accession = DocumentReads.accession(stored);
        var outcome = RecordedOutcome.newBuilder().setAccession(accession).setRecordedAt(committedAt).build();
        changes.sort((left, right) -> Integer.compare(left.getSelector().getPartValue(), right.getSelector().getPartValue()));
        var event = DocumentEvent.newBuilder().setEventId(eventId(address, revision)).setAddress(address)
                .setRevision(revision).setPreviousRevision(currentRevision).setOperationId(request.getOperationId())
                .setOccurredAt(committedAt).setAccessioned(Accessioned.newBuilder().addAllChanges(changes)
                        .setValidationStatus(validation.getStatus())).build();
        for (Message record : List.of(stored, accession, event)) {
            var checked = VALIDATOR.validate(record);
            if (!checked.valid()) throw new IllegalStateException("Built record is invalid: " + checked.violations().getFirst());
        }
        var result = catalog.commit(new CatalogRecords.Commit(address, request.getOperationId(), intentSha,
                request.getExpected(), stored, outcome, event, newObjects, carriedObjects, artifacts,
                authorized.grantIds(), authorized.credential()));
        return switch (result) {
            case CatalogRecords.Committed ignored -> SaveDocumentResponse.newBuilder()
                    .setOperationId(request.getOperationId()).setAccession(accession).build();
            case CatalogRecords.Recorded again -> replay(caller, request, intentSha, again.outcome());
            case CatalogRecords.Conflict conflict -> throw aborted(address, request.getExpected(), conflict.currentRevision(),
                    conflict.currentTombstone());
            case CatalogRecords.SourcePurged purged -> reject(caller, request, address, intentSha,
                    RejectionReason.REJECTION_REASON_SOURCE_PURGED, "A carried object is purged", List.of());
            case CatalogRecords.WriteDenied denied -> throw WriterAuthority.denied(denied.reason());
        };
    }

    // ------------------------------------------------------------------ structure

    private static void requireNoUnknownCommandFields(SaveDocumentRequest request) {
        var pending = new java.util.ArrayDeque<Message>();
        pending.add(request);
        while (!pending.isEmpty()) {
            var message = pending.removeFirst();
            if (!message.getUnknownFields().asMap().isEmpty())
                throw malformedException("Unsupported field in the save command: " + message.getDescriptorForType().getName());
            for (var entry : message.getAllFields().entrySet()) {
                var field = entry.getKey();
                // Content has its own schema admission rules. This walk checks
                // only commands, including nested owners, selectors and locators.
                if (message == request && field.getNumber() == SaveDocumentRequest.DOCUMENT_FIELD_NUMBER) continue;
                if (field.getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
                if (field.isRepeated()) {
                    for (var child : (List<?>) entry.getValue()) pending.add((Message) child);
                } else {
                    pending.add((Message) entry.getValue());
                }
            }
        }
    }

    /** Validates the write and source selectors and returns the written parts. */
    private static Map<DocumentPart, WriteAction> selectors(SaveDocumentRequest request) {
        var writes = new EnumMap<DocumentPart, WriteAction>(DocumentPart.class);
        for (var write : request.getWritesList()) {
            var part = supported(write.getSelector());
            if (writes.put(part, write.getAction()) != null) throw malformedException("Duplicate write selector " + part);
        }
        var sourced = new HashSet<DocumentPart>();
        for (var source : request.getSourcesList()) {
            var part = supported(source.getSelector());
            if (writes.containsKey(part) || !sourced.add(part))
                throw malformedException("Selector " + part + " is named twice");
            DocumentReads.requireAddress(source.getFromAddress());
            if (!source.getFromAddress().getDocId().equals(targetDocId(request)))
                throw malformedException("A source part must come from a document with the same doc_id");
        }
        for (var lineage : request.getDerivedFromList()) DocumentReads.requireAddress(lineage.getAddress());
        if (writes.isEmpty() && sourced.isEmpty()) throw malformedException("A save writes or sources at least one part");
        return writes;
    }

    private static String targetDocId(SaveDocumentRequest request) {
        return request.getTarget().hasAddress() ? request.getTarget().getAddress().getDocId() : request.getDocument().getDocId();
    }

    private static DocumentPart supported(PartSelector selector) {
        if (!selector.hasPart()) throw malformedException("Segmentations and embedding sets are not supported by this server yet");
        return selector.getPart();
    }

    private static Document normalizedDocument(SaveDocumentRequest request) {
        var document = request.getDocument();
        var search = document.getSearchMetadata();
        if (search.getSemanticResultsCount() > 0 || search.getSegmentationsCount() > 0 || search.getEmbeddingSetsCount() > 0)
            throw malformedException("Semantic results, segmentations and embedding sets are not supported by this server yet");
        // Decision 35: fields the compiled Document does not know arrive as unknown fields and
        // are decoded with the latest registered version; one that version does not know either
        // is a recorded SCHEMA_MISSING after authorization, never dropped. Structure bounds and
        // Any locations are still request structure, refused before anything is recorded.
        try {
            DocumentCandidateAdmission.requireStructure(document);
        } catch (DocumentCandidateAdmission.ContentRefused refused) {
            throw malformedException(refused.getMessage());
        }
        if (request.getTarget().hasAddress()) {
            var docId = request.getTarget().getAddress().getDocId();
            if (!document.getDocId().isEmpty() && !document.getDocId().equals(docId))
                throw malformedException("Document doc_id differs from the target address");
            document = document.toBuilder().setDocId(docId).build();
        }
        return document;
    }

    private static NodeAddress resolveAddress(SaveDocumentRequest request, Document document) {
        if (request.getTarget().hasAddress()) {
            DocumentReads.requireAddress(request.getTarget().getAddress());
            return request.getTarget().getAddress();
        }
        if (!document.hasOwnership() || document.getOwnership().getAccountId().isBlank())
            throw malformedException("An intake datasource target needs document ownership with an account");
        var account = document.getOwnership().getAccountId();
        var datasource = request.getTarget().getUseDatasourceId();
        if (!document.getOwnership().getDatasourceId().equals(datasource))
            throw malformedException("The target datasource differs from the document's ownership");
        var address = NodeAddress.newBuilder().setDocId(document.getDocId()).setGraphAddressId(datasource)
                .setAccountId(account).setGraphId("intake:" + account).build();
        DocumentReads.requireAddress(address);
        return address;
    }

    /**
     * Splits the request document, decoded with the latest registered Document version,
     * and refuses content for any part not listed as PUT. Fields that version does not
     * know stay in their fragment as unknown fields for admission to refuse.
     */
    private static Map<DocumentPart, ByteString> split(Document request, Descriptors.Descriptor documentType,
            Map<DocumentPart, WriteAction> writes) {
        final DynamicMessage document;
        try {
            document = DynamicMessage.parseFrom(documentType, request.toByteString());
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException("A parsed request document failed to parse again", impossible);
        }
        final ai.protomolt.proto.repo.codec.PartLayout layout;
        try {
            layout = PartLayouts.document(documentType);
        } catch (IllegalArgumentException host) {
            throw new AccessionException(Status.Code.INTERNAL, "The registered Document lacks a part field: "
                    + host.getMessage(), null, host);
        }
        var prototype = DynamicMessage.getDefaultInstance(documentType);
        var fragments = new EnumMap<DocumentPart, ByteString>(DocumentPart.class);
        var bare = DynamicMessage.newBuilder(documentType).setField(documentType.findFieldByName("doc_id"),
                request.getDocId()).build().toByteString();
        for (var split : DocumentPartCodec.split(document, layout)) {
            // Deterministic bytes: a client sending map entries in another order must get
            // the same content digest, the same intent, and a replay on retry.
            final ai.protomolt.proto.repo.codec.PartObject part;
            try {
                part = DocumentPartCodec.deterministic(split, prototype);
            } catch (InvalidProtocolBufferException impossible) {
                throw new IllegalStateException("A freshly split fragment failed to parse", impossible);
            }
            var selector = switch (part.part()) {
                case DOCUMENT_PART_CORE -> DocumentPart.DOCUMENT_PART_CORE;
                case DOCUMENT_PART_BLOBS -> DocumentPart.DOCUMENT_PART_BLOBS;
                case DOCUMENT_PART_PARSED -> DocumentPart.DOCUMENT_PART_PARSED;
                default -> throw malformedException("Unsupported document part " + part.part());
            };
            var bytes = ByteString.copyFrom(part.bytes());
            boolean put = writes.get(selector) == WriteAction.WRITE_ACTION_PUT;
            if (!put && !(selector == DocumentPart.DOCUMENT_PART_CORE && bytes.equals(bare)))
                throw malformedException("Content for " + selector + " is present but the part is not written");
            if (put) fragments.put(selector, bytes);
        }
        for (var write : writes.entrySet())
            if (write.getValue() == WriteAction.WRITE_ACTION_PUT && !fragments.containsKey(write.getKey()))
                throw malformedException("PUT " + write.getKey() + " has no content; write EMPTY instead");
        return fragments;
    }

    /**
     * The revision's ownership: from a CORE write (PUT or source), else carried. Its account, datasource and
     * connector are fixed at creation; any authorized writer may change security and source
     * owner (decision 30).
     */
    private static OwnershipContext ownership(Document document, NodeAddress address,
            Optional<StoredManifest> base, boolean corePut) {
        if (base.isEmpty() && (!corePut || !document.hasOwnership()))
            throw malformedException("Creating a document needs a CORE PUT with ownership");
        var ownership = corePut ? document.getOwnership() : base.get().getOwnership();
        if (!ownership.getAccountId().equals(address.getAccountId()))
            throw malformedException("Document ownership names another account");
        // Decision 30: the account, datasource and connector are fixed at creation for every
        // caller, process authority included. Security and source_owner may be changed by any
        // authorized writer; the write check under the current ACL has already run.
        if (base.isPresent() && corePut) {
            var fixed = base.get().getOwnership();
            boolean sameConnector = ownership.hasConnectorId() == fixed.hasConnectorId()
                    && (!fixed.hasConnectorId() || ownership.getConnectorId().equals(fixed.getConnectorId()));
            if (!ownership.getAccountId().equals(fixed.getAccountId())
                    || !ownership.getDatasourceId().equals(fixed.getDatasourceId()) || !sameConnector)
                throw malformedException("A document's account, datasource and connector are fixed when it is created");
        }
        return ownership;
    }

    private static void requireOwnerMatches(SaveOwner owner, OwnershipContext ownership) {
        if (owner.hasIntake() && !owner.getIntake().getDatasourceId().equals(ownership.getDatasourceId()))
            throw WriterAuthority.denied("The intake owner differs from the document's datasource");
    }

    /**
     * Source and lineage revisions follow the same current-ownership read gate as
     * {@link DocumentReads}, within the target document's account, so an ACL
     * tightening or tombstone on the source document cannot be bypassed by citing
     * an earlier revision.
     */
    private Optional<StoredManifest> readableSource(RepositoryCaller caller, String account,
            NodeAddress sourceAddress, long revision) {
        if (!sourceAddress.getAccountId().equals(account)) return Optional.empty();
        var current = catalog.current(sourceAddress)
                .flatMap(state -> catalog.manifest(sourceAddress, state.revision()));
        if (current.isEmpty() || current.get().getTombstone()
                || !DocumentReads.canRead(caller, sourceAddress, current.get().getOwnership()))
            return Optional.empty();
        var target = revision == current.get().getRevision() ? current : catalog.manifest(sourceAddress, revision);
        return target.filter(manifest -> !manifest.getTombstone());
    }

    // ------------------------------------------------------------------ outcomes

    private SaveDocumentResponse replay(RepositoryCaller caller, SaveDocumentRequest request, String intentSha,
            CatalogRecords.Outcome recorded) {
        if (!recorded.intentSha256().equals(intentSha))
            throw new AccessionException(Status.Code.ALREADY_EXISTS, "Operation ID was used with a different intent",
                    OperationReuse.newBuilder().setOperationId(request.getOperationId())
                            .setRecordedIntentSha256(recorded.intentSha256()).setRequestIntentSha256(intentSha).build());
        var outcome = recorded.outcome();
        if (outcome.hasRejection()) throw rejectionException(outcome.getRejection());
        // An authorized replay needs current read access, not the original writer grant.
        var address = outcome.getAccession().getAddress();
        var current = catalog.current(address)
                .flatMap(state -> catalog.manifest(address, state.revision()));
        if (current.isEmpty() || !DocumentReads.canRead(caller, address, current.get().getOwnership()))
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller may not read this document", null);
        return SaveDocumentResponse.newBuilder().setOperationId(request.getOperationId())
                .setAccession(outcome.getAccession()).setReplayed(true).build();
    }

    private SaveDocumentResponse conflict(RepositoryCaller caller, SaveDocumentRequest request, String intentSha,
            NodeAddress address) {
        var recorded = catalog.outcome(address.getAccountId(), request.getOperationId());
        if (recorded.isPresent()) return replay(caller, request, intentSha, recorded.get());
        var current = catalog.current(address);
        throw aborted(address, request.getExpected(), current.map(CatalogRecords.Current::revision).orElse(0L),
                current.map(CatalogRecords.Current::tombstone).orElse(false));
    }

    private SaveDocumentResponse reject(RepositoryCaller caller, SaveDocumentRequest request, NodeAddress address,
            String intentSha, RejectionReason reason, String message, List<FieldViolation> violations) {
        var rejection = SaveRejection.newBuilder().setReason(reason).setMessage(message).addAllViolations(violations).build();
        var outcome = RecordedOutcome.newBuilder().setRejection(rejection).setRecordedAt(timestamp(clock.instant())).build();
        var stored = catalog.recordRejection(address.getAccountId(), request.getOperationId(), intentSha, outcome);
        return replay(caller, request, intentSha, stored);
    }

    static AccessionException aborted(NodeAddress address, ExpectedRevision expected, long current, boolean tombstone) {
        return new AccessionException(Status.Code.ABORTED, "Expected revision is stale; current revision is " + current,
                RevisionConflict.newBuilder().setAddress(address).setExpected(expected).setCurrentRevision(current)
                        .setCurrentIsTombstone(tombstone).build());
    }

    static AccessionException rejectionException(SaveRejection rejection) {
        var code = switch (rejection.getReason()) {
            case REJECTION_REASON_MALFORMED_REQUEST, REJECTION_REASON_VALIDATION_FAILED, REJECTION_REASON_INVALID_BYTES,
                 REJECTION_REASON_UPLOAD_MISMATCH -> Status.Code.INVALID_ARGUMENT;
            case REJECTION_REASON_OPAQUE_NOT_ALLOWED, REJECTION_REASON_WRITE_DENIED -> Status.Code.PERMISSION_DENIED;
            default -> Status.Code.FAILED_PRECONDITION;
        };
        return new AccessionException(code, rejection.getMessage(), rejection);
    }

    static SaveRejection rejection(RejectionReason reason, String message) {
        return SaveRejection.newBuilder().setReason(reason).setMessage(message).build();
    }

    static SaveRejection malformed(String message) {
        return rejection(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST, message);
    }

    static AccessionException malformedException(String message) {
        return new AccessionException(Status.Code.INVALID_ARGUMENT, message, malformed(message));
    }

    // ------------------------------------------------------------------ helpers

    /** The registered Document descriptor a definition names, cached by its artifact digest. */
    private Descriptors.Descriptor documentType(DocumentSchemaAdmission.Definition container) {
        var digest = container.metadata().getArtifactSha256();
        var cached = types.get(digest);
        if (cached != null) return cached;
        var type = DocumentCandidateAdmission.containerType(container);
        if (types.size() > 64) types.clear();
        types.put(digest, type);
        return type;
    }

    /** Writes one object and checks the stack's reference against the bytes it was given. */
    private static StackRef put(Stack stack, ByteString bytes) {
        final StackRef ref;
        try {
            ref = stack.put(bytes.toByteArray());
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Stack write failed", null, failure);
        }
        if (!ref.getSha256().equals(sha256(bytes)) || ref.getLength() != bytes.size())
            throw new IllegalStateException("Stack digest differs from the bytes written");
        return ref;
    }

    private ByteString readVerified(StackRef ref) {
        try (var stream = drives.byId(ref.getStackId()).open(ref)) {
            return ByteString.readFrom(stream);
        } catch (StackDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
        }
    }

    private static PartChange change(DocumentPart part, ChangeKind kind, String sha256) {
        return PartChange.newBuilder().setSelector(PartSelector.newBuilder().setPart(part)).setKind(kind).setSha256(sha256).build();
    }

    static ai.protomolt.proto.repo.v1.DocumentPart v1(DocumentPart part) {
        var mapped = PARTS.get(part);
        if (mapped == null) throw new AccessionException(Status.Code.DATA_LOSS, "Stored part " + part + " is not supported", null);
        return mapped;
    }

    private static DocumentPart v2(ai.protomolt.proto.repo.v1.DocumentPart part) {
        return PARTS.entrySet().stream().filter(entry -> entry.getValue() == part).map(Map.Entry::getKey).findFirst()
                .orElse(DocumentPart.DOCUMENT_PART_CORE);
    }

    static String eventId(NodeAddress address, long revision) {
        var name = "accession|" + address.getAccountId() + "|" + address.getGraphId() + "|"
                + address.getGraphAddressId() + "|" + address.getDocId() + "|" + revision;
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }

    static String sha256(ByteString bytes) { return SaveIntents.sha256Hex(bytes.toByteArray()); }

    static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}
