package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaRetention;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.codec.SegmentationDigests;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
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
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
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
 * Parts are CORE, BLOBS and PARSED, one CHUNKS object per segmentation and one
 * EMBEDDINGS object per embedding set (requirements §5). Legacy semantic results
 * (field 13) are refused as unsupported; opaque mode is refused by host policy.
 * <p>
 * Semantic parts are checked over the composed candidate by
 * {@link DocumentSemanticAdmission}. Carried parts are read only where that
 * needs their content (requirements §9, §15.1): a carried CORE, BLOBS or PARSED
 * part when a segmentation being verified sources text from it; a carried CHUNKS
 * part when a written embedding set binds to it (its segment IDs) or when CORE,
 * BLOBS or PARSED changes in this save (its source may be the changed text, and
 * only its content says which part that is). A carried EMBEDDINGS part is never
 * read: its binding is compared by the digests the manifest records.
 */
public final class DocumentSaves {
    static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final Map<DocumentPart, ai.protomolt.proto.repo.v1.DocumentPart> PARTS = Map.of(
            DocumentPart.DOCUMENT_PART_CORE, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE,
            DocumentPart.DOCUMENT_PART_BLOBS, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_BLOBS,
            DocumentPart.DOCUMENT_PART_PARSED, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_PARSED);
    static final PartSelector CORE_SELECTOR = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
    static final PartSelector BLOBS_SELECTOR = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS).build();
    /** Codec order: CORE, BLOBS, CHUNKS by segmentation ID, PARSED, EMBEDDINGS by key, IDs in unsigned UTF-8 order. */
    static final java.util.Comparator<PartSelector> CODEC_ORDER = (left, right) -> {
        int order = Integer.compare(rank(left), rank(right));
        if (order != 0) return order;
        var leftKey = key(left);
        var rightKey = key(right);
        for (int i = 0; i < Math.min(leftKey.size(), rightKey.size()); i++) {
            int cmp = SegmentationDigests.compareUtf8(leftKey.get(i), rightKey.get(i));
            if (cmp != 0) return cmp;
        }
        return Integer.compare(leftKey.size(), rightKey.size());
    };

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
        // client's fragments, raw blob bytes included; the stored BLOBS fragment leaves them out
        // and a stored CHUNKS fragment may gain the server-computed content digest.
        var fragments = split(document, documentType, writes);
        var storedFragments = new LinkedHashMap<PartSelector, ByteString>(fragments);
        List<RawBlobs.Raw> rawBlobs = List.of();
        if (fragments.containsKey(BLOBS_SELECTOR)) {
            var extracted = RawBlobs.extract(fragments.get(BLOBS_SELECTOR));
            storedFragments.put(BLOBS_SELECTOR, extracted.stored());
            rawBlobs = extracted.blobs();
        }
        var contents = new HashMap<Integer, SaveIntents.Content>();
        for (int index = 0; index < request.getWritesCount(); index++) {
            var write = request.getWrites(index);
            if (write.getAction() != WriteAction.WRITE_ACTION_PUT) continue;
            var bytes = fragments.get(write.getSelector());
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
        var ownership = ownership(document, address, base, writes.get(CORE_SELECTOR) == WriteAction.WRITE_ACTION_PUT);
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
            return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_OPAQUE_NOT_ALLOWED,
                    "Host policy does not allow opaque content", List.of());

        // S10: compose the candidate from carried references, sources and writes.
        long revision = currentRevision + 1;
        var parts = new LinkedHashMap<PartSelector, StoredPartEntry>();
        base.ifPresent(stored -> stored.getPartsList().forEach(part -> parts.put(part.getEntry().getSelector(), part)));
        var carried = new HashSet<>(parts.keySet());
        var kinds = new LinkedHashMap<PartSelector, ChangeKind>();
        var sourced = new LinkedHashMap<PartSelector, StackRef>();
        for (var source : request.getSourcesList()) {
            var selector = source.getSelector();
            var from = readableSource(caller, account, source.getFromAddress(), source.getFromRevision());
            if (from.isEmpty())
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND,
                        "Source revision " + source.getFromRevision() + " is not readable", List.of());
            var entry = from.get().getPartsList().stream().filter(p -> p.getEntry().getSelector().equals(selector)).findFirst();
            if (entry.isEmpty())
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND,
                        "Source revision has no " + describe(selector) + " part", List.of());
            var copied = entry.get().toBuilder();
            copied.getEntryBuilder().setWrittenInRevision(revision).setOwner(request.getOwner()).clearContentIncluded();
            parts.put(selector, copied.build());
            carried.remove(selector);
            if (entry.get().hasRef()) sourced.put(selector, entry.get().getRef());
            kinds.put(selector, ChangeKind.CHANGE_KIND_SOURCED);
        }
        for (var lineage : request.getDerivedFromList()) {
            if (readableSource(caller, account, lineage.getAddress(), lineage.getRevision()).isEmpty())
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND,
                        "Lineage revision " + lineage.getRevision() + " is not readable", List.of());
        }
        for (var write : request.getWritesList()) {
            var selector = write.getSelector();
            var previous = parts.get(selector);
            carried.remove(selector);
            var entry = PartManifestEntry.newBuilder().setSelector(selector).setWrittenInRevision(revision)
                    .setOwner(request.getOwner());
            switch (write.getAction()) {
                case WRITE_ACTION_PUT -> {
                    var bytes = storedFragments.get(selector);
                    entry.setState(PartState.PART_STATE_PRESENT).setSizeBytes(bytes.size()).setSha256(sha256(bytes));
                    kinds.put(selector, previous != null && previous.getEntry().getState() == PartState.PART_STATE_PRESENT
                            ? ChangeKind.CHANGE_KIND_REPLACED : ChangeKind.CHANGE_KIND_ADDED);
                }
                case WRITE_ACTION_EMPTY -> {
                    entry.setState(PartState.PART_STATE_EMPTY);
                    kinds.put(selector, ChangeKind.CHANGE_KIND_EMPTIED);
                }
                case WRITE_ACTION_DELETE -> {
                    entry.setState(PartState.PART_STATE_DELETED).setDeletedReason(write.getDeletedReason());
                    kinds.put(selector, ChangeKind.CHANGE_KIND_DELETED);
                }
                default -> throw malformedException("Unsupported write action");
            }
            parts.put(selector, StoredPartEntry.newBuilder().setEntry(entry).build());
        }
        if (!parts.containsKey(CORE_SELECTOR)
                || parts.get(CORE_SELECTOR).getEntry().getState() != PartState.PART_STATE_PRESENT)
            throw malformedException("A document revision needs a PRESENT CORE part");

        // Sourced bytes are read once, for the semantic rules and for typed admission.
        var sourcedBytes = new LinkedHashMap<PartSelector, ByteString>();
        for (var entry : sourced.entrySet()) sourcedBytes.put(entry.getKey(), readVerified(entry.getValue()));

        // S10b: segmentations, source bindings and embedding sets over the composed candidate.
        var semantic = semantic(documentType, parts, writes, storedFragments, sourcedBytes);
        if (semantic.rejection() != null)
            return reject(caller, request, address, intentSha, authorized, semantic.rejection().reason(),
                    semantic.rejection().message(), semantic.rejection().violations());

        // S11: typed validation of written and sourced bytes. Carried parts are not read.
        var admitted = new ArrayList<DocumentCandidateAdmission.Fragment>();
        var admittedSelectors = new ArrayList<PartSelector>();
        for (var write : request.getWritesList())
            if (write.getAction() == WriteAction.WRITE_ACTION_PUT) {
                admitted.add(fragment(write.getSelector(), storedFragments.get(write.getSelector())));
                admittedSelectors.add(write.getSelector());
            }
        ByteString sourcedCore = null;
        for (var entry : sourcedBytes.entrySet()) {
            admitted.add(fragment(entry.getKey(), entry.getValue()));
            admittedSelectors.add(entry.getKey());
            if (entry.getKey().equals(CORE_SELECTOR)) sourcedCore = entry.getValue();
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
                            .setPart(violated(v, admittedSelectors)).setFieldPath(v.fieldPath())
                            .setRuleId(v.ruleId()).setMessage(v.message()).build()).toList();
                    return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
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
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_SCHEMA_MISSING,
                        missing.getMessage(), List.of());
            } catch (DocumentCandidateAdmission.InvalidPayload invalid) {
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_INVALID_BYTES,
                        invalid.getMessage(), List.of());
            } catch (DocumentCandidateAdmission.SchemaUnavailable unavailable) {
                throw new AccessionException(Status.Code.UNAVAILABLE, "Schema registry unavailable", null, unavailable);
            } catch (DocumentCandidateAdmission.ContentRefused refused) {
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
                        "The composed document cannot be admitted: " + refused.getMessage(), List.of());
            } catch (DocumentCandidateAdmission.BudgetExhausted budget) {
                throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                        "Admission host budget exhausted: " + budget.getMessage(), null, budget);
            } catch (DocumentCandidateAdmission.HostFailure | InvalidProtocolBufferException host) {
                // Not the caller's content: surface it, never as a recorded outcome against the operation ID.
                throw new AccessionException(Status.Code.INTERNAL, "Admission host failure: " + host.getMessage(), null, host);
            }
        }
        // Gap 9: carried parts keep the expected revision's schema entries; their bytes are unchanged.
        base.ifPresent(stored -> stored.getValidation().getResolvedOccurrencesList().stream()
                .filter(occurrence -> carried.contains(occurrence.getOccurrence().getPart()))
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
            var selector = write.getSelector();
            var ref = put(stack, storedFragments.get(selector));
            newObjects.add(ref);
            var entry = parts.get(selector).toBuilder().setRef(ref);
            if (selector.equals(BLOBS_SELECTOR)) entry.addAllBlobRefs(blobRefs);
            parts.put(selector, entry.build());
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
                .sorted((left, right) -> CODEC_ORDER.compare(left.getEntry().getSelector(), right.getEntry().getSelector()))
                .toList();
        var digest = CandidateDigestInput.newBuilder().setEncodingVersion(1);
        for (var part : ordered) {
            var entry = part.getEntry();
            digest.addParts(CandidatePart.newBuilder().setSelector(entry.getSelector()).setState(entry.getState())
                    .setSha256(entry.getSha256()).setLength(entry.getSizeBytes()));
            if (entry.getState() != PartState.PART_STATE_PRESENT) continue;
            // Gap 9: a carried part keeps the binding it was admitted under; written and sourced
            // parts were decoded with the latest registered Document version.
            var binding = carried.contains(entry.getSelector())
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
        var changes = new ArrayList<PartChange>();
        for (var change : kinds.entrySet())
            changes.add(PartChange.newBuilder().setSelector(change.getKey()).setKind(change.getValue())
                    .setSha256(parts.get(change.getKey()).getEntry().getSha256()).build());
        changes.sort((left, right) -> CODEC_ORDER.compare(left.getSelector(), right.getSelector()));
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
            case CatalogRecords.SourcePurged purged -> reject(caller, request, address, intentSha, authorized,
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

    /** Validates the write and source selectors and returns the written parts in request order. */
    private static Map<PartSelector, WriteAction> selectors(SaveDocumentRequest request) {
        var writes = new LinkedHashMap<PartSelector, WriteAction>();
        for (var write : request.getWritesList()) {
            var selector = supported(write.getSelector());
            if (writes.put(selector, write.getAction()) != null)
                throw malformedException("Duplicate write selector " + describe(selector));
        }
        var sourced = new HashSet<PartSelector>();
        for (var source : request.getSourcesList()) {
            var selector = supported(source.getSelector());
            if (writes.containsKey(selector) || !sourced.add(selector))
                throw malformedException("Selector " + describe(selector) + " is named twice");
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

    /** Request validation already proved the selector names exactly one object; this names its kind for messages. */
    private static PartSelector supported(PartSelector selector) {
        return switch (selector.getSelectorCase()) {
            case PART, SEGMENTATION_ID, EMBEDDING_SET -> selector;
            default -> throw malformedException("A selector names a part, a segmentation or an embedding set");
        };
    }

    private static Document normalizedDocument(SaveDocumentRequest request) {
        var document = request.getDocument();
        if (document.getSearchMetadata().getSemanticResultsCount() > 0)
            throw malformedException("Legacy semantic results are not supported by this server; write segmentations and embedding sets");
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
    private static Map<PartSelector, ByteString> split(Document request, Descriptors.Descriptor documentType,
            Map<PartSelector, WriteAction> writes) {
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
        var fragments = new LinkedHashMap<PartSelector, ByteString>();
        final List<ai.protomolt.proto.repo.codec.PartObject> splits;
        try {
            splits = DocumentPartCodec.split(document, layout);
        } catch (IllegalArgumentException duplicate) {
            throw malformedException(duplicate.getMessage());
        }
        for (var split : splits) {
            // Deterministic bytes: a client sending map entries in another order must get
            // the same content digest, the same intent, and a replay on retry.
            final ai.protomolt.proto.repo.codec.PartObject part;
            try {
                part = DocumentPartCodec.deterministic(split, prototype);
            } catch (InvalidProtocolBufferException impossible) {
                throw new IllegalStateException("A freshly split fragment failed to parse", impossible);
            }
            var selector = selector(part);
            var bytes = ByteString.copyFrom(part.bytes());
            boolean put = writes.get(selector) == WriteAction.WRITE_ACTION_PUT;
            if (!put && !(selector.equals(CORE_SELECTOR) && !coreHasContent(bytes, documentType, layout)))
                throw malformedException("Content for " + describe(selector) + " is present but the part is not written");
            if (put) fragments.put(selector, bytes);
        }
        for (var write : writes.entrySet())
            if (write.getValue() == WriteAction.WRITE_ACTION_PUT && !fragments.containsKey(write.getKey()))
                throw malformedException("PUT " + describe(write.getKey()) + " has no content; write EMPTY instead");
        return fragments;
    }

    /**
     * Whether a split CORE fragment carries anything beyond the document identity. A
     * save that writes only semantic parts sends them inside {@code search_metadata},
     * and the split keeps that parent set after emptying it; an emptied parent of a
     * chunked or keyed part is not CORE content.
     */
    private static boolean coreHasContent(ByteString core, Descriptors.Descriptor documentType,
            ai.protomolt.proto.repo.codec.PartLayout layout) {
        final DynamicMessage fragment;
        try {
            fragment = DynamicMessage.parseFrom(documentType, core);
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException("A freshly split CORE fragment failed to parse", impossible);
        }
        if (!fragment.getUnknownFields().asMap().isEmpty()) return true;
        var parents = new HashSet<Descriptors.FieldDescriptor>();
        layout.chunkedFields().forEach(field -> parents.add(field.parent()));
        layout.keyedFields().forEach(field -> parents.add(field.parent()));
        for (var entry : fragment.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.equals(layout.identityField())) continue;
            if (parents.contains(field) && entry.getValue() instanceof Message parent
                    && parent.getAllFields().isEmpty() && parent.getUnknownFields().asMap().isEmpty()) continue;
            return true;
        }
        return false;
    }

    /** The v2 selector of a split fragment. */
    private static PartSelector selector(ai.protomolt.proto.repo.codec.PartObject part) {
        return switch (part.part()) {
            case DOCUMENT_PART_CORE -> CORE_SELECTOR;
            case DOCUMENT_PART_BLOBS -> BLOBS_SELECTOR;
            case DOCUMENT_PART_PARSED -> PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).build();
            case DOCUMENT_PART_CHUNKS -> {
                if (part.keyValues().size() != 1)
                    throw malformedException("Legacy semantic results are not supported by this server");
                yield PartSelector.newBuilder().setSegmentationId(part.keyValues().getFirst()).build();
            }
            case DOCUMENT_PART_EMBEDDINGS -> PartSelector.newBuilder().setEmbeddingSet(EmbeddingSetKey.newBuilder()
                    .setSegmentationId(part.keyValues().get(0)).setEmbeddingSetId(part.keyValues().get(1))).build();
            default -> throw malformedException("Unsupported document part " + part.part());
        };
    }

    // ------------------------------------------------------------------ semantic parts

    /** A recorded refusal of the semantic candidate. */
    private record SemanticRejection(RejectionReason reason, String message, List<FieldViolation> violations) {}

    /** The semantic verdict; {@code rejection} is null when the candidate passed and the stored entries were finalized. */
    private record SemanticOutcome(SemanticRejection rejection) {}

    /**
     * Composes the semantic candidate (requirements §5, §9), decides which carried
     * parts it must read, runs {@link DocumentSemanticAdmission}, and on acceptance
     * finalizes the written CHUNKS fragments with their computed digests and records
     * {@code segmentation_digest} on every CHUNKS and EMBEDDINGS entry.
     */
    private SemanticOutcome semantic(Descriptors.Descriptor documentType, Map<PartSelector, StoredPartEntry> parts,
            Map<PartSelector, WriteAction> writes, Map<PartSelector, ByteString> storedFragments,
            Map<PartSelector, ByteString> sourcedBytes) {
        var changedParts = new HashSet<ai.protomolt.proto.repo.v1.DocumentPart>();
        for (var selector : writes.keySet()) if (selector.hasPart()) changedParts.add(v1(selector.getPart()));
        for (var selector : sourcedBytes.keySet()) if (selector.hasPart()) changedParts.add(v1(selector.getPart()));

        // Every PRESENT semantic entry of the composed candidate, by selector.
        var present = new LinkedHashMap<PartSelector, StoredPartEntry>();
        for (var entry : parts.entrySet())
            if (!entry.getKey().hasPart() && entry.getValue().getEntry().getState() == PartState.PART_STATE_PRESENT)
                present.put(entry.getKey(), entry.getValue());
        if (present.isEmpty()) return new SemanticOutcome(null);

        // Written and sourced content is in hand; carried content is read only when a rule needs it.
        var segmentations = new LinkedHashMap<String, Segmentation>();
        var verify = new HashMap<String, Boolean>();
        var carriedDigests = new LinkedHashMap<String, String>();
        var embeddingSets = new LinkedHashMap<EmbeddingSetKey, EmbeddingSet>();
        var carriedBindings = new LinkedHashMap<EmbeddingSetKey, String>();
        var carriedChunks = new LinkedHashMap<String, StoredPartEntry>();
        for (var entry : present.entrySet()) {
            var selector = entry.getKey();
            var bytes = storedFragments.containsKey(selector) ? storedFragments.get(selector) : sourcedBytes.get(selector);
            if (selector.hasSegmentationId()) {
                if (bytes != null) {
                    segmentations.put(selector.getSegmentationId(), segmentation(bytes));
                    verify.put(selector.getSegmentationId(), true);
                } else {
                    carriedChunks.put(selector.getSegmentationId(), entry.getValue());
                }
            } else if (bytes != null) {
                embeddingSets.put(selector.getEmbeddingSet(), embeddingSet(bytes));
            } else {
                carriedBindings.put(selector.getEmbeddingSet(), storedDigest(entry.getValue()));
            }
        }
        for (var chunk : carriedChunks.entrySet()) {
            var id = chunk.getKey();
            boolean bound = embeddingSets.keySet().stream().anyMatch(key -> key.getSegmentationId().equals(id));
            if (!bound && changedParts.isEmpty()) {
                carriedDigests.put(id, storedDigest(chunk.getValue()));
                continue;
            }
            var segmentation = segmentation(readVerified(chunk.getValue().getRef()));
            if (!segmentation.getSegmentationId().equals(id))
                throw new AccessionException(Status.Code.DATA_LOSS, "Stored CHUNKS object carries another segmentation", null);
            var source = segmentation.getSource();
            segmentations.put(id, segmentation);
            verify.put(id, source.hasFieldPath() && changedParts.contains(DocumentSemanticAdmission.sourcePart(source.getFieldPath())));
        }

        // The composed view holds every part a verified field path reads from.
        var view = DynamicMessage.newBuilder(documentType);
        var neededParts = new HashSet<ai.protomolt.proto.repo.v1.DocumentPart>();
        for (var segmentation : segmentations.values())
            if (verify.get(segmentation.getSegmentationId()) && segmentation.getSource().hasFieldPath())
                neededParts.add(DocumentSemanticAdmission.sourcePart(segmentation.getSource().getFieldPath()));
        for (var part : neededParts) {
            if (part == ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CHUNKS
                    || part == ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_EMBEDDINGS) continue;
            var selector = PartSelector.newBuilder().setPart(v2(part)).build();
            var bytes = storedFragments.containsKey(selector) ? storedFragments.get(selector) : sourcedBytes.get(selector);
            var entry = parts.get(selector);
            if (bytes == null && entry != null && entry.getEntry().getState() == PartState.PART_STATE_PRESENT && entry.hasRef())
                bytes = readVerified(entry.getRef());
            if (bytes == null) continue;
            try {
                view.mergeFrom(bytes);
            } catch (InvalidProtocolBufferException corrupt) {
                throw new AccessionException(Status.Code.DATA_LOSS, "Stored " + part + " part does not decode", null, corrupt);
            }
        }

        var candidate = new DocumentSemanticAdmission.Candidate(
                segmentations.values().stream().map(segmentation -> new DocumentSemanticAdmission.PresentSegmentation(
                        segmentation, verify.get(segmentation.getSegmentationId()))).toList(),
                carriedDigests, List.copyOf(embeddingSets.values()), carriedBindings, view);
        final DocumentSemanticAdmission.Accepted accepted;
        switch (DocumentSemanticAdmission.check(candidate)) {
            case DocumentSemanticAdmission.Rejected rejected -> {
                var violations = rejected.violations().stream().map(v -> FieldViolation.newBuilder()
                        .setPart(v.embeddingSetId().isEmpty() ? PartSelector.newBuilder().setSegmentationId(v.segmentationId()).build()
                                : PartSelector.newBuilder().setEmbeddingSet(EmbeddingSetKey.newBuilder()
                                        .setSegmentationId(v.segmentationId()).setEmbeddingSetId(v.embeddingSetId())).build())
                        .setFieldPath(v.fieldPath()).setRuleId(v.ruleId()).setMessage(v.message()).build()).toList();
                var reason = rejected.kind() == DocumentSemanticAdmission.Kind.SEGMENTATION_MISMATCH
                        ? RejectionReason.REJECTION_REASON_SEGMENTATION_MISMATCH : RejectionReason.REJECTION_REASON_VALIDATION_FAILED;
                var message = rejected.kind() == DocumentSemanticAdmission.Kind.SEGMENTATION_MISMATCH
                        ? "An embedding set does not bind to its segmentation in the composed document"
                        : "A segmentation failed validation against the composed document";
                return new SemanticOutcome(new SemanticRejection(reason, message, violations));
            }
            case DocumentSemanticAdmission.Accepted ok -> accepted = ok;
        }

        // Finalize: written CHUNKS fragments carry the computed digest; every semantic entry records its binding.
        var prototype = DynamicMessage.getDefaultInstance(documentType);
        for (var entry : present.entrySet()) {
            var selector = entry.getKey();
            var stored = entry.getValue();
            if (selector.hasSegmentationId()) {
                var id = selector.getSegmentationId();
                var digest = accepted.digests().get(id);
                if (storedFragments.containsKey(selector)) {
                    var finalized = accepted.finalized().get(id);
                    var bytes = withSegmentation(storedFragments.get(selector), finalized, selector, prototype);
                    storedFragments.put(selector, bytes);
                    var builder = stored.toBuilder();
                    builder.getEntryBuilder().setSizeBytes(bytes.size()).setSha256(sha256(bytes)).setSegmentationDigest(digest);
                    parts.put(selector, builder.build());
                } else if (!stored.getEntry().getSegmentationDigest().equals(digest)) {
                    throw new AccessionException(Status.Code.DATA_LOSS, "Stored segmentation " + id
                            + " digest differs from its content", null);
                }
            } else if (storedFragments.containsKey(selector) || sourcedBytes.containsKey(selector)) {
                var builder = stored.toBuilder();
                builder.getEntryBuilder().setSegmentationDigest(embeddingSets.get(selector.getEmbeddingSet()).getSegmentationDigest());
                parts.put(selector, builder.build());
            }
        }
        return new SemanticOutcome(null);
    }

    /** The one segmentation a CHUNKS fragment carries. */
    private static Segmentation segmentation(ByteString fragment) {
        try {
            var document = Document.parseFrom(fragment);
            if (document.getSearchMetadata().getSegmentationsCount() != 1)
                throw new AccessionException(Status.Code.DATA_LOSS, "CHUNKS object does not hold exactly one segmentation", null);
            return document.getSearchMetadata().getSegmentations(0);
        } catch (InvalidProtocolBufferException corrupt) {
            throw new AccessionException(Status.Code.DATA_LOSS, "CHUNKS object does not decode", null, corrupt);
        }
    }

    /** The one embedding set an EMBEDDINGS fragment carries. */
    private static EmbeddingSet embeddingSet(ByteString fragment) {
        try {
            var document = Document.parseFrom(fragment);
            if (document.getSearchMetadata().getEmbeddingSetsCount() != 1)
                throw new AccessionException(Status.Code.DATA_LOSS, "EMBEDDINGS object does not hold exactly one embedding set", null);
            return document.getSearchMetadata().getEmbeddingSets(0);
        } catch (InvalidProtocolBufferException corrupt) {
            throw new AccessionException(Status.Code.DATA_LOSS, "EMBEDDINGS object does not decode", null, corrupt);
        }
    }

    /** A carried semantic entry's recorded digest; a PRESENT entry without one is a damaged record. */
    private static String storedDigest(StoredPartEntry entry) {
        var digest = entry.getEntry().getSegmentationDigest();
        if (digest.isEmpty())
            throw new AccessionException(Status.Code.DATA_LOSS, "Stored " + describe(entry.getEntry().getSelector())
                    + " entry records no segmentation digest", null);
        return digest;
    }

    /** The CHUNKS fragment with its segmentation replaced, re-encoded deterministically. */
    private static ByteString withSegmentation(ByteString fragment, Segmentation finalized, PartSelector selector,
            Message prototype) {
        try {
            var document = Document.parseFrom(fragment).toBuilder();
            document.getSearchMetadataBuilder().setSegmentations(0, finalized);
            var part = ai.protomolt.proto.repo.codec.PartObject.keyed(ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CHUNKS,
                    List.of(selector.getSegmentationId()), document.build().toByteArray(), "");
            return ByteString.copyFrom(DocumentPartCodec.deterministic(part, prototype).bytes());
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException("A written CHUNKS fragment failed to parse again", impossible);
        }
    }

    /** The selector a typed-admission violation belongs to: keyed parts by the element index in the admitted view. */
    private static PartSelector violated(DocumentCandidateAdmission.Violation violation, List<PartSelector> admitted) {
        var part = violation.part();
        if (part != ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CHUNKS
                && part != ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_EMBEDDINGS)
            return PartSelector.newBuilder().setPart(v2(part)).build();
        var keyed = admitted.stream().filter(selector -> part == ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CHUNKS
                ? selector.hasSegmentationId() : selector.hasEmbeddingSet()).toList();
        var matcher = java.util.regex.Pattern.compile("^search_metadata\\.(?:segmentations|embedding_sets)\\[(\\d+)\\]")
                .matcher(violation.fieldPath());
        int index = matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
        if (index >= keyed.size())
            throw new AccessionException(Status.Code.INTERNAL, "Validation names element " + index + " of " + keyed.size()
                    + " admitted " + part + " fragments", null);
        return keyed.get(index);
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
        if (owner.hasIntake() && owner.getIntake().hasConnectorId()
                && (!ownership.hasConnectorId()
                    || !owner.getIntake().getConnectorId().equals(ownership.getConnectorId())))
            throw WriterAuthority.denied("The intake owner differs from the document's connector");
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
        if (recorded.outcome().hasRejection()) RejectionAccess.require(catalog, caller, recorded);
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
            String intentSha, WriterAuthority.Authorized authorized, RejectionReason reason, String message, List<FieldViolation> violations) {
        var rejection = SaveRejection.newBuilder().setReason(reason).setMessage(message).addAllViolations(violations).build();
        var outcome = RecordedOutcome.newBuilder().setRejection(rejection).setRecordedAt(timestamp(clock.instant())).build();
        var result = catalog.recordRejection(address.getAccountId(), request.getOperationId(), intentSha, outcome,
                authorized.grantIds(), authorized.credential(), address, request.getExpected().hasNone());
        return switch (result) {
            case CatalogRecords.Recorded stored -> replay(caller, request, intentSha, stored.outcome());
            case CatalogRecords.WriteDenied denied -> throw WriterAuthority.denied(denied.reason());
        };
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

    static ai.protomolt.proto.repo.v1.DocumentPart v1(DocumentPart part) {
        var mapped = PARTS.get(part);
        if (mapped == null) throw new AccessionException(Status.Code.DATA_LOSS, "Stored part " + part + " is not supported", null);
        return mapped;
    }

    private static DocumentPart v2(ai.protomolt.proto.repo.v1.DocumentPart part) {
        return PARTS.entrySet().stream().filter(entry -> entry.getValue() == part).map(Map.Entry::getKey).findFirst()
                .orElse(DocumentPart.DOCUMENT_PART_CORE);
    }

    /** The admission fragment of a stored or written object: its layout part and key. */
    static DocumentCandidateAdmission.Fragment fragment(PartSelector selector, ByteString bytes) {
        return switch (selector.getSelectorCase()) {
            case PART -> new DocumentCandidateAdmission.Fragment(v1(selector.getPart()), bytes);
            case SEGMENTATION_ID -> new DocumentCandidateAdmission.Fragment(
                    ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CHUNKS, List.of(selector.getSegmentationId()), bytes);
            case EMBEDDING_SET -> new DocumentCandidateAdmission.Fragment(
                    ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_EMBEDDINGS,
                    List.of(selector.getEmbeddingSet().getSegmentationId(), selector.getEmbeddingSet().getEmbeddingSetId()), bytes);
            default -> throw new AccessionException(Status.Code.DATA_LOSS, "Stored part has no selector", null);
        };
    }

    /** The selector's part number: the first key of codec order. */
    private static int rank(PartSelector selector) {
        return switch (selector.getSelectorCase()) {
            case SEGMENTATION_ID -> DocumentPart.DOCUMENT_PART_CHUNKS_VALUE;
            case EMBEDDING_SET -> DocumentPart.DOCUMENT_PART_EMBEDDINGS_VALUE;
            default -> selector.getPartValue();
        };
    }

    /** The selector's key values, empty for a part selector. */
    private static List<String> key(PartSelector selector) {
        return switch (selector.getSelectorCase()) {
            case SEGMENTATION_ID -> List.of(selector.getSegmentationId());
            case EMBEDDING_SET -> List.of(selector.getEmbeddingSet().getSegmentationId(), selector.getEmbeddingSet().getEmbeddingSetId());
            default -> List.of();
        };
    }

    /** A selector for messages: the part name, or the keyed part with its IDs. */
    static String describe(PartSelector selector) {
        return switch (selector.getSelectorCase()) {
            case SEGMENTATION_ID -> "CHUNKS " + selector.getSegmentationId();
            case EMBEDDING_SET -> "EMBEDDINGS " + selector.getEmbeddingSet().getSegmentationId() + "/"
                    + selector.getEmbeddingSet().getEmbeddingSetId();
            default -> selector.getPart().name().replace("DOCUMENT_PART_", "");
        };
    }

    static String eventId(NodeAddress address, long revision) {
        // Protobuf field lengths keep separators inside address components unambiguous.
        var identity = Accession.newBuilder().setAddress(address).setRevision(revision).build();
        var name = ByteString.copyFromUtf8("protomolt/accession/v1\0").concat(ByteString.copyFrom(
                ai.protomolt.proto.descriptors.MessageFingerprints.deterministicBytes(identity)));
        return UUID.nameUUIDFromBytes(name.toByteArray()).toString();
    }

    static String sha256(ByteString bytes) { return SaveIntents.sha256Hex(bytes.toByteArray()); }

    static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}
