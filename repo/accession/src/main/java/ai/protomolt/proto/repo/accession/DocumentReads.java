package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.catalog.CatalogDataLoss;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentConcepts;
import ai.protomolt.proto.repo.admission.DocumentLinks;
import ai.protomolt.proto.repo.admission.DocumentRetainedDefinitions;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.codec.DocumentFragmentConfinement;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.RepositoryAnyResolution;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceKeyType;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceMapKey;
import ai.protomolt.proto.repo.v1.RepositoryResolvedSchema;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The v2 reads (requirements §8; change map §4 steps R1-R7). A read resolves the
 * current revision once, or reads the exact revision asked for and never
 * another. Part bytes are verified against their stack reference before they
 * are decoded; a mismatch is DATA_LOSS. Access follows the document's current
 * ownership, stored as binary protobuf in the catalog.
 * <p>
 * A typed read decodes each selected part with the schemas its revision retained
 * (requirements §9, §15.14): the part's {@code fragment_schema} and the schema of
 * every Any occurrence recorded for it, read from the catalog and checked against
 * their digests. There is no schema registry here, so a stopped registry cannot
 * affect a read and no live schema can stand in for a retained one. An unreachable
 * catalog is UNAVAILABLE; a retained artifact that is absent from a reachable
 * catalog, or fails its digest, is DATA_LOSS.
 */
public final class DocumentReads {
    /** Largest payload of one part-data frame of a streamed read. */
    private static final int READ_FRAME_BYTES = 1024 * 1024;
    /** Full protobuf type name of {@link Any}. */
    private static final String ANY_FULL_NAME = Any.getDescriptor().getFullName();
    /** Maximum distinct retained schema artifacts per revision read. */
    private static final int MAX_RETAINED_ARTIFACTS = 64;
    /** Maximum structural nesting depth of a CORE fragment. */
    private static final int MAX_CORE_DEPTH = 64;
    /** Maximum message node count of a CORE fragment. */
    private static final int MAX_CORE_NODES = 1_000_000;
    /** Maximum type URL length on a retained Any envelope. */
    private static final int MAX_TYPE_URL_LENGTH = 4096;
    /** Maximum message types reachable from a retained Any payload schema. */
    private static final int MAX_PAYLOAD_SCHEMA_TYPES = 4096;
    /** Maximum fields reachable from a retained Any payload schema. */
    private static final int MAX_PAYLOAD_SCHEMA_FIELDS = 65536;

    private final Catalog catalog;
    private final DocumentSaves.Drives drives;
    private final DocumentSchemaAdmission.Limits limits;
    private final DocumentAdmissionReservations reservations;
    private final Clock clock;

    /**
     * Constructs the v2 document read coordinator.
     *
     * @param catalog the repository catalog
     * @param drives the configured stacks
     * @param limits the admission structural limits
     * @param reservations the admission memory reservation tracker
     * @param clock the clock used when evaluating retained schema conditions
     */
    public DocumentReads(Catalog catalog, DocumentSaves.Drives drives, DocumentSchemaAdmission.Limits limits,
            DocumentAdmissionReservations reservations, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog);
        this.drives = Objects.requireNonNull(drives);
        this.limits = Objects.requireNonNull(limits);
        this.reservations = Objects.requireNonNull(reservations);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Reads and merges the selected parts of a document revision.
     * <p>
     * Each selected part's catalog length is validated against the host's part size
     * ceiling and reserved against the read byte budget before anything is opened or
     * allocated; a length the remaining budget does not cover fails fast with
     * {@code RESOURCE_EXHAUSTED} and never reaches the stack. The merged response
     * aliases the buffered part bytes, so every part's reservation is held until the
     * response is built and handed off, then released.
     *
     * @param caller the authenticated repository caller
     * @param request the document read request
     * @return the merged document, manifest, and validation record
     */
    public GetDocumentResponse getDocument(RepositoryCaller caller, GetDocumentRequest request) {
        return guarded(() -> {
            valid(request);
            var stored = resolve(caller, request.getAddress(), request.getRevision());
            var selected = selected(request.getSelection());
            var document = Document.newBuilder();
            var leases = new java.util.ArrayList<DocumentAdmissionReservations.Lease>();
            try {
                for (var part : stored.getPartsList()) {
                    var entry = part.getEntry();
                    if (!selected.apply(entry.getSelector()) || entry.getState() != PartState.PART_STATE_PRESENT) continue;
                    if (entry.getOpaque() && !request.getAllowOpaque())
                        throw new AccessionException(Status.Code.FAILED_PRECONDITION,
                                "The revision holds opaque content; request its opaque representation explicitly", null);
                    requirePartWithinCeiling(part.getRef());
                    leases.add(Objects.requireNonNull(
                            reservations.reserve(part.getRef().getLength()), "reservation lease"));
                    var bytes = read(part.getRef());
                    // Opaque occurrences are verified undecoded against their record inside
                    // decodeRetained; typed siblings in the same part still decode (§9).
                    decodeRetained(stored, entry.getSelector(), bytes);
                    // Raw blobs live behind claim checks: the server fills storage_ref (decision 45).
                    if (entry.getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS)
                        bytes = RawBlobs.withClaimChecks(bytes, part.getBlobRefsList());
                    try {
                        document.mergeFrom(bytes);
                    } catch (InvalidProtocolBufferException corrupt) {
                        throw new AccessionException(Status.Code.DATA_LOSS, "Stored part does not decode", null, corrupt);
                    }
                }
                return GetDocumentResponse.newBuilder().setDocument(document)
                        .setManifest(manifest(stored, selected)).setValidation(stored.getValidation())
                        .putAllMetadata(stored.getMetadataMap()).build();
            } finally {
                for (var lease : leases) lease.close();
            }
        });
    }

    /**
     * Streams the manifest, selected part data frames, and completion marker for a document revision.
     *
     * @param caller the authenticated repository caller
     * @param request the streaming document read request
     * @param observer the response stream observer
     */
    public void readDocument(
            RepositoryCaller caller,
            ReadDocumentRequest request,
            io.grpc.stub.StreamObserver<ReadDocumentResponse> observer) {
        new DocumentStreams(this).readDocument(caller, request, observer);
    }

    /**
     * Streams a raw blob's header and byte range from its stack object.
     *
     * @param caller the authenticated repository caller
     * @param request the streaming blob read request
     * @param observer the response stream observer
     */
    public void readBlob(
            RepositoryCaller caller,
            ReadBlobRequest request,
            io.grpc.stub.StreamObserver<ReadBlobResponse> observer) {
        new DocumentStreams(this).readBlob(caller, request, observer);
    }

    /**
     * Reads a document revision's manifest without opening any stack object.
     *
     * @param caller the authenticated repository caller
     * @param request the manifest request
     * @return the manifest and validation record
     */
    public GetManifestResponse getManifest(RepositoryCaller caller, GetManifestRequest request) {
        return guarded(() -> {
            valid(request);
            var stored = resolve(caller, request.getAddress(), request.getRevision());
            return GetManifestResponse.newBuilder().setManifest(manifest(stored, selector -> false))
                    .setValidation(stored.getValidation()).build();
        });
    }

    /**
     * The streamed read of one pinned revision (requirements §8, §11): the manifest
     * frame first, then each selected PRESENT part's bytes in one or more frames,
     * then the completion frame. Every part's bytes are verified against their
     * stack reference, and non-opaque parts against their retained schemas, before
     * any of its frames are emitted; raw blobs go out with their claim checks
     * filled (decision 45). An opaque part the request does not allow fails the
     * item before any frame is emitted. A prefix without the completion frame is
     * not a successful read.
     * <p>
     * One part is buffered at a time: its catalog length is validated against the host's
     * part size ceiling and reserved against the read byte budget before anything is
     * opened or allocated, and the reservation is released once the part's final frame
     * callback returns, before the next part is read, so a bulk read never holds
     * reservations for all of a document's parts at once.
     */
    public void readDocument(RepositoryCaller caller, ReadDocumentRequest request,
            java.util.function.Consumer<ReadDocumentResponse> frames) {
        Objects.requireNonNull(frames);
        guarded(() -> {
            valid(request);
            var stored = resolve(caller, request.getAddress(), request.getRevision());
            var selected = selected(request.getSelection());
            var parts = new java.util.ArrayList<StoredPartEntry>();
            for (var part : stored.getPartsList()) {
                var entry = part.getEntry();
                if (!selected.apply(entry.getSelector()) || entry.getState() != PartState.PART_STATE_PRESENT) continue;
                if (entry.getOpaque() && !request.getAllowOpaque())
                    throw new AccessionException(Status.Code.FAILED_PRECONDITION,
                            "The revision holds opaque content; request its opaque representation explicitly", null);
                parts.add(part);
            }
            frames.accept(ReadDocumentResponse.newBuilder().setManifest(manifest(stored, selected)).build());
            for (var part : parts) {
                var entry = part.getEntry();
                requirePartWithinCeiling(part.getRef());
                try (var ignored = Objects.requireNonNull(
                        reservations.reserve(part.getRef().getLength()), "reservation lease")) {
                    var bytes = read(part.getRef());
                    if (!entry.getOpaque()) decodeRetained(stored, entry.getSelector(), bytes);
                    if (entry.getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS)
                        bytes = RawBlobs.withClaimChecks(bytes, part.getBlobRefsList());
                    long offset = 0;
                    do {
                        int length = (int) Math.min(READ_FRAME_BYTES, (long) bytes.size() - offset);
                        frames.accept(ReadDocumentResponse.newBuilder().setPartData(PartData.newBuilder()
                                .setSelector(entry.getSelector()).setOffset(offset)
                                .setData(bytes.substring((int) offset, (int) offset + length))
                                .setLast(offset + length == bytes.size())).build());
                        offset += length;
                    } while (offset < bytes.size());
                }
            }
            frames.accept(ReadDocumentResponse.newBuilder().setComplete(ReadComplete.getDefaultInstance()).build());
            return null;
        });
    }

    /**
     * Returns the descriptor set bytes a revision retained, by their SHA-256
     * (requirements §8, §9). The digest grants nothing: the caller must be able to
     * read the named document, and the resolved revision's validation record must
     * reference the digest as a retained descriptor. A document the caller cannot
     * read, a revision that does not exist and a digest the revision does not
     * reference all answer the same {@code NOT_FOUND}, so the call cannot probe
     * which digests exist. Metadata and source digests are not descriptors and are
     * not served.
     * <p>
     * The artifact's catalog length is checked against the retained descriptor
     * ceiling and reserved against the read byte budget before its bytes are
     * loaded; the reservation is held until the response is built. The bytes are
     * hashed again here before they are returned, independent of the catalog's own
     * check. A referenced artifact absent from a reachable catalog, or whose bytes
     * differ from the digest or from the recorded length, is {@code DATA_LOSS}.
     *
     * @param caller the authenticated repository caller
     * @param request the retained schema read request
     * @return the resolved revision, the digest and the verified descriptor set bytes
     */
    public GetSchemaDescriptorResponse getSchemaDescriptor(RepositoryCaller caller, GetSchemaDescriptorRequest request) {
        return guarded(() -> {
            valid(request);
            requireBinding(caller);
            var digest = request.getDescriptorSha256();
            StoredManifest stored;
            try {
                stored = resolve(caller, request.getAddress(), request.getRevision());
            } catch (AccessionException refused) {
                if (refused.code() != Status.Code.NOT_FOUND && refused.code() != Status.Code.PERMISSION_DENIED) throw refused;
                throw schemaNotFound();
            }
            if (!retainedDescriptors(stored.getValidation()).contains(digest)) throw schemaNotFound();
            long length = catalog.artifactLength(digest).orElseThrow(() -> lost(
                    "Retained schema descriptor " + digest + " is referenced but absent from the catalog", null));
            if (length < 1)
                throw lost("Retained schema descriptor " + digest + " records an empty length", null);
            if (length > DocumentRetainedDefinitions.MAX_DESCRIPTOR_BYTES)
                throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED, "Retained schema descriptor records "
                        + length + " bytes, above the " + DocumentRetainedDefinitions.MAX_DESCRIPTOR_BYTES
                        + " byte descriptor ceiling", null);
            try (var ignored = Objects.requireNonNull(reservations.reserve(length), "reservation lease")) {
                var bytes = catalog.artifact(digest).orElseThrow(() -> lost(
                        "Retained schema descriptor " + digest + " is referenced but absent from the catalog", null));
                if (bytes.size() != length)
                    throw lost("Retained schema descriptor " + digest + " differs from its recorded length", null);
                if (!DocumentPartCodec.sha256Hex(bytes).equals(digest))
                    throw lost("Retained schema descriptor " + digest + " differs from its digest", null);
                return GetSchemaDescriptorResponse.newBuilder().setRevision(stored.getRevision())
                        .setDescriptorSha256(digest).setDescriptorSet(bytes).build();
            }
        });
    }

    /**
     * Every descriptor digest a validation record references, wherever a
     * {@link RepositorySchemaAssetReference} sits in it: part fragment schemas, Any
     * occurrence schemas and grant snapshot schemas. The walk follows the record's
     * descriptor, so a reference added to the record later is covered without a
     * change here.
     */
    static Set<String> retainedDescriptors(RevisionValidation validation) {
        var digests = new HashSet<String>();
        collectDescriptors(validation, digests);
        return digests;
    }

    private static void collectDescriptors(Message message, Set<String> digests) {
        if (message instanceof RepositorySchemaAssetReference reference) {
            digests.add(reference.getDescriptorSha256());
            return;
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (var element : (List<?>) field.getValue()) collectDescriptors((Message) element, digests);
            } else {
                collectDescriptors((Message) field.getValue(), digests);
            }
        }
    }

    private static AccessionException schemaNotFound() {
        return notFound("Schema descriptor not found");
    }

    /**
     * Lists a document's revisions in descending order.
     *
     * @param caller the authenticated repository caller
     * @param request the revision listing request
     * @return a page of revision summaries
     */
    public ListRevisionsResponse listRevisions(RepositoryCaller caller, ListRevisionsRequest request) {
        return guarded(() -> {
            valid(request);
            current(caller, request.getAddress());
            long before = Long.MAX_VALUE;
            if (!request.getPageToken().isEmpty()) {
                try {
                    before = Long.parseLong(request.getPageToken());
                } catch (NumberFormatException invalid) {
                    before = 0;
                }
                if (before < 1 || !Long.toString(before).equals(request.getPageToken()))
                    throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Invalid page token", null);
            }
            var page = catalog.revisions(request.getAddress(), before, request.getPageSize() + 1);
            var shown = page.subList(0, Math.min(page.size(), request.getPageSize()));
            // One bounded query for this page's events, not the document's whole outbox (#327).
            var events = new java.util.HashMap<Long, DocumentEvent>();
            for (var event : catalog.events(request.getAddress(), shown.stream().map(StoredManifest::getRevision).toList()))
                events.put(event.getRevision(), event);
            var response = ListRevisionsResponse.newBuilder();
            for (var stored : shown) {
                var event = events.get(stored.getRevision());
                if (event == null) throw new AccessionException(Status.Code.DATA_LOSS, "Revision has no event", null);
                var summary = RevisionSummary.newBuilder().setRevision(stored.getRevision())
                        .setPreviousRevision(stored.getPreviousRevision()).setCommittedAt(stored.getCommittedAt())
                        .setOperationId(stored.getOperationId()).setTombstone(stored.getTombstone())
                        .setValidationStatus(stored.getValidation().getStatus())
                        .addAllChanges(event.getAccessioned().getChangesList());
                if (stored.hasOwner()) summary.setOwner(stored.getOwner());
                response.addRevisions(summary);
            }
            if (page.size() > request.getPageSize())
                response.setNextPageToken(Long.toString(page.get(request.getPageSize() - 1).getRevision()));
            return response.build();
        });
    }

    /**
     * Looks up a recorded operation outcome in an account.
     *
     * @param caller the authenticated repository caller
     * @param request the operation outcome lookup request
     * @return the recorded outcome or {@code NoRecordedOutcome}
     */
    public GetOperationOutcomeResponse outcome(RepositoryCaller caller, GetOperationOutcomeRequest request) {
        return guarded(() -> {
            valid(request);
            requireBinding(caller);
            requireAccount(caller, request.getAccountId());
            var recorded = catalog.outcome(request.getAccountId(), request.getOperationId());
            if (recorded.isEmpty())
                return GetOperationOutcomeResponse.newBuilder().setNone(NoRecordedOutcome.getDefaultInstance()).build();
            var outcome = recorded.get().outcome();
            if (outcome.hasAccession() || outcome.hasUnchanged())
                current(caller, DocumentSaves.recordedAccession(outcome).getAddress());
            else if (outcome.hasRejection()) RejectionAccess.require(catalog, caller, recorded.get());
            return GetOperationOutcomeResponse.newBuilder().setCommitted(outcome).build();
        });
    }

    DocumentSaves.Drives drives() {
        return drives;
    }

    DocumentAdmissionReservations reservations() {
        return reservations;
    }

    // ------------------------------------------------------------------ resolution

    /** The current revision's manifest, after read access is checked against it. */
    private StoredManifest current(RepositoryCaller caller, NodeAddress address) {
        requireBinding(caller);
        requireAddress(address);
        requireAccount(caller, address.getAccountId());
        var stored = catalog.currentManifest(address).orElseThrow(() -> notFound("Document not found"));
        if (!canRead(caller, address, stored.getOwnership()))
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller may not read this document", null);

        return stored;
    }

    StoredManifest resolve(RepositoryCaller caller, NodeAddress address, RevisionSelector selector) {
        var latest = current(caller, address);
        if (!selector.hasRevision() && latest.getTombstone()) throw notFound("Document is deleted");
        var stored = (!selector.hasRevision() || selector.getRevision() == latest.getRevision()) ? latest
                : catalog.manifest(address, selector.getRevision())
                        .orElseThrow(() -> notFound("Revision " + selector.getRevision() + " does not exist"));
        // §15.22: reading a revision fails closed on corrupt retained grant evidence.
        for (var occurrence : stored.getValidation().getOpaqueOccurrencesList())
            DocumentSaves.requireGrantEvidenceIntact(occurrence.getWriterGrant());
        return stored;
    }

    /**
     * Decodes one stored fragment with the schemas its revision retained: the part's
     * fragment schema and every recorded Any occurrence's schema, through the same
     * admission engine a save runs. The occurrences found must be the ones recorded.
     * Concept membership is not decided again: it was decided against the mounts at
     * admission, and a reader's mounts must not turn stored content into data loss.
     *
     * @return the links and concept tags the fragment holds (requirements §17); empty outside CORE
     */
    Retained decodeRetained(StoredManifest stored, PartSelector part, ByteString bytes) {
        var validation = stored.getValidation();
        var binding = validation.getPartSchemasList().stream().filter(schema -> schema.getPart().equals(part)).findFirst()
                .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS, "Part has no retained schema binding", null));
        var recorded = validation.getResolvedOccurrencesList().stream()
                .filter(occurrence -> occurrence.getOccurrence().getPart().equals(part)).toList();
        var recordedOpaque = validation.getOpaqueOccurrencesList().stream()
                .filter(occurrence -> occurrence.getOccurrence().getPart().equals(part)).toList();
        var cachedArtifacts = new HashMap<String, Optional<ByteString>>();
        DocumentRetainedDefinitions.Reader retained = hash -> cachedArtifacts.computeIfAbsent(hash, catalog::artifact);
        try {
            var container = DocumentRetainedDefinitions.definition(binding.getFragmentSchema(), retained);
            var byReference = new HashMap<RepositorySchemaAssetReference, DocumentSchemaAdmission.Definition>();
            var byTypeUrl = new HashMap<String, DocumentSchemaAdmission.Definition>();
            for (var occurrence : recorded) {
                var definition = byReference.computeIfAbsent(
                        occurrence.getSchema(), schema -> DocumentRetainedDefinitions.definition(schema, retained));
                var previous = byTypeUrl.putIfAbsent(occurrence.getSchema().getTypeUrl(), definition);
                if (previous != null && !previous.equals(definition))
                    throw lost("The revision retains two schemas for " + occurrence.getSchema().getTypeUrl(), null);
            }
            var fragment = DocumentSaves.fragment(part, bytes);
            var attested = new HashSet<DocumentCandidateAdmission.AttestedOpaque>();
            for (var occurrence : recordedOpaque)
                attested.add(new DocumentCandidateAdmission.AttestedOpaque(fragment.part(),
                        occurrence.getOccurrence().getStepsList(), occurrence.getTypeUrl()));
            try (var result = DocumentCandidateAdmission.check(new DocumentCandidateAdmission.Request(
                    stored.getAddress().getDocId(), List.of(fragment), container, attested),
                    selection -> byTypeUrl.get(selection.typeUrl()), limits, reservations, clock.instant(), () -> {},
                    DocumentSchemaAdmission.ADMITTED_VALIDATOR)) {
                if (result instanceof DocumentCandidateAdmission.Rejected rejected)
                    throw lost("Stored part fails its retained rules: " + rejected.violations().getFirst().message(), null);
                var accepted = (DocumentCandidateAdmission.Accepted) result;
                var found = new HashSet<ResolvedOccurrence>();
                for (var occurrence : accepted.occurrences())
                    found.add(ResolvedOccurrence.newBuilder().setOccurrence(OccurrenceLocator.newBuilder().setPart(part)
                                    .addAllSteps(occurrence.steps())).setSchema(occurrence.schema())
                            .setPayloadSha256(occurrence.payloadSha256()).setPayloadLength(occurrence.payloadLength()).build());
                if (!found.equals(new HashSet<>(recorded)))
                    throw lost("Stored part's Any occurrences differ from the retained record", null);
                var expectedOpaque = new HashSet<DocumentCandidateAdmission.OpaqueFound>();
                for (var occurrence : recordedOpaque)
                    expectedOpaque.add(new DocumentCandidateAdmission.OpaqueFound(fragment.part(),
                            occurrence.getOccurrence().getStepsList(), occurrence.getTypeUrl(),
                            occurrence.getSha256(), occurrence.getLength()));
                if (!new HashSet<>(accepted.opaque()).equals(expectedOpaque))
                    throw lost("Stored part's opaque occurrences differ from the retained record", null);
                return new Retained(accepted.links(), accepted.concepts());
            }
        } catch (DocumentSchemaAdmission.DataLoss | DocumentCandidateAdmission.SchemaMissing
                 | DocumentCandidateAdmission.OpaqueMismatch
                 | DocumentCandidateAdmission.InvalidPayload | DocumentCandidateAdmission.ContentRefused
                 | DocumentCandidateAdmission.HostFailure | InvalidProtocolBufferException failure) {
            throw lost("Stored part does not decode with its retained schema: " + failure.getMessage(), failure);
        } catch (DocumentCandidateAdmission.BudgetExhausted | ClosedDescriptorSet.LimitExceededException budget) {
            throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED, "Retained schema budget exhausted: "
                    + budget.getMessage(), null, budget);
        }
    }

    /**
     * What a stored fragment holds for the catalog projections (requirements §17).
     *
     * @param links its links
     * @param concepts its concept tags
     */
    record Retained(List<DocumentLinks.Link> links, List<DocumentConcepts.Tag> concepts) {}

    /** The catalog these reads resolve against. */
    Catalog catalog() { return catalog; }

    /**
     * The links of a revision's CORE part, decoded with the schemas the revision
     * retained (requirements §17). The part's catalog length is reserved against the
     * read budget while it is buffered. A tombstone has none.
     *
     * @param stored the revision, already resolved under the caller's read access
     * @return the links in field order
     */
    List<DocumentLinks.Link> coreLinks(StoredManifest stored) {
        if (stored.getTombstone()) return List.of();
        var core = stored.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().equals(DocumentSaves.CORE_SELECTOR)).findFirst()
                .filter(part -> part.getEntry().getState() == PartState.PART_STATE_PRESENT && part.hasRef())
                .orElseThrow(() -> lost("Revision " + stored.getRevision() + " has no CORE object", null));
        requirePartWithinCeiling(core.getRef());
        try (var lease = Objects.requireNonNull(reservations.reserve(core.getRef().getLength()), "reservation lease")) {
            return decodeCoreLinks(stored, read(core.getRef()));
        }
    }

    /**
     * Decodes a revision's CORE fragment with its retained schemas and extracts its
     * links without re-running candidate rule evaluation or concept tag extraction
     * (issue #608). Every recorded resolved and opaque Any occurrence in CORE is
     * verified against the stored bytes.
     *
     * @param stored the resolved revision manifest
     * @param bytes the verified CORE part bytes
     * @return the links of CORE in field order
     */
    private List<DocumentLinks.Link> decodeCoreLinks(StoredManifest stored, ByteString bytes) {
        var validation = stored.getValidation();
        var binding = validation.getPartSchemasList().stream()
                .filter(schema -> schema.getPart().equals(DocumentSaves.CORE_SELECTOR)).findFirst()
                .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS, "Part has no retained schema binding", null));
        var recorded = validation.getResolvedOccurrencesList().stream()
                .filter(occurrence -> occurrence.getOccurrence().getPart().equals(DocumentSaves.CORE_SELECTOR)).toList();
        var recordedOpaque = validation.getOpaqueOccurrencesList().stream()
                .filter(occurrence -> occurrence.getOccurrence().getPart().equals(DocumentSaves.CORE_SELECTOR)).toList();
        var leases = new ArrayList<DocumentAdmissionReservations.Lease>();
        var artifacts = new HashMap<String, ByteString>();
        var retainedBytes = new long[1];
        DocumentRetainedDefinitions.Reader retained = hash -> {
            var cached = artifacts.get(hash);
            if (cached != null) return Optional.of(cached);
            var loaded = catalog.artifact(hash);
            if (loaded.isEmpty()) return Optional.empty();
            var artifact = loaded.get();
            if (!artifact.isEmpty()) {
                if (artifacts.size() >= MAX_RETAINED_ARTIFACTS
                        || artifact.size() > limits.maxRetainedBytes() - retainedBytes[0])
                    throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                            "Retained schema budget exhausted: retained artifact union exceeds limit", null);
                leases.add(Objects.requireNonNull(reservations.reserve(artifact.size()), "reservation lease"));
                retainedBytes[0] += artifact.size();
                artifacts.put(hash, artifact);
            }
            return loaded;
        };
        try {
            var container = DocumentRetainedDefinitions.definition(binding.getFragmentSchema(), retained);
            var containerType = DocumentCandidateAdmission.containerType(container);
            var byReference = new HashMap<RepositorySchemaAssetReference, RetainedSchema>();
            var byTypeUrl = new HashMap<String, RetainedSchema>();
            for (var occurrence : recorded) {
                var schema = byReference.get(occurrence.getSchema());
                if (schema == null) {
                    if (byReference.size() + 1 >= limits.maxBindings())
                        throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                                "Retained schema budget exhausted: schema binding count exceeds limit", null);
                    var definition = DocumentRetainedDefinitions.definition(occurrence.getSchema(), retained);
                    schema = new RetainedSchema(
                            occurrence.getSchema(), definition, DocumentCandidateAdmission.containerType(definition));
                    byReference.put(occurrence.getSchema(), schema);
                }
                var previous = byTypeUrl.putIfAbsent(occurrence.getSchema().getTypeUrl(), schema);
                if (previous != null && !previous.definition().equals(schema.definition()))
                    throw lost("The revision retains two schemas for " + occurrence.getSchema().getTypeUrl(), null);
            }
            if (bytes.size() > limits.maxFragmentBytes())
                throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                        "Retained schema budget exhausted: candidate fragments exceed byte limit", null);
            new MessageWireBudget(limits.maxWireValues(), limits.maxWireDepth(), () -> {}).check(bytes, containerType);
            var input = bytes.newCodedInput();
            input.setRecursionLimit(limits.maxWireDepth());
            var parsed = DynamicMessage.getDefaultInstance(containerType).getParserForType().parseFrom(input);
            input.checkLastTagWas(0);
            if (input.getTotalBytesRead() != bytes.size())
                throw new InvalidProtocolBufferException("Fragment was not completely consumed");
            requireCoreLayout(containerType);
            requireValidCoreContainer(parsed, "", 0, new int[1]);
            DocumentFragmentConfinement.requireConfined(
                    parsed, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE, stored.getAddress().getDocId());
            DocumentLinks.requireLinkSchema(containerType);
            var links = new ArrayList<>(DocumentLinks.extract(parsed, ""));
            var attestedOpaque = new HashMap<List<RepositorySchemaOccurrenceStep>, OpaqueOccurrence>();
            var expectedOpaque = new HashSet<DocumentCandidateAdmission.OpaqueFound>();
            for (var occurrence : recordedOpaque) {
                var steps = List.copyOf(occurrence.getOccurrence().getStepsList());
                attestedOpaque.put(steps, occurrence);
                expectedOpaque.add(new DocumentCandidateAdmission.OpaqueFound(
                        ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE,
                        steps, occurrence.getTypeUrl(), occurrence.getSha256(), occurrence.getLength()));
            }
            var state = new CoreLinkWalkState(byTypeUrl, attestedOpaque, limits.maxDecodedBytes());
            var structuredField = containerType.findFieldByNumber(Document.STRUCTURED_DATA_FIELD_NUMBER);
            if (structuredField != null && parsed.hasField(structuredField)) {
                var at = new ArrayList<RepositorySchemaOccurrenceStep>();
                at.add(RepositorySchemaOccurrenceStep.newBuilder()
                        .setFieldNumber(Document.STRUCTURED_DATA_FIELD_NUMBER).build());
                walkCoreAny((Message) parsed.getField(structuredField), structuredField.getName(),
                        at, 0, null, state, links);
            }
            if (!state.foundResolved.equals(new HashSet<>(recorded)))
                throw lost("Stored part's Any occurrences differ from the retained record", null);
            if (!state.foundOpaque.equals(expectedOpaque))
                throw lost("Stored part's opaque occurrences differ from the retained record", null);
            return List.copyOf(links);
        } catch (DocumentSchemaAdmission.DataLoss | DocumentCandidateAdmission.HostFailure
                 | IOException
                 | IllegalArgumentException | UnsupportedOperationException failure) {
            if (failure instanceof ClosedDescriptorSet.LimitExceededException
                    || failure instanceof MessageWireBudget.LimitExceededException)
                throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                        "Retained schema budget exhausted: " + failure.getMessage(), null, failure);
            throw lost("Stored part does not decode with its retained schema: " + failure.getMessage(), failure);
        } finally {
            for (var lease : leases) lease.close();
        }
    }

    /**
     * A retained payload schema reference bound to its verified definition and descriptor.
     *
     * @param reference the retained schema asset reference
     * @param definition the verified retained schema definition
     * @param type the container message descriptor resolved from {@code definition}
     */
    private record RetainedSchema(
            RepositorySchemaAssetReference reference,
            DocumentSchemaAdmission.Definition definition,
            Descriptors.Descriptor type) {}

    /** Mutable traversal state for one CORE link decode. */
    private static final class CoreLinkWalkState {
        private final Map<String, RetainedSchema> byTypeUrl;
        private final Map<List<RepositorySchemaOccurrenceStep>, OpaqueOccurrence> attestedOpaque;
        private final Set<ResolvedOccurrence> foundResolved = new HashSet<>();
        private final Set<DocumentCandidateAdmission.OpaqueFound> foundOpaque = new HashSet<>();
        private final Set<Descriptors.Descriptor> preparedSchemas = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Map<Descriptors.Descriptor, Boolean> declaresLinks = new IdentityHashMap<>();
        private final Map<Descriptors.FieldDescriptor, LinkField> linkFields = new IdentityHashMap<>();
        private long decodedRemaining;
        private long schemaFields;
        private long anyCount;

        /** Creates walk state for one CORE link decode. */
        CoreLinkWalkState(
                Map<String, RetainedSchema> byTypeUrl,
                Map<List<RepositorySchemaOccurrenceStep>, OpaqueOccurrence> attestedOpaque,
                long decodedRemaining) {
            this.byTypeUrl = byTypeUrl;
            this.attestedOpaque = attestedOpaque;
            this.decodedRemaining = decodedRemaining;
        }

        /** Checks and memoizes whether {@code type} declares any {@link DocumentLink} fields. */
        boolean declaresLinks(Descriptors.Descriptor type) {
            var known = declaresLinks.get(type);
            if (known != null) return known;
            boolean result = DocumentLinks.declaresLinks(type);
            declaresLinks.put(type, result);
            return result;
        }

        /** Resolves and memoizes the {@link LinkField} option declared on {@code field}. */
        LinkField linkField(Descriptors.FieldDescriptor field) {
            var cached = linkFields.get(field);
            if (cached != null) return cached;
            var resolved = DocumentLinks.linkField(field).orElseThrow(() ->
                    new IllegalArgumentException(field.getFullName() + ": a " + DocumentLinks.LINK_TYPE + " field needs the link option"));
            linkFields.put(field, resolved);
            return resolved;
        }

        /** Verifies structural descriptor limits for {@code root} and its reachable message types once per decode. */
        void prepareSchema(Descriptors.Descriptor root) {
            if (preparedSchemas.contains(root)) return;
            var pending = new ArrayList<Descriptors.Descriptor>();
            addSchemaType(root, pending);
            for (int i = 0; i < pending.size(); i++) {
                var type = pending.get(i);
                if (type.toProto().getExtensionRangeCount() != 0)
                    throw new UnsupportedOperationException("payload schema needs extension resolution");
                schemaFields += type.getFields().size();
                if (schemaFields > MAX_PAYLOAD_SCHEMA_FIELDS)
                    throw new IllegalArgumentException("schema field count exceeds limit");
                for (var field : type.getFields()) {
                    if (field.isMapField()) requireMapEntryDescriptor(field.getMessageType());
                    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE)
                        addSchemaType(field.getMessageType(), pending);
                }
            }
        }

        /** Enqueues {@code type} for structural schema verification if not yet visited. */
        private void addSchemaType(Descriptors.Descriptor type, ArrayList<Descriptors.Descriptor> pending) {
            if (preparedSchemas.contains(type)) return;
            if (preparedSchemas.size() == MAX_PAYLOAD_SCHEMA_TYPES)
                throw new IllegalArgumentException("schema message count exceeds limit");
            preparedSchemas.add(type);
            pending.add(type);
        }
    }

    /**
     * Decodes a retained {@link Any} occurrence in CORE, records its resolved or opaque
     * occurrence, and recursively extracts {@link DocumentLinks.Link} values.
     */
    private void walkCoreAny(
            Message any,
            String prefix,
            ArrayList<RepositorySchemaOccurrenceStep> at,
            int depth,
            Descriptors.FieldDescriptor underMap,
            CoreLinkWalkState state,
            List<DocumentLinks.Link> links) throws IOException {
        if (!any.getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("unsupported Any envelope fields");
        var anyType = any.getDescriptorForType();
        var urlField = anyType.findFieldByNumber(1);
        var valueField = anyType.findFieldByNumber(2);
        if (anyType.getFields().size() != 2 || urlField == null || valueField == null
                || !urlField.getName().equals("type_url") || !valueField.getName().equals("value")
                || urlField.isRepeated() || valueField.isRepeated()
                || urlField.isRequired() || valueField.isRequired()
                || urlField.getContainingOneof() != null || valueField.getContainingOneof() != null
                || urlField.hasDefaultValue() || valueField.hasDefaultValue()
                || urlField.getType() != Descriptors.FieldDescriptor.Type.STRING
                || valueField.getType() != Descriptors.FieldDescriptor.Type.BYTES)
            throw new IllegalArgumentException("invalid Any envelope descriptor");
        var typeUrl = (String) any.getField(urlField);
        var value = (ByteString) any.getField(valueField);
        var currentSteps = List.copyOf(at);
        var opaque = state.attestedOpaque.get(currentSteps);
        if (opaque != null) {
            if (!opaque.getTypeUrl().equals(typeUrl))
                throw new IllegalArgumentException("Attested opaque type at CORE is "
                        + opaque.getTypeUrl() + " but the envelope is " + typeUrl);
            state.foundOpaque.add(new DocumentCandidateAdmission.OpaqueFound(
                    ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE,
                    currentSteps, typeUrl, DocumentPartCodec.sha256Hex(value), value.size()));
            return;
        }
        if (typeUrl.isEmpty() || typeUrl.length() > MAX_TYPE_URL_LENGTH)
            throw new IllegalArgumentException("invalid Any type URL");
        if (depth > limits.maxWireDepth())
            throw new IllegalArgumentException("aggregate payload depth exceeds limit");
        if (state.decodedRemaining < 1 || value.size() > state.decodedRemaining)
            throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                    "Retained schema budget exhausted: candidate decoded payload budget exhausted", null);
        var schema = state.byTypeUrl.get(typeUrl);
        if (schema == null)
            throw new IllegalArgumentException("No registered schema for " + typeUrl);
        String suffix = "/" + schema.type().getFullName();
        if (!typeUrl.endsWith(suffix) || typeUrl.length() <= suffix.length())
            throw new IllegalArgumentException("accepted type URL does not identify the bound schema");
        state.decodedRemaining -= value.size();
        state.prepareSchema(schema.type());
        new MessageWireBudget(limits.maxWireValues(), limits.maxWireDepth(), () -> {}).check(value, schema.type());
        var input = value.newCodedInput();
        input.setRecursionLimit(limits.maxWireDepth() - depth);
        var decoded = DynamicMessage.parseFrom(schema.type(), input);
        input.checkLastTagWas(0);
        if (input.getTotalBytesRead() != value.size())
            throw new InvalidProtocolBufferException("incomplete payload decode");
        boolean declaresLinks = state.declaresLinks(schema.type());
        if (underMap != null && declaresLinks)
            throw new IllegalArgumentException(underMap.getFullName() + ": a link cannot sit under a map value");
        var valueSha256 = DocumentPartCodec.sha256Hex(value);
        var boundary = RepositorySchemaOccurrenceStep.newBuilder()
                .setAnyBoundary(RepositoryAnyResolution.newBuilder()
                        .setTypeUrl(typeUrl)
                        .setValueSha256(valueSha256)
                        .setValueSizeBytes(value.size())
                        .setResolved(RepositoryResolvedSchema.newBuilder()
                                .setSchema(schema.definition().metadata().getSchema())
                                .setArtifactSha256(schema.reference().getDescriptorSha256())))
                .build();
        at.add(boundary);
        var resolved = ResolvedOccurrence.newBuilder()
                .setOccurrence(OccurrenceLocator.newBuilder()
                        .setPart(DocumentSaves.CORE_SELECTOR)
                        .addAllSteps(at))
                .setSchema(schema.reference())
                .setPayloadSha256(valueSha256)
                .setPayloadLength(value.size())
                .build();
        if (!state.foundResolved.add(resolved))
            throw new IllegalArgumentException("duplicate schema occurrence path");
        try {
            walkCorePayload(decoded, prefix, at, depth, underMap, state, links);
        } finally {
            at.removeLast();
        }
    }

    /**
     * Walks a decoded payload message inside CORE, extracting {@link DocumentLinks.Link}
     * fields in descriptor order and descending into nested messages and {@link Any} fields.
     */
    private void walkCorePayload(
            Message message,
            String prefix,
            ArrayList<RepositorySchemaOccurrenceStep> at,
            int depth,
            Descriptors.FieldDescriptor underMap,
            CoreLinkWalkState state,
            List<DocumentLinks.Link> links) throws IOException {
        if (depth > limits.maxWireDepth())
            throw new IllegalArgumentException("aggregate payload depth exceeds limit");
        if (!message.getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("unknown candidate fields cannot receive typed archival evidence");
        var type = message.getDescriptorForType();
        if (type.getFullName().equals(ANY_FULL_NAME)) {
            if (++state.anyCount > limits.maxWireValues())
                throw new IllegalArgumentException("Any occurrence limit exceeded");
            walkCoreAny(message, prefix, at, depth + 1, underMap, state, links);
            return;
        }
        for (var entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            var path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            if (field.isMapField()) {
                var keyField = field.getMessageType().findFieldByNumber(1);
                var valueField = field.getMessageType().findFieldByNumber(2);
                var keys = new HashSet<>();
                at.add(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(field.getNumber()).build());
                try {
                    for (var item : (List<?>) entry.getValue()) {
                        var mapEntry = (Message) item;
                        if (depth + 1 > limits.maxWireDepth())
                            throw new IllegalArgumentException("aggregate payload depth exceeds limit");
                        if (!mapEntry.getUnknownFields().asMap().isEmpty())
                            throw new IllegalArgumentException("unknown candidate fields cannot receive typed archival evidence");
                        var key = mapEntry.getField(keyField);
                        if (!keys.add(key))
                            throw new IllegalArgumentException("duplicate map keys cannot receive typed archival evidence");
                        if (valueField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                                && mapEntry.hasField(valueField)) {
                            at.add(RepositorySchemaOccurrenceStep.newBuilder()
                                    .setMapKey(mapKeyStep(keyField.getType(), key)).build());
                            try {
                                walkCorePayload((Message) mapEntry.getField(valueField), path, at,
                                        depth + 2, underMap != null ? underMap : field, state, links);
                            } finally {
                                at.removeLast();
                            }
                        }
                    }
                } finally {
                    at.removeLast();
                }
                continue;
            }
            boolean isLink = field.getMessageType().getFullName().equals(DocumentLinks.LINK_TYPE);
            var values = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            at.add(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(field.getNumber()).build());
            try {
                LinkField declared = isLink ? state.linkField(field) : null;
                for (int i = 0; i < values.size(); i++) {
                    var value = (Message) values.get(i);
                    if (field.isRepeated())
                        at.add(RepositorySchemaOccurrenceStep.newBuilder().setRepeatedIndex(i).build());
                    try {
                        if (isLink) {
                            if (depth + 1 > limits.maxWireDepth())
                                throw new IllegalArgumentException("aggregate payload depth exceeds limit");
                            links.add(new DocumentLinks.Link(
                                    path, field.isRepeated() ? i : 0, field.isRepeated(),
                                    declared, parseLinkValue(field, value)));
                        } else {
                            walkCorePayload(value, field.isRepeated() ? path + "[" + i + "]" : path,
                                    at, depth + 1, underMap, state, links);
                        }
                    } finally {
                        if (field.isRepeated()) at.removeLast();
                    }
                }
            } finally {
                at.removeLast();
            }
        }
    }

    /** Verifies that the retained CORE container descriptor matches {@link Document}'s layout for {@code structured_data}. */
    private static void requireCoreLayout(Descriptors.Descriptor type) {
        var compiled = Document.getDescriptor();
        if (!type.getFullName().equals(compiled.getFullName()))
            throw new IllegalArgumentException("containing schema differs from supported Document layout");
        var field = type.findFieldByNumber(Document.STRUCTURED_DATA_FIELD_NUMBER);
        if (field == null) return;
        var expected = compiled.findFieldByNumber(Document.STRUCTURED_DATA_FIELD_NUMBER);
        if (field.getType() != expected.getType() || field.isRepeated() != expected.isRepeated()
                || field.isMapField() != expected.isMapField()
                || !field.getMessageType().getFullName().equals(expected.getMessageType().getFullName()))
            throw new IllegalArgumentException("containing schema differs from supported Document layout at "
                    + type.getFullName() + " field " + Document.STRUCTURED_DATA_FIELD_NUMBER);
    }

    /** Verifies that the CORE container has no unknown fields and no {@link Any} roots outside {@code structured_data}. */
    private static void requireValidCoreContainer(Message message, String path, int depth, int[] visited) {
        if (depth > MAX_CORE_DEPTH)
            throw new IllegalArgumentException("Document nesting exceeds 64 levels at " + path);
        if (++visited[0] > MAX_CORE_NODES)
            throw new IllegalArgumentException("Document exceeds 1,000,000 nodes");
        if (!message.getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException(DocumentSchemaAdmission.UNKNOWN_DOCUMENT_FIELDS);
        if (message.getDescriptorForType().getFullName().equals(ANY_FULL_NAME)) {
            if (!path.equals("structured_data"))
                throw new IllegalArgumentException("An Any at " + path + " is not allowed");
            return;
        }
        for (var entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            String childPath = path.isEmpty() ? field.getName() : path + "." + field.getName();
            if (field.isRepeated()) {
                for (var child : (List<?>) entry.getValue())
                    requireValidCoreContainer((Message) child, childPath, depth + 1, visited);
            } else {
                requireValidCoreContainer((Message) entry.getValue(), childPath, depth + 1, visited);
            }
        }
    }

    /** Verifies that a protobuf map-entry descriptor conforms to the synthetic map entry layout. */
    private static void requireMapEntryDescriptor(Descriptors.Descriptor entry) {
        var key = entry.findFieldByNumber(1);
        var value = entry.findFieldByNumber(2);
        if (entry.getFields().size() != 2 || key == null || value == null
                || !key.getName().equals("key") || !value.getName().equals("value")
                || key.isRepeated() || value.isRepeated() || key.isRequired() || value.isRequired()
                || key.getContainingOneof() != null || value.getContainingOneof() != null
                || key.hasDefaultValue() || value.hasDefaultValue())
            throw new IllegalArgumentException("invalid map entry descriptor");
        switch (key.getType()) {
            case STRING, BOOL, INT32, SINT32, SFIXED32, UINT32, FIXED32,
                    INT64, SINT64, SFIXED64, UINT64, FIXED64 -> {}
            default -> throw new IllegalArgumentException("invalid map entry descriptor key type");
        }
    }

    /** Encodes a protobuf map key into a {@link RepositoryOccurrenceMapKey} locator step. */
    private static RepositoryOccurrenceMapKey mapKeyStep(Descriptors.FieldDescriptor.Type type, Object value) {
        var result = RepositoryOccurrenceMapKey.newBuilder();
        switch (type) {
            case STRING -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue((String) value);
            case BOOL -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_BOOL).setBoolValue((Boolean) value);
            case INT32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT32).setSignedValue((Integer) value);
            case SINT32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT32).setSignedValue((Integer) value);
            case SFIXED32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED32).setSignedValue((Integer) value);
            case UINT32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT32).setUnsignedValue(Integer.toUnsignedLong((Integer) value));
            case FIXED32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED32).setUnsignedValue(Integer.toUnsignedLong((Integer) value));
            case INT64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT64).setSignedValue((Long) value);
            case SINT64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT64).setSignedValue((Long) value);
            case SFIXED64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED64).setSignedValue((Long) value);
            case UINT64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT64).setUnsignedValue((Long) value);
            case FIXED64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED64).setUnsignedValue((Long) value);
            default -> throw new IllegalArgumentException("unsupported map key type");
        }
        return result.build();
    }

    /** Decodes a dynamic {@link DocumentLink} message and verifies that it carries no unknown fields. */
    private static DocumentLink parseLinkValue(Descriptors.FieldDescriptor field, Message value) {
        if (hasUnknownFields(value))
            throw new IllegalArgumentException(field.getFullName() + ": the link value carries fields this server does not know");
        if (value instanceof DocumentLink typed) return typed;
        final DocumentLink link;
        try {
            link = DocumentLink.parseFrom(value.toByteString());
        } catch (InvalidProtocolBufferException unreadable) {
            throw new IllegalArgumentException(field.getFullName() + ": the link value does not decode as " + DocumentLinks.LINK_TYPE, unreadable);
        }
        if (hasUnknownFields(link))
            throw new IllegalArgumentException(field.getFullName() + ": the link value carries fields this server does not know");
        return link;
    }

    /** Returns {@code true} if {@code message} or any nested sub-message carries unknown fields. */
    private static boolean hasUnknownFields(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) return true;
        for (var value : message.getAllFields().values()) {
            if (value instanceof Message nested && hasUnknownFields(nested)) return true;
            if (value instanceof List<?> list) {
                for (var element : list) {
                    if (element instanceof Message nested && hasUnknownFields(nested)) return true;
                }
            }
        }
        return false;
    }

    /** Builds a {@link Status.Code#DATA_LOSS} {@link AccessionException}. */
    private static AccessionException lost(String message, Throwable cause) {
        return new AccessionException(Status.Code.DATA_LOSS, message, null, cause);
    }

    /**
     * Refuses a part whose recorded catalog length no admitted save could have written:
     * a negative length is a corrupt record (DATA_LOSS); a length above the member's
     * fragment ceiling is refused RESOURCE_EXHAUSTED before anything is opened or
     * allocated, the same fail-fast surface as an exhausted read budget.
     */
    void requirePartWithinCeiling(StackRef ref) {
        long length = ref.getLength();
        if (length < 0)
            throw new AccessionException(Status.Code.DATA_LOSS,
                    "Part records a negative length: " + length, null);
        if (length > limits.maxFragmentBytes())
            throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                    "Part records " + length + " bytes, above the " + limits.maxFragmentBytes()
                            + " byte part ceiling", null);
    }

    /**
     * Buffers one stack object in full into a single array of exactly its catalog
     * length, which callers reserve against the read budget before this method runs.
     * The stack stream is read straight into that array, so the read allocates no
     * scratch chunk and no growable output beyond the reserved bytes, and the returned
     * {@link ByteString} wraps the array without copying it. Reading to end of stream
     * lets the verifying stack stream check length and digest; a stream that ends
     * early or runs past the catalog length is DATA_LOSS.
     */
    ByteString read(StackRef ref) {
        long length = ref.getLength();
        if (length < 0 || length > Integer.MAX_VALUE)
            throw new AccessionException(Status.Code.DATA_LOSS,
                    "Part records an unreadable length: " + length, null);
        try (var stream = drives.byId(ref.getStackId()).open(ref)) {
            var bytes = new byte[(int) length];
            int filled = stream.readNBytes(bytes, 0, bytes.length);
            if (filled != bytes.length || stream.read() >= 0)
                throw new AccessionException(Status.Code.DATA_LOSS,
                        "Stored object length differs from its reference: " + length + " bytes", null);
            return UnsafeByteOperations.unsafeWrap(bytes);
        } catch (StackDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
        }
    }

    static Function<PartSelector, Boolean> selected(PartSelection selection) {
        if (selection.getSelectorsCount() == 0
                && !selection.getAllSegmentations() && !selection.getAllEmbeddingSets()) return selector -> true;
        var selectors = new HashSet<PartSelector>();
        for (var selector : selection.getSelectorsList())
            if (!selectors.add(selector)) throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Duplicate selector", null);
        return selector -> selectors.contains(selector)
                || (selection.getAllSegmentations() && selector.hasSegmentationId())
                || (selection.getAllEmbeddingSets() && selector.hasEmbeddingSet());
    }

    static DocumentManifest manifest(StoredManifest stored, Function<PartSelector, Boolean> included) {
        var manifest = DocumentManifest.newBuilder().setAddress(stored.getAddress()).setRevision(stored.getRevision())
                .setTombstone(stored.getTombstone()).setCommittedAt(stored.getCommittedAt());
        for (var part : stored.getPartsList())
            manifest.addParts(part.getEntry().toBuilder().setContentIncluded(
                    part.getEntry().getState() == PartState.PART_STATE_PRESENT && included.apply(part.getEntry().getSelector())));
        return manifest.build();
    }

    // ------------------------------------------------------------------ shared rules

    /** The public accession of a stored manifest. */
    static Accession accession(StoredManifest stored) {
        var accession = Accession.newBuilder().setAddress(stored.getAddress()).setRevision(stored.getRevision())
                .setPreviousRevision(stored.getPreviousRevision()).setCommittedAt(stored.getCommittedAt())
                .setManifest(manifest(stored, selector -> false)).setValidation(stored.getValidation())
                .setTombstone(stored.getTombstone()).addAllDerivedFrom(stored.getDerivedFromList());
        if (stored.hasOwner()) accession.setOwner(stored.getOwner());
        return accession.build();
    }

    static void requireBinding(RepositoryCaller caller) {
        Objects.requireNonNull(caller, "caller");
        if (!caller.processAuthority() && caller.accountIds().isEmpty())
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller is bound to no account", null);
    }

    static void requireAccount(RepositoryCaller caller, String accountId) {
        if (!caller.processAuthority() && !caller.accountIds().contains(accountId))
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller is not bound to account '" + accountId + "'", null);
    }

    /** NodeAddress imports no validation rules, so every handler checks its four components. */
    static void requireAddress(NodeAddress address) {
        if (address.getDocId().isBlank() || address.getGraphAddressId().isBlank() || address.getAccountId().isBlank()
                || address.getGraphId().isBlank() || !address.getUnknownFields().asMap().isEmpty())
            throw DocumentSaves.malformedException("An address needs a non-blank doc_id, graph_address_id, account_id and graph_id");
    }

    /** Inherited policy has no resolver in v2, so an inheriting document is refused rather than read as resolved. */
    static boolean canRead(RepositoryCaller caller, NodeAddress address, OwnershipContext ownership) {
        return DocumentAccessPolicy.allows(caller, address.getAccountId(), ownership.hasSecurity() ? ownership.getSecurity() : null,
                null, Access.ACCESS_READ);
    }

    static AccessionException translate(RepositoryException failure) {
        var code = switch (failure.code()) {
            case INVALID_ARGUMENT -> Status.Code.INVALID_ARGUMENT;
            case NOT_FOUND -> Status.Code.NOT_FOUND;
            case FAILED_PRECONDITION -> Status.Code.FAILED_PRECONDITION;
            case PERMISSION_DENIED -> Status.Code.PERMISSION_DENIED;
            case CONFLICT -> Status.Code.ABORTED;
            case RESOURCE_EXHAUSTED -> Status.Code.RESOURCE_EXHAUSTED;
            case UNAVAILABLE -> Status.Code.UNAVAILABLE;
            case DATA_LOSS -> Status.Code.DATA_LOSS;
            default -> Status.Code.INTERNAL;
        };
        return new AccessionException(code, failure.getMessage(), null, failure);
    }

    static void valid(com.google.protobuf.Message request) {
        var result = DocumentSaves.VALIDATOR.validate(request);
        if (!result.valid())
            throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Invalid request: " + result.violations().getFirst(), null);
    }

    static AccessionException notFound(String message) {
        return new AccessionException(Status.Code.NOT_FOUND, message, null);
    }

    static <T> T guarded(java.util.function.Supplier<T> read) {
        try {
            return read.get();
        } catch (CatalogDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (CatalogException failure) {
            if (failure.unavailable()) throw new AccessionException(Status.Code.UNAVAILABLE, "Catalog unavailable", null, failure);
            throw failure;
        } catch (RepositoryException failure) {
            throw translate(failure);
        }
    }

}
