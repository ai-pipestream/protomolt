package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentConcepts;
import ai.protomolt.proto.repo.admission.DocumentLinks;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaRetention;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission;
import ai.protomolt.proto.repo.catalog.CatalogEntries;
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
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.BlobWrite;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.spi.ConceptSchemeCatalog;
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
import java.util.OptionalLong;
import java.util.Set;
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
 * (field 13) are refused as unsupported rather than stored in CORE.
 * Explicit opaque occurrences (requirements §9) are accepted only under
 * OPAQUE_ALLOWED with the host policy enabled: each attested occurrence is
 * attributed to a producing step under an active step grant, or carried forward
 * from a previously committed occurrence byte-identically, and is never decoded.
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
    private static final System.Logger LOG = System.getLogger(DocumentSaves.class.getName());
    static final ProtoValidator VALIDATOR = ProtoValidator.create();
    /** Rule recorded once per opaque occurrence: its payload rules were not evaluated. */
    static final String OPAQUE_NOT_EVALUATED_RULE = "protomolt.opaque/v1:payload-not-evaluated";
    private static final Map<DocumentPart, ai.protomolt.proto.repo.v1.DocumentPart> PARTS = Map.of(
            DocumentPart.DOCUMENT_PART_CORE, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE,
            DocumentPart.DOCUMENT_PART_BLOBS, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_BLOBS,
            DocumentPart.DOCUMENT_PART_PARSED, ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_PARSED);
    static final PartSelector CORE_SELECTOR = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
    static final PartSelector BLOBS_SELECTOR = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS).build();
    private static final String SEGMENTATIONS_PREFIX = "search_metadata.segmentations[";
    private static final String EMBEDDING_SETS_PREFIX = "search_metadata.embedding_sets[";
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

    /**
     * Named drives and the {@link Stack} each resolves to, supporting either a fixed
     * host-wide map or dynamic per-account drive resolution backed by a drive ledger.
     */
    public static final class Drives {
        private final String defaultDrive;
        private final Map<String, Stack> stacks;
        private final java.util.function.BiFunction<String, String, Optional<Stack>> byAccountAndDrive;
        private final java.util.function.Function<String, Optional<Stack>> byStackId;

        /**
         * Creates a static drive table shared across all accounts.
         *
         * @param defaultDrive the drive name used when a save request leaves {@code drive} empty
         * @param stacks the configured drive-name to {@link Stack} mappings
         */
        public Drives(String defaultDrive, Map<String, Stack> stacks) {
            this.defaultDrive = Objects.requireNonNull(defaultDrive, "defaultDrive");
            this.stacks = Map.copyOf(stacks);
            if (!this.stacks.containsKey(defaultDrive)) {
                throw new IllegalArgumentException("Default drive has no stack");
            }
            this.byAccountAndDrive = (account, drive) -> Optional.ofNullable(this.stacks.get(drive));
            this.byStackId = stackId -> this.stacks.values().stream()
                    .filter(stack -> stack.stackId().equals(stackId))
                    .findFirst();
        }

        private Drives(
                String defaultDrive,
                java.util.function.BiFunction<String, String, Optional<Stack>> byAccountAndDrive,
                java.util.function.Function<String, Optional<Stack>> byStackId) {
            if (defaultDrive == null || defaultDrive.isBlank()) {
                throw new IllegalArgumentException("Default drive must not be blank");
            }
            this.defaultDrive = defaultDrive;
            this.stacks = Map.of();
            this.byAccountAndDrive = Objects.requireNonNull(byAccountAndDrive, "byAccountAndDrive");
            this.byStackId = Objects.requireNonNull(byStackId, "byStackId");
        }

        /**
         * Creates a dynamic drive resolver that resolves {@code (accountId, driveName)} on each save
         * and {@code stackId} on each read or purge.
         *
         * @param defaultDrive the drive name used when a save request leaves {@code drive} empty
         * @param byAccountAndDrive resolves an account ID and effective drive name to a {@link Stack}
         * @param byStackId resolves a recorded stack ID to a {@link Stack}
         * @return the dynamic drive resolver
         */
        public static Drives dynamic(
                String defaultDrive,
                java.util.function.BiFunction<String, String, Optional<Stack>> byAccountAndDrive,
                java.util.function.Function<String, Optional<Stack>> byStackId) {
            return new Drives(defaultDrive, byAccountAndDrive, byStackId);
        }

        /**
         * Returns the default drive name used when a save request omits {@code drive}.
         *
         * @return the default drive name
         */
        public String defaultDrive() {
            return defaultDrive;
        }

        /**
         * Returns the static drive table, or an empty map when configured dynamically.
         *
         * @return the static drive map
         */
        public Map<String, Stack> stacks() {
            return stacks;
        }

        Stack resolve(String drive) {
            return resolve("", drive);
        }

        Stack resolve(String account, String drive) {
            var target = drive.isEmpty() ? defaultDrive : drive;
            return byAccountAndDrive.apply(account, target)
                    .orElseThrow(() -> malformedException("Unknown drive " + drive));
        }

        Stack byId(String stackId) {
            return byStackId.apply(stackId)
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
    private final boolean opaqueAllowed;
    private final ProtoValidator contentValidator;

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
        this(catalog, drives, resolver, limits, reservations, clock, schemas, false);
    }

    /** A compiled-schema host with an explicit opaque host policy. */
    public DocumentSaves(Catalog catalog, Drives drives, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Clock clock,
            boolean opaqueAllowed) {
        this(catalog, drives, resolver, limits, reservations, clock, DocumentSchemas.compiled(), opaqueAllowed);
    }

    /**
     * @param schemas the latest registered {@code Document} version, asked on every save
     * @param opaqueAllowed host policy: attested opaque occurrences may be accessioned
     */
    public DocumentSaves(Catalog catalog, Drives drives, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Clock clock,
            DocumentSchemas schemas, boolean opaqueAllowed) {
        this(catalog, drives, resolver, limits, reservations, clock, schemas, opaqueAllowed, ConceptSchemeCatalog.empty());
    }

    /**
     * @param schemas the latest registered {@code Document} version, asked on every save
     * @param opaqueAllowed host policy: attested opaque occurrences may be accessioned
     * @param conceptSchemes the host's mounted concept schemes, which decide the membership
     *        of every concept_schemes-bound value a save admits; with none mounted, a bound
     *        value is refused ({@code concept.unmounted})
     */
    public DocumentSaves(Catalog catalog, Drives drives, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Clock clock,
            DocumentSchemas schemas, boolean opaqueAllowed, ConceptSchemeCatalog conceptSchemes) {
        this.contentValidator = DocumentSchemaAdmission.contentValidator(Objects.requireNonNull(conceptSchemes));
        this.catalog = Objects.requireNonNull(catalog);
        this.drives = Objects.requireNonNull(drives);
        this.resolver = Objects.requireNonNull(resolver);
        this.limits = Objects.requireNonNull(limits);
        this.reservations = Objects.requireNonNull(reservations);
        this.clock = Objects.requireNonNull(clock);
        this.schemas = Objects.requireNonNull(schemas);
        this.opaqueAllowed = opaqueAllowed;
        this.authority = new WriterAuthority(catalog, DocumentSchemaRetention.retain(
                BuiltinDocumentSchema.definition(WriterGrant.getDescriptor())));
    }

    /** Seam for tests that need another container identity as the only registered version. */
    DocumentSaves(Catalog catalog, Drives drives, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Clock clock,
            DocumentSchemaAdmission.Definition containerDefinition) {
        this(catalog, drives, resolver, limits, reservations, clock, () -> containerDefinition);
    }

    /** The committed upload response together with the verified payload digest and byte length. */
    record UploadResult(UploadDocumentResponse response, String sha256, long sizeBytes) {}

    /** Pre-validated and pre-authorized upload state ready to consume the payload stream. */
    record PreparedUpload(
            RepositoryCaller caller,
            UploadHeader header,
            SaveDocumentRequest request,
            NodeAddress address,
            DocumentSchemaAdmission.Definition container,
            Descriptors.Descriptor documentType,
            Map<PartSelector, WriteAction> writes,
            Map<PartSelector, ByteString> fragments,
            Map<PartSelector, ByteString> storedFragments,
            BlobBag storedBlobBag,
            List<RawBlobs.Raw> rawBlobs,
            Stack stack,
            Optional<CatalogRecords.Current> current,
            Optional<StoredManifest> base,
            OwnershipContext ownership,
            WriterAuthority.Authorized authorized,
            CatalogRecords.Outcome recorded) {}

    /**
     * Commits one unary document save.
     *
     * @param caller the authenticated repository caller
     * @param request the save request
     * @return the committed save response
     */
    public SaveDocumentResponse save(RepositoryCaller caller, SaveDocumentRequest request) {
        return guarded(() -> saveChecked(caller, request));
    }

    /**
     * Streams a raw blob or an encoded part fragment and commits the save in one
     * accession operation (requirements §11).
     *
     * @param caller the authenticated repository caller
     * @param header the upload header carrying the save request and payload selector
     * @param body the payload stream
     * @return the committed upload response
     */
    public UploadDocumentResponse upload(RepositoryCaller caller, UploadHeader header, java.io.InputStream body) {
        return uploadResult(caller, header, body, () -> false).response();
    }

    UploadResult uploadResult(RepositoryCaller caller, UploadHeader header, java.io.InputStream body,
            java.util.function.BooleanSupplier cancelled) {
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(cancelled, "cancelled");
        var prepared = precheckUpload(caller, header);
        return executeUpload(prepared, body, cancelled);
    }

    PreparedUpload precheckUpload(RepositoryCaller caller, UploadHeader header) {
        return guarded(() -> {
            Objects.requireNonNull(header, "header");
            var headerStructure = VALIDATOR.validate(header);
            if (!headerStructure.valid())
                throw malformedException("Invalid upload header: " + headerStructure.violations().getFirst());
            requireNoUnknownCommandFields(header);
            var request = header.getSave();
            var structure = VALIDATOR.validate(request);
            if (!structure.valid())
                throw malformedException("Invalid save request: " + structure.violations().getFirst());
            DocumentReads.requireBinding(caller);
            var writes = selectors(request);
            var document = normalizedDocument(request);

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

            var address = resolveAddress(request, document);
            var account = address.getAccountId();
            DocumentReads.requireAccount(caller, account);
            var stack = drives.resolve(account, request.getDrive());

            final Map<PartSelector, ByteString> fragments;
            final Map<PartSelector, ByteString> storedFragments;
            BlobBag storedBlobBag = null;
            List<RawBlobs.Raw> rawBlobs = List.of();
            PartSelector streamedPart = null;

            if (header.hasBlobId()) {
                var blobId = header.getBlobId();
                if (blobId.isBlank()) throw malformedException("Uploaded blob_id must not be blank");
                if (writes.get(BLOBS_SELECTOR) != WriteAction.WRITE_ACTION_PUT)
                    throw malformedException("Uploading a raw blob requires a BLOBS PUT write");
                fragments = split(document, documentType, writes, null);
                var extracted = RawBlobs.extractForUpload(fragments.get(BLOBS_SELECTOR), blobId);
                storedFragments = new LinkedHashMap<>(fragments);
                storedFragments.put(BLOBS_SELECTOR, extracted.stored());
                storedBlobBag = extracted.bag();
                rawBlobs = extracted.blobs();
            } else if (header.hasEncodedPart()) {
                streamedPart = supported(header.getEncodedPart().getSelector());
                if (writes.get(streamedPart) != WriteAction.WRITE_ACTION_PUT)
                    throw malformedException("Streamed part " + describe(streamedPart) + " must be listed as a PUT write");
                fragments = split(document, documentType, writes, streamedPart);
                storedFragments = new LinkedHashMap<>(fragments);
                if (!streamedPart.equals(BLOBS_SELECTOR) && fragments.containsKey(BLOBS_SELECTOR)) {
                    var extracted = RawBlobs.extract(fragments.get(BLOBS_SELECTOR));
                    storedFragments.put(BLOBS_SELECTOR, extracted.stored());
                    storedBlobBag = extracted.bag();
                    rawBlobs = extracted.blobs();
                }
            } else {
                throw malformedException("An upload carries a raw blob or an encoded part");
            }

            var recorded = catalog.outcome(account, request.getOperationId());
            if (recorded.isPresent()) {
                if (recorded.get().outcome().hasRejection()) {
                    RejectionAccess.require(catalog, caller, recorded.get());
                } else {
                    var outcomeAddress = recorded.get().outcome().getAccession().getAddress();
                    var cur = catalog.current(outcomeAddress).flatMap(state -> catalog.manifest(outcomeAddress, state.revision()));
                    if (cur.isEmpty() || !DocumentReads.canRead(caller, outcomeAddress, cur.get().getOwnership()))
                        throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller may not read this document", null);
                }
                return new PreparedUpload(caller, header, request, address, container, documentType, writes,
                        fragments, storedFragments, storedBlobBag, rawBlobs, stack, Optional.empty(), Optional.empty(), null, null,
                        recorded.get());
            }

            var current = catalog.current(address);
            boolean creating = request.getExpected().hasNone();
            var base = current.map(state -> catalog.manifest(address, state.revision())
                    .orElseThrow(() -> new IllegalStateException("Current revision has no manifest")));
            if (base.isPresent() && !DocumentAccessPolicy.allows(caller, account, base.get().getOwnership().getSecurity(),
                    null, Access.ACCESS_WRITE))
                throw WriterAuthority.denied("The caller may not write this document");

            final OwnershipContext ownership;
            final WriterAuthority.Authorized authorized;
            if (header.hasBlobId() || !CORE_SELECTOR.equals(streamedPart)) {
                ownership = ownership(document, address, base,
                        writes.get(CORE_SELECTOR) == WriteAction.WRITE_ACTION_PUT);
                requireOwnerMatches(request.getOwner(), ownership);
                authorized = authority.authorize(caller, request.getOwner(), account,
                        ownership.hasConnectorId() ? Optional.of(ownership.getConnectorId()) : Optional.empty(), creating);
            } else if (base.isPresent()) {
                ownership = base.get().getOwnership();
                requireOwnerMatches(request.getOwner(), ownership);
                authorized = authority.authorize(caller, request.getOwner(), account,
                        ownership.hasConnectorId() ? Optional.of(ownership.getConnectorId()) : Optional.empty(), false);
            } else {
                ownership = document.hasOwnership() ? ownership(document, address, base, true) : null;
                if (ownership != null) requireOwnerMatches(request.getOwner(), ownership);
                Optional<String> preConnector = ownership != null && ownership.hasConnectorId()
                        ? Optional.of(ownership.getConnectorId())
                        : (request.getOwner().hasIntake() && request.getOwner().getIntake().hasConnectorId()
                                ? Optional.of(request.getOwner().getIntake().getConnectorId())
                                : Optional.empty());
                authorized = authority.authorize(caller, request.getOwner(), account, preConnector, true);
            }
            if (caller.processAuthority() && request.getOpaqueAttestationsCount() > 0)
                throw WriterAuthority.denied("Process authority may not attest opaque content");

            long currentRevision = current.map(CatalogRecords.Current::revision).orElse(0L);
            if ((!creating && current.isEmpty())
                    || (creating ? current.isPresent() : currentRevision != request.getExpected().getRevision()))
                throw aborted(address, request.getExpected(), currentRevision,
                        current.map(CatalogRecords.Current::tombstone).orElse(false));

            return new PreparedUpload(caller, header, request, address, container, documentType, writes,
                    fragments, storedFragments, storedBlobBag, rawBlobs, stack, current, base, ownership, authorized, null);
        });
    }

    UploadResult executeUpload(PreparedUpload p, java.io.InputStream body, java.util.function.BooleanSupplier cancelled) {
        return guarded(() -> {
            long expectedLength = p.header().hasDeclaredLength() ? p.header().getDeclaredLength() : Stack.UNKNOWN_LENGTH;
            var tracker = new UploadStreamTracker(body, expectedLength, cancelled);

            if (p.recorded() != null) {
                byte[] buffer = new byte[DocumentStreams.CHUNK_SIZE];
                try {
                    while (tracker.read(buffer) >= 0) {
                        // Drain in bounded chunks to compute the retry stream's digest and byte count.
                    }
                    tracker.verifyAtEnd();
                } catch (CancelledUploadException cancel) {
                    throw new AccessionException(Status.Code.CANCELLED, "Upload cancelled", null, cancel);
                } catch (UploadProtocolException protocol) {
                    throw protocol.accession();
                } catch (UploadMismatchException mismatch) {
                    throw rejectionException(rejection(
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH, mismatch.getMessage()));
                } catch (IOException failure) {
                    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted())
                        throw new AccessionException(Status.Code.CANCELLED, "Upload cancelled", null, failure);
                    throw new AccessionException(Status.Code.UNAVAILABLE, "Upload stream read failed", null, failure);
                }
                var actualSha256 = tracker.sha256Hex();
                long actualLength = tracker.count();
                var intentSha = uploadIntentSha(p, actualSha256, actualLength);
                var replayed = replay(p.caller(), p.request(), intentSha, p.recorded());
                if (!p.header().getExpectedSha256().isEmpty()
                        && !actualSha256.equalsIgnoreCase(p.header().getExpectedSha256())) {
                    throw rejectionException(rejection(
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH,
                            "Uploaded SHA-256 " + actualSha256 + " differs from expected "
                                    + p.header().getExpectedSha256()));
                }
                return new UploadResult(UploadDocumentResponse.newBuilder().setSaved(replayed).build(),
                        actualSha256, actualLength);
            }

            if (p.header().hasBlobId()) {
                final StackRef uploadedRef;
                try {
                    uploadedRef = p.stack().put(tracker, tracker.expectedLength());
                    tracker.verifyAtEnd();
                } catch (CancelledUploadException cancel) {
                    throw new AccessionException(Status.Code.CANCELLED, "Upload cancelled", null, cancel);
                } catch (UploadProtocolException protocol) {
                    throw protocol.accession();
                } catch (UploadMismatchException mismatch) {
                    var intentSha = uploadIntentSha(p, tracker.sha256Hex(), tracker.count());
                    reject(p.caller(), p.request(), p.address(), intentSha, p.authorized(),
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH, mismatch.getMessage(), List.of());
                    throw new AssertionError("unreachable");
                } catch (IOException failure) {
                    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted())
                        throw new AccessionException(Status.Code.CANCELLED, "Upload cancelled", null, failure);
                    throw new AccessionException(Status.Code.UNAVAILABLE, "Stack write failed", null, failure);
                }
                if (p.header().hasDeclaredLength() && uploadedRef.getLength() != p.header().getDeclaredLength()) {
                    var cleanupFailure = deleteUnreferenced(p.stack(), uploadedRef);
                    var intentSha = uploadIntentSha(p, uploadedRef.getSha256(), uploadedRef.getLength());
                    withCleanupSuppressed(cleanupFailure, () -> reject(
                            p.caller(), p.request(), p.address(), intentSha, p.authorized(),
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH,
                            "Uploaded length " + uploadedRef.getLength() + " differs from declared length "
                                    + p.header().getDeclaredLength(), List.of()));
                }
                if (!p.header().getExpectedSha256().isEmpty()
                        && !uploadedRef.getSha256().equalsIgnoreCase(p.header().getExpectedSha256())) {
                    var cleanupFailure = deleteUnreferenced(p.stack(), uploadedRef);
                    var intentSha = uploadIntentSha(p, uploadedRef.getSha256(), uploadedRef.getLength());
                    withCleanupSuppressed(cleanupFailure, () -> reject(
                            p.caller(), p.request(), p.address(), intentSha, p.authorized(),
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH,
                            "Uploaded SHA-256 " + uploadedRef.getSha256() + " differs from expected "
                                    + p.header().getExpectedSha256(), List.of()));
                }
                var declared = RawBlobs.stored(p.storedBlobBag(), p.header().getBlobId())
                        .orElseThrow(() -> new IllegalStateException("Uploaded blob is absent from its checked BLOBS fragment"));
                var declaredMismatch = RawBlobs.declaredMismatch(declared, uploadedRef.getLength(), uploadedRef::getSha256);
                if (declaredMismatch.isPresent()) {
                    var cleanupFailure = deleteUnreferenced(p.stack(), uploadedRef);
                    var intentSha = uploadIntentSha(p, uploadedRef.getSha256(), uploadedRef.getLength());
                    withCleanupSuppressed(cleanupFailure, () -> reject(
                            p.caller(), p.request(), p.address(), intentSha, p.authorized(),
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH, declaredMismatch.get(), List.of()));
                }
                var intentSha = uploadIntentSha(p, uploadedRef.getSha256(), uploadedRef.getLength());
                var concurrent = catalog.outcome(p.address().getAccountId(), p.request().getOperationId());
                if (concurrent.isPresent()) {
                    var cleanupFailure = deleteUnreferenced(p.stack(), uploadedRef);
                    var replayed = withCleanupSuppressed(
                            cleanupFailure, () -> replay(p.caller(), p.request(), intentSha, concurrent.get()));
                    return new UploadResult(UploadDocumentResponse.newBuilder().setSaved(replayed).build(),
                            uploadedRef.getSha256(), uploadedRef.getLength());
                }
                var blobRef = BlobObjectRef.newBuilder().setBlobId(p.header().getBlobId()).setRef(uploadedRef).build();
                var saved = finishSave(p.caller(), p.request(), p.address(), intentSha, p.container(), p.documentType(), p.writes(),
                        p.current(), p.base(), p.ownership(), p.authorized(), p.storedFragments(), p.storedBlobBag(),
                        p.rawBlobs(), List.of(blobRef));
                return new UploadResult(UploadDocumentResponse.newBuilder().setSaved(saved).build(),
                        uploadedRef.getSha256(), uploadedRef.getLength());
            }

            var streamedPart = supported(p.header().getEncodedPart().getSelector());
            long maxBytes = limits.maxFragmentBytes();
            var leases = new ArrayList<DocumentAdmissionReservations.Lease>();
            try {
                var out = ByteString.newOutput();
                byte[] buffer = new byte[DocumentStreams.CHUNK_SIZE];
                boolean budgetExceeded = false;
                try {
                    int r;
                    while ((r = tracker.read(buffer)) >= 0) {
                        if (!budgetExceeded) {
                            if (out.size() + r > maxBytes) {
                                budgetExceeded = true;
                            } else {
                                if (r > 0) {
                                    leases.add(Objects.requireNonNull(
                                            reservations.reserve(r), "reservation lease"));
                                }
                                out.write(buffer, 0, r);
                            }
                        }
                    }
                    tracker.verifyAtEnd();
                } catch (CancelledUploadException cancel) {
                    throw new AccessionException(Status.Code.CANCELLED, "Upload cancelled", null, cancel);
                } catch (UploadProtocolException protocol) {
                    throw protocol.accession();
                } catch (UploadMismatchException mismatch) {
                    var intentSha = uploadIntentSha(p, tracker.sha256Hex(), tracker.count());
                    reject(p.caller(), p.request(), p.address(), intentSha, p.authorized(),
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH, mismatch.getMessage(), List.of());
                    throw new AssertionError("unreachable");
                } catch (IOException failure) {
                    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted())
                        throw new AccessionException(Status.Code.CANCELLED, "Upload cancelled", null, failure);
                    throw new AccessionException(Status.Code.UNAVAILABLE, "Upload stream read failed", null, failure);
                }
                var actualSha256 = tracker.sha256Hex();
                long actualLength = tracker.count();
                if (!p.header().getExpectedSha256().isEmpty()
                        && !actualSha256.equalsIgnoreCase(p.header().getExpectedSha256())) {
                    var intentSha = uploadIntentSha(p, actualSha256, actualLength);
                    reject(p.caller(), p.request(), p.address(), intentSha, p.authorized(),
                            RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH,
                            "Uploaded SHA-256 " + actualSha256 + " differs from expected "
                                    + p.header().getExpectedSha256(), List.of());
                }
                if (budgetExceeded)
                    throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED,
                            "Admission host budget exhausted: candidate fragments exceed byte limit", null);
                var streamedBytes = out.toByteString();
                if (streamedBytes.isEmpty())
                    throw malformedException("PUT " + describe(streamedPart) + " has no content; write EMPTY instead");

                var intentSha = uploadIntentSha(p, actualSha256, actualLength);
                var concurrent = catalog.outcome(p.address().getAccountId(), p.request().getOperationId());
                if (concurrent.isPresent()) {
                    var replayed = replay(p.caller(), p.request(), intentSha, concurrent.get());
                    return new UploadResult(UploadDocumentResponse.newBuilder().setSaved(replayed).build(),
                            actualSha256, actualLength);
                }

                final Document parsedFragment;
                try {
                    parsedFragment = Document.parseFrom(streamedBytes);
                } catch (InvalidProtocolBufferException invalid) {
                    reject(p.caller(), p.request(), p.address(), intentSha, p.authorized(),
                            RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
                            "The composed document cannot be admitted: " + describe(streamedPart) + " fragment does not parse", List.of());
                    throw new AssertionError("unreachable");
                }
                var search = parsedFragment.getSearchMetadata();
                if (search.getSemanticResultsCount() > 0 || search.getSegmentationsCount() > 0 || search.getEmbeddingSetsCount() > 0)
                    throw malformedException("Semantic results, segmentations and embedding sets are not supported by this server yet");
                try {
                    DocumentCandidateAdmission.requireStructure(parsedFragment);
                } catch (DocumentCandidateAdmission.ContentRefused refused) {
                    throw malformedException(refused.getMessage());
                }
                var ownership = p.ownership();
                var authorized = p.authorized();
                if (streamedPart.equals(CORE_SELECTOR)) {
                    ownership = ownership(parsedFragment, p.address(), p.base(), true);
                    requireOwnerMatches(p.request().getOwner(), ownership);
                    authorized = authority.authorize(p.caller(), p.request().getOwner(), p.address().getAccountId(),
                            ownership.hasConnectorId() ? Optional.of(ownership.getConnectorId()) : Optional.empty(),
                            p.request().getExpected().hasNone());
                }
                var storedFragments = new LinkedHashMap<>(p.storedFragments());
                var storedBlobBag = p.storedBlobBag();
                var rawBlobs = p.rawBlobs();
                if (streamedPart.equals(BLOBS_SELECTOR)) {
                    var extracted = RawBlobs.extract(streamedBytes, parsedFragment);
                    storedFragments.put(BLOBS_SELECTOR, extracted.stored());
                    storedBlobBag = extracted.bag();
                    rawBlobs = extracted.blobs();
                } else {
                    storedFragments.put(streamedPart, streamedBytes);
                }
                var saved = finishSave(p.caller(), p.request(), p.address(), intentSha, p.container(), p.documentType(), p.writes(),
                        p.current(), p.base(), ownership, authorized, storedFragments, storedBlobBag, rawBlobs, List.of());
                return new UploadResult(UploadDocumentResponse.newBuilder().setSaved(saved).build(),
                        actualSha256, actualLength);
            } finally {
                for (int i = leases.size() - 1; i >= 0; i--) {
                    leases.get(i).close();
                }
            }
        });
    }

    private static <T> T guarded(java.util.function.Supplier<T> call) {
        try {
            return call.get();
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

        // S3: call number and account scope. The drive existence check (S4) moves
        // inside the reservation below: with the dynamic resolver a cold drive's
        // first resolve registers its stack in the catalog, and an over-budget
        // save must be refused before any catalog write. Malformed drive names
        // are already rejected by the S1 structure check.
        var address = resolveAddress(request, document);
        var account = address.getAccountId();
        DocumentReads.requireAccount(caller, account);

        // S6-S7: content identity, intent, and the early outcome lookup. The intent binds the
        // client's fragments, raw blob bytes included; the stored BLOBS fragment leaves them out
        // and a stored CHUNKS fragment may gain the server-computed content digest.
        var fragments = split(document, documentType, writes, null);
        var storedFragments = new LinkedHashMap<PartSelector, ByteString>(fragments);
        BlobBag storedBlobBag = null;
        List<RawBlobs.Raw> rawBlobs = List.of();
        if (fragments.containsKey(BLOBS_SELECTOR)) {
            var extracted = RawBlobs.extract(fragments.get(BLOBS_SELECTOR));
            storedFragments.put(BLOBS_SELECTOR, extracted.stored());
            storedBlobBag = extracted.bag();
            rawBlobs = extracted.blobs();
        }
        // The unary save reservation: one lease over the admitted serialized
        // fragments and raw blob bytes, taken here, where the buffers come into
        // being, and held until finishSave returns, so the lease spans intent
        // computation, admission and the stack writes. An over-budget save is
        // RESOURCE_EXHAUSTED before any catalog or stack write, matching the
        // read path, and the lease releases on success and on failure. The
        // per-write path in put() takes no lease of its own.
        long bufferedBytes = 0;
        for (var fragment : storedFragments.values()) bufferedBytes += fragment.size();
        for (var raw : rawBlobs) bufferedBytes += raw.data().size();
        try (var reserved = reservations.reserve(bufferedBytes)) {
            // S4: drive existence, inside the lease so a resolve failure releases
            // it, and after the reserve so a cold drive's stack registration
            // never happens for a save the budget refuses.
            drives.resolve(account, request.getDrive());
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
            // S9: process authority is never valid for opaque writes (requirements §3, §11).
            if (caller.processAuthority() && (request.getValidationMode() == ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED
                    || request.getOpaqueAttestationsCount() > 0))
                throw WriterAuthority.denied("Process authority may not save opaque content");

            return finishSave(caller, request, address, intentSha, container, documentType, writes, current, base,
                    ownership, authorized, storedFragments, storedBlobBag, rawBlobs, List.of());
        }
    }

    private SaveDocumentResponse finishSave(RepositoryCaller caller, SaveDocumentRequest request, NodeAddress address,
            String intentSha, DocumentSchemaAdmission.Definition container, Descriptors.Descriptor documentType,
            Map<PartSelector, WriteAction> writes, Optional<CatalogRecords.Current> current,
            Optional<StoredManifest> base, OwnershipContext ownership, WriterAuthority.Authorized authorized,
            Map<PartSelector, ByteString> storedFragments, BlobBag storedBlobBag, List<RawBlobs.Raw> rawBlobs,
            List<BlobObjectRef> preUploadedBlobs) {
        var account = address.getAccountId();
        boolean creating = request.getExpected().hasNone();
        // The writer is authorized: from here on refusals are recorded outcomes.
        long currentRevision = current.map(CatalogRecords.Current::revision).orElse(0L);
        if (creating ? current.isPresent() : currentRevision != request.getExpected().getRevision())
            return conflict(caller, request, intentSha, address);
        var carriedBlobs = carriedBlobs(storedBlobBag, rawBlobs, preUploadedBlobs, base);
        boolean opaqueMode = request.getValidationMode() == ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED;
        if (opaqueMode && !opaqueAllowed)
            return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_OPAQUE_NOT_ALLOWED,
                    "Host policy does not allow opaque content", List.of());
        // Explicit opaque attestations resolve to a verified producer or a verified
        // carried source before composition; every refusal from here is recorded.
        var resolvedOpaque = new LinkedHashMap<OccurrenceLocator, ResolvedOpaque>();
        var opaqueSourceRefs = new ArrayList<StackRef>();
        if (opaqueMode) {
            var grantIds = new ArrayList<>(authorized.grantIds());
            try {
                for (var attestation : request.getOpaqueAttestationsList()) {
                    var resolved = resolveAttestation(caller, request, account, creating, attestation, grantIds, opaqueSourceRefs);
                    if (resolvedOpaque.putIfAbsent(attestation.getOccurrence(), resolved) != null)
                        throw malformedException("Duplicate opaque attestation occurrence");
                }
            } catch (OpaqueRefusal refusal) {
                return reject(caller, request, address, intentSha, authorized, refusal.reason, refusal.getMessage(), List.of());
            }
            if (!grantIds.equals(authorized.grantIds()))
                authorized = new WriterAuthority.Authorized(authorized.grant(), authorized.process(),
                        List.copyOf(grantIds), authorized.credential());
        }

        // S10: compose the candidate from carried references, sources and writes.
        long revision = currentRevision + 1;
        var parts = new LinkedHashMap<PartSelector, StoredPartEntry>();
        base.ifPresent(stored -> stored.getPartsList().forEach(part -> parts.put(part.getEntry().getSelector(), part)));
        var carried = new HashSet<>(parts.keySet());
        var kinds = new LinkedHashMap<PartSelector, ChangeKind>();
        var sourced = new LinkedHashMap<PartSelector, StackRef>();
        // The raw blob bindings this save makes; only these take its BlobWrite.
        var newBindings = new HashSet<BindingKey>();
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
            // A sourced part's opaque occurrences carry forward from its source
            // revision; they never enter a typed save or a refusing host silently.
            var sourcedOpaque = from.get().getValidation().getOpaqueOccurrencesList().stream()
                    .filter(occurrence -> occurrence.getOccurrence().getPart().equals(selector)).toList();
            // S9 before any recorded outcome: process authority never sources opaque parts.
            if (!sourcedOpaque.isEmpty() && caller.processAuthority())
                throw WriterAuthority.denied("Process authority may not save opaque content");
            if (!sourcedOpaque.isEmpty() && !(opaqueMode && opaqueAllowed))
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_OPAQUE_NOT_ALLOWED,
                        "Host policy does not allow opaque content", List.of());
            for (var original : sourcedOpaque) {
                requireGrantEvidenceIntact(original.getWriterGrant());
                var synthesized = OpaqueAttestation.newBuilder().setOccurrence(original.getOccurrence())
                        .setTypeUrl(original.getTypeUrl()).setOwner(original.getOwner())
                        .setCarriedFrom(OpaqueSource.newBuilder()
                                .setSource(SourceRevision.newBuilder().setAddress(source.getFromAddress())
                                        .setRevision(source.getFromRevision()))
                                .setOccurrence(original.getOccurrence())).build();
                resolvedOpaque.putIfAbsent(original.getOccurrence(), new ResolvedOpaque(synthesized,
                        original.getOwner(), original.getWriterGrant(), Optional.of(original)));
            }
            var copied = entry.get().toBuilder();
            copied.getEntryBuilder().setWrittenInRevision(revision).setOwner(request.getOwner()).clearContentIncluded()
                    .clearWrittenAt();
            // A sourced part is written in this revision, so it and its raw blob bindings take
            // this revision's stamp below; only carry-forward keeps an earlier one.
            for (int index = 0; index < copied.getBlobRefsCount(); index++) {
                copied.getBlobRefsBuilder(index).clearWritten();
                newBindings.add(new BindingKey(selector, copied.getBlobRefs(index).getBlobId()));
            }
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
        // S9 before any recorded outcome: process authority never carries opaque parts forward.
        if (caller.processAuthority() && base.isPresent() && base.get().getValidation().getOpaqueOccurrencesList().stream()
                .anyMatch(occurrence -> carried.contains(occurrence.getOccurrence().getPart())))
            throw WriterAuthority.denied("Process authority may not save opaque content");

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
        var admissionOpaque = new ArrayList<DocumentCandidateAdmission.OpaqueFound>();
        List<DocumentLinks.Link> coreLinks = List.of();
        List<DocumentConcepts.Tag> coreConcepts = List.of();
        if (!admitted.isEmpty()) {
            var attested = new HashSet<DocumentCandidateAdmission.AttestedOpaque>();
            for (var resolved : resolvedOpaque.values())
                attested.add(new DocumentCandidateAdmission.AttestedOpaque(
                        fragment(resolved.attestation().getOccurrence().getPart(), ByteString.empty()).part(),
                        resolved.attestation().getOccurrence().getStepsList(), resolved.attestation().getTypeUrl()));
            try (var result = DocumentCandidateAdmission.check(
                    new DocumentCandidateAdmission.Request(address.getDocId(), admitted, container, attested),
                    resolver, limits, reservations, clock.instant(), () -> {}, contentValidator)) {
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
                admissionOpaque.addAll(accepted.opaque());
                coreLinks = accepted.links();
                coreConcepts = accepted.concepts();
                fragmentSchema = accepted.container();
                for (var occurrence : accepted.occurrences())
                    validation.addResolvedOccurrences(ResolvedOccurrence.newBuilder()
                            .setOccurrence(OccurrenceLocator.newBuilder()
                                    .setPart(PartSelector.newBuilder().setPart(v2(occurrence.part())))
                                    .addAllSteps(occurrence.steps()))
                            .setSchema(occurrence.schema()).setPayloadSha256(occurrence.payloadSha256())
                            .setPayloadLength(occurrence.payloadLength()));
                artifacts.putAll(accepted.artifacts());
            } catch (DocumentCandidateAdmission.OpaqueMismatch mismatch) {
                return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                        mismatch.getMessage(), List.of());
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

        // Opaque verdict: written and sourced occurrences must each match their
        // verified attestation exactly; unchanged parts carry theirs forward whole.
        var allOpaque = new ArrayList<OpaqueOccurrence>();
        try {
            var foundByLocator = new HashMap<OccurrenceLocator, DocumentCandidateAdmission.OpaqueFound>();
            for (var found : admissionOpaque)
                foundByLocator.put(OccurrenceLocator.newBuilder()
                        .setPart(PartSelector.newBuilder().setPart(v2(found.part()))).addAllSteps(found.steps()).build(), found);
            for (var entry : resolvedOpaque.entrySet()) {
                var found = foundByLocator.remove(entry.getKey());
                if (found == null)
                    throw new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                            "Attested opaque occurrence is not present in the candidate");
                var resolved = entry.getValue();
                if (resolved.original().isPresent()) {
                    var original = resolved.original().get();
                    if (!found.typeUrl().equals(original.getTypeUrl()) || !found.sha256().equals(original.getSha256())
                            || found.length() != original.getLength())
                        throw new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                                "Carried opaque bytes differ from their committed source");
                }
                var occurrence = OpaqueOccurrence.newBuilder().setOccurrence(entry.getKey())
                        .setTypeUrl(found.typeUrl()).setSha256(found.sha256()).setLength(found.length())
                        .setOwner(resolved.owner()).setWriterGrant(resolved.grant());
                if (resolved.attestation().hasCarriedFrom()) occurrence.setCarriedFrom(resolved.attestation().getCarriedFrom());
                allOpaque.add(occurrence.build());
            }
        } catch (OpaqueRefusal refusal) {
            return reject(caller, request, address, intentSha, authorized, refusal.reason, refusal.getMessage(), List.of());
        }
        var carriedOpaque = base.map(stored -> stored.getValidation().getOpaqueOccurrencesList().stream()
                .filter(occurrence -> carried.contains(occurrence.getOccurrence().getPart())).toList())
                .orElse(List.of());
        carriedOpaque.forEach(occurrence -> requireGrantEvidenceIntact(occurrence.getWriterGrant()));
        if (!carriedOpaque.isEmpty() && !(opaqueMode && opaqueAllowed))
            return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_OPAQUE_NOT_ALLOWED,
                    "Host policy does not allow opaque content", List.of());
        allOpaque.addAll(carriedOpaque);
        if (!allOpaque.isEmpty()) {
            validation.setStatus(ValidationStatus.VALIDATION_STATUS_OPAQUE_ACCEPTED);
            var opaqueParts = new HashSet<PartSelector>();
            for (var occurrence : allOpaque) {
                validation.addOpaqueOccurrences(occurrence);
                validation.addRuleResults(RuleResult.newBuilder().setRuleId(OPAQUE_NOT_EVALUATED_RULE)
                        .setState(RuleResultState.RULE_RESULT_STATE_NOT_EVALUATED));
                opaqueParts.add(occurrence.getOccurrence().getPart());
            }
            for (var part : List.copyOf(parts.keySet())) {
                var rebuilt = parts.get(part).toBuilder();
                rebuilt.getEntryBuilder().setOpaque(opaqueParts.contains(part));
                parts.put(part, rebuilt.build());
            }
        }

        // S11b: static link targets (requirements §17), a point-in-time catalog read outside
        // the commit transaction. A carried CORE is not admitted and its links are not rechecked.
        var linkRefusal = linkRefusal(caller, account, coreLinks);
        if (linkRefusal.isPresent())
            return reject(caller, request, address, intentSha, authorized, linkRefusal.get().reason(),
                    linkRefusal.get().message(), linkRefusal.get().violations());
        var linkRows = DocumentLinkReads.rows(coreLinks);
        // S11c: concept tags (requirements §17) must fit the catalog's tag index, refused by name.
        var unstorableTags = coreConcepts.stream().flatMap(tag -> CatalogEntries.unstorableConcept(tag.fieldPath()).stream()
                .map(problem -> FieldViolation.newBuilder().setPart(CORE_SELECTOR).setFieldPath(tag.at())
                        .setRuleId("concept-catalog-storable").setMessage(problem).build())).toList();
        if (!unstorableTags.isEmpty())
            return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
                    "A concept tag does not fit the catalog", unstorableTags);

        // S12: write new bytes to the drive's stack, before the commit transaction. Each raw
        // blob is its own object, bound to its blob ID on the BLOBS entry (decision 45).
        var stack = drives.resolve(account, request.getDrive());
        var newObjects = new ArrayList<StackRef>();
        var blobRefs = new ArrayList<BlobObjectRef>();
        for (var uploaded : preUploadedBlobs) {
            newObjects.add(uploaded.getRef());
            blobRefs.add(uploaded);
        }
        // Reuse only this document's authorized base revision, in the selected
        // stack. Admission above still validates every PUT; this changes storage
        // allocation, not the revision, writer evidence or operation identity.
        var reusable = new HashMap<ContentIdentity, StackRef>();
        base.ifPresent(stored -> stored.getPartsList().forEach(part -> {
            if (part.hasRef()) rememberReusable(stack, reusable, part.getRef());
            part.getBlobRefsList().forEach(blob -> rememberReusable(stack, reusable, blob.getRef()));
        }));
        for (var raw : rawBlobs) {
            var ref = putOrReuse(stack, raw.data(), reusable, newObjects);
            blobRefs.add(BlobObjectRef.newBuilder().setBlobId(raw.blobId()).setRef(ref).build());
        }
        for (var ref : blobRefs) newBindings.add(new BindingKey(BLOBS_SELECTOR, ref.getBlobId()));
        // Blob-level carry-forward: the expected revision's binding, stamp included, with
        // no bytes sent and no copy; its object joins the carried-object purge check below.
        blobRefs.addAll(carriedBlobs);
        if (!blobRefs.isEmpty() && storedBlobBag != null)
            blobRefs = new ArrayList<>(RawBlobs.orderRefs(storedBlobBag, blobRefs));
        for (var write : request.getWritesList()) {
            if (write.getAction() != WriteAction.WRITE_ACTION_PUT) continue;
            var selector = write.getSelector();
            var ref = putOrReuse(stack, storedFragments.get(selector), reusable, newObjects);
            var entry = parts.get(selector).toBuilder().setRef(ref);
            if (selector.equals(BLOBS_SELECTOR)) entry.addAllBlobRefs(blobRefs);
            parts.put(selector, entry.build());
        }
        // Carried and sourced parts reference their objects, raw blobs included, without re-upload.
        // An explicit carried_from source part joins them so a purge of it fails the
        // commit as SOURCE_PURGED instead of qualifying vanished opaque bytes.
        var carriedObjects = new ArrayList<StackRef>();
        for (var part : parts.values()) {
            if (part.hasRef() && !newObjects.contains(part.getRef())) carriedObjects.add(part.getRef());
            for (var blob : part.getBlobRefsList())
                if (!newObjects.contains(blob.getRef())) carriedObjects.add(blob.getRef());
        }
        for (var ref : opaqueSourceRefs)
            if (!newObjects.contains(ref) && !carriedObjects.contains(ref)) carriedObjects.add(ref);

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
        // Every part and binding this revision wrote takes its commit time; carried ones keep theirs.
        var writtenBuilder = BlobWrite.newBuilder().setRevision(revision).setCommittedAt(committedAt);
        if (request.getOwner().hasStep()) writtenBuilder.setStep(request.getOwner().getStep());
        var written = writtenBuilder.build();
        var stamped = ordered.stream().map(part -> stampNewWrites(part, revision, written, newBindings)).toList();
        var stored = StoredManifest.newBuilder().setAddress(address).setRevision(revision)
                .setPreviousRevision(currentRevision).addAllParts(stamped).setValidation(validation)
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
        // The catalog entry's lookup follows the CORE part: projected from the written or
        // sourced bytes admitted above, carried by the catalog when CORE is unchanged.
        var coreBytes = writes.get(CORE_SELECTOR) == WriteAction.WRITE_ACTION_PUT
                ? storedFragments.get(CORE_SELECTOR) : sourcedBytes.get(CORE_SELECTOR);
        var lookup = coreBytes == null ? Optional.<DocumentLookup>empty()
                : Optional.of(admittedLookup(CatalogEntries.lookup(parseCore(coreBytes, documentType)), stored.getValidation()));
        // The catalog indexes the title and metadata keys and holds every lookup string in a
        // text column, so what it cannot store is refused here by field, not inside the commit.
        var unstorable = lookup.map(CatalogEntries::unstorable).orElse(List.of());
        if (!unstorable.isEmpty())
            return reject(caller, request, address, intentSha, authorized, RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
                    "The CORE part carries lookup fields the catalog cannot store", unstorable.stream()
                            .map(problem -> FieldViolation.newBuilder().setPart(CORE_SELECTOR).setFieldPath(problem.fieldPath())
                                    .setRuleId("catalog-lookup-storable").setMessage(problem.message()).build()).toList());
        var result = catalog.commit(new CatalogRecords.Commit(address, request.getOperationId(), intentSha,
                request.getExpected(), stored, outcome, event, newObjects, carriedObjects, artifacts,
                authorized.grantIds(), authorized.credential(), lookup,
                lookup.isPresent() ? linkRows : List.of(), lookup.isPresent() ? conceptRows(coreConcepts) : List.of()));
        return switch (result) {
            case CatalogRecords.Committed ignored -> SaveDocumentResponse.newBuilder()
                    .setOperationId(request.getOperationId()).setAccession(accession).build();
            case CatalogRecords.Recorded again -> replay(caller, request, intentSha, again.outcome());
            case CatalogRecords.Conflict conflict -> throw aborted(address, request.getExpected(), conflict.currentRevision(),
                    conflict.currentTombstone());
            case CatalogRecords.SourcePurged purged -> reject(caller, request, address, intentSha, authorized,
                    RejectionReason.REJECTION_REASON_SOURCE_PURGED, "A carried object is purged", List.of());
            case CatalogRecords.TargetPurged ignored -> reject(caller, request, address, intentSha, authorized,
                    RejectionReason.REJECTION_REASON_SOURCE_PURGED, "The destination document was purged", List.of());
            case CatalogRecords.WriteDenied denied -> throw WriterAuthority.denied(denied.reason());
        };
    }

    /**
     * The expected revision's bindings for the blobs of a BLOBS PUT that carry
     * neither inline data nor a streamed upload. Each must name a raw blob of the
     * expected revision, and any size or SHA-256 it declares must match that blob's
     * object; anything else is a malformed request.
     */
    private static List<BlobObjectRef> carriedBlobs(BlobBag storedBlobBag,
            List<RawBlobs.Raw> rawBlobs, List<BlobObjectRef> preUploadedBlobs, Optional<StoredManifest> base) {
        if (storedBlobBag == null) return List.of();
        var bound = new HashSet<String>();
        rawBlobs.forEach(raw -> bound.add(raw.blobId()));
        preUploadedBlobs.forEach(ref -> bound.add(ref.getBlobId()));
        var unbound = RawBlobs.unbound(storedBlobBag, bound);
        if (unbound.isEmpty()) return List.of();
        var previous = new HashMap<String, BlobObjectRef>();
        base.ifPresent(stored -> stored.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().equals(BLOBS_SELECTOR))
                .forEach(part -> part.getBlobRefsList().forEach(ref -> previous.put(ref.getBlobId(), ref))));
        var carried = new ArrayList<BlobObjectRef>(unbound.size());
        for (var blob : unbound) {
            var ref = previous.get(blob.getBlobId());
            if (ref == null)
                throw malformedException("Blob " + blob.getBlobId() + " has no inline data and names no raw blob of the "
                        + "expected revision; a raw file is sent inline, uploaded, or carried forward by its blob_id");
            RawBlobs.requireDeclaredFacts(blob, ref.getRef().getLength(), ref.getRef()::getSha256);
            carried.add(ref);
        }
        return carried;
    }

    /** One raw blob binding of one part. */
    record BindingKey(PartSelector part, String blobId) {}

    /**
     * The part with this revision's write recorded: {@code written_at} on an entry
     * written or sourced in this revision, and the {@link BlobWrite} on each raw blob
     * binding this revision made. Carried entries and bindings are left as they were,
     * stamped or not: a binding without a stamp is never given this revision's.
     */
    static StoredPartEntry stampNewWrites(StoredPartEntry part, long revision, BlobWrite written,
            Set<BindingKey> newBindings) {
        var selector = part.getEntry().getSelector();
        boolean entryWritten = part.getEntry().getWrittenInRevision() == revision && !part.getEntry().hasWrittenAt();
        boolean bindingsWritten = part.getBlobRefsList().stream()
                .anyMatch(ref -> newBindings.contains(new BindingKey(selector, ref.getBlobId())));
        if (!entryWritten && !bindingsWritten) return part;
        var stamped = part.toBuilder();
        if (entryWritten) stamped.getEntryBuilder().setWrittenAt(written.getCommittedAt());
        for (int index = 0; index < stamped.getBlobRefsCount(); index++)
            if (newBindings.contains(new BindingKey(selector, stamped.getBlobRefs(index).getBlobId())))
                stamped.getBlobRefsBuilder(index).setWritten(written);
        return stamped.build();
    }

    private static String uploadIntentSha(PreparedUpload p, String payloadSha256, long payloadLength) {
        var contents = new HashMap<Integer, SaveIntents.Content>();
        var streamedPart = p.header().hasEncodedPart()
                ? supported(p.header().getEncodedPart().getSelector()) : null;
        for (int index = 0; index < p.request().getWritesCount(); index++) {
            var write = p.request().getWrites(index);
            if (write.getAction() != WriteAction.WRITE_ACTION_PUT) continue;
            var selector = write.getSelector();
            if (p.header().hasBlobId() && selector.equals(BLOBS_SELECTOR)) {
                contents.put(index, RawBlobs.uploadedContent(p.storedFragments().get(BLOBS_SELECTOR),
                        p.rawBlobs(), p.header().getBlobId(), payloadSha256, payloadLength));
            } else if (selector.equals(streamedPart)) {
                contents.put(index, new SaveIntents.Content(payloadSha256, payloadLength));
            } else {
                var bytes = p.fragments().get(selector);
                contents.put(index, new SaveIntents.Content(sha256(bytes), bytes.size()));
            }
        }
        return SaveIntents.sha256(SaveIntents.intent(p.request(), p.address(), contents));
    }

    private static Exception deleteUnreferenced(Stack stack, StackRef ref) {
        try {
            stack.delete(ref);
            return null;
        } catch (IOException | RuntimeException cleanupFailure) {
            LOG.log(
                    System.Logger.Level.WARNING,
                    () -> "Failed to delete unreferenced upload object stack_id="
                            + ref.getStackId() + " key=" + ref.getKey(),
                    cleanupFailure);
            return cleanupFailure;
        }
    }

    private static <T> T withCleanupSuppressed(Exception cleanupFailure, java.util.function.Supplier<T> action) {
        try {
            return action.get();
        } catch (RuntimeException | Error primary) {
            if (cleanupFailure != null && primary != cleanupFailure) {
                primary.addSuppressed(cleanupFailure);
            }
            throw primary;
        }
    }

    // ------------------------------------------------------------------ structure

    static void requireNoUnknownCommandFields(Message root) {
        var pending = new java.util.ArrayDeque<Message>();
        pending.add(root);
        while (!pending.isEmpty()) {
            var message = pending.removeFirst();
            if (!message.getUnknownFields().asMap().isEmpty())
                throw malformedException("Unsupported field in the command: " + message.getDescriptorForType().getName());
            for (var entry : message.getAllFields().entrySet()) {
                var field = entry.getKey();
                // Content has its own schema admission rules. This walk checks
                // only commands, including nested owners, selectors and locators.
                if (message instanceof SaveDocumentRequest && field.getNumber() == SaveDocumentRequest.DOCUMENT_FIELD_NUMBER) continue;
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
        // S1: attestations are request structure. They need OPAQUE_ALLOWED, supported
        // part selectors (never an NPE from an unmapped part) and unique locators.
        if (!request.getOpaqueAttestationsList().isEmpty()
                && request.getValidationMode() != ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
            throw malformedException("Opaque attestations require VALIDATION_MODE_OPAQUE_ALLOWED");
        var attestedLocators = new HashSet<OccurrenceLocator>();
        for (var attestation : request.getOpaqueAttestationsList()) {
            attestedSupported(attestation.getOccurrence().getPart());
            if (attestation.hasCarriedFrom()) attestedSupported(attestation.getCarriedFrom().getOccurrence().getPart());
            if (!attestedLocators.add(attestation.getOccurrence()))
                throw malformedException("Duplicate opaque attestation occurrence");
        }
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

    /** Opaque attestations name occurrences in the unkeyed parts only (slice 3 contract). */
    private static void attestedSupported(PartSelector selector) {
        if (selector.getSelectorCase() != PartSelector.SelectorCase.PART || !PARTS.containsKey(selector.getPart()))
            throw malformedException("An opaque attestation names a CORE, BLOBS or PARSED part");
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
     * and refuses content for any part not listed as PUT (or for a part supplied via
     * an {@code UploadDocument} encoded-part stream). Fields that version does not
     * know stay in their fragment as unknown fields for admission to refuse.
     */
    private static Map<PartSelector, ByteString> split(Document request, Descriptors.Descriptor documentType,
            Map<PartSelector, WriteAction> writes, PartSelector streamedPart) {
        final Message document;
        if (documentType == Document.getDescriptor()) {
            document = request;
        } else {
            try {
                document = DynamicMessage.parseFrom(documentType, request.toByteString());
            } catch (InvalidProtocolBufferException impossible) {
                throw new IllegalStateException("A parsed request document failed to parse again", impossible);
            }
        }
        final ai.protomolt.proto.repo.codec.PartLayout layout;
        try {
            layout = PartLayouts.document(documentType);
        } catch (IllegalArgumentException host) {
            throw new AccessionException(Status.Code.INTERNAL, "The registered Document lacks a part field: "
                    + host.getMessage(), null, host);
        }
        var fragments = new LinkedHashMap<PartSelector, ByteString>();
        final List<ai.protomolt.proto.repo.codec.PartObject> splits;
        try {
            splits = DocumentPartCodec.splitDeterministic(document, layout);
        } catch (IllegalArgumentException duplicate) {
            throw malformedException(duplicate.getMessage());
        }
        for (var part : splits) {
            var selector = selector(part);
            var bytes = ByteString.copyFrom(part.bytes());
            if (selector.equals(streamedPart)) {
                if (selector.equals(CORE_SELECTOR) && !coreHasContent(bytes, documentType, layout)) continue;
                throw malformedException("Streamed part " + describe(selector) + " must not also carry inline content in document");
            }
            boolean put = writes.get(selector) == WriteAction.WRITE_ACTION_PUT;
            if (!put && !(selector.equals(CORE_SELECTOR) && !coreHasContent(bytes, documentType, layout)))
                throw malformedException("Content for " + describe(selector) + " is present but the part is not written");
            if (put) fragments.put(selector, bytes);
        }
        for (var write : writes.entrySet())
            if (write.getValue() == WriteAction.WRITE_ACTION_PUT && !write.getKey().equals(streamedPart)
                    && !fragments.containsKey(write.getKey()))
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
        var fragment = parseCore(core, documentType);
        if (!fragment.getUnknownFields().asMap().isEmpty()) return true;
        var parents = layout.nestedParents();
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
        int index = keyedElementIndex(violation.fieldPath());
        if (index >= keyed.size())
            throw new AccessionException(Status.Code.INTERNAL, "Validation names element " + index + " of " + keyed.size()
                    + " admitted " + part + " fragments", null);
        return keyed.get(index);
    }

    private static int keyedElementIndex(String fieldPath) {
        int start;
        if (fieldPath.startsWith(SEGMENTATIONS_PREFIX)) {
            start = SEGMENTATIONS_PREFIX.length();
        } else if (fieldPath.startsWith(EMBEDDING_SETS_PREFIX)) {
            start = EMBEDDING_SETS_PREFIX.length();
        } else {
            return 0;
        }
        int end = fieldPath.indexOf(']', start);
        if (end <= start) return 0;
        for (int i = start; i < end; i++) {
            char c = fieldPath.charAt(i);
            if (c < '0' || c > '9') return 0;
        }
        return Integer.parseInt(fieldPath.substring(start, end));
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

    /** The catalog rows of a CORE part's concept tags. */
    static List<CatalogRecords.ConceptTag> conceptRows(List<DocumentConcepts.Tag> tags) {
        return tags.stream().map(tag -> new CatalogRecords.ConceptTag(tag.fieldPath(), tag.ordinal(), tag.schemeId(),
                tag.conceptId())).toList();
    }

    /**
     * A CORE lookup as the catalog records it: a structured_data stored opaque was
     * never decoded, so it has no admitted type and a link's target_types never
     * matches it (requirements §17).
     *
     * @param lookup the lookup projected from the CORE bytes
     * @param validation the revision's validation record
     * @return the lookup, without structured_data_type when that root is opaque
     */
    static DocumentLookup admittedLookup(DocumentLookup lookup, RevisionValidation validation) {
        var root = RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(Document.STRUCTURED_DATA_FIELD_NUMBER).build();
        boolean opaque = validation.getOpaqueOccurrencesList().stream().anyMatch(occurrence ->
                occurrence.getOccurrence().getPart().equals(CORE_SELECTOR)
                        && occurrence.getOccurrence().getStepsList().equals(List.of(root)));
        return opaque ? lookup.toBuilder().clearStructuredDataType().build() : lookup;
    }

    /** Decodes admitted CORE bytes as the host's document type; they were verified to parse. */
    private static Message parseCore(ByteString core, Descriptors.Descriptor documentType) {
        try {
            return documentType == Document.getDescriptor()
                    ? Document.parseFrom(core)
                    : DynamicMessage.parseFrom(documentType, core);
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException("An admitted CORE fragment failed to parse", impossible);
        }
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
     * Why the admitted CORE part's links refuse the save, if they do (requirements §17).
     * A static target must be in the saving account, exist unpurged, be readable by the
     * saving caller, and hold the named revision when one is pinned; otherwise
     * LINK_TARGET_NOT_FOUND, one reason for all four, as a read's NOT_FOUND is. When the
     * link field lists target_types, the target's structured_data type at the pinned
     * revision, or at its current one, must be listed; otherwise LINK_TARGET_TYPE. A
     * tombstoned current target is accepted unpinned: the link is born dangling. A
     * dynamic target's filter must select something and be consistent, as a listing's
     * is; it is not evaluated here. A pinned revision must be a document revision, not
     * a tombstone. Every link must also fit the catalog's edge index, checked first, so
     * an oversized path or key is refused by name rather than failing inside the commit.
     */
    private Optional<SemanticRejection> linkRefusal(RepositoryCaller caller, String account, List<DocumentLinks.Link> links) {
        var violations = new ArrayList<FieldViolation>();
        for (var link : links) {
            var target = link.link().hasStaticTarget() ? Optional.of(link.link().getStaticTarget().getAddress())
                    : Optional.<NodeAddress>empty();
            CatalogEntries.unstorableLink(link.fieldPath(), link.field().getRelation(), target)
                    .ifPresent(problem -> violations.add(linkViolation(link, "link-catalog-storable", problem)));
        }
        if (!violations.isEmpty())
            return Optional.of(new SemanticRejection(RejectionReason.REJECTION_REASON_VALIDATION_FAILED,
                    "A link does not fit the catalog", List.copyOf(violations)));
        for (var link : links) {
            var at = link.at();
            if (link.link().hasDynamicTarget()) {
                var dynamic = link.link().getDynamicTarget();
                if (dynamic.getFilter().equals(DocumentFilter.getDefaultInstance()))
                    violations.add(linkViolation(link, "link-dynamic-filter-required", "a dynamic link's filter sets at least one field"));
                try {
                    DocumentLists.requireConsistentFilter(dynamic.getFilter());
                    DocumentLists.requireConsistentSort(dynamic.getSort());
                } catch (AccessionException inconsistent) {
                    violations.add(linkViolation(link, "link-dynamic-filter-consistent", inconsistent.getMessage()));
                }
                continue;
            }
            var target = link.link().getStaticTarget();
            var address = target.getAddress();
            var pinned = target.hasRevision() ? OptionalLong.of(target.getRevision()) : OptionalLong.empty();
            var found = !address.getAccountId().equals(account) || address.getGraphId().isBlank()
                    || address.getGraphAddressId().isBlank() || address.getDocId().isBlank()
                    ? Optional.<CatalogRecords.LinkTarget>empty() : catalog.linkTarget(address, pinned);
            if (found.isEmpty() || !DocumentReads.canRead(caller, address, found.get().entry().getOwnership())
                    || (pinned.isPresent() && (found.get().pinned().isEmpty() || found.get().pinned().get().tombstone())))
                return Optional.of(new SemanticRejection(RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND,
                        "The target of link " + at + " is not found", List.of()));
            if (link.field().getTargetTypesCount() == 0) continue;
            Optional<String> type;
            if (pinned.isPresent()) type = found.get().pinned().get().structuredDataType();
            else if (found.get().tombstone()) continue;
            else type = found.get().currentType();
            if (type.isEmpty() || !link.field().getTargetTypesList().contains(type.get()))
                return Optional.of(new SemanticRejection(RejectionReason.REJECTION_REASON_LINK_TARGET_TYPE,
                        "The target of link " + at + " holds " + type.map(name -> "structured_data " + name)
                                .orElse("no structured_data") + ", which the link does not allow", List.of()));
        }
        return violations.isEmpty() ? Optional.empty() : Optional.of(new SemanticRejection(
                RejectionReason.REJECTION_REASON_VALIDATION_FAILED, "A dynamic link's selector is invalid", List.copyOf(violations)));
    }

    private static FieldViolation linkViolation(DocumentLinks.Link link, String ruleId, String message) {
        return FieldViolation.newBuilder().setPart(CORE_SELECTOR)
                .setFieldPath(link.at())
                .setRuleId(ruleId).setMessage(message).build();
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

    // ------------------------------------------------------------------ opaque

    /** One attestation after producer or carried-source verification. */
    private record ResolvedOpaque(OpaqueAttestation attestation, WriteProvenance owner,
                                  WriterGrantEvidence grant, Optional<OpaqueOccurrence> original) {}

    /** A recorded opaque refusal, converted to a rejection outcome by the caller. */
    private static final class OpaqueRefusal extends RuntimeException {
        private final RejectionReason reason;
        OpaqueRefusal(RejectionReason reason, String message) { super(message); this.reason = reason; }
    }

    /**
     * Verifies one attestation (requirements §9). A carried occurrence keeps its
     * original producer and grant snapshot, needs no producer grant from this
     * caller, and is never rechecked against live grants. A newly produced
     * occurrence is creation-only and needs the caller's active step grant for the
     * producing step and exact type URL; its grant joins the commit recheck.
     */
    private ResolvedOpaque resolveAttestation(RepositoryCaller caller, SaveDocumentRequest request, String account,
            boolean creating, OpaqueAttestation attestation, List<String> grantIds, List<StackRef> sourceRefs) {
        if (attestation.hasCarriedFrom()) {
            var carriedFrom = attestation.getCarriedFrom();
            var source = readableSource(caller, account, carriedFrom.getSource().getAddress(),
                    carriedFrom.getSource().getRevision())
                    .orElseThrow(() -> new OpaqueRefusal(RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND,
                            "Opaque source revision is not readable"));
            var original = source.getValidation().getOpaqueOccurrencesList().stream()
                    .filter(occurrence -> occurrence.getOccurrence().equals(carriedFrom.getOccurrence())).findFirst()
                    .orElseThrow(() -> new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                            "The opaque source revision holds no such occurrence"));
            requireGrantEvidenceIntact(original.getWriterGrant());
            if (!attestation.getTypeUrl().equals(original.getTypeUrl()))
                throw new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                        "Attested opaque type differs from its carried source");
            if (attestation.hasOwner() && !attestation.getOwner().equals(original.getOwner()))
                throw new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                        "Attested opaque owner differs from its carried source");
            source.getPartsList().stream()
                    .filter(part -> part.getEntry().getSelector().equals(carriedFrom.getOccurrence().getPart()))
                    .filter(StoredPartEntry::hasRef).findFirst()
                    .ifPresent(part -> sourceRefs.add(part.getRef()));
            return new ResolvedOpaque(attestation, original.getOwner(), original.getWriterGrant(),
                    Optional.of(original));
        }
        if (!creating)
            throw new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                    "New opaque content may only be produced when a document is created");
        WriteProvenance owner = attestation.hasOwner() ? attestation.getOwner()
                : request.getOwner().hasStep() ? request.getOwner().getStep() : null;
        if (owner == null)
            throw new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                    "Opaque content needs a producing step owner");
        var producer = authority.authorizeOpaqueProducer(caller, owner, account, attestation.getTypeUrl())
                .orElseThrow(() -> new OpaqueRefusal(RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED,
                        "No active step grant authorizes this opaque producer and type"));
        if (!grantIds.contains(producer.grantId())) grantIds.add(producer.grantId());
        return new ResolvedOpaque(attestation, owner, producer.evidence(), Optional.empty());
    }

    /**
     * Retained grant evidence must still match its own digest and ID (requirements
     * §15.22). Corrupt evidence is DATA_LOSS; a live grant is never substituted.
     */
    static void requireGrantEvidenceIntact(WriterGrantEvidence evidence) {
        if (!sha256(evidence.getGrantSnapshot()).equals(evidence.getWriterGrantSha256()))
            throw new AccessionException(Status.Code.DATA_LOSS, "Retained writer grant snapshot fails its digest", null);
        final WriterGrant snapshot;
        try {
            snapshot = WriterGrant.parseFrom(evidence.getGrantSnapshot());
        } catch (InvalidProtocolBufferException corrupt) {
            throw new AccessionException(Status.Code.DATA_LOSS, "Retained writer grant snapshot does not decode", null, corrupt);
        }
        if (!snapshot.getGrantId().equals(evidence.getWriterGrantId()))
            throw new AccessionException(Status.Code.DATA_LOSS, "Retained writer grant snapshot names another grant", null);
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
            case CatalogRecords.Recorded stored -> {
                var replayed = replay(caller, request, intentSha, stored.outcome());
                if (reason == RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH) {
                    throw rejectionException(rejection);
                }
                yield replayed;
            }
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

    private record ContentIdentity(String sha256, long length) {}

    private static void rememberReusable(Stack stack, Map<ContentIdentity, StackRef> reusable, StackRef ref) {
        if (ref.getStackId().equals(stack.stackId()))
            reusable.putIfAbsent(new ContentIdentity(ref.getSha256(), ref.getLength()), ref);
    }

    private StackRef putOrReuse(Stack stack, ByteString bytes, Map<ContentIdentity, StackRef> reusable,
            List<StackRef> newObjects) {
        var expectedSha256 = sha256(bytes);
        var existing = reusable.get(new ContentIdentity(expectedSha256, bytes.size()));
        if (existing != null) return existing;
        var ref = put(stack, bytes, expectedSha256);
        newObjects.add(ref);
        rememberReusable(stack, reusable, ref);
        return ref;
    }

    /**
     * Writes one object and checks the stack's reference against the bytes it was given.
     *
     * <p>The write streams the fragment ({@code ByteString.newInput} copies nothing;
     * a body past the stack's part buffer uploads in parts). The write takes no
     * reservation of its own: the bytes are already covered by the save's lease
     * (the unary path reserves at fragment admission, the streamed path reserves
     * per received chunk), so a second lease here would charge them twice. The
     * stack computes the digest over the streamed bytes itself; the reference it
     * returns must restate the expected digest and length or the write is refused.
     */
    private StackRef put(Stack stack, ByteString bytes, String expectedSha256) {
        final StackRef ref;
        try {
            ref = stack.put(bytes.newInput(), bytes.size());
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Stack write failed", null, failure);
        }
        if (!ref.getSha256().equals(expectedSha256) || ref.getLength() != bytes.size())
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

    static String sha256(ByteString bytes) { return SaveIntents.sha256Hex(bytes); }

    static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    static final class CancelledUploadException extends IOException {
        CancelledUploadException(String message) { super(message); }
    }

    static final class UploadMismatchException extends IOException {
        UploadMismatchException(String message) { super(message); }
    }

    static final class UploadProtocolException extends IOException {
        private final AccessionException accession;
        UploadProtocolException(AccessionException accession) {
            super(accession.getMessage(), accession);
            this.accession = accession;
        }
        AccessionException accession() { return accession; }
    }

    private static final class UploadStreamTracker extends java.io.InputStream {
        private final java.io.InputStream in;
        private final long declaredLength;
        private final java.util.function.BooleanSupplier cancelled;
        private final java.security.MessageDigest digest = SaveIntents.newSha256();
        private long count;
        private boolean eofSeen;

        UploadStreamTracker(java.io.InputStream in, long declaredLength, java.util.function.BooleanSupplier cancelled) {
            this.in = Objects.requireNonNull(in, "in");
            this.declaredLength = declaredLength;
            this.cancelled = Objects.requireNonNull(cancelled, "cancelled");
        }

        long expectedLength() { return declaredLength; }
        long count() { return count; }

        String sha256Hex() {
            try {
                var clone = (java.security.MessageDigest) digest.clone();
                return java.util.HexFormat.of().formatHex(clone.digest());
            } catch (CloneNotSupportedException fallback) {
                return java.util.HexFormat.of().formatHex(digest.digest());
            }
        }

        @Override public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override public int read(byte[] buffer, int offset, int length) throws IOException {
            checkActive();
            if (length == 0) return 0;
            final int read;
            try {
                read = in.read(buffer, offset, length);
            } catch (CancelledUploadException | UploadProtocolException | UploadMismatchException passthrough) {
                throw passthrough;
            } catch (IOException failure) {
                if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted())
                    throw new CancelledUploadException("Upload stream cancelled");
                throw failure;
            }
            checkActive();
            if (read < 0) {
                eofSeen = true;
                if (declaredLength >= 0 && count < declaredLength)
                    throw new UploadMismatchException("Uploaded stream ended at " + count + " of " + declaredLength + " declared bytes");
                return -1;
            }
            digest.update(buffer, offset, read);
            count += read;
            if (declaredLength >= 0 && count > declaredLength)
                throw new UploadMismatchException("Uploaded stream exceeded declared length of " + declaredLength + " bytes");
            return read;
        }

        void verifyAtEnd() throws IOException {
            checkActive();
            if (!eofSeen) {
                if (read() >= 0)
                    throw new UploadMismatchException("Uploaded stream exceeded declared length of " + declaredLength + " bytes");
            }
            if (declaredLength >= 0 && count != declaredLength)
                throw new UploadMismatchException("Uploaded stream length " + count + " differs from declared length " + declaredLength);
        }

        private void checkActive() throws CancelledUploadException {
            if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted())
                throw new CancelledUploadException("Upload stream cancelled");
        }
    }
}
