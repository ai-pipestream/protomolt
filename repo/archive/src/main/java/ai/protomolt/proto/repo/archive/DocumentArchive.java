package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveEntryFacts;
import ai.protomolt.proto.repo.archive.v1.ArchiveStats;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.RenditionStats;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsResponse;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.RevisionSummary;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.SaveRejection;
import ai.protomolt.proto.repo.v2.StreamedPart;
import ai.protomolt.proto.repo.v2.UnchangedPolicy;
import ai.protomolt.proto.repo.v2.UploadHeader;
import ai.protomolt.proto.repo.v2.ValidationMode;
import ai.protomolt.proto.repo.v2.WriteAction;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.rpc.Code;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The archive over v2 documents (docs/design/archive.md, "The archive over v2
 * documents"): {@code ArchiveService} reads as v2 reads projected into the
 * archive view, and archive writes as one v2 save each.
 *
 * <p>Every read is one v2 read per revision it serves. Every write resolves its
 * operation ID, asks {@code GetOperationOutcome} before reading anything else,
 * and replays a recorded outcome whose {@code archive.content_sha256} request
 * metadata matches, unless the ID was derived for a last-write-wins request and
 * the recorded version is no longer current; only with no recorded outcome does
 * it read the current revision and save. Every save asks v2 to record an unchanged outcome
 * ({@code UNCHANGED_POLICY_RECORD_OUTCOME}) instead of a new revision when it
 * changes nothing, which is what {@code deduplicated} reports: the archive does
 * no dedup of its own. v2 save validation is the only write gate: the translator
 * checks only what the archive contract adds (rendition naming, the PARSED
 * fragment's shape, {@code filename} needing an original).</p>
 *
 * <p>Not served here, each a follow-up under #535: {@code ListEntries}
 * classification filters, and content profiles and classifications (the asset
 * layer's output).</p>
 */
public final class DocumentArchive {

    /** The request metadata key a translated save records the archive content digest under. */
    public static final String CONTENT_SHA256_METADATA = "archive.content_sha256";

    /**
     * Default maximum total rendition bytes a single unary {@link #getEntry(GetEntryRequest)}
     * response will assemble in memory (32 MiB). Callers needing larger renditions must
     * stream them with {@code ReadRendition}.
     */
    public static final long DEFAULT_MAX_UNARY_GET_BYTES = 32L * 1024 * 1024;

    static final int DEFAULT_PAGE_SIZE = 100;
    private static final int STATS_SCAN_PAGE_SIZE = 500;
    private static final String DEFAULT_DATASOURCE_ID = "ds-archive";

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final PartSelection CORE_AND_BLOBS = PartSelection.newBuilder()
            .addSelectors(ArchiveCoordinates.CORE).addSelectors(ArchiveCoordinates.BLOBS).build();
    private static final PartSelection BLOBS_ONLY = PartSelection.newBuilder()
            .addSelectors(ArchiveCoordinates.BLOBS).build();

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final DocumentServiceGrpc.DocumentServiceFutureStub futureDocuments;
    private final ArchiveBindings bindings;
    private final ArchiveViews views;
    private final Clock clock;
    private final long maxUnaryGetBytes;

    /**
     * Constructs a {@link DocumentArchive} with {@link #DEFAULT_MAX_UNARY_GET_BYTES}.
     *
     * @param documents the v2 document service the archive is stored through
     * @param bindings the archive definitions
     * @param clock stamps {@code created_at} on an entry's creating save
     */
    public DocumentArchive(Channel documents, ArchiveBindings bindings, Clock clock) {
        this(documents, bindings, clock, DEFAULT_MAX_UNARY_GET_BYTES);
    }

    /**
     * Constructs a {@link DocumentArchive} with an explicit unary {@code GetEntry} byte limit.
     *
     * @param documents the v2 document service the archive is stored through
     * @param bindings the archive definitions
     * @param clock stamps {@code created_at} on an entry's creating save
     * @param maxUnaryGetBytes maximum total rendition bytes assembled in one unary {@code GetEntry}
     */
    public DocumentArchive(
            Channel documents, ArchiveBindings bindings, Clock clock, long maxUnaryGetBytes) {
        Objects.requireNonNull(documents, "documents");
        Objects.requireNonNull(bindings, "bindings");
        if (maxUnaryGetBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxUnaryGetBytes must be positive: " + maxUnaryGetBytes);
        }
        this.documents = DocumentServiceGrpc.newBlockingStub(documents);
        this.futureDocuments = DocumentServiceGrpc.newFutureStub(documents);
        this.bindings = bindings instanceof DocumentArchiveBindings
                ? bindings
                : ArchiveBindings.overDocuments(
                        documents, DEFAULT_DATASOURCE_ID, "", (acct, drive) -> true, bindings);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.maxUnaryGetBytes = maxUnaryGetBytes;
        this.views = new ArchiveViews();
    }

    DocumentServiceGrpc.DocumentServiceBlockingStub documents() {
        return documents;
    }

    ArchiveViews views() {
        return views;
    }

    // --------------------------------------------------- archive definitions

    /**
     * {@code CreateArchive}: stores a new archive definition in v2 documents with
     * {@code ExpectedRevision.none} so duplicate archive names in the same account
     * fail with {@code ALREADY_EXISTS}.
     */
    public CreateArchiveResponse createArchive(CreateArchiveRequest request) {
        requireValid(request);
        Archive archive = request.getArchive();
        if (archive.getVersioning() != VersioningPolicy.VERSIONING_POLICY_NONE
                && archive.getVersioning() != VersioningPolicy.VERSIONING_POLICY_RETAINED) {
            throw ArchiveErrors.invalid(
                    "archive.versioning", "must be NONE or RETAINED; nothing is assumed");
        }
        Archive created = bindings.create(archive, now());
        requireValidView(created);
        return CreateArchiveResponse.newBuilder().setArchive(created).build();
    }

    /** {@code GetArchive}: fetches one archive definition by account and archive name. */
    public GetArchiveResponse getArchive(GetArchiveRequest request) {
        requireValid(request);
        Archive archive = bindings.get(request.getAccountId(), request.getArchive())
                .orElseThrow(() -> ArchiveErrors.notFound(
                        "Account " + request.getAccountId() + " has no archive " + request.getArchive()));
        requireValidView(archive);
        return GetArchiveResponse.newBuilder().setArchive(archive).build();
    }

    /** {@code ListArchives}: lists an account's archive definitions with pagination. */
    public ListArchivesResponse listArchives(ListArchivesRequest request) {
        requireValid(request);
        ArchiveBindings.Page page = bindings.list(
                request.getAccountId(), pageSize(request.getLimit()), request.getContinuationToken());
        ListArchivesResponse.Builder response = ListArchivesResponse.newBuilder()
                .setNextContinuationToken(page.nextContinuationToken());
        for (Archive archive : page.archives()) {
            requireValidView(archive);
            response.addArchives(archive);
        }
        return response.build();
    }

    /**
     * {@code GetArchiveStats}: computes exact entry, version, current-byte,
     * retained-byte and per-rendition counters over the archive's live v2 documents.
     */
    public GetArchiveStatsResponse getArchiveStats(GetArchiveStatsRequest request) {
        requireValid(request);
        ArchiveBinding binding = binding(request.getAccountId(), request.getArchive());
        boolean currentOnly = binding.versioning() == VersioningPolicy.VERSIONING_POLICY_NONE;
        long totalEntries = 0L;
        long totalVersions = 0L;
        long currentBytes = 0L;
        long retainedBytes = 0L;
        Map<String, long[]> renditionTotals = new TreeMap<>();

        String pageToken = "";
        do {
            ListDocumentsResponse page = documents.listDocuments(ListDocumentsRequest.newBuilder()
                    .setAccountId(binding.accountId())
                    .setGraphId(ArchiveCoordinates.graphId(binding.accountId()))
                    .setGraphAddressId(binding.archive())
                    .setPageSize(STATS_SCAN_PAGE_SIZE)
                    .setPageToken(pageToken)
                    .build());
            List<DocumentSummary> rows = page.getDocumentsList();
            List<GetDocumentResponse> currentDocs = fetchCurrentDocuments(rows);
            for (int i = 0; i < rows.size(); i++) {
                DocumentSummary row = rows.get(i);
                GetDocumentResponse currentDoc = currentDocs.get(i);
                VersionManifest currentManifest = views.manifest(currentDoc);
                totalEntries++;
                currentBytes += currentManifest.getTotalBytes();
                if (currentOnly || row.getRevision() <= 1L) {
                    totalVersions++;
                    retainedBytes += currentManifest.getTotalBytes();
                    for (RenditionManifestEntry entry : currentManifest.getRenditionsList()) {
                        if (entry.getState() == RenditionState.RENDITION_STATE_PRESENT) {
                            accumulateRendition(renditionTotals, entry);
                        }
                    }
                } else {
                    EntryVersionAccounting accounting =
                            accountRetainedEntry(row.getAddress(), row.getRevision(), currentManifest);
                    totalVersions += accounting.versions();
                    retainedBytes += accounting.retainedBytes();
                    for (RenditionManifestEntry entry : accounting.uniqueObjects()) {
                        accumulateRendition(renditionTotals, entry);
                    }
                }
            }
            pageToken = page.getNextPageToken();
        } while (!pageToken.isEmpty());

        ArchiveStats.Builder stats = ArchiveStats.newBuilder()
                .setAccountId(binding.accountId())
                .setArchive(binding.archive())
                .setEntries(totalEntries)
                .setVersions(totalVersions)
                .setRetainedBytes(retainedBytes)
                .setCurrentBytes(currentBytes);
        for (Map.Entry<String, long[]> entry : renditionTotals.entrySet()) {
            long[] counts = entry.getValue();
            if (counts[0] == 0L && counts[1] == 0L) {
                continue;
            }
            stats.addRenditions(RenditionStats.newBuilder()
                    .setRenditionName(entry.getKey())
                    .setObjectCount(counts[0])
                    .setTotalBytes(counts[1]));
        }
        return GetArchiveStatsResponse.newBuilder().setStats(stats).build();
    }

    private static void accumulateRendition(
            Map<String, long[]> renditionTotals, RenditionManifestEntry entry) {
        long[] counts = renditionTotals.computeIfAbsent(
                entry.getRendition().getName(), ignored -> new long[2]);
        counts[0] += 1L;
        counts[1] += entry.getSizeBytes();
    }

    private record EntryVersionAccounting(
            long versions, long retainedBytes, List<RenditionManifestEntry> uniqueObjects) {
    }

    private EntryVersionAccounting accountRetainedEntry(
            NodeAddress address, long currentRevision, VersionManifest currentManifest) {
        Map<String, RenditionManifestEntry> unique = new LinkedHashMap<>();
        indexPresentRenditions(unique, currentManifest);
        long versions = 0L;
        String pageToken = "";
        do {
            ListRevisionsResponse revisions = documents.listRevisions(ListRevisionsRequest.newBuilder()
                    .setAddress(address)
                    .setPageSize(STATS_SCAN_PAGE_SIZE)
                    .setPageToken(pageToken)
                    .build());
            List<ListenableFuture<GetDocumentResponse>> historicalReads = new ArrayList<>();
            for (RevisionSummary rev : revisions.getRevisionsList()) {
                if (rev.getTombstone()) {
                    continue;
                }
                versions++;
                if (rev.getRevision() != currentRevision) {
                    historicalReads.add(futureDocuments.getDocument(GetDocumentRequest.newBuilder()
                            .setAddress(address)
                            .setRevision(revision(rev.getRevision()))
                            .setSelection(BLOBS_ONLY)
                            .build()));
                }
            }
            for (GetDocumentResponse historical : awaitAll(historicalReads)) {
                indexPresentRenditions(unique, views.manifest(historical));
            }
            pageToken = revisions.getNextPageToken();
        } while (!pageToken.isEmpty());
        long retainedBytes = 0L;
        for (RenditionManifestEntry entry : unique.values()) {
            retainedBytes += entry.getSizeBytes();
        }
        return new EntryVersionAccounting(
                Math.max(1L, versions), retainedBytes, List.copyOf(unique.values()));
    }

    private static void indexPresentRenditions(
            Map<String, RenditionManifestEntry> unique, VersionManifest manifest) {
        for (RenditionManifestEntry entry : manifest.getRenditionsList()) {
            if (entry.getState() == RenditionState.RENDITION_STATE_PRESENT) {
                String key = entry.getRendition().getName()
                        + '\0' + entry.getRendition().getSubKey()
                        + '\0' + entry.getSha256();
                unique.putIfAbsent(key, entry);
            }
        }
    }

    // ----------------------------------------------------------------- reads

    /** {@code GetEntryManifest}: the version's manifest and the entry's current metadata. */
    public GetEntryManifestResponse getEntryManifest(GetEntryManifestRequest request) {
        requireValid(request);
        ArchiveBinding binding = binding(request.getAddress());
        NodeAddress address = ArchiveCoordinates.address(request.getAddress());
        GetDocumentResponse current = read(address, current(), CORE_AND_BLOBS);
        GetDocumentResponse served = served(binding, address, current, request.getVersion());
        return GetEntryManifestResponse.newBuilder()
                .setInfo(views.entryInfo(current))
                .setManifest(views.manifest(served))
                .build();
    }

    /**
     * {@code GetEntry}: the version's manifest, the entry's current metadata and
     * the bytes of the selected PRESENT renditions (every one when the request
     * names none). Bytes are attached, never projected, and bounded across the
     * whole response by {@code maxUnaryGetBytes}.
     */
    public GetEntryResponse getEntry(GetEntryRequest request) {
        requireValid(request);
        ArchiveBinding binding = binding(request.getAddress());
        NodeAddress address = ArchiveCoordinates.address(request.getAddress());
        GetDocumentResponse current = read(address, current(), CORE_AND_BLOBS);
        GetDocumentResponse served = served(binding, address, current, request.getVersion());
        VersionManifest manifest = views.manifest(served);
        Set<String> wanted = new HashSet<>(request.getRenditionsList());
        GetEntryResponse.Builder response = GetEntryResponse.newBuilder()
                .setInfo(views.entryInfo(current))
                .setManifest(manifest);
        long cumulativeBytes = 0L;
        for (RenditionManifestEntry row : manifest.getRenditionsList()) {
            if (row.getState() != RenditionState.RENDITION_STATE_PRESENT
                    || (!wanted.isEmpty() && !wanted.contains(row.getRendition().getName()))) {
                continue;
            }
            long rowSize = row.getSizeBytes();
            if (rowSize > maxUnaryGetBytes || cumulativeBytes > maxUnaryGetBytes - rowSize) {
                throw unaryByteLimitExceeded(
                        request.getAddress().getEntryId(),
                        manifest.getVersion(),
                        row.getRendition(),
                        cumulativeBytes + rowSize);
            }
            ByteString data = readWhole(
                    address,
                    manifest.getVersion(),
                    row,
                    maxUnaryGetBytes - cumulativeBytes,
                    request.getAddress().getEntryId());
            cumulativeBytes += data.size();
            response.addRenditions(RenditionContent.newBuilder()
                    .setRendition(row.getRendition())
                    .setData(data));
        }
        return response.build();
    }

    private StatusRuntimeException unaryByteLimitExceeded(
            String entryId, long version, RenditionDescriptor rendition, long requestedBytes) {
        String name = ArchiveCoordinates.blobId(rendition.getName(), rendition.getSubKey());
        return Status.RESOURCE_EXHAUSTED
                .withDescription("GetEntry for entry " + entryId + " at version " + version
                        + " exceeds the unary response limit of " + maxUnaryGetBytes
                        + " bytes at rendition " + name + " (" + requestedBytes
                        + " bytes); stream large renditions with ReadRendition")
                .asRuntimeException();
    }

    /**
     * Resolves a {@code ReadRendition} to the revision, manifest row and byte
     * range it serves; {@link RenditionReads} streams the bytes.
     */
    RenditionReads.Plan planRead(ReadRenditionRequest request) {
        requireValid(request);
        ArchiveBinding binding = binding(request.getAddress());
        NodeAddress address = ArchiveCoordinates.address(request.getAddress());
        GetDocumentResponse served;
        if (request.getVersion() == 0) {
            served = read(address, current(), BLOBS_ONLY);
        } else {
            GetDocumentResponse current = read(address, current(), BLOBS_ONLY);
            served = served(binding, address, current, request.getVersion());
        }
        VersionManifest manifest = views.manifest(served);
        RenditionManifestEntry row = manifest.getRenditionsList().stream()
                .filter(candidate -> candidate.getRendition().getName().equals(request.getRendition())
                        && candidate.getRendition().getSubKey().equals(request.getSubKey()))
                .findFirst()
                .orElseThrow(() -> ArchiveErrors.notFound("Version " + manifest.getVersion() + " of entry "
                        + request.getAddress().getEntryId() + " has no rendition " + rowName(request)));
        if (row.getState() != RenditionState.RENDITION_STATE_PRESENT) {
            throw ArchiveErrors.failedPrecondition("Rendition " + rowName(request) + " of version "
                    + manifest.getVersion() + " is " + row.getState() + "; its bytes are not readable");
        }
        long size = row.getSizeBytes();
        if (request.getOffset() > size || request.getLength() > size - request.getOffset()) {
            throw ArchiveErrors.invalid("offset, length", "requested byte range [" + request.getOffset() + ", +"
                    + request.getLength() + ") lies outside the " + size + "-byte rendition");
        }
        long length = request.getLength() == 0 ? size - request.getOffset() : request.getLength();
        return new RenditionReads.Plan(address, manifest.getVersion(), row, request.getOffset(), length);
    }

    private static String rowName(ReadRenditionRequest request) {
        return ArchiveCoordinates.blobId(request.getRendition(), request.getSubKey());
    }

    /**
     * {@code ListVersions}: one {@code VersionSummary} per revision, newest first.
     * An archive with {@code VERSIONING_POLICY_NONE} shows its current revision only.
     */
    public ListVersionsResponse listVersions(ListVersionsRequest request) {
        requireValid(request);
        ArchiveBinding binding = binding(request.getAddress());
        NodeAddress address = ArchiveCoordinates.address(request.getAddress());
        boolean currentOnly = binding.versioning() == VersioningPolicy.VERSIONING_POLICY_NONE;
        if (currentOnly && !request.getContinuationToken().isEmpty()) {
            throw ArchiveErrors.invalid("continuation_token", "archive " + binding.archive()
                    + " keeps VERSIONING_POLICY_NONE and lists one version; there is no next page");
        }
        ListRevisionsResponse revisions = documents.listRevisions(ListRevisionsRequest.newBuilder()
                .setAddress(address)
                .setPageSize(currentOnly ? 1 : pageSize(request.getLimit()))
                .setPageToken(request.getContinuationToken())
                .build());
        ListVersionsResponse.Builder response = ListVersionsResponse.newBuilder();
        revisions.getRevisionsList().forEach(revision -> response.addSummaries(views.summary(revision)));
        if (!currentOnly) {
            response.setNextContinuationToken(revisions.getNextPageToken());
        }
        return response.build();
    }

    /**
     * {@code ListEntries}: the archive's live documents, each projected through its
     * {@code DocumentLookup}, with {@code total_count} populated for the archive.
     * With {@code include_manifests}, each row's current revision is fetched
     * concurrently at its known revision and projected through {@link ArchiveViews#manifest}
     * without repeating the archive-binding lookup or {@code EntryInfo} projection.
     *
     * @throws StatusRuntimeException {@code UNIMPLEMENTED} naming
     *         {@code classification_state} when a classification filter is asked
     *         for: classifications are not stored on v2 documents
     */
    public ListEntriesResponse listEntries(ListEntriesRequest request) {
        requireValid(request);
        if (request.getClassificationState() != ClassificationState.CLASSIFICATION_STATE_UNSPECIFIED) {
            throw ArchiveErrors.unimplemented("classification_state: the archive over v2 documents stores no"
                    + " classification to filter on");
        }
        ArchiveBinding binding = binding(request.getAccountId(), request.getArchive());
        ListDocumentsResponse page = documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(binding.accountId())
                .setGraphId(ArchiveCoordinates.graphId(binding.accountId()))
                .setGraphAddressId(binding.archive())
                .setPageSize(pageSize(request.getLimit()))
                .setPageToken(request.getContinuationToken())
                .build());
        long totalCount = request.getContinuationToken().isEmpty() && page.getNextPageToken().isEmpty()
                ? page.getDocumentsCount()
                : countLiveEntries(binding);
        ListEntriesResponse.Builder response = ListEntriesResponse.newBuilder()
                .setNextContinuationToken(page.getNextPageToken())
                .setTotalCount(totalCount);
        List<DocumentSummary> rows = page.getDocumentsList();
        for (DocumentSummary row : rows) {
            response.addEntries(views.entryInfo(row));
        }
        if (request.getIncludeManifests() && !rows.isEmpty()) {
            for (GetDocumentResponse doc : fetchCurrentDocuments(rows)) {
                response.addManifests(views.manifest(doc));
            }
        }
        return response.build();
    }

    private long countLiveEntries(ArchiveBinding binding) {
        long count = 0L;
        String pageToken = "";
        do {
            ListDocumentsResponse page = documents.listDocuments(ListDocumentsRequest.newBuilder()
                    .setAccountId(binding.accountId())
                    .setGraphId(ArchiveCoordinates.graphId(binding.accountId()))
                    .setGraphAddressId(binding.archive())
                    .setPageSize(STATS_SCAN_PAGE_SIZE)
                    .setPageToken(pageToken)
                    .build());
            count += page.getDocumentsCount();
            pageToken = page.getNextPageToken();
        } while (!pageToken.isEmpty());
        return count;
    }

    private List<GetDocumentResponse> fetchCurrentDocuments(List<DocumentSummary> rows) {
        if (rows.isEmpty()) {
            return List.of();
        }
        List<ListenableFuture<GetDocumentResponse>> futures = new ArrayList<>(rows.size());
        for (DocumentSummary row : rows) {
            futures.add(futureDocuments.getDocument(GetDocumentRequest.newBuilder()
                    .setAddress(row.getAddress())
                    .setRevision(revision(row.getRevision()))
                    .setSelection(CORE_AND_BLOBS)
                    .build()));
        }
        return awaitAll(futures);
    }

    private static List<GetDocumentResponse> awaitAll(
            List<ListenableFuture<GetDocumentResponse>> futures) {
        List<GetDocumentResponse> results = new ArrayList<>(futures.size());
        for (int i = 0; i < futures.size(); i++) {
            try {
                results.add(futures.get(i).get());
            } catch (InterruptedException interrupted) {
                for (int j = i; j < futures.size(); j++) {
                    futures.get(j).cancel(true);
                }
                Thread.currentThread().interrupt();
                throw Status.CANCELLED
                        .withDescription("Interrupted while reading archive entry manifests")
                        .withCause(interrupted)
                        .asRuntimeException();
            } catch (ExecutionException failure) {
                for (int j = i + 1; j < futures.size(); j++) {
                    futures.get(j).cancel(true);
                }
                if (failure.getCause() instanceof StatusRuntimeException status) {
                    throw status;
                }
                if (failure.getCause() instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw Status.INTERNAL.withCause(failure.getCause()).asRuntimeException();
            }
        }
        return results;
    }

    // ---------------------------------------------------------------- writes

    /**
     * {@code PutEntry} as one {@code SaveDocument}: CORE always, BLOBS when a raw
     * rendition is written (every other current blob listed by ID for blob-level
     * carry-forward), PARSED when {@code parsed} is written.
     */
    public PutEntryResponse putEntry(PutEntryRequest request) {
        requireValid(request);
        ArchiveBinding binding = binding(request.getAddress());
        NodeAddress address = ArchiveCoordinates.address(request.getAddress());
        Map<String, RenditionContent> written = writtenRenditions(request);
        String contentSha256 = ArchiveOperationKeys.putEntryContentSha256(request);
        String operationId = ArchiveOperationKeys.operationId(request, contentSha256);

        boolean derivedLastWriteWins = request.getOperationId().isEmpty() && request.getExpectedVersion() == 0;
        Optional<Recorded> recorded = recordedOutcome(address, operationId, contentSha256, derivedLastWriteWins);
        if (recorded.isPresent()) {
            return putResponse(recorded.get().revision(), operationId, recorded.get().unchanged(), true);
        }

        Optional<GetDocumentResponse> current = readCurrent(address);
        ExpectedRevision expected = request.getExpectedVersion() > 0
                ? ExpectedRevision.newBuilder().setRevision(request.getExpectedVersion()).build()
                : lastWriteWins(address, current);

        Document.Builder document = core(binding, request.getAddress(), current, request.getTitle(),
                request.getContentType(), request.getSourceUri(),
                request.hasSourceModifiedAt() ? request.getSourceModifiedAt() : null,
                request.getMetadataMap(), facts(request.hasDeclared() ? request.getDeclared() : null,
                        request.hasOrigin() ? request.getOrigin() : null));
        List<Blob> currentBlobs = current.map(read -> read.getDocument().getBlobBag().getBlobs().getBlobList())
                .orElse(List.of());
        boolean blobsChange = written.keySet().stream().anyMatch(id -> !id.equals(ArchiveCoordinates.PARSED));
        boolean originalPresent = written.containsKey(ArchiveCoordinates.ORIGINAL)
                || currentBlobs.stream().anyMatch(blob -> blob.getBlobId().equals(ArchiveCoordinates.ORIGINAL));
        if (!request.getFilename().isEmpty() && !originalPresent) {
            throw ArchiveErrors.invalid("filename", "describes the original file, but the entry has no "
                    + ArchiveCoordinates.ORIGINAL + " rendition written or carried");
        }
        boolean filenameChange = !request.getFilename().isEmpty() && currentBlobs.stream()
                .filter(blob -> blob.getBlobId().equals(ArchiveCoordinates.ORIGINAL))
                .noneMatch(blob -> blob.getFilename().equals(request.getFilename()));
        if (blobsChange || (filenameChange && !written.containsKey(ArchiveCoordinates.ORIGINAL))) {
            document.setBlobBag(BlobBag.newBuilder().setBlobs(blobs(currentBlobs, written, request.getFilename())));
        }
        RenditionContent parsed = written.get(ArchiveCoordinates.PARSED);
        if (parsed != null) {
            document.putAllParserResults(parsedFragment(parsed, request.getAddress().getEntryId())
                    .getParserResultsMap());
        }

        SaveDocumentRequest.Builder save = SaveDocumentRequest.newBuilder()
                .setOperationId(operationId)
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(expected)
                .setDocument(document)
                .addWrites(put(ArchiveCoordinates.CORE))
                .setOwner(owner(binding, request.getWrittenBy()))
                .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                .setUnchanged(UnchangedPolicy.UNCHANGED_POLICY_RECORD_OUTCOME)
                .setDrive(binding.drive())
                .putMetadata(CONTENT_SHA256_METADATA, contentSha256);
        if (document.hasBlobBag()) {
            save.addWrites(put(ArchiveCoordinates.BLOBS));
        }
        if (parsed != null) {
            save.addWrites(put(ArchiveCoordinates.PARSED_PART));
        }
        SaveDocumentResponse saved;
        try {
            saved = documents.saveDocument(save.build());
        } catch (StatusRuntimeException failure) {
            Recorded winner = afterLostRace(address, operationId, contentSha256, derivedLastWriteWins, failure);
            return putResponse(winner.revision(), operationId, winner.unchanged(), true);
        }
        if (saved.getReplayed() && derivedLastWriteWins) {
            // v2 replayed another attempt of this operation that committed after our lookup.
            requireStillCurrent(address, operationId, saved.getAccession().getRevision());
        }
        GetDocumentResponse committed = read(address, revision(saved.getAccession().getRevision()), CORE_AND_BLOBS);
        return putResponse(committed, operationId, saved.getUnchanged(), saved.getReplayed());
    }

    /**
     * The renditions a {@code PutEntry} writes, keyed by blob ID, with the archive's
     * own naming rules: no rendition twice, no sub-key on {@code parsed}.
     */
    private static Map<String, RenditionContent> writtenRenditions(PutEntryRequest request) {
        Map<String, RenditionContent> written = new LinkedHashMap<>();
        for (int index = 0; index < request.getRenditionsCount(); index++) {
            RenditionDescriptor rendition = request.getRenditions(index).getRendition();
            if (rendition.getName().equals(ArchiveCoordinates.PARSED) && !rendition.getSubKey().isEmpty()) {
                throw ArchiveErrors.invalid("renditions[" + index + "].rendition.sub_key",
                        "the parsed rendition is the document's PARSED part and takes no sub-key");
            }
            String blobId = ArchiveCoordinates.blobId(rendition.getName(), rendition.getSubKey());
            if (written.put(blobId, request.getRenditions(index)) != null) {
                throw ArchiveErrors.invalid("renditions[" + index + "].rendition",
                        "rendition " + blobId + " is written twice");
            }
        }
        return written;
    }

    /**
     * The PARSED fragment a {@code parsed} rendition carries, as v2 stores it: an
     * encoded {@code Document} holding {@code doc_id}, equal to the entry's
     * {@code entry_id}, and {@code parser_results}, and nothing else.
     */
    static Document parsedFragment(RenditionContent parsed, String entryId) {
        Document fragment;
        try {
            fragment = Document.parseFrom(parsed.getData());
        } catch (InvalidProtocolBufferException e) {
            throw ArchiveErrors.invalid("renditions[parsed].data",
                    "is not an encoded Document PARSED fragment: " + e.getMessage());
        }
        if (!fragment.getDocId().equals(entryId)) {
            throw ArchiveErrors.invalid("renditions[parsed].data",
                    "names doc_id '" + fragment.getDocId() + "', not this entry's " + entryId
                            + "; the PARSED fragment carries the entry_id as its doc_id");
        }
        Document identified = Document.newBuilder().setDocId(entryId)
                .putAllParserResults(fragment.getParserResultsMap()).build();
        if (!fragment.equals(identified)) {
            throw ArchiveErrors.invalid("renditions[parsed].data",
                    "carries Document fields outside doc_id and parser_results; the parsed rendition is the"
                            + " PARSED part only");
        }
        return identified;
    }

    /**
     * The BLOBS fragment of a save: every current blob in stored order, as a
     * carry-forward stub (metadata, no bytes, no storage reference) unless this
     * save writes it, then the newly written blobs.
     */
    private static Blobs.Builder blobs(List<Blob> current, Map<String, RenditionContent> written, String filename) {
        Blobs.Builder blobs = Blobs.newBuilder();
        Set<String> placed = new HashSet<>();
        for (Blob blob : current) {
            RenditionContent replacement = written.get(blob.getBlobId());
            Blob.Builder next = replacement != null
                    ? writtenBlob(blob.getBlobId(), replacement)
                    : carriedStub(blob);
            if (blob.getBlobId().equals(ArchiveCoordinates.ORIGINAL)) {
                if (!filename.isEmpty()) {
                    next.setFilename(filename);
                } else if (blob.hasFilename()) {
                    next.setFilename(blob.getFilename());
                }
            }
            blobs.addBlob(next);
            placed.add(blob.getBlobId());
        }
        for (Map.Entry<String, RenditionContent> entry : written.entrySet()) {
            if (entry.getKey().equals(ArchiveCoordinates.PARSED) || placed.contains(entry.getKey())) {
                continue;
            }
            Blob.Builder next = writtenBlob(entry.getKey(), entry.getValue());
            if (entry.getKey().equals(ArchiveCoordinates.ORIGINAL) && !filename.isEmpty()) {
                next.setFilename(filename);
            }
            blobs.addBlob(next);
        }
        return blobs;
    }

    /**
     * A current blob listed by ID for blob-level carry-forward: its stored
     * metadata without the facts the server fills on read (storage reference,
     * size, checksum, write record), so the BLOBS fragment of a save that changes
     * nothing equals the stored one.
     */
    static Blob.Builder carriedStub(Blob blob) {
        return blob.toBuilder().clearStorageRef().clearWritten().clearSizeBytes().clearChecksum().clearChecksumType();
    }

    private static Blob.Builder writtenBlob(String blobId, RenditionContent content) {
        Blob.Builder blob = Blob.newBuilder().setBlobId(blobId).setData(content.getData());
        RenditionDescriptor rendition = content.getRendition();
        if (!rendition.getMediaType().isEmpty()) {
            blob.setMimeType(rendition.getMediaType());
        }
        if (!rendition.getSchemaSubject().isEmpty()) {
            blob.setSchemaSubject(rendition.getSchemaSubject());
        }
        return blob;
    }

    /**
     * The CORE of a save: the current revision's CORE with each field the request
     * sets replacing its stored value, as the ledger's blank-keeps rule did.
     * {@code created_at} is stamped on the creating save and carried after.
     */
    Document.Builder core(ArchiveBinding binding, EntryAddress entry, Optional<GetDocumentResponse> current,
            String title, String contentType, String sourceUri, Timestamp sourceModifiedAt,
            Map<String, String> metadata, FactsSource facts) {
        Document.Builder document = Document.newBuilder()
                .setDocId(entry.getEntryId())
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(binding.accountId())
                        .setDatasourceId(binding.datasourceId()));
        SearchMetadata.Builder search = current
                .map(read -> read.getDocument().getSearchMetadata().toBuilder())
                .orElseGet(() -> SearchMetadata.newBuilder().setCreationDate(now()));
        if (!title.isEmpty()) {
            search.setTitle(title);
        }
        if (!contentType.isEmpty()) {
            search.setSourceMimeType(contentType);
        }
        if (!sourceUri.isEmpty()) {
            search.setSourceUri(sourceUri);
        }
        if (sourceModifiedAt != null) {
            search.setLastModifiedDate(sourceModifiedAt);
        }
        if (!metadata.isEmpty()) {
            search.clearMetadata().putAllMetadata(metadata);
        }
        document.setSearchMetadata(search);
        ArchiveEntryFacts.Builder stored = current
                .filter(read -> read.getDocument().hasStructuredData())
                .map(read -> unpackFacts(read.getDocument().getStructuredData()).toBuilder())
                .orElseGet(ArchiveEntryFacts::newBuilder);
        if (facts != null) {
            facts.applyTo(stored);
        }
        if (!stored.build().equals(ArchiveEntryFacts.getDefaultInstance())) {
            document.setStructuredData(Any.pack(stored.build()));
        }
        return document;
    }

    /** A request that may carry a format declaration and an object-store origin. */
    interface FactsSource {
        void applyTo(ArchiveEntryFacts.Builder facts);
    }

    private static ArchiveEntryFacts unpackFacts(Any structured) {
        if (!structured.is(ArchiveEntryFacts.class)) {
            throw ArchiveErrors.failedPrecondition("The entry's structured_data is " + structured.getTypeUrl()
                    + ", not " + ArchiveEntryFacts.getDescriptor().getFullName() + "; it is not an archive entry");
        }
        try {
            return structured.unpack(ArchiveEntryFacts.class);
        } catch (InvalidProtocolBufferException e) {
            throw ArchiveErrors.dataLoss("The entry's stored ArchiveEntryFacts do not decode", e);
        }
    }

    private PutEntryResponse putResponse(GetDocumentResponse revision, String operationId,
            boolean deduplicated, boolean replayed) {
        VersionManifest manifest = views.manifest(revision);
        return PutEntryResponse.newBuilder()
                .setVersion(manifest.getVersion())
                .setRootChecksum(manifest.getRootChecksum())
                .setTotalBytes(manifest.getTotalBytes())
                .setDeduplicated(deduplicated)
                .setManifest(manifest)
                .setOperationId(operationId)
                .setReplayed(replayed)
                .build();
    }

    static PartWrite put(PartSelector selector) {
        return PartWrite.newBuilder().setSelector(selector).setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    /**
     * The save owner: the archive's intake datasource, or the step module named
     * by {@code written_by.module}, which then needs a step writer grant like any
     * step save.
     */
    static SaveOwner owner(ArchiveBinding binding, WriteAttribution writtenBy) {
        if (!writtenBy.getModule().isEmpty()) {
            return SaveOwner.newBuilder()
                    .setStep(WriteProvenance.newBuilder().setModuleId(writtenBy.getModule()))
                    .build();
        }
        return SaveOwner.newBuilder()
                .setIntake(IntakeOrigin.newBuilder().setDatasourceId(binding.datasourceId()))
                .build();
    }

    // ------------------------------------------------------ outcome lookups

    /** A recorded outcome this request replays: the revision it names, and whether it was an unchanged save. */
    record Recorded(GetDocumentResponse revision, boolean unchanged) {
    }

    /**
     * The recorded outcome of an earlier save under {@code operationId}, compared
     * by the archive content digest it recorded (prerequisite 7), never by a
     * revision read: a committed or unchanged outcome with this request's digest
     * is returned with the revision it names, and a rejection with this
     * request's digest is thrown as recorded.
     *
     * @throws StatusRuntimeException {@code ALREADY_EXISTS} naming
     *         {@code operation_id} when the recorded digest differs or is absent, on
     *         any arm, or the outcome names another entry; the recorded rejection's
     *         status, with its {@code SaveRejection} detail, when this request was
     *         rejected before; {@code FAILED_PRECONDITION} when
     *         {@code derivedLastWriteWins} and the recorded version is no longer
     *         current (see {@link #requireStillCurrent})
     */
    Optional<Recorded> recordedOutcome(NodeAddress address, String operationId, String contentSha256,
            boolean derivedLastWriteWins) {
        GetOperationOutcomeResponse outcome = documents.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(address.getAccountId())
                .setOperationId(operationId)
                .build());
        if (!outcome.hasCommitted()) {
            return Optional.empty();
        }
        RecordedOutcome recorded = outcome.getCommitted();
        String recordedDigest = recorded.getRequestMetadataOrDefault(CONTENT_SHA256_METADATA, "");
        if (!recordedDigest.equals(contentSha256)) {
            throw ArchiveErrors.alreadyExists("operation_id", operationId
                    + " is recorded for a request with other content");
        }
        if (recorded.hasRejection()) {
            throw recordedRejection(recorded.getRejection());
        }
        Accession accession = recorded.hasUnchanged() ? recorded.getUnchanged() : recorded.getAccession();
        if (!accession.getAddress().equals(address)) {
            throw ArchiveErrors.alreadyExists("operation_id", operationId + " is recorded for "
                    + accession.getAddress().getGraphId() + "/" + accession.getAddress().getGraphAddressId()
                    + "/" + accession.getAddress().getDocId() + ", not this entry");
        }
        if (derivedLastWriteWins) {
            requireStillCurrent(address, operationId, accession.getRevision());
        }
        return Optional.of(new Recorded(read(address, revision(accession.getRevision()), CORE_AND_BLOBS),
                recorded.hasUnchanged()));
    }

    /**
     * Refuses to replay a derived last-write-wins ID whose recorded version has
     * been replaced. A derived ID cannot tell a retry from a second write of the
     * same content: replaying would report a write that is not current, and
     * saving again would let a stale retry overwrite a later writer. The caller
     * resolves it by naming the current version or a new operation ID.
     *
     * @throws StatusRuntimeException {@code FAILED_PRECONDITION} naming the
     *         recorded version, the current one and both remedies
     */
    private void requireStillCurrent(NodeAddress address, String operationId, long recordedVersion) {
        RevisionSummary newest = documents.listRevisions(ListRevisionsRequest.newBuilder()
                        .setAddress(address).setPageSize(1).build())
                .getRevisionsList().stream().findFirst()
                .orElseThrow(() -> ArchiveErrors.dataLoss("Entry " + address.getDocId()
                        + " has a recorded outcome but no revisions", null));
        if (newest.getRevision() == recordedVersion) {
            return;
        }
        throw ArchiveErrors.failedPrecondition("operation_id " + operationId + " was derived from this content,"
                + " which was written as version " + recordedVersion + " and has since been replaced by "
                + (newest.getTombstone() ? "a deletion at " : "") + "version " + newest.getRevision()
                + ". To write it again, send expected_version = " + newest.getRevision() + " or a new operation_id.");
    }

    /**
     * A failed save of an operation that may have raced another attempt of itself:
     * the outcome lookup runs once more, and a recorded matching outcome is the
     * answer. Anything else is the original failure, unchanged. A derived
     * last-write-wins ID keeps the currency rule of the first lookup: when the
     * winner's version has already been replaced, the answer is the
     * {@code FAILED_PRECONDITION} of {@link #requireStillCurrent}, not a replay.
     */
    Recorded afterLostRace(NodeAddress address, String operationId, String contentSha256,
            boolean derivedLastWriteWins, StatusRuntimeException failure) {
        Status.Code code = failure.getStatus().getCode();
        if (code != Status.Code.ABORTED && code != Status.Code.ALREADY_EXISTS) {
            throw failure;
        }
        Optional<Recorded> winner;
        try {
            // Two attempts of one operation raced: the winner has just committed.
            winner = recordedOutcome(address, operationId, contentSha256, false);
        } catch (StatusRuntimeException lookupFailure) {
            failure.addSuppressed(lookupFailure);
            throw failure;
        }
        Recorded recorded = winner.orElseThrow(() -> failure);
        if (derivedLastWriteWins) {
            requireStillCurrent(address, operationId, recorded.revision().getManifest().getRevision());
        }
        return recorded;
    }

    private static StatusRuntimeException recordedRejection(SaveRejection rejection) {
        Code code = switch (rejection.getReason()) {
            case REJECTION_REASON_MALFORMED_REQUEST, REJECTION_REASON_VALIDATION_FAILED,
                 REJECTION_REASON_INVALID_BYTES, REJECTION_REASON_UPLOAD_MISMATCH -> Code.INVALID_ARGUMENT;
            case REJECTION_REASON_OPAQUE_NOT_ALLOWED, REJECTION_REASON_WRITE_DENIED -> Code.PERMISSION_DENIED;
            default -> Code.FAILED_PRECONDITION;
        };
        return StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
                .setCode(code.getNumber())
                .setMessage("The operation was rejected: " + rejection.getMessage())
                .addDetails(Any.pack(rejection))
                .build());
    }

    // --------------------------------------------------------------- helpers

    ArchiveBinding binding(EntryAddress address) {
        return binding(address.getAccountId(), address.getArchive());
    }

    ArchiveBinding binding(String accountId, String archive) {
        return bindings.find(accountId, archive).orElseThrow(() -> ArchiveErrors.notFound(
                "Account " + accountId + " has no archive " + archive));
    }

    /**
     * The revision a read at {@code version} serves: the current one for 0, and
     * under {@code VERSIONING_POLICY_NONE} only the current one.
     */
    GetDocumentResponse served(ArchiveBinding binding, NodeAddress address, GetDocumentResponse current, long version) {
        long currentRevision = current.getManifest().getRevision();
        if (version == 0 || version == currentRevision) {
            return current;
        }
        if (binding.versioning() == VersioningPolicy.VERSIONING_POLICY_NONE) {
            throw ArchiveErrors.failedPrecondition("Archive " + binding.archive()
                    + " keeps VERSIONING_POLICY_NONE and serves only its current version " + currentRevision
                    + "; version " + version + " is kept by the repository but not served by the archive");
        }
        return read(address, revision(version), CORE_AND_BLOBS);
    }

    GetDocumentResponse read(NodeAddress address, RevisionSelector revision, PartSelection selection) {
        return documents.getDocument(GetDocumentRequest.newBuilder()
                .setAddress(address)
                .setRevision(revision)
                .setSelection(selection)
                .build());
    }

    Optional<GetDocumentResponse> readCurrent(NodeAddress address) {
        try {
            return Optional.of(read(address, current(), CORE_AND_BLOBS));
        } catch (StatusRuntimeException failure) {
            if (failure.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw failure;
        }
    }

    /**
     * The expected revision of a last-write-wins save ({@code expected_version}
     * 0): the current revision of a live entry; the deletion's revision when the
     * entry is deleted, since v2 keeps the tombstone as the current revision and
     * would refuse {@code ExpectedRevision.none} with {@code ABORTED}; and none
     * when the entry has no revisions. A newest revision that is live although
     * {@code current} was not found is another writer's: none is sent, and the
     * save loses that race as any concurrent last-write-wins save does.
     */
    ExpectedRevision lastWriteWins(NodeAddress address, Optional<GetDocumentResponse> current) {
        if (current.isPresent()) {
            return ExpectedRevision.newBuilder().setRevision(current.get().getManifest().getRevision()).build();
        }
        ExpectedRevision none = ExpectedRevision.newBuilder().setNone(true).build();
        try {
            return documents.listRevisions(ListRevisionsRequest.newBuilder().setAddress(address).setPageSize(1).build())
                    .getRevisionsList().stream().findFirst()
                    .filter(RevisionSummary::getTombstone)
                    .map(deletion -> ExpectedRevision.newBuilder().setRevision(deletion.getRevision()).build())
                    .orElse(none);
        } catch (StatusRuntimeException failure) {
            if (failure.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return none;
            }
            throw failure;
        }
    }

    static RevisionSelector current() {
        return RevisionSelector.newBuilder().setCurrent(true).build();
    }

    static RevisionSelector revision(long revision) {
        return RevisionSelector.newBuilder().setRevision(revision).build();
    }

    private static int pageSize(int limit) {
        return limit == 0 ? DEFAULT_PAGE_SIZE : limit;
    }

    Timestamp now() {
        Instant instant = clock.instant();
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    /**
     * The whole bytes of one PRESENT rendition: a raw blob through
     * {@code ReadBlob}, PARSED through the part frames of {@code ReadDocument}.
     * Each is checked against the remaining unary byte budget while streaming and
     * against the manifest row's length and SHA-256 on completion.
     */
    private ByteString readWhole(
            NodeAddress address,
            long version,
            RenditionManifestEntry row,
            long remainingByteBudget,
            String entryId) {
        List<ByteString> chunks = new ArrayList<>();
        try (Context.CancellableContext callContext = Context.current().withCancellation()) {
            callContext.run(() -> {
                long[] readBytes = {0L};
                if (row.getRendition().getName().equals(ArchiveCoordinates.PARSED)) {
                    Iterator<ReadDocumentResponse> frames = documents.readDocument(ReadDocumentRequest.newBuilder()
                            .setAddress(address).setRevision(revision(version))
                            .setSelection(PartSelection.newBuilder().addSelectors(ArchiveCoordinates.PARSED_PART))
                            .build());
                    frames.forEachRemaining(frame -> {
                        if (frame.hasPartData()) {
                            ByteString chunk = frame.getPartData().getData();
                            if (chunk.size() > remainingByteBudget - readBytes[0]) {
                                throw unaryByteLimitExceeded(
                                        entryId,
                                        version,
                                        row.getRendition(),
                                        (maxUnaryGetBytes - remainingByteBudget) + readBytes[0] + chunk.size());
                            }
                            readBytes[0] += chunk.size();
                            chunks.add(chunk);
                        }
                    });
                } else {
                    Iterator<ReadBlobResponse> frames = documents.readBlob(ReadBlobRequest.newBuilder()
                            .setAddress(address).setRevision(revision(version))
                            .setBlobId(ArchiveCoordinates.blobId(
                                    row.getRendition().getName(), row.getRendition().getSubKey()))
                            .build());
                    frames.forEachRemaining(frame -> {
                        if (frame.hasData()) {
                            ByteString chunk = frame.getData();
                            if (chunk.size() > remainingByteBudget - readBytes[0]) {
                                throw unaryByteLimitExceeded(
                                        entryId,
                                        version,
                                        row.getRendition(),
                                        (maxUnaryGetBytes - remainingByteBudget) + readBytes[0] + chunk.size());
                            }
                            readBytes[0] += chunk.size();
                            chunks.add(chunk);
                        }
                    });
                }
            });
        }
        ByteString data = ByteString.copyFrom(chunks);
        if (data.size() != row.getSizeBytes() || !RenditionReads.sha256Hex(data).equals(row.getSha256())) {
            throw ArchiveErrors.dataLoss("Rendition " + row.getRendition().getName() + " of version " + version
                    + " read back " + data.size() + " bytes that do not match its manifest row", null);
        }
        return data;
    }

    /**
     * Validates an archive request against its own validate.v1 rules.
     *
     * @throws StatusRuntimeException {@code INVALID_ARGUMENT} naming each
     *         offending field path and rule
     */
    static void requireValid(Message request) {
        ValidationResult result = VALIDATOR.validate(request);
        if (!result.valid()) {
            String violations = result.violations().stream()
                    .map(violation -> violation.path() + " (" + violation.ruleId() + ": " + violation.message() + ")")
                    .collect(Collectors.joining("; "));
            throw Status.INVALID_ARGUMENT.withDescription(violations).asRuntimeException();
        }
    }

    /**
     * Validates a projected or assembled view against its own validate.v1 rules.
     *
     * @throws StatusRuntimeException {@code DATA_LOSS} naming each offending field
     *         path and rule: the stored revision passed v2 validation, so a view
     *         that breaks its rules is not the stored truth
     */
    static void requireValidView(Message view) {
        ValidationResult result = VALIDATOR.validate(view);
        if (!result.valid()) {
            String violations = result.violations().stream()
                    .map(violation -> violation.path() + " (" + violation.ruleId() + ": " + violation.message() + ")")
                    .collect(Collectors.joining("; "));
            throw ArchiveErrors.dataLoss(view.getDescriptorForType().getFullName()
                    + " breaks its own rules: " + violations, null);
        }
    }

    // --------------------------------------------------------------- uploads

    /**
     * What an upload's header frame resolves to: a replayed outcome, a recorded
     * outcome to replay once the bytes are checked, or the v2 upload to open.
     */
    sealed interface UploadStart permits Replay, Verify, Pending {
    }

    /** An earlier identical upload's recorded outcome; nothing is uploaded. */
    record Replay(UploadRenditionResponse response) implements UploadStart {
    }

    /**
     * The recorded outcome of an earlier upload under a supplied operation ID
     * whose header named no {@code expected_sha256}. The header digest cannot
     * tell its bytes apart, so the bytes sent again are digested, never
     * uploaded, and the outcome is replayed only when they match the recorded
     * rendition (see {@link #requireRecordedBytes}).
     */
    record Verify(UploadRenditionResponse response) implements UploadStart {
    }

    /**
     * The v2 upload header to send, and what finishing the upload needs.
     * {@code derivedLastWriteWins} carries the currency rule into a lost race
     * (see {@link #afterLostRace}). {@code checkBytes} is set when the header
     * named no {@code expected_sha256}: a lost race is then replayed only for the
     * same bytes.
     */
    record Pending(NodeAddress address, String operationId, String contentSha256, boolean derivedLastWriteWins,
            RenditionDescriptor rendition, UploadHeader header, boolean checkBytes) implements UploadStart {
    }

    /**
     * Checks that the bytes of an upload match the rendition its recorded
     * outcome names before that outcome is replayed.
     *
     * @throws StatusRuntimeException {@code ALREADY_EXISTS} naming
     *         {@code operation_id} when the SHA-256 or the length differs
     */
    static void requireRecordedBytes(UploadRenditionResponse recorded, String sha256, long sizeBytes) {
        if (!recorded.getSha256().equals(sha256) || recorded.getSizeBytes() != sizeBytes) {
            throw ArchiveErrors.alreadyExists("operation_id", recorded.getOperationId()
                    + " is recorded for an upload of other bytes (SHA-256 " + recorded.getSha256() + ", "
                    + recorded.getSizeBytes() + " bytes)");
        }
    }

    /**
     * Resolves an {@code UploadRendition} header frame: the outcome lookup first,
     * then the current revision and the one {@code UploadDocument} header whose
     * save lists every other current blob by ID for carry-forward.
     */
    UploadStart startUpload(UploadRenditionHeader header) {
        requireValid(header);
        ArchiveBinding binding = binding(header.getAddress());
        NodeAddress address = ArchiveCoordinates.address(header.getAddress());
        RenditionDescriptor rendition = header.getRendition();
        boolean parsed = rendition.getName().equals(ArchiveCoordinates.PARSED);
        if (parsed && !rendition.getSubKey().isEmpty()) {
            throw ArchiveErrors.invalid("rendition.sub_key",
                    "the parsed rendition is the document's PARSED part and takes no sub-key");
        }
        String contentSha256 = ArchiveOperationKeys.uploadContentSha256(header);
        String operationId = ArchiveOperationKeys.operationId(header, contentSha256);
        boolean derivedLastWriteWins = header.getOperationId().isEmpty() && header.getExpectedVersion() == 0;
        Optional<Recorded> recorded = recordedOutcome(address, operationId, contentSha256, derivedLastWriteWins);
        boolean checkBytes = header.getExpectedSha256().isEmpty();
        if (recorded.isPresent()) {
            UploadRenditionResponse replay = uploadResponse(recorded.get().revision(), rendition, operationId, true);
            return checkBytes ? new Verify(replay) : new Replay(replay);
        }

        Optional<GetDocumentResponse> current = readCurrent(address);
        Document.Builder document = core(binding, header.getAddress(), current, "", "", "", null, Map.of(),
                facts(header.hasDeclared() ? header.getDeclared() : null,
                        header.hasOrigin() ? header.getOrigin() : null));
        SaveDocumentRequest.Builder save = SaveDocumentRequest.newBuilder()
                .setOperationId(operationId)
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(header.getExpectedVersion() > 0
                        ? ExpectedRevision.newBuilder().setRevision(header.getExpectedVersion()).build()
                        : lastWriteWins(address, current))
                .addWrites(put(ArchiveCoordinates.CORE))
                .setOwner(owner(binding, header.getWrittenBy()))
                .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                .setUnchanged(UnchangedPolicy.UNCHANGED_POLICY_RECORD_OUTCOME)
                .setDrive(binding.drive())
                .putMetadata(CONTENT_SHA256_METADATA, contentSha256);
        UploadHeader.Builder upload = UploadHeader.newBuilder()
                .setDeclaredLength(header.getSizeBytes())
                .setExpectedSha256(header.getExpectedSha256());
        if (parsed) {
            save.addWrites(put(ArchiveCoordinates.PARSED_PART));
            upload.setEncodedPart(StreamedPart.newBuilder().setSelector(ArchiveCoordinates.PARSED_PART));
        } else {
            String blobId = ArchiveCoordinates.blobId(rendition.getName(), rendition.getSubKey());
            List<Blob> currentBlobs = current.map(read -> read.getDocument().getBlobBag().getBlobs().getBlobList())
                    .orElse(List.of());
            Blobs.Builder blobs = Blobs.newBuilder();
            Blob.Builder streamed = Blob.newBuilder().setBlobId(blobId);
            if (!rendition.getMediaType().isEmpty()) {
                streamed.setMimeType(rendition.getMediaType());
            }
            if (!rendition.getSchemaSubject().isEmpty()) {
                streamed.setSchemaSubject(rendition.getSchemaSubject());
            }
            boolean placed = false;
            for (Blob blob : currentBlobs) {
                if (blob.getBlobId().equals(blobId)) {
                    if (blob.hasFilename()) {
                        streamed.setFilename(blob.getFilename());
                    }
                    blobs.addBlob(streamed);
                    placed = true;
                } else {
                    blobs.addBlob(carriedStub(blob));
                }
            }
            if (!placed) {
                blobs.addBlob(streamed);
            }
            document.setBlobBag(BlobBag.newBuilder().setBlobs(blobs));
            save.addWrites(put(ArchiveCoordinates.BLOBS));
            upload.setBlobId(blobId);
        }
        save.setDocument(document);
        upload.setSave(save);
        return new Pending(address, operationId, contentSha256, derivedLastWriteWins, rendition, upload.build(),
                checkBytes);
    }

    /** The archive receipt of a committed (or raced and replayed) upload. */
    UploadRenditionResponse finishUpload(Pending pending, SaveDocumentResponse saved) {
        if (saved.getReplayed() && pending.derivedLastWriteWins()) {
            // v2 replayed another attempt of this operation that committed after our lookup.
            requireStillCurrent(pending.address(), pending.operationId(), saved.getAccession().getRevision());
        }
        GetDocumentResponse committed = read(pending.address(), revision(saved.getAccession().getRevision()),
                CORE_AND_BLOBS);
        return uploadResponse(committed, pending.rendition(), pending.operationId(), saved.getReplayed());
    }

    UploadRenditionResponse uploadResponse(GetDocumentResponse revision, RenditionDescriptor rendition,
            String operationId, boolean replayed) {
        VersionManifest manifest = views.manifest(revision);
        RenditionManifestEntry row = manifest.getRenditionsList().stream()
                .filter(candidate -> candidate.getRendition().getName().equals(rendition.getName())
                        && candidate.getRendition().getSubKey().equals(rendition.getSubKey()))
                .findFirst()
                .orElseThrow(() -> ArchiveErrors.dataLoss("Revision " + manifest.getVersion()
                        + " recorded for this upload lists no rendition "
                        + ArchiveCoordinates.blobId(rendition.getName(), rendition.getSubKey()), null));
        return UploadRenditionResponse.newBuilder()
                .setVersion(manifest.getVersion())
                .setSha256(row.getSha256())
                .setSizeBytes(row.getSizeBytes())
                .setRootChecksum(manifest.getRootChecksum())
                .setOperationId(operationId)
                .setReplayed(replayed)
                .build();
    }

    /** The facts a write sets: each one present replaces the stored one, an absent one keeps it. */
    static FactsSource facts(FormatFact declared, ObjectStoreOrigin origin) {
        return stored -> {
            if (declared != null) {
                stored.setDeclared(declared);
            }
            if (origin != null) {
                stored.setOrigin(origin);
            }
        };
    }
}
