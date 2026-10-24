package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveStats;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.BlobWrite;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.BlobHeader;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DocumentArchive} and {@link DocumentArchiveBindings} covering
 * unary {@code GetEntry} byte limits (#620), pipelined {@code ListEntries} manifest
 * reads (#621), archive definitions (#627), and archive stats / entry counts (#628).
 */
class DocumentArchiveTest {

    private static final String ACCOUNT = "acct-test";
    private static final String ARCHIVE = "reports";
    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-04-19T12:00:00Z"), ZoneOffset.UTC);
    private static final Timestamp NOW =
            Timestamp.newBuilder().setSeconds(1_776_600_000L).build();
    private static final SaveOwner INTAKE_OWNER =
            SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds")).build();

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void rejectsNonPositiveMaxUnaryGetBytes() throws Exception {
        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {});
        ArchiveBindings bindings = (acct, name) -> Optional.empty();
        assertThatThrownBy(() -> new DocumentArchive(channel, bindings, FIXED_CLOCK, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxUnaryGetBytes");
    }

    @Test
    void unaryGetEntryRejectsOversizedStreamChunkDuringReadWhole() throws Exception {
        ByteString declaredBytes = ByteString.copyFromUtf8("0123456789");
        ByteString oversizedStream = ByteString.copyFromUtf8("0123456789-overflow");
        NodeAddress node = ArchiveCoordinates.address(
                EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive(ARCHIVE).setEntryId("e1").build());
        GetDocumentResponse stored = documentWithBlob(node, 1L, "original", declaredBytes);

        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override
            public void getDocument(
                    GetDocumentRequest request, StreamObserver<GetDocumentResponse> responseObserver) {
                responseObserver.onNext(stored);
                responseObserver.onCompleted();
            }

            @Override
            public void readBlob(
                    ReadBlobRequest request, StreamObserver<ReadBlobResponse> responseObserver) {
                responseObserver.onNext(ReadBlobResponse.newBuilder()
                        .setHeader(BlobHeader.getDefaultInstance())
                        .build());
                responseObserver.onNext(
                        ReadBlobResponse.newBuilder().setData(oversizedStream).build());
                responseObserver.onCompleted();
            }
        });

        ArchiveBindings bindings = (acct, name) -> Optional.of(
                new ArchiveBinding(acct, name, "ds", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(
                channel, bindings, FIXED_CLOCK, declaredBytes.size());

        StatusRuntimeException failure = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.getEntry(GetEntryRequest.newBuilder()
                        .setAddress(EntryAddress.newBuilder()
                                .setAccountId(ACCOUNT)
                                .setArchive(ARCHIVE)
                                .setEntryId("e1"))
                        .build()));
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(failure.getStatus().getDescription()).contains("ReadRendition");
    }

    @Test
    void listEntriesWithIncludeManifestsReadsExactRevisionOncePerRowAndSetsTotalCount() throws Exception {
        AtomicInteger bindingLookups = new AtomicInteger();
        List<GetDocumentRequest> getRequests = new ArrayList<>();
        ByteString body = ByteString.copyFromUtf8("entry-body");
        NodeAddress e1 = ArchiveCoordinates.address(
                EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive(ARCHIVE).setEntryId("e1").build());
        NodeAddress e2 = ArchiveCoordinates.address(
                EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive(ARCHIVE).setEntryId("e2").build());

        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override
            public void listDocuments(
                    ListDocumentsRequest request, StreamObserver<ListDocumentsResponse> responseObserver) {
                responseObserver.onNext(ListDocumentsResponse.newBuilder()
                        .addDocuments(summary(e1, 3L, "Doc 1"))
                        .addDocuments(summary(e2, 5L, "Doc 2"))
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void getDocument(
                    GetDocumentRequest request, StreamObserver<GetDocumentResponse> responseObserver) {
                synchronized (getRequests) {
                    getRequests.add(request);
                }
                long rev = request.getAddress().getDocId().equals("e1") ? 3L : 5L;
                responseObserver.onNext(
                        documentWithBlob(request.getAddress(), rev, "original", body));
                responseObserver.onCompleted();
            }
        });

        ArchiveBindings bindings = (acct, name) -> {
            bindingLookups.incrementAndGet();
            return Optional.of(new ArchiveBinding(
                    acct, name, "ds", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        };
        DocumentArchive archive = new DocumentArchive(channel, bindings, FIXED_CLOCK);

        ListEntriesResponse response = archive.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setIncludeManifests(true)
                .build());

        assertThat(bindingLookups.get()).isEqualTo(1);
        assertThat(response.getTotalCount()).isEqualTo(2L);
        assertThat(response.getEntriesCount()).isEqualTo(2);
        assertThat(response.getManifestsCount()).isEqualTo(2);
        assertThat(getRequests).hasSize(2);
        assertThat(getRequests)
                .allSatisfy(req -> assertThat(req.getRevision().hasRevision()).isTrue());
    }

    @Test
    void createGetListArchivesAndStatsOverInMemoryDocumentService() throws Exception {
        Map<NodeAddress, Document> definitions = new LinkedHashMap<>();
        List<SaveDocumentRequest> entrySaves = new ArrayList<>();
        ByteString pdf = ByteString.copyFromUtf8("pdf-bytes");
        NodeAddress entryNode = ArchiveCoordinates.address(
                EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive(ARCHIVE).setEntryId("e1").build());

        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override
            public void getOperationOutcome(
                    GetOperationOutcomeRequest request,
                    StreamObserver<GetOperationOutcomeResponse> responseObserver) {
                responseObserver.onNext(GetOperationOutcomeResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }

            @Override
            public void saveDocument(
                    SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> responseObserver) {
                NodeAddress addr = request.getTarget().getAddress();
                if (!addr.getGraphId().startsWith(DocumentArchiveBindings.DEFINITION_GRAPH_PREFIX)) {
                    synchronized (entrySaves) {
                        entrySaves.add(request);
                    }
                    responseObserver.onNext(SaveDocumentResponse.newBuilder()
                            .setAccession(Accession.newBuilder().setAddress(addr).setRevision(1L))
                            .build());
                    responseObserver.onCompleted();
                    return;
                }
                if (definitions.containsKey(addr)) {
                    responseObserver.onError(Status.ABORTED.asRuntimeException());
                    return;
                }
                definitions.put(addr, request.getDocument());
                responseObserver.onNext(SaveDocumentResponse.newBuilder()
                        .setAccession(Accession.newBuilder().setAddress(addr).setRevision(1L))
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void getDocument(
                    GetDocumentRequest request, StreamObserver<GetDocumentResponse> responseObserver) {
                NodeAddress addr = request.getAddress();
                if (addr.equals(entryNode)) {
                    responseObserver.onNext(documentWithBlob(entryNode, 1L, "original", pdf));
                    responseObserver.onCompleted();
                    return;
                }
                Document def = definitions.get(addr);
                if (def == null) {
                    responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
                    return;
                }
                responseObserver.onNext(GetDocumentResponse.newBuilder()
                        .setDocument(def)
                        .setManifest(DocumentManifest.newBuilder()
                                .setAddress(addr)
                                .setRevision(1L)
                                .setCommittedAt(NOW))
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void listDocuments(
                    ListDocumentsRequest request, StreamObserver<ListDocumentsResponse> responseObserver) {
                ListDocumentsResponse.Builder builder = ListDocumentsResponse.newBuilder();
                if (request.getGraphId().startsWith(DocumentArchiveBindings.DEFINITION_GRAPH_PREFIX)) {
                    for (Map.Entry<NodeAddress, Document> e : definitions.entrySet()) {
                        if (e.getKey().getAccountId().equals(request.getAccountId())) {
                            SearchMetadata sm = e.getValue().getSearchMetadata();
                            builder.addDocuments(DocumentSummary.newBuilder()
                                    .setAddress(e.getKey())
                                    .setRevision(1L)
                                    .setCommittedAt(NOW)
                                    .setLookup(DocumentLookup.newBuilder()
                                            .setTitle(sm.getTitle())
                                            .setCreationDate(sm.getCreationDate())
                                            .putAllMetadata(sm.getMetadataMap())));
                        }
                    }
                } else if (request.getGraphAddressId().equals(ARCHIVE)) {
                    builder.addDocuments(summary(entryNode, 1L, "Doc 1"));
                }
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            }
        });

        ArchiveBindings bindings = ArchiveBindings.overDocuments(
                channel, "ds", (acct, drive) -> "intake".equals(drive));
        DocumentArchive archive = new DocumentArchive(channel, bindings, FIXED_CLOCK);

        Archive created = archive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setName(ARCHIVE)
                        .setDriveName("intake")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build()).getArchive();
        assertThat(created.getName()).isEqualTo(ARCHIVE);
        assertThat(bindings.find(ACCOUNT, ARCHIVE).orElseThrow().drive()).isEqualTo("intake");

        assertThat(archive.getArchive(GetArchiveRequest.newBuilder()
                .setAccountId(ACCOUNT).setArchive(ARCHIVE).build()).getArchive()).isEqualTo(created);
        assertThat(archive.listArchives(ListArchivesRequest.newBuilder()
                .setAccountId(ACCOUNT).build()).getArchivesList()).containsExactly(created);

        archive.putEntry(PutEntryRequest.newBuilder()
                .setAddress(EntryAddress.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setArchive(ARCHIVE)
                        .setEntryId("e1"))
                .setTitle("Doc 1")
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("original")
                                .setMediaType("application/pdf"))
                        .setData(pdf))
                .build());
        assertThat(entrySaves).hasSize(1);
        assertThat(entrySaves.getFirst().getDrive()).isEqualTo("intake");

        ArchiveStats stats = archive.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(ACCOUNT).setArchive(ARCHIVE).build()).getStats();
        assertThat(stats.getEntries()).isEqualTo(1L);
        assertThat(stats.getVersions()).isEqualTo(1L);
        assertThat(stats.getCurrentBytes()).isEqualTo(pdf.size());
        assertThat(stats.getRetainedBytes()).isEqualTo(pdf.size());
    }

    private void startService(DocumentServiceGrpc.DocumentServiceImplBase service) throws Exception {
        String name = "doc-archive-unit-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    private static DocumentSummary summary(NodeAddress address, long revision, String title) {
        return DocumentSummary.newBuilder()
                .setAddress(address)
                .setRevision(revision)
                .setCommittedAt(NOW)
                .setLookup(DocumentLookup.newBuilder().setTitle(title).setCreationDate(NOW))
                .build();
    }

    private static GetDocumentResponse documentWithBlob(
            NodeAddress address, long revision, String blobId, ByteString data) {
        String sha = RenditionReads.sha256Hex(data);
        return GetDocumentResponse.newBuilder()
                .setManifest(DocumentManifest.newBuilder()
                        .setAddress(address)
                        .setRevision(revision)
                        .setCommittedAt(NOW)
                        .addParts(PartManifestEntry.newBuilder()
                                .setSelector(ArchiveCoordinates.CORE)
                                .setState(PartState.PART_STATE_PRESENT)
                                .setSizeBytes(32)
                                .setSha256("a".repeat(64))
                                .setWrittenInRevision(revision)
                                .setWrittenAt(NOW)
                                .setOwner(INTAKE_OWNER))
                        .addParts(PartManifestEntry.newBuilder()
                                .setSelector(ArchiveCoordinates.BLOBS)
                                .setState(PartState.PART_STATE_PRESENT)
                                .setSizeBytes(64)
                                .setSha256("b".repeat(64))
                                .setWrittenInRevision(revision)
                                .setWrittenAt(NOW)
                                .setOwner(INTAKE_OWNER)))
                .setDocument(Document.newBuilder()
                        .setDocId(address.getDocId())
                        .setOwnership(OwnershipContext.newBuilder()
                                .setAccountId(address.getAccountId())
                                .setDatasourceId("ds"))
                        .setSearchMetadata(SearchMetadata.newBuilder()
                                .setTitle("Title")
                                .setCreationDate(NOW))
                        .setBlobBag(BlobBag.newBuilder()
                                .setBlobs(Blobs.newBuilder()
                                        .addBlob(Blob.newBuilder()
                                                .setBlobId(blobId)
                                                .setMimeType("application/pdf")
                                                .setSizeBytes(data.size())
                                                .setChecksum(sha)
                                                .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256)
                                                .setWritten(BlobWrite.newBuilder()
                                                        .setRevision(revision)
                                                        .setCommittedAt(NOW))))))
                .setValidation(RevisionValidation.newBuilder()
                        .setStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                        .setCandidateSha256("c".repeat(64))
                        .setValidatorProfile("v1"))
                .build();
    }
}
