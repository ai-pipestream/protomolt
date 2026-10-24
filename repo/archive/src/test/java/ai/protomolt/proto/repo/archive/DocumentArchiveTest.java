package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationKind;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationState;
import ai.protomolt.proto.repo.archive.v1.ArchiveStats;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteEntryRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.PruneVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
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
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DeleteDocumentResponse;
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
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsResponse;
import ai.protomolt.proto.repo.v2.NoRecordedOutcome;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.RevisionSummary;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.SaveRejection;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
            public void listRevisions(
                    ListRevisionsRequest request,
                    StreamObserver<ListRevisionsResponse> responseObserver) {
                responseObserver.onNext(ListRevisionsResponse.getDefaultInstance());
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

    @Test
    void deleteEntryCommitsTombstoneReplaysByOperationIdAndServesGetArchiveMutation() throws Exception {
        EntryAddress entryAddress = EntryAddress.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setEntryId("e-del")
                .build();
        NodeAddress node = ArchiveCoordinates.address(entryAddress);
        ByteString pdf = ByteString.copyFromUtf8("pdf-bytes");
        Map<String, RecordedOutcome> outcomes = new LinkedHashMap<>();
        List<DeleteDocumentRequest> deleteCalls = new ArrayList<>();
        boolean[] deleted = {false};

        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override
            public void getOperationOutcome(
                    GetOperationOutcomeRequest request,
                    StreamObserver<GetOperationOutcomeResponse> responseObserver) {
                RecordedOutcome outcome = outcomes.get(request.getOperationId());
                if (outcome == null) {
                    responseObserver.onNext(GetOperationOutcomeResponse.newBuilder()
                            .setNone(NoRecordedOutcome.getDefaultInstance())
                            .build());
                } else {
                    responseObserver.onNext(GetOperationOutcomeResponse.newBuilder()
                            .setCommitted(outcome)
                            .build());
                }
                responseObserver.onCompleted();
            }

            @Override
            public void getDocument(
                    GetDocumentRequest request, StreamObserver<GetDocumentResponse> responseObserver) {
                if (deleted[0] || !request.getAddress().equals(node)) {
                    responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
                    return;
                }
                responseObserver.onNext(documentWithBlob(node, 2L, "original", pdf));
                responseObserver.onCompleted();
            }

            @Override
            public void listRevisions(
                    ListRevisionsRequest request, StreamObserver<ListRevisionsResponse> responseObserver) {
                ListRevisionsResponse.Builder builder = ListRevisionsResponse.newBuilder();
                if (request.getAddress().equals(node)) {
                    if (deleted[0]) {
                        builder.addRevisions(RevisionSummary.newBuilder()
                                .setRevision(3L)
                                .setPreviousRevision(2L)
                                .setTombstone(true)
                                .setCommittedAt(NOW));
                    }
                    builder.addRevisions(RevisionSummary.newBuilder()
                            .setRevision(2L)
                            .setPreviousRevision(1L)
                            .setCommittedAt(NOW));
                }
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            }

            @Override
            public void deleteDocument(
                    DeleteDocumentRequest request,
                    StreamObserver<DeleteDocumentResponse> responseObserver) {
                deleteCalls.add(request);
                if (deleted[0] || request.getExpected().getRevision() != 2L) {
                    responseObserver.onError(Status.ABORTED.asRuntimeException());
                    return;
                }
                deleted[0] = true;
                Accession accession = Accession.newBuilder()
                        .setAddress(node)
                        .setRevision(3L)
                        .setPreviousRevision(2L)
                        .setTombstone(true)
                        .setCommittedAt(NOW)
                        .build();
                outcomes.put(
                        request.getOperationId(),
                        RecordedOutcome.newBuilder()
                                .setAccession(accession)
                                .setRecordedAt(NOW)
                                .build());
                responseObserver.onNext(DeleteDocumentResponse.newBuilder()
                        .setOperationId(request.getOperationId())
                        .setAccession(accession)
                        .build());
                responseObserver.onCompleted();
            }
        });

        ArchiveBindings bindings = (acct, name) -> Optional.of(
                new ArchiveBinding(acct, name, "ds", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchiveGrpcService archiveService =
                new DocumentArchiveGrpcService(channel, bindings, FIXED_CLOCK);
        DocumentArchive archive = archiveService.archive();

        String mutationServerName = "mutation-unit-" + UUID.randomUUID();
        Server mutationServer = InProcessServerBuilder.forName(mutationServerName)
                .directExecutor()
                .addService(new DocumentArchiveMutationGrpcService(archive))
                .build()
                .start();
        ManagedChannel mutationChannel =
                InProcessChannelBuilder.forName(mutationServerName).directExecutor().build();
        try {
            ArchiveMutationServiceGrpc.ArchiveMutationServiceBlockingStub mutationStub =
                    ArchiveMutationServiceGrpc.newBlockingStub(mutationChannel);
            String operationId = "11111111-2222-4333-8444-555555555555";
            ArchiveMutationRequest request = ArchiveMutationRequest.newBuilder()
                    .setOperationId(operationId)
                    .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(entryAddress))
                    .build();

            ArchiveMutationReceipt receipt = mutationStub.archiveMutation(request).getReceipt();
            assertThat(receipt.getOperationId()).isEqualTo(operationId);
            assertThat(receipt.getAddress()).isEqualTo(entryAddress);
            assertThat(receipt.getKind()).isEqualTo(ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_ENTRY);
            assertThat(receipt.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
            assertThat(receipt.getEntryDeleted()).isTrue();
            assertThat(receipt.getVersionsRemoved()).isZero();
            assertThat(receipt.getVersionsTombstoned()).isZero();
            assertThat(receipt.getObjectsTargeted()).isZero();
            assertThat(receipt.getObjectsPending()).isZero();
            assertThat(receipt.getObjectsConfirmedAbsent()).isZero();
            assertThat(receipt.getObservedAt()).isEqualTo(NOW);
            assertThat(receipt.getStatusRevision()).isEqualTo(1L);
            assertThat(receipt.getCommandSha256())
                    .isEqualTo(ArchiveOperationKeys.mutationCommandSha256(request));
            assertThat(deleteCalls).hasSize(1);
            assertThat(deleteCalls.getFirst().getExpected().getRevision()).isEqualTo(2L);

            // Idempotent replay with the same operation_id does not invoke deleteDocument again.
            ArchiveMutationReceipt replay = mutationStub.archiveMutation(request).getReceipt();
            assertThat(replay).isEqualTo(receipt);
            assertThat(deleteCalls).hasSize(1);

            // GetArchiveMutation returns the same receipt.
            ArchiveMutationReceipt fetched = mutationStub.getArchiveMutation(
                    GetArchiveMutationRequest.newBuilder()
                            .setAccountId(ACCOUNT)
                            .setOperationId(operationId)
                            .build()).getReceipt();
            assertThat(fetched).isEqualTo(receipt);

            // Unknown operation ID returns NOT_FOUND.
            StatusRuntimeException missing = catchThrowableOfType(
                    StatusRuntimeException.class,
                    () -> mutationStub.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                            .setAccountId(ACCOUNT)
                            .setOperationId("99999999-2222-4333-8444-555555555555")
                            .build()));
            assertThat(missing.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        } finally {
            mutationChannel.shutdownNow();
            mutationServer.shutdownNow();
        }
    }

    @Test
    void deleteEntryRefusesVersionConflictsOperationIdReuseAndRefusesDeleteRenditionAndPruneVersionsByName()
            throws Exception {
        EntryAddress entryAddress = EntryAddress.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setEntryId("e-conflict")
                .build();
        NodeAddress node = ArchiveCoordinates.address(entryAddress);
        String saveOpId = "22222222-2222-4333-8444-555555555555";
        String rejectedUploadOpId = "33333333-2222-4333-8444-555555555555";
        String rejectedMutationOpId = "77777777-2222-4333-8444-555555555555";
        String deleteEntryCommandSha256 =
                ArchiveOperationKeys.deleteEntryCommandSha256(entryAddress);

        Map<String, RecordedOutcome> recordedOutcomes = new LinkedHashMap<>();
        recordedOutcomes.put(
                saveOpId,
                RecordedOutcome.newBuilder()
                        .setAccession(Accession.newBuilder()
                                .setAddress(node)
                                .setRevision(1L)
                                .setTombstone(false)
                                .setCommittedAt(NOW))
                        .setRecordedAt(NOW)
                        .build());
        recordedOutcomes.put(
                rejectedUploadOpId,
                RecordedOutcome.newBuilder()
                        .setRejection(SaveRejection.newBuilder()
                                .setReason(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH)
                                .setMessage("upload sha256 mismatch"))
                        .putRequestMetadata(DocumentArchive.CONTENT_SHA256_METADATA, "f".repeat(64))
                        .setRecordedAt(NOW)
                        .build());
        recordedOutcomes.put(
                rejectedMutationOpId,
                RecordedOutcome.newBuilder()
                        .setRejection(SaveRejection.newBuilder()
                                .setReason(RejectionReason.REJECTION_REASON_WRITE_DENIED)
                                .setMessage("writer denied"))
                        .putRequestMetadata(
                                DocumentArchive.MUTATION_KIND_METADATA,
                                ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_ENTRY.name())
                        .putRequestMetadata(
                                DocumentArchive.MUTATION_SHA256_METADATA,
                                deleteEntryCommandSha256)
                        .setRecordedAt(NOW)
                        .build());

        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override
            public void getOperationOutcome(
                    GetOperationOutcomeRequest request,
                    StreamObserver<GetOperationOutcomeResponse> responseObserver) {
                RecordedOutcome outcome = recordedOutcomes.get(request.getOperationId());
                if (outcome != null) {
                    responseObserver.onNext(GetOperationOutcomeResponse.newBuilder()
                            .setCommitted(outcome)
                            .build());
                } else {
                    responseObserver.onNext(GetOperationOutcomeResponse.newBuilder()
                            .setNone(NoRecordedOutcome.getDefaultInstance())
                            .build());
                }
                responseObserver.onCompleted();
            }

            @Override
            public void saveDocument(
                    SaveDocumentRequest request,
                    StreamObserver<SaveDocumentResponse> responseObserver) {
                if (recordedOutcomes.containsKey(request.getOperationId())) {
                    responseObserver.onError(Status.ALREADY_EXISTS
                            .withDescription("operation_id already recorded")
                            .asRuntimeException());
                    return;
                }
                if (request.getDerivedFromCount() > 0) {
                    recordedOutcomes.put(
                            request.getOperationId(),
                            RecordedOutcome.newBuilder()
                                    .setRejection(SaveRejection.newBuilder()
                                            .setReason(RejectionReason.REJECTION_REASON_UNSPECIFIED)
                                            .setMessage("unreadable lineage"))
                                    .putAllRequestMetadata(request.getMetadataMap())
                                    .setRecordedAt(NOW)
                                    .build());
                    responseObserver.onError(Status.FAILED_PRECONDITION
                            .withDescription("unreadable lineage")
                            .asRuntimeException());
                    return;
                }
                responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
            }

            @Override
            public void deleteDocument(
                    DeleteDocumentRequest request,
                    StreamObserver<DeleteDocumentResponse> responseObserver) {
                responseObserver.onError(Status.ABORTED
                        .withDescription("expected revision " + request.getExpected().getRevision() + " != 2")
                        .asRuntimeException());
            }
        });

        ArchiveBindings bindings = (acct, name) -> Optional.of(
                new ArchiveBinding(acct, name, "ds", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(channel, bindings, FIXED_CLOCK);

        // Explicit stale expectedVersion fails with ABORTED.
        StatusRuntimeException stale = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.deleteEntry(
                        "44444444-2222-4333-8444-555555555555",
                        DeleteEntryRequest.newBuilder().setAddress(entryAddress).build(),
                        1L,
                        "stale delete"));
        assertThat(stale.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);

        // Reusing an operation_id recorded for a save fails with ALREADY_EXISTS on DeleteEntry
        // and returns NOT_FOUND on GetArchiveMutation.
        StatusRuntimeException reusedOnDelete = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.deleteEntry(
                        saveOpId, DeleteEntryRequest.newBuilder().setAddress(entryAddress).build()));
        assertThat(reusedOnDelete.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(reusedOnDelete.getStatus().getDescription()).contains("operation_id");

        StatusRuntimeException saveOnGetMutation = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setOperationId(saveOpId)
                        .build()));
        assertThat(saveOnGetMutation.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);

        // A rejected upload's operation_id is NOT a mutation: reusing it on DeleteEntry is
        // ALREADY_EXISTS (not INVALID_ARGUMENT), and GetArchiveMutation returns NOT_FOUND.
        StatusRuntimeException rejectedUploadOnDelete = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.deleteEntry(
                        rejectedUploadOpId,
                        DeleteEntryRequest.newBuilder().setAddress(entryAddress).build()));
        assertThat(rejectedUploadOnDelete.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(rejectedUploadOnDelete.getStatus().getDescription()).contains("operation_id");

        StatusRuntimeException rejectedUploadOnGet = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setOperationId(rejectedUploadOpId)
                        .build()));
        assertThat(rejectedUploadOnGet.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);

        // A recorded mutation rejection with matching command SHA-256 is rethrown on both
        // DeleteEntry and GetArchiveMutation, and refused with ALREADY_EXISTS for another entry.
        StatusRuntimeException rejectedDelete = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.deleteEntry(
                        rejectedMutationOpId,
                        DeleteEntryRequest.newBuilder().setAddress(entryAddress).build()));
        assertThat(rejectedDelete.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

        StatusRuntimeException rejectedGet = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setOperationId(rejectedMutationOpId)
                        .build()));
        assertThat(rejectedGet.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

        // DeleteRendition is refused by name with FAILED_PRECONDITION and remains queryable.
        String deleteRenditionOpId = "55555555-2222-4333-8444-555555555555";
        StatusRuntimeException deleteRenditionRefused = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.archiveMutation(ArchiveMutationRequest.newBuilder()
                        .setOperationId(deleteRenditionOpId)
                        .setDeleteRendition(DeleteRenditionRequest.newBuilder()
                                .setAddress(entryAddress)
                                .setRendition("original")
                                .setReason("GDPR erasure"))
                        .build()));
        assertThat(deleteRenditionRefused.getStatus().getCode())
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(deleteRenditionRefused.getStatus().getDescription())
                .contains("DeleteRendition");

        StatusRuntimeException queriedDeleteRendition = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setOperationId(deleteRenditionOpId)
                        .build()));
        assertThat(queriedDeleteRendition.getStatus().getCode())
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(queriedDeleteRendition.getStatus().getDescription())
                .contains("DeleteRendition");

        // PruneVersions is refused by name with FAILED_PRECONDITION and remains queryable.
        String pruneVersionsOpId = "66666666-2222-4333-8444-555555555555";
        StatusRuntimeException pruneVersionsRefused = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.archiveMutation(ArchiveMutationRequest.newBuilder()
                        .setOperationId(pruneVersionsOpId)
                        .setPruneVersions(PruneVersionsRequest.newBuilder()
                                .setAddress(entryAddress)
                                .setKeepLatest(1))
                        .build()));
        assertThat(pruneVersionsRefused.getStatus().getCode())
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(pruneVersionsRefused.getStatus().getDescription())
                .contains("PruneVersions");

        StatusRuntimeException queriedPruneVersions = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setOperationId(pruneVersionsOpId)
                        .build()));
        assertThat(queriedPruneVersions.getStatus().getCode())
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(queriedPruneVersions.getStatus().getDescription())
                .contains("PruneVersions");

        // Reusing an existing operation_id on DeleteRendition or PruneVersions is ALREADY_EXISTS.
        StatusRuntimeException reusedOnRendition = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.deleteRendition(
                        saveOpId,
                        DeleteRenditionRequest.newBuilder()
                                .setAddress(entryAddress)
                                .setRendition("original")
                                .setReason("GDPR erasure")
                                .build()));
        assertThat(reusedOnRendition.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    }

    @Test
    void refuseUnsupportedMutationPropagatesUnconfirmedSaveFailureAndUnimplemented()
            throws Exception {
        EntryAddress entryAddress = EntryAddress.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setEntryId("e-unconfirmed")
                .build();
        String unimplementedOpId = "88888888-2222-4333-8444-555555555555";
        String unrecordedPreconditionOpId = "99999999-2222-4333-8444-555555555555";

        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override
            public void getOperationOutcome(
                    GetOperationOutcomeRequest request,
                    StreamObserver<GetOperationOutcomeResponse> responseObserver) {
                responseObserver.onNext(GetOperationOutcomeResponse.newBuilder()
                        .setNone(NoRecordedOutcome.getDefaultInstance())
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void saveDocument(
                    SaveDocumentRequest request,
                    StreamObserver<SaveDocumentResponse> responseObserver) {
                if (request.getOperationId().equals(unimplementedOpId)) {
                    responseObserver.onError(Status.UNIMPLEMENTED
                            .withDescription("saveDocument not mounted")
                            .asRuntimeException());
                    return;
                }
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("pre-rejection failure without recorded outcome")
                        .asRuntimeException());
            }
        });

        ArchiveBindings bindings = (acct, name) -> Optional.of(
                new ArchiveBinding(acct, name, "ds", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(channel, bindings, FIXED_CLOCK);

        StatusRuntimeException unimplemented = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.deleteRendition(
                        unimplementedOpId,
                        DeleteRenditionRequest.newBuilder()
                                .setAddress(entryAddress)
                                .setRendition("original")
                                .setReason("GDPR erasure")
                                .build()));
        assertThat(unimplemented.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);

        StatusRuntimeException unconfirmed = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.pruneVersions(
                        unrecordedPreconditionOpId,
                        PruneVersionsRequest.newBuilder()
                                .setAddress(entryAddress)
                                .setKeepLatest(1)
                                .build()));
        assertThat(unconfirmed.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(unconfirmed.getStatus().getDescription())
                .contains("pre-rejection failure without recorded outcome");

        StatusRuntimeException notFoundAfterUnconfirmed = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> archive.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setOperationId(unrecordedPreconditionOpId)
                        .build()));
        assertThat(notFoundAfterUnconfirmed.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }

    @Test
    void flowControlledUploadWaitsForDownstreamReadinessAndResumesOnReadyCallback() throws Exception {
        ByteString chunkBytes = ByteString.copyFromUtf8("flow-controlled-bytes");
        NodeAddress entryNode = ArchiveCoordinates.address(
                EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive(ARCHIVE).setEntryId("e-flow").build());
        AtomicBoolean committed = new AtomicBoolean(false);

        startService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override
            public void getOperationOutcome(
                    GetOperationOutcomeRequest request,
                    StreamObserver<GetOperationOutcomeResponse> responseObserver) {
                responseObserver.onNext(GetOperationOutcomeResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }

            @Override
            public void listRevisions(
                    ListRevisionsRequest request,
                    StreamObserver<ListRevisionsResponse> responseObserver) {
                responseObserver.onNext(ListRevisionsResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }

            @Override
            public StreamObserver<UploadDocumentRequest> uploadDocument(
                    StreamObserver<UploadDocumentResponse> responseObserver) {
                return new StreamObserver<>() {
                    @Override
                    public void onNext(UploadDocumentRequest value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(t);
                    }

                    @Override
                    public void onCompleted() {
                        committed.set(true);
                        responseObserver.onNext(UploadDocumentResponse.newBuilder()
                                .setSaved(SaveDocumentResponse.newBuilder()
                                        .setAccession(Accession.newBuilder().setAddress(entryNode).setRevision(1L)))
                                .build());
                        responseObserver.onCompleted();
                    }
                };
            }

            @Override
            public void getDocument(
                    GetDocumentRequest request, StreamObserver<GetDocumentResponse> responseObserver) {
                if (!committed.get()) {
                    responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
                    return;
                }
                responseObserver.onNext(documentWithBlob(entryNode, 1L, "original", chunkBytes));
                responseObserver.onCompleted();
            }
        });

        AtomicBoolean downstreamReady = new AtomicBoolean(false);
        AtomicReference<ClientCall.Listener<?>> uploadListener = new AtomicReference<>();
        Channel throttledChannel = ClientInterceptors.intercept(channel, new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
                if (!method.getFullMethodName().equals(
                        DocumentServiceGrpc.getUploadDocumentMethod().getFullMethodName())) {
                    return call;
                }
                return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        uploadListener.set(responseListener);
                        super.start(responseListener, headers);
                    }

                    @Override
                    public boolean isReady() {
                        return downstreamReady.get();
                    }
                };
            }
        });

        ArchiveBindings bindings = (acct, name) -> Optional.of(
                new ArchiveBinding(acct, name, "ds", "intake", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(throttledChannel, bindings, FIXED_CLOCK);
        RenditionUploads uploads = new RenditionUploads(archive);
        CompletableFuture<UploadRenditionResponse> done = new CompletableFuture<>();
        RenditionUploads.FlowControlledUpload stream = uploads.uploadFlowControlled(new StreamObserver<>() {
            private UploadRenditionResponse result;

            @Override
            public void onNext(UploadRenditionResponse value) {
                result = value;
            }

            @Override
            public void onError(Throwable t) {
                done.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                done.complete(result);
            }
        });

        assertThat(stream.isReady()).isTrue();
        stream.onNext(UploadRenditionRequest.newBuilder()
                .setHeader(UploadRenditionHeader.newBuilder()
                        .setOperationId("op-flow-1")
                        .setSizeBytes(chunkBytes.size())
                        .setAddress(EntryAddress.newBuilder()
                                .setAccountId(ACCOUNT)
                                .setArchive(ARCHIVE)
                                .setEntryId("e-flow"))
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("original")
                                .setMediaType("application/pdf")))
                .build());

        // Downstream is not ready: isReady() is false and awaitReady times out until onReady fires.
        assertThat(stream.isReady()).isFalse();
        assertThat(stream.awaitReady(TimeUnit.MILLISECONDS.toNanos(15))).isFalse();

        CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(() -> {
            try {
                return stream.awaitReady(TimeUnit.SECONDS.toNanos(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        downstreamReady.set(true);
        uploadListener.get().onReady();

        assertThat(waiter.get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stream.isReady()).isTrue();

        stream.onNext(UploadRenditionRequest.newBuilder().setChunk(chunkBytes).build());
        stream.onCompleted();

        UploadRenditionResponse response = done.get(5, TimeUnit.SECONDS);
        assertThat(response.getVersion()).isEqualTo(1L);
        assertThat(response.getSizeBytes()).isEqualTo(chunkBytes.size());
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
