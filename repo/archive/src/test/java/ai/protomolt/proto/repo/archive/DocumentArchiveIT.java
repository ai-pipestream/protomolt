package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.repo.accession.V2RepositoryFixture;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveEntryFacts;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationKind;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationState;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.ArchiveStats;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteEntryRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PruneVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionReadHeader;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.RenditionStats;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersionSummary;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.SaveRejection;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The archive over v2 documents end to end: archive gRPC calls through
 * {@link DocumentArchiveGrpcService} into a real {@code DocumentService} over
 * PostgreSQL and LocalStack S3, with typed admission of the entry's facts.
 */
@Testcontainers
class DocumentArchiveIT {

    static final String ACCOUNT = "acct-archive-it";
    static final String RETAINED = "reports";
    static final String CURRENT_ONLY = "latest";
    static final String DATASOURCE = "ds-archive";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    static V2RepositoryFixture repo;
    static ManagedChannel documentsChannel;
    static Server archiveServer;
    static ManagedChannel archiveChannel;
    static ArchiveServiceGrpc.ArchiveServiceBlockingStub archive;
    static ArchiveServiceGrpc.ArchiveServiceStub asyncArchive;
    static ArchiveMutationServiceGrpc.ArchiveMutationServiceBlockingStub mutations;
    static DocumentArchive archiveEngine;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static final Gate GATE = new Gate();
    static ManagedChannel gatedDocumentsChannel;
    static Server gatedArchiveServer;
    static ManagedChannel gatedArchiveChannel;
    static ArchiveServiceGrpc.ArchiveServiceBlockingStub gatedArchive;
    static ArchiveServiceGrpc.ArchiveServiceStub gatedAsyncArchive;
    static ArchiveMutationServiceGrpc.ArchiveMutationServiceBlockingStub gatedMutations;

    @BeforeAll
    static void boot() throws Exception {
        var facts = BuiltinDocumentSchema.definition(ArchiveEntryFacts.getDescriptor());
        var factsTypeUrl = "type.googleapis.com/" + ArchiveEntryFacts.getDescriptor().getFullName();
        var searchMetadata = V2RepositoryFixture.searchMetadataResolver();
        repo = V2RepositoryFixture.builder(POSTGRES, LOCALSTACK, "archive-v2-it")
                .resolver(selection -> factsTypeUrl.equals(selection.typeUrl()) ? facts : searchMetadata.select(selection))
                .build();
        documentsChannel = InProcessChannelBuilder.forName("archive-v2-it").build();
        documents = DocumentServiceGrpc.newBlockingStub(documentsChannel);
        ArchiveBindings bindings = (account, name) -> switch (name) {
            case RETAINED -> Optional.of(new ArchiveBinding(account, name, DATASOURCE, "",
                    VersioningPolicy.VERSIONING_POLICY_RETAINED));
            case CURRENT_ONLY -> Optional.of(new ArchiveBinding(account, name, DATASOURCE, "",
                    VersioningPolicy.VERSIONING_POLICY_NONE));
            default -> Optional.empty();
        };
        DocumentArchiveGrpcService archiveGrpcService =
                new DocumentArchiveGrpcService(documentsChannel, bindings, Clock.systemUTC());
        archiveEngine = archiveGrpcService.archive();
        archiveServer = InProcessServerBuilder.forName("archive-v2-it-archive")
                .addService(archiveGrpcService)
                .addService(new DocumentArchiveMutationGrpcService(archiveEngine))
                .build().start();
        archiveChannel = InProcessChannelBuilder.forName("archive-v2-it-archive").build();
        archive = ArchiveServiceGrpc.newBlockingStub(archiveChannel);
        asyncArchive = ArchiveServiceGrpc.newStub(archiveChannel);
        mutations = ArchiveMutationServiceGrpc.newBlockingStub(archiveChannel);

        // A second archive whose v2 calls pass the gate, for races arranged step by step.
        gatedDocumentsChannel = InProcessChannelBuilder.forName("archive-v2-it").intercept(GATE).build();
        DocumentArchiveGrpcService gatedGrpcService =
                new DocumentArchiveGrpcService(gatedDocumentsChannel, bindings, Clock.systemUTC());
        gatedArchiveServer = InProcessServerBuilder.forName("archive-v2-it-gated")
                .addService(gatedGrpcService)
                .addService(new DocumentArchiveMutationGrpcService(gatedGrpcService.archive()))
                .build().start();
        gatedArchiveChannel = InProcessChannelBuilder.forName("archive-v2-it-gated").build();
        gatedArchive = ArchiveServiceGrpc.newBlockingStub(gatedArchiveChannel);
        gatedAsyncArchive = ArchiveServiceGrpc.newStub(gatedArchiveChannel);
        gatedMutations = ArchiveMutationServiceGrpc.newBlockingStub(gatedArchiveChannel);
    }

    @AfterAll
    static void shutdown() {
        gatedArchiveChannel.shutdownNow();
        gatedArchiveServer.shutdownNow();
        gatedDocumentsChannel.shutdownNow();
        archiveChannel.shutdownNow();
        archiveServer.shutdownNow();
        documentsChannel.shutdownNow();
        repo.close();
    }

    // ----------------------------------------------------------- fixtures

    static EntryAddress entry(String archiveName) {
        return EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive(archiveName)
                .setEntryId("entry-" + UUID.randomUUID()).build();
    }

    static RenditionContent rendition(String name, String subKey, String mediaType, ByteString data) {
        RenditionDescriptor.Builder descriptor = RenditionDescriptor.newBuilder().setName(name).setSubKey(subKey);
        if (!mediaType.isEmpty()) {
            descriptor.setMediaType(mediaType);
        }
        return RenditionContent.newBuilder().setRendition(descriptor).setData(data).build();
    }

    /** A PARSED fragment as v2 stores it: the entry's doc_id and parser_results. */
    static ByteString parsedFragment(EntryAddress address, String parser) {
        return Document.newBuilder()
                .setDocId(address.getEntryId())
                .putParserResults(parser, ParserResult.newBuilder().setParserName(parser).setParserVersion("1")
                        .setStatus(ParseStatus.PARSE_STATUS_OK).build())
                .build().toByteString();
    }

    static final ByteString PDF = ByteString.copyFromUtf8("%PDF-1.7 the quarterly report");
    static final ByteString PAGE = ByteString.copyFromUtf8("page one rendered");
    static final ByteString MARKDOWN = ByteString.copyFromUtf8("# Quarterly report");

    static PutEntryRequest.Builder firstPut(EntryAddress address) {
        return PutEntryRequest.newBuilder()
                .setAddress(address)
                .setTitle("Quarterly report")
                .setFilename("q3.pdf")
                .setContentType("application/pdf")
                .setSourceUri("s3://bucket/q3.pdf")
                .setSourceModifiedAt(Timestamp.newBuilder().setSeconds(1_700_000_000))
                .addRenditions(rendition("original", "", "application/pdf", PDF))
                .addRenditions(rendition("page", "0001", "text/plain", PAGE))
                .addRenditions(rendition("parsed", "", "", parsedFragment(address, "tika")))
                .putMetadata("team", "finance");
    }

    static Status.Code code(Runnable call) {
        try {
            call.run();
        } catch (StatusRuntimeException failure) {
            return failure.getStatus().getCode();
        }
        throw new AssertionError("The call succeeded");
    }

    static String description(Runnable call) {
        try {
            call.run();
        } catch (StatusRuntimeException failure) {
            return failure.getStatus().getDescription();
        }
        throw new AssertionError("The call succeeded");
    }

    static RenditionManifestEntry row(VersionManifest manifest, String name, String subKey) {
        return manifest.getRenditionsList().stream()
                .filter(row -> row.getRendition().getName().equals(name) && row.getRendition().getSubKey().equals(subKey))
                .findFirst().orElseThrow(() -> new AssertionError("no row " + name + "/" + subKey));
    }

    static int revisions(EntryAddress address) {
        try {
            return documents.listRevisions(ListRevisionsRequest.newBuilder()
                    .setAddress(ArchiveCoordinates.address(address)).setPageSize(100).build()).getRevisionsCount();
        } catch (StatusRuntimeException failure) {
            if (failure.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return 0;
            }
            throw failure;
        }
    }

    static List<ReadRenditionResponse> read(ReadRenditionRequest request) {
        List<ReadRenditionResponse> frames = new ArrayList<>();
        archive.readRendition(request).forEachRemaining(frames::add);
        return frames;
    }

    static ByteString data(List<ReadRenditionResponse> frames) {
        return ByteString.copyFrom(frames.stream().filter(ReadRenditionResponse::hasData)
                .map(ReadRenditionResponse::getData).toList());
    }

    // ---------------------------------------------------------------- tests

    @Test
    void aPutIsOneRevisionReadBackThroughEveryRead() {
        EntryAddress address = entry(RETAINED);
        PutEntryResponse put = archive.putEntry(firstPut(address).build());

        assertThat(put.getVersion()).isEqualTo(1);
        assertThat(put.getDeduplicated()).isFalse();
        assertThat(put.getReplayed()).isFalse();
        assertThat(UUID.fromString(put.getOperationId()).version()).isEqualTo(5);
        assertThat(put.getEntryUuid()).isEmpty();
        VersionManifest manifest = put.getManifest();
        assertThat(manifest.getAddress()).isEqualTo(address);
        assertThat(manifest.getRenditionsList()).extracting(r -> ArchiveCoordinates.blobId(
                r.getRendition().getName(), r.getRendition().getSubKey()))
                .containsExactly("original", "page/0001", "parsed");
        RenditionManifestEntry original = row(manifest, "original", "");
        assertThat(original.getState()).isEqualTo(RenditionState.RENDITION_STATE_PRESENT);
        assertThat(original.getSizeBytes()).isEqualTo(PDF.size());
        assertThat(original.getSha256()).isEqualTo(RenditionReads.sha256Hex(PDF));
        assertThat(original.getRendition().getMediaType()).isEqualTo("application/pdf");
        assertThat(original.getWrittenAt()).isEqualTo(manifest.getCreatedAt());
        RenditionManifestEntry parsed = row(manifest, "parsed", "");
        assertThat(parsed.getRendition().getMediaType()).isEqualTo("application/x-protobuf");
        assertThat(parsed.getState()).isEqualTo(RenditionState.RENDITION_STATE_PRESENT);
        assertThat(parsed.getWrittenAt()).isEqualTo(manifest.getCreatedAt());
        assertThat(manifest.getTotalBytes()).isEqualTo(PDF.size() + PAGE.size() + parsed.getSizeBytes());
        assertThat(manifest.getRootChecksum()).hasSize(64);
        assertThat(put.getRootChecksum()).isEqualTo(manifest.getRootChecksum());

        // The stored document is the section 1 call number with the archive's ownership.
        var stored = documents.getDocument(GetDocumentRequest.newBuilder()
                .setAddress(ArchiveCoordinates.address(address)).build());
        assertThat(stored.getManifest().getAddress().getGraphId()).isEqualTo("archive:" + ACCOUNT);
        assertThat(stored.getManifest().getAddress().getGraphAddressId()).isEqualTo(RETAINED);
        assertThat(stored.getDocument().getOwnership().getDatasourceId()).isEqualTo(DATASOURCE);
        assertThat(stored.getMetadataMap()).containsEntry(DocumentArchive.CONTENT_SHA256_METADATA,
                ArchiveOperationKeys.putEntryContentSha256(firstPut(address).build()));

        GetEntryManifestResponse inspected = archive.getEntryManifest(GetEntryManifestRequest.newBuilder()
                .setAddress(address).build());
        assertThat(inspected.getManifest()).isEqualTo(manifest);
        EntryInfo info = inspected.getInfo();
        assertThat(info.getTitle()).isEqualTo("Quarterly report");
        assertThat(info.getFilename()).isEqualTo("q3.pdf");
        assertThat(info.getContentType()).isEqualTo("application/pdf");
        assertThat(info.getSourceUri()).isEqualTo("s3://bucket/q3.pdf");
        assertThat(info.getSourceModifiedAt().getSeconds()).isEqualTo(1_700_000_000);
        assertThat(info.getCurrentVersion()).isEqualTo(1);
        assertThat(info.hasCreatedAt()).isTrue();
        assertThat(info.getMetadataMap()).containsExactly(Map.entry("team", "finance"));
        assertThat(manifest.getMetadataSnapshot()).isEqualTo(info);

        GetEntryResponse whole = archive.getEntry(GetEntryRequest.newBuilder().setAddress(address).build());
        assertThat(whole.getRenditionsList()).extracting(RenditionContent::getData)
                .containsExactly(PDF, PAGE, parsedFragment(address, "tika"));
        GetEntryResponse filtered = archive.getEntry(GetEntryRequest.newBuilder().setAddress(address)
                .addRenditions("page").build());
        assertThat(filtered.getRenditionsList()).extracting(r -> r.getRendition().getSubKey()).containsExactly("0001");

        List<ReadRenditionResponse> frames = read(ReadRenditionRequest.newBuilder().setAddress(address)
                .setRendition("original").build());
        RenditionReadHeader header = frames.getFirst().getHeader();
        assertThat(header.getVersion()).isEqualTo(1);
        assertThat(header.getWholeObject()).isTrue();
        assertThat(header.getSha256()).isEqualTo(original.getSha256());
        assertThat(data(frames)).isEqualTo(PDF);
        List<ReadRenditionResponse> range = read(ReadRenditionRequest.newBuilder().setAddress(address)
                .setRendition("parsed").setOffset(2).setLength(5).build());
        assertThat(range.getFirst().getHeader().getWholeObject()).isFalse();
        assertThat(data(range)).isEqualTo(parsedFragment(address, "tika").substring(2, 7));

        ListEntriesResponse listed = archive.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(ACCOUNT).setArchive(RETAINED).setLimit(1000).setIncludeManifests(true).build());
        int index = listed.getEntriesList().stream().map(EntryInfo::getAddress).toList().indexOf(address);
        assertThat(index).isNotNegative();
        EntryInfo listedInfo = listed.getEntries(index);
        assertThat(listedInfo.getTitle()).isEqualTo("Quarterly report");
        assertThat(listedInfo.getFilename()).isEmpty();
        assertThat(listedInfo.getMetadataMap()).containsEntry("team", "finance");
        assertThat(listed.getManifests(index)).isEqualTo(manifest);

        assertThat(archive.listVersions(ListVersionsRequest.newBuilder().setAddress(address).build())
                .getSummariesList()).containsExactly(VersionSummary.newBuilder()
                .setVersion(1).setCreatedAt(manifest.getCreatedAt()).build());
    }

    @Test
    void aLaterPutCarriesUnwrittenRenditionsForwardWithTheirOriginalStamps() {
        EntryAddress address = entry(RETAINED);
        VersionManifest first = archive.putEntry(firstPut(address).build()).getManifest();
        PutEntryResponse second = archive.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("markdown", "", "text/markdown", MARKDOWN))
                .build());

        VersionManifest manifest = second.getManifest();
        assertThat(manifest.getVersion()).isEqualTo(2);
        assertThat(manifest.getRenditionsList()).extracting(r -> ArchiveCoordinates.blobId(
                r.getRendition().getName(), r.getRendition().getSubKey()))
                .containsExactly("original", "page/0001", "markdown", "parsed");
        // Carried renditions keep the bytes and the write time of revision 1.
        assertThat(row(manifest, "original", "")).isEqualTo(row(first, "original", ""));
        assertThat(row(manifest, "page", "0001")).isEqualTo(row(first, "page", "0001"));
        assertThat(row(manifest, "parsed", "").getWrittenAt()).isEqualTo(first.getCreatedAt());
        assertThat(row(manifest, "markdown", "").getWrittenAt()).isEqualTo(manifest.getCreatedAt());
        assertThat(manifest.getCreatedAt()).isNotEqualTo(first.getCreatedAt());
        // Blank fields keep the stored CORE, and created_at is the creating save's.
        assertThat(manifest.getMetadataSnapshot().getTitle()).isEqualTo("Quarterly report");
        assertThat(manifest.getMetadataSnapshot().getFilename()).isEqualTo("q3.pdf");
        assertThat(manifest.getMetadataSnapshot().getCreatedAt())
                .isEqualTo(first.getMetadataSnapshot().getCreatedAt());

        // Version 1 stays readable with the metadata it was saved with.
        GetEntryManifestResponse old = archive.getEntryManifest(GetEntryManifestRequest.newBuilder()
                .setAddress(address).setVersion(1).build());
        assertThat(old.getManifest()).isEqualTo(first);
        assertThat(old.getInfo().getCurrentVersion()).isEqualTo(2);
        assertThat(archive.listVersions(ListVersionsRequest.newBuilder().setAddress(address).build())
                .getSummariesList()).extracting(VersionSummary::getVersion).containsExactly(2L, 1L);
    }

    @Test
    void aParsedFragmentWithoutTheEntrysDocIdIsInvalid() {
        EntryAddress address = entry(RETAINED);
        ByteString anonymous = Document.newBuilder()
                .putParserResults("tika", ParserResult.newBuilder().setParserName("tika").setParserVersion("1")
                        .setStatus(ParseStatus.PARSE_STATUS_OK).build())
                .build().toByteString();
        PutEntryRequest request = PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(rendition("parsed", "", "", anonymous)).build();
        assertThat(code(() -> archive.putEntry(request))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(description(() -> archive.putEntry(request))).contains("renditions[parsed]", "doc_id");
        assertThat(revisions(address)).isZero();
    }

    @Test
    void anUnchangedPutUnderANewIdIsDeduplicatedAndSavesNothing() {
        EntryAddress address = entry(RETAINED);
        PutEntryResponse first = archive.putEntry(firstPut(address).setOperationId("dedupe-1-" + address.getEntryId()).build());
        PutEntryResponse again = archive.putEntry(firstPut(address).setOperationId("dedupe-2-" + address.getEntryId()).build());
        assertThat(again.getDeduplicated()).isTrue();
        assertThat(again.getReplayed()).isFalse();
        // The deduplicated answer is a recorded v2 outcome bound to revision 1.
        var recorded = documents.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(ACCOUNT).setOperationId(again.getOperationId()).build()).getCommitted();
        assertThat(recorded.getUnchanged().getRevision()).isEqualTo(1);
        assertThat(recorded.getRequestMetadataMap()).containsKey(DocumentArchive.CONTENT_SHA256_METADATA);
        assertThat(again.getVersion()).isEqualTo(first.getVersion());
        assertThat(again.getManifest()).isEqualTo(first.getManifest());
        assertThat(revisions(address)).isEqualTo(1);

        PutEntryResponse changed = archive.putEntry(firstPut(address).setTitle("Renamed")
                .setOperationId("dedupe-3-" + address.getEntryId()).build());
        assertThat(changed.getDeduplicated()).isFalse();
        assertThat(changed.getVersion()).isEqualTo(2);
    }

    /**
     * Writer A saves revision 1 and loses its response; writer B saves revision 2.
     * With a supplied operation ID, A's retry replays revision 1 and leaves
     * revision 2 current. With a derived ID and last write wins, the retry is
     * indistinguishable from writing A's content again, so it is refused
     * FAILED_PRECONDITION naming both versions, and naming the current version as
     * expected_version writes it as revision 3.
     */
    @Test
    void aLostResponseRetryAfterAnotherWriterReplaysOnlyUnderASuppliedId() {
        for (boolean supplied : new boolean[] {false, true}) {
            EntryAddress address = entry(RETAINED);
            PutEntryRequest a = supplied
                    ? firstPut(address).setOperationId("writer-a-" + address.getEntryId()).build()
                    : firstPut(address).build();
            PutEntryResponse landed = archive.putEntry(a);
            assertThat(landed.getVersion()).isEqualTo(1);
            PutEntryResponse b = archive.putEntry(PutEntryRequest.newBuilder().setAddress(address)
                    .setTitle("Writer B").build());
            assertThat(b.getVersion()).isEqualTo(2);

            if (!supplied) {
                assertSuperseded(() -> archive.putEntry(a), landed.getOperationId(), 1, "version 2", 2);
                assertThat(revisions(address)).isEqualTo(2);
                PutEntryResponse rewritten = archive.putEntry(a.toBuilder().setExpectedVersion(2).build());
                assertThat(rewritten.getReplayed()).isFalse();
                assertThat(rewritten.getVersion()).isEqualTo(3);
                assertThat(archive.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(address).build())
                        .getInfo().getTitle()).isEqualTo(a.getTitle());
                continue;
            }
            PutEntryResponse retry = archive.putEntry(a);
            assertThat(retry.getReplayed()).isTrue();
            assertThat(retry.getVersion()).isEqualTo(1);
            assertThat(retry.getOperationId()).isEqualTo(landed.getOperationId());
            assertThat(retry.getManifest()).isEqualTo(landed.getManifest());
            assertThat(revisions(address)).isEqualTo(2);
            assertThat(archive.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(address).build())
                    .getInfo().getTitle()).isEqualTo("Writer B");
        }
    }

    /**
     * Codex #15035: writer A's put matches revision 1 and is deduplicated; its
     * response is lost; writer B lands revision 2; A's retry must never save A's
     * content again as revision 3. Under a supplied ID it answers revision 1 with
     * replayed set; under a derived ID it is FAILED_PRECONDITION naming both
     * versions. Either way revision 2 stays current.
     */
    @Test
    void aDeduplicatedPutRetriedAfterAnotherWriterNeverSavesAgain() {
        for (boolean supplied : new boolean[] {false, true}) {
            EntryAddress address = entry(RETAINED);
            archive.putEntry(firstPut(address).setOperationId("first-writer-" + address.getEntryId()).build());
            PutEntryRequest a = supplied
                    ? firstPut(address).setOperationId("writer-a-" + address.getEntryId()).build()
                    : firstPut(address).build();
            PutEntryResponse deduplicated = archive.putEntry(a);
            assertThat(deduplicated.getDeduplicated()).isTrue();
            assertThat(deduplicated.getVersion()).isEqualTo(1);

            PutEntryResponse b = archive.putEntry(PutEntryRequest.newBuilder().setAddress(address)
                    .setTitle("Writer B").build());
            assertThat(b.getVersion()).isEqualTo(2);

            if (!supplied) {
                assertSuperseded(() -> archive.putEntry(a), deduplicated.getOperationId(), 1, "version 2", 2);
                assertThat(revisions(address)).isEqualTo(2);
                assertThat(archive.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(address).build())
                        .getInfo().getTitle()).isEqualTo("Writer B");
                continue;
            }
            PutEntryResponse retry = archive.putEntry(a);
            assertThat(retry.getReplayed()).isTrue();
            assertThat(retry.getDeduplicated()).isTrue();
            assertThat(retry.getVersion()).isEqualTo(1);
            assertThat(retry.getOperationId()).isEqualTo(deduplicated.getOperationId());
            assertThat(retry.getManifest()).isEqualTo(deduplicated.getManifest());
            assertThat(revisions(address)).isEqualTo(2);
            assertThat(archive.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(address).build())
                    .getInfo().getTitle()).isEqualTo("Writer B");
            if (supplied) {
                // The unchanged outcome binds the supplied ID to A's content.
                PutEntryRequest reuse = a.toBuilder().setTitle("Writer A again").build();
                assertThat(code(() -> archive.putEntry(reuse))).isEqualTo(Status.Code.ALREADY_EXISTS);
                assertThat(revisions(address)).isEqualTo(2);
            }
        }
    }

    /**
     * A derived last-write-wins write of content the entry held before, after a
     * deletion replaced it, is refused naming the deletion; naming the
     * tombstone's version as expected_version writes the content again.
     */
    @Test
    void aDerivedRewriteAfterADeletionNamesTheDeletion() {
        EntryAddress address = entry(RETAINED);
        PutEntryRequest a = firstPut(address).build();
        PutEntryResponse landed = archive.putEntry(a);
        documents.deleteDocument(DeleteDocumentRequest.newBuilder().setAddress(ArchiveCoordinates.address(address))
                .setExpected(ExpectedRevision.newBuilder().setRevision(1)).setOperationId("delete-" + address.getEntryId())
                .build());
        assertSuperseded(() -> archive.putEntry(a), landed.getOperationId(), 1, "a deletion at version 2", 2);
        assertThat(archive.putEntry(a.toBuilder().setExpectedVersion(2).build()).getVersion()).isEqualTo(3);
    }

    /**
     * Last write wins onto a deleted entry: expected_version 0 asks for no check,
     * so a put or an upload of new content saves over the deletion's revision
     * rather than being ABORTED by the tombstone v2 keeps as current.
     */
    @Test
    void lastWriteWinsOntoADeletedEntrySavesOverTheDeletion() throws Exception {
        EntryAddress address = entry(RETAINED);
        archive.putEntry(firstPut(address).build());
        delete(address, 1);
        PutEntryResponse restated = archive.putEntry(firstPut(address).setTitle("Quarterly report, restated").build());
        assertThat(restated.getReplayed()).isFalse();
        assertThat(restated.getVersion()).isEqualTo(3);
        assertThat(archive.getEntry(GetEntryRequest.newBuilder().setAddress(address).build()).getInfo().getTitle())
                .isEqualTo("Quarterly report, restated");

        delete(address, 3);
        ByteString scan = randomBytes(5_000, 21);
        UploadRenditionResponse uploaded = upload(UploadRenditionHeader.newBuilder().setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("scan").setSubKey("0001").setMediaType("image/tiff"))
                .setSizeBytes(scan.size()).setExpectedSha256(RenditionReads.sha256Hex(scan)).build(), scan, 1_000);
        assertThat(uploaded.getReplayed()).isFalse();
        assertThat(uploaded.getVersion()).isEqualTo(5);
        VersionManifest current = archive.getEntryManifest(GetEntryManifestRequest.newBuilder()
                .setAddress(address).build()).getManifest();
        assertThat(row(current, "scan", "0001").getSha256()).isEqualTo(RenditionReads.sha256Hex(scan));
    }

    private void delete(EntryAddress address, long revision) {
        documents.deleteDocument(DeleteDocumentRequest.newBuilder().setAddress(ArchiveCoordinates.address(address))
                .setExpected(ExpectedRevision.newBuilder().setRevision(revision))
                .setOperationId("delete-" + address.getEntryId() + "-" + revision).build());
    }

    /** The FAILED_PRECONDITION of a derived ID whose recorded version was replaced, naming the remedy. */
    static void assertSuperseded(ThrowingCallable call, String operationId, long recorded, String replacedBy,
            long current) {
        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class, call);
        assertThat(failure).as("the call is refused").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(failure.getStatus().getDescription()).isEqualTo("operation_id " + operationId
                + " was derived from this content, which was written as version " + recorded
                + " and has since been replaced by " + replacedBy + ". To write it again, send expected_version = "
                + current + " or a new operation_id.");
    }

    @Test
    void aRetryWithNoOtherWriterReplays() {
        EntryAddress address = entry(RETAINED);
        PutEntryResponse landed = archive.putEntry(firstPut(address).build());
        PutEntryResponse retry = archive.putEntry(firstPut(address).build());
        assertThat(retry.getReplayed()).isTrue();
        assertThat(retry.getDeduplicated()).isFalse();
        assertThat(retry.getVersion()).isEqualTo(landed.getVersion());
        assertThat(revisions(address)).isEqualTo(1);
    }

    /**
     * Concurrent attempts of one operation: one commits, and every other attempt
     * either finds its outcome first or loses the race and replays the winner.
     * None fails and none saves a second revision.
     */
    @Test
    void concurrentAttemptsOfOneOperationAllAnswerWithTheWinner() throws Exception {
        EntryAddress address = entry(RETAINED);
        PutEntryRequest request = firstPut(address).build();
        var pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        try {
            var start = new java.util.concurrent.CountDownLatch(1);
            List<java.util.concurrent.Future<PutEntryResponse>> attempts = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                attempts.add(pool.submit(() -> {
                    start.await();
                    return archive.putEntry(request);
                }));
            }
            start.countDown();
            List<PutEntryResponse> answers = new ArrayList<>();
            for (var attempt : attempts) {
                answers.add(attempt.get(60, TimeUnit.SECONDS));
            }
            assertThat(answers).extracting(PutEntryResponse::getVersion).containsOnly(1L);
            assertThat(answers).extracting(PutEntryResponse::getReplayed).containsOnlyOnce(false);
            assertThat(answers).extracting(PutEntryResponse::getManifest).containsOnly(answers.getFirst().getManifest());
            assertThat(revisions(address)).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------- arranged races

    /**
     * Holds the next v2 call of one method until released, so a test can commit
     * other writes between an archive write's outcome lookup and its save.
     */
    static final class Gate implements ClientInterceptor {
        private final AtomicReference<String> armed = new AtomicReference<>();
        private volatile CountDownLatch arrived = new CountDownLatch(0);
        private volatile CountDownLatch release = new CountDownLatch(0);

        void arm(MethodDescriptor<?, ?> method) {
            arrived = new CountDownLatch(1);
            release = new CountDownLatch(1);
            armed.set(method.getFullMethodName());
        }

        void awaitArrival() throws InterruptedException {
            assertThat(arrived.await(30, TimeUnit.SECONDS)).as("the gated v2 call arrives").isTrue();
        }

        void release() {
            release.countDown();
        }

        @Override
        public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
            if (armed.compareAndSet(method.getFullMethodName(), null)) {
                arrived.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return next.newCall(method, options);
        }
    }

    /** The result of a background call, its gRPC failure thrown as itself. */
    static <T> T result(Future<T> call) throws Exception {
        try {
            return call.get(60, TimeUnit.SECONDS);
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof StatusRuntimeException status) {
                throw status;
            }
            throw failure;
        }
    }

    /**
     * Codex #15554, PutEntry: attempt A1 of a derived operation finds no outcome,
     * reads an empty entry and is held before its save. Writer X commits version
     * 1, attempt A2 of the same operation reads it and commits version 2, and
     * writer B commits version 3. A1's save then fails against A2's recorded
     * intent, and its lost-race lookup finds version 2, which B has replaced:
     * A1 is refused as superseded instead of replaying version 2. The same race
     * with a supplied operation ID still replays.
     */
    @Test
    void aLostRaceOfASupersededDerivedPutIsRefused() throws Exception {
        assertLostRace(firstPut(entry(RETAINED)).build());
    }

    @Test
    void aLostRaceOfASuppliedIdPutStillReplays() throws Exception {
        EntryAddress address = entry(RETAINED);
        PutEntryResponse answer = assertLostRace(firstPut(address).setOperationId("raced-" + address.getEntryId()).build());
        assertThat(answer.getReplayed()).isTrue();
        assertThat(answer.getVersion()).isEqualTo(2);
    }

    /** Runs the race above for {@code a}: the derived ID is refused, a supplied ID's answer is returned. */
    private PutEntryResponse assertLostRace(PutEntryRequest a) throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            GATE.arm(DocumentServiceGrpc.getSaveDocumentMethod());
            Future<PutEntryResponse> a1 = pool.submit(() -> gatedArchive.putEntry(a));
            GATE.awaitArrival();
            archive.putEntry(a.toBuilder().clearOperationId().setTitle("Writer X").build());
            PutEntryResponse a2 = archive.putEntry(a);
            assertThat(a2.getVersion()).isEqualTo(2);
            assertThat(a2.getReplayed()).isFalse();
            archive.putEntry(a.toBuilder().clearOperationId().setTitle("Writer B").build());
            GATE.release();
            if (!a.getOperationId().isEmpty()) {
                return result(a1);
            }
            assertSuperseded(() -> result(a1), a2.getOperationId(), 2, "version 3", 3);
            assertThat(revisions(a.getAddress())).isEqualTo(3);
            return null;
        } finally {
            GATE.release();
            pool.shutdownNow();
        }
    }

    /**
     * The PutEntry race where A1 and A2 send the same save: both read version 1,
     * A2 commits version 2, and B replaces it with version 3. v2 answers A1's
     * identical save with a replay of version 2 rather than a failure, and a
     * derived ID is refused as superseded there too.
     */
    @Test
    void aReplayedRaceOfASupersededDerivedPutIsRefused() throws Exception {
        EntryAddress address = entry(RETAINED);
        PutEntryRequest a = firstPut(address).build();
        archive.putEntry(a.toBuilder().setTitle("Writer X").build());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            GATE.arm(DocumentServiceGrpc.getSaveDocumentMethod());
            Future<PutEntryResponse> a1 = pool.submit(() -> gatedArchive.putEntry(a));
            GATE.awaitArrival();
            PutEntryResponse a2 = archive.putEntry(a);
            assertThat(a2.getVersion()).isEqualTo(2);
            archive.putEntry(a.toBuilder().setTitle("Writer B").build());
            GATE.release();
            assertSuperseded(() -> result(a1), a2.getOperationId(), 2, "version 3", 3);
            assertThat(revisions(address)).isEqualTo(3);
        } finally {
            GATE.release();
            pool.shutdownNow();
        }
    }

    /**
     * Codex #15554, UploadRendition: the races above for a derived upload ID. With
     * an empty entry, A1's save fails against A2's recorded intent and takes the
     * lost-race lookup; with writer X first, A1 and A2 send the same save and v2
     * replays A2's version. Both are refused once B has replaced that version.
     */
    @Test
    void aLostRaceOfASupersededDerivedUploadIsRefused() throws Exception {
        assertUploadRace(false);
    }

    @Test
    void aReplayedRaceOfASupersededDerivedUploadIsRefused() throws Exception {
        assertUploadRace(true);
    }

    private void assertUploadRace(boolean writerXFirst) throws Exception {
        EntryAddress address = entry(RETAINED);
        ByteString a = randomBytes(5_000, 41);
        ByteString x = randomBytes(5_000, 42);
        ByteString b = randomBytes(5_000, 43);
        UploadRenditionHeader headerA = UploadRenditionHeader.newBuilder().setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("scan").setSubKey("0001").setMediaType("image/tiff"))
                .setSizeBytes(a.size()).setExpectedSha256(RenditionReads.sha256Hex(a)).build();
        UploadRenditionHeader headerX = headerA.toBuilder().setExpectedSha256(RenditionReads.sha256Hex(x)).build();
        if (writerXFirst) {
            upload(headerX, x, 1_000);
        }
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            GATE.arm(DocumentServiceGrpc.getUploadDocumentMethod());
            Future<UploadRenditionResponse> a1 = pool.submit(() -> upload(gatedAsyncArchive, headerA, a, 1_000));
            GATE.awaitArrival();
            if (!writerXFirst) {
                upload(headerX, x, 1_000);
            }
            UploadRenditionResponse a2 = upload(headerA, a, 1_000);
            assertThat(a2.getVersion()).isEqualTo(2);
            assertThat(a2.getReplayed()).isFalse();
            upload(headerA.toBuilder().setExpectedSha256(RenditionReads.sha256Hex(b)).build(), b, 1_000);
            GATE.release();
            assertSuperseded(() -> result(a1), a2.getOperationId(), 2, "version 3", 3);
            assertThat(revisions(address)).isEqualTo(3);
        } finally {
            GATE.release();
            pool.shutdownNow();
        }
    }

    @Test
    void aSuppliedIdReusedWithOtherContentIsAlreadyExists() {
        EntryAddress address = entry(RETAINED);
        String id = "reuse-" + address.getEntryId();
        archive.putEntry(firstPut(address).setOperationId(id).build());
        PutEntryRequest other = firstPut(address).setOperationId(id).setTitle("Different").build();
        assertThat(code(() -> archive.putEntry(other))).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(description(() -> archive.putEntry(other))).contains("operation_id");
        EntryAddress elsewhere = entry(RETAINED);
        PutEntryRequest otherEntry = firstPut(elsewhere).setOperationId(id).build();
        assertThat(code(() -> archive.putEntry(otherEntry))).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(revisions(address)).isEqualTo(1);
    }

    @Test
    void aStaleExpectedVersionIsAbortedAndSavesNothing() {
        EntryAddress address = entry(RETAINED);
        archive.putEntry(firstPut(address).build());
        archive.putEntry(PutEntryRequest.newBuilder().setAddress(address).setTitle("v2").build());
        PutEntryRequest stale = PutEntryRequest.newBuilder().setAddress(address).setTitle("v3")
                .setExpectedVersion(1).build();
        assertThat(code(() -> archive.putEntry(stale))).isEqualTo(Status.Code.ABORTED);
        assertThat(revisions(address)).isEqualTo(2);
        PutEntryResponse current = archive.putEntry(stale.toBuilder().setExpectedVersion(2).build());
        assertThat(current.getVersion()).isEqualTo(3);
    }

    @Test
    void versioningNoneServesTheCurrentVersionOnly() {
        EntryAddress address = entry(CURRENT_ONLY);
        archive.putEntry(firstPut(address).build());
        archive.putEntry(PutEntryRequest.newBuilder().setAddress(address).setTitle("second").build());
        assertThat(revisions(address)).isEqualTo(2);
        assertThat(archive.listVersions(ListVersionsRequest.newBuilder().setAddress(address).build())
                .getSummariesList()).extracting(VersionSummary::getVersion).containsExactly(2L);
        GetEntryManifestRequest old = GetEntryManifestRequest.newBuilder().setAddress(address).setVersion(1).build();
        assertThat(code(() -> archive.getEntryManifest(old))).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(description(() -> archive.getEntryManifest(old))).contains("VERSIONING_POLICY_NONE");
        assertThat(code(() -> read(ReadRenditionRequest.newBuilder().setAddress(address).setVersion(1)
                .setRendition("original").build()))).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(archive.getEntryManifest(old.toBuilder().setVersion(2).build()).getManifest().getVersion())
                .isEqualTo(2);
    }

    @Test
    void theArchiveContractRefusesByName() {
        EntryAddress address = entry(RETAINED);
        PutEntryRequest noOriginal = PutEntryRequest.newBuilder().setAddress(address).setFilename("x.pdf").build();
        assertThat(code(() -> archive.putEntry(noOriginal))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(description(() -> archive.putEntry(noOriginal))).startsWith("filename");

        PutEntryRequest subKeyed = PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(rendition("parsed", "p1", "", parsedFragment(address, "tika"))).build();
        assertThat(description(() -> archive.putEntry(subKeyed))).contains("rendition.sub_key");

        ByteString notParsedOnly = Document.newBuilder().setDocId(address.getEntryId())
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("smuggled"))
                .putParserResults("tika", ParserResult.newBuilder().setParserName("tika").build())
                .build().toByteString();
        PutEntryRequest smuggled = PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(rendition("parsed", "", "", notParsedOnly)).build();
        assertThat(code(() -> archive.putEntry(smuggled))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(description(() -> archive.putEntry(smuggled))).contains("renditions[parsed]");

        ByteString otherEntry = Document.newBuilder().setDocId("another-entry")
                .putParserResults("tika", ParserResult.newBuilder().setParserName("tika").build())
                .build().toByteString();
        PutEntryRequest misaddressed = PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(rendition("parsed", "", "", otherEntry)).build();
        assertThat(description(() -> archive.putEntry(misaddressed))).contains("another-entry");

        PutEntryRequest twice = PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(rendition("page", "1", "text/plain", PAGE))
                .addRenditions(rendition("page", "1", "text/plain", PAGE)).build();
        assertThat(description(() -> archive.putEntry(twice))).contains("written twice");

        PutEntryRequest badMime = PutEntryRequest.newBuilder().setAddress(address).setContentType("pdf").build();
        assertThat(code(() -> archive.putEntry(badMime))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(description(() -> archive.putEntry(badMime))).contains("content_type");

        EntryAddress unknown = address.toBuilder().setArchive("no-such-archive").build();
        assertThat(code(() -> archive.putEntry(PutEntryRequest.newBuilder().setAddress(unknown).build())))
                .isEqualTo(Status.Code.NOT_FOUND);

        assertThat(code(() -> archive.listEntries(ListEntriesRequest.newBuilder().setAccountId(ACCOUNT)
                .setArchive(RETAINED).setClassificationState(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED)
                .build()))).isEqualTo(Status.Code.UNIMPLEMENTED);
        assertThat(code(() -> archive.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(ACCOUNT).setArchive("no-such-archive").build()))).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(revisions(address)).isZero();
    }

    @Test
    void readsOfAMissingRenditionOrAnOutsideRangeAreRefused() {
        EntryAddress address = entry(RETAINED);
        archive.putEntry(firstPut(address).build());
        assertThat(code(() -> read(ReadRenditionRequest.newBuilder().setAddress(address)
                .setRendition("thumbnail").build()))).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(code(() -> read(ReadRenditionRequest.newBuilder().setAddress(address)
                .setRendition("original").setOffset(PDF.size() + 1).build()))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(code(() -> read(ReadRenditionRequest.newBuilder().setAddress(address)
                .setRendition("original").setOffset(1).setLength(PDF.size()).build())))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        // A range that ends exactly at the object's end is served.
        assertThat(data(read(ReadRenditionRequest.newBuilder().setAddress(address)
                .setRendition("original").setOffset(1).setLength(PDF.size() - 1).build())))
                .isEqualTo(PDF.substring(1));
    }

    // ------------------------------------------------------------- uploads

    static UploadRenditionResponse upload(UploadRenditionHeader header, ByteString bytes, int chunk) throws Exception {
        return upload(asyncArchive, header, bytes, chunk);
    }

    static UploadRenditionResponse upload(ArchiveServiceGrpc.ArchiveServiceStub stub, UploadRenditionHeader header,
            ByteString bytes, int chunk) throws Exception {
        var result = new CompletableFuture<UploadRenditionResponse>();
        StreamObserver<UploadRenditionRequest> requests = stub.uploadRendition(new StreamObserver<>() {
            @Override
            public void onNext(UploadRenditionResponse value) {
                result.complete(value);
            }

            @Override
            public void onError(Throwable failure) {
                result.completeExceptionally(failure);
            }

            @Override
            public void onCompleted() {
                result.completeExceptionally(new AssertionError("completed with no response"));
            }
        });
        requests.onNext(UploadRenditionRequest.newBuilder().setHeader(header).build());
        for (int start = 0; start < bytes.size(); start += chunk) {
            requests.onNext(UploadRenditionRequest.newBuilder()
                    .setChunk(bytes.substring(start, Math.min(bytes.size(), start + chunk))).build());
        }
        requests.onCompleted();
        try {
            return result.get(60, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException failure) {
            if (failure.getCause() instanceof StatusRuntimeException status) {
                throw status;
            }
            throw failure;
        }
    }

    static ByteString randomBytes(int size, long seed) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return ByteString.copyFrom(bytes);
    }

    @Test
    void anUploadStreamsOneRenditionBesideTheCarriedOnesAndReplaysOnRetry() throws Exception {
        EntryAddress address = entry(RETAINED);
        VersionManifest first = archive.putEntry(firstPut(address).build()).getManifest();
        ByteString scan = randomBytes(300_000, 7);
        UploadRenditionHeader header = UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("scan").setSubKey("0001")
                        .setMediaType("image/tiff"))
                .setSizeBytes(scan.size())
                .setExpectedSha256(RenditionReads.sha256Hex(scan))
                .build();

        UploadRenditionResponse uploaded = upload(header, scan, 40_000);
        assertThat(uploaded.getVersion()).isEqualTo(2);
        assertThat(uploaded.getSizeBytes()).isEqualTo(scan.size());
        assertThat(uploaded.getSha256()).isEqualTo(RenditionReads.sha256Hex(scan));
        assertThat(uploaded.getReplayed()).isFalse();
        assertThat(uploaded.getObjectKey()).isEmpty();
        assertThat(UUID.fromString(uploaded.getOperationId()).version()).isEqualTo(5);

        VersionManifest manifest = archive.getEntryManifest(GetEntryManifestRequest.newBuilder()
                .setAddress(address).build()).getManifest();
        assertThat(manifest.getRootChecksum()).isEqualTo(uploaded.getRootChecksum());
        assertThat(row(manifest, "original", "")).isEqualTo(row(first, "original", ""));
        assertThat(row(manifest, "scan", "0001").getRendition().getMediaType()).isEqualTo("image/tiff");

        List<ReadRenditionResponse> frames = read(ReadRenditionRequest.newBuilder().setAddress(address)
                .setRendition("scan").setSubKey("0001").build());
        assertThat(frames.subList(1, frames.size())).allSatisfy(frame ->
                assertThat(frame.getData().size()).isBetween(1, RenditionReads.MAX_FRAME_BYTES));
        assertThat(frames.size()).isGreaterThan(5);
        assertThat(data(frames)).isEqualTo(scan);

        UploadRenditionResponse retry = upload(header, scan, 40_000);
        assertThat(retry.getReplayed()).isTrue();
        assertThat(retry.getVersion()).isEqualTo(2);
        assertThat(retry.getOperationId()).isEqualTo(uploaded.getOperationId());
        assertThat(revisions(address)).isEqualTo(2);
    }

    /**
     * Codex #15035: a recorded rejection is returned only to a retry of the same
     * archive request. Reusing the supplied operation ID with other content is
     * ALREADY_EXISTS, not the earlier rejection.
     */
    @Test
    void aRecordedRejectionIsReplayedOnlyForTheSameContent() throws Exception {
        EntryAddress address = entry(RETAINED);
        ByteString bytes = randomBytes(5_000, 13);
        UploadRenditionHeader mismatched = UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("original").setMediaType("application/pdf"))
                .setSizeBytes(bytes.size())
                .setExpectedSha256("0".repeat(64))
                .setOperationId("rejected-" + address.getEntryId())
                .build();
        StatusRuntimeException first = catchThrowableOfType(StatusRuntimeException.class,
                () -> upload(mismatched, bytes, 1024));
        assertThat(first.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(documents.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(ACCOUNT).setOperationId(mismatched.getOperationId()).build())
                .getCommitted().hasRejection()).isTrue();

        StatusRuntimeException retry = catchThrowableOfType(StatusRuntimeException.class,
                () -> upload(mismatched, bytes, 1024));
        assertThat(retry.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(StatusProto.fromThrowable(retry).getDetailsList())
                .anySatisfy(detail -> assertThat(detail.is(SaveRejection.class)).isTrue());

        UploadRenditionHeader corrected = mismatched.toBuilder()
                .setExpectedSha256(RenditionReads.sha256Hex(bytes)).build();
        StatusRuntimeException reuse = catchThrowableOfType(StatusRuntimeException.class,
                () -> upload(corrected, bytes, 1024));
        assertThat(reuse.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(reuse.getStatus().getDescription()).contains("operation_id");

        PutEntryRequest put = PutEntryRequest.newBuilder().setAddress(address)
                .setOperationId(mismatched.getOperationId()).setTitle("other request").build();
        assertThat(code(() -> archive.putEntry(put))).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(revisions(address)).isZero();
    }

    @Test
    void anUploadCanCreateTheEntryAndWriteParsed() throws Exception {
        EntryAddress address = entry(RETAINED);
        ByteString original = randomBytes(10_000, 11);
        UploadRenditionResponse created = upload(UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("original").setMediaType("application/pdf"))
                .setSizeBytes(original.size())
                .setOperationId("create-" + address.getEntryId())
                .build(), original, 4096);
        assertThat(created.getVersion()).isEqualTo(1);
        ByteString parsed = parsedFragment(address, "grparse");
        UploadRenditionResponse parsedUpload = upload(UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("parsed"))
                .setSizeBytes(parsed.size())
                .setExpectedSha256(RenditionReads.sha256Hex(parsed))
                .build(), parsed, 7);
        assertThat(parsedUpload.getVersion()).isEqualTo(2);
        GetEntryResponse entry = archive.getEntry(GetEntryRequest.newBuilder().setAddress(address).build());
        assertThat(entry.getRenditionsList()).extracting(RenditionContent::getData).containsExactly(original, parsed);
        assertThat(entry.getInfo().hasCreatedAt()).isTrue();
    }

    /**
     * An upload's derived ID holds its header's expected_version: re-uploading
     * bytes the rendition held before is refused naming both versions under last
     * write wins, lands when the header names the current version, and a stale
     * expected_version is ABORTED.
     */
    @Test
    void anUploadRewriteNamesTheCurrentVersion() throws Exception {
        EntryAddress address = entry(RETAINED);
        ByteString x = randomBytes(5_000, 11);
        ByteString y = randomBytes(5_000, 12);
        UploadRenditionHeader headerX = UploadRenditionHeader.newBuilder().setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("scan").setSubKey("0001").setMediaType("image/tiff"))
                .setSizeBytes(x.size()).setExpectedSha256(RenditionReads.sha256Hex(x)).build();
        UploadRenditionHeader headerY = headerX.toBuilder().setExpectedSha256(RenditionReads.sha256Hex(y)).build();
        UploadRenditionResponse first = upload(headerX, x, 1_000);
        assertThat(upload(headerY, y, 1_000).getVersion()).isEqualTo(2);

        assertSuperseded(() -> upload(headerX, x, 1_000), first.getOperationId(), 1, "version 2", 2);
        assertThat(code(() -> {
            try {
                upload(headerX.toBuilder().setExpectedVersion(1).build(), x, 1_000);
            } catch (Exception failure) {
                throw failure instanceof StatusRuntimeException status ? status : new IllegalStateException(failure);
            }
        })).isEqualTo(Status.Code.ABORTED);
        UploadRenditionResponse rewritten = upload(headerX.toBuilder().setExpectedVersion(2).build(), x, 1_000);
        assertThat(rewritten.getReplayed()).isFalse();
        assertThat(rewritten.getVersion()).isEqualTo(3);
        assertThat(rewritten.getOperationId()).isNotEqualTo(first.getOperationId());
        VersionManifest current = archive.getEntryManifest(GetEntryManifestRequest.newBuilder()
                .setAddress(address).build()).getManifest();
        assertThat(row(current, "scan", "0001").getSha256()).isEqualTo(RenditionReads.sha256Hex(x));
    }

    /**
     * A supplied operation ID with no expected_sha256 has a content digest that
     * covers no byte: a retry of the same bytes replays, and other bytes of the
     * same length are ALREADY_EXISTS rather than a replay of the first upload.
     */
    @Test
    void aSuppliedIdUploadWithoutADigestReplaysOnlyTheSameBytes() throws Exception {
        EntryAddress address = entry(RETAINED);
        ByteString x = randomBytes(5_000, 31);
        ByteString y = randomBytes(5_000, 32);
        UploadRenditionHeader header = UploadRenditionHeader.newBuilder().setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("scan").setSubKey("0001").setMediaType("image/tiff"))
                .setSizeBytes(x.size()).setOperationId("scan-" + address.getEntryId()).build();
        UploadRenditionResponse first = upload(header, x, 1_000);
        assertThat(first.getReplayed()).isFalse();
        assertThat(first.getVersion()).isEqualTo(1);

        UploadRenditionResponse retry = upload(header, x, 1_000);
        assertThat(retry.getReplayed()).isTrue();
        assertThat(retry.getVersion()).isEqualTo(1);
        assertThat(retry.getSha256()).isEqualTo(RenditionReads.sha256Hex(x));

        StatusRuntimeException other = catchThrowableOfType(StatusRuntimeException.class, () -> upload(header, y, 1_000));
        assertThat(other).as("other bytes are refused").isNotNull();
        assertThat(other.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(other.getStatus().getDescription()).isEqualTo("operation_id: " + header.getOperationId()
                + " is recorded for an upload of other bytes (SHA-256 " + RenditionReads.sha256Hex(x) + ", 5000 bytes)");
        assertThat(revisions(address)).isEqualTo(1);
    }

    @Test
    void anUploadWithoutIdOrDigestAndAWrongLengthAreRefused() {
        EntryAddress address = entry(RETAINED);
        ByteString bytes = ByteString.copyFromUtf8("twelve bytes");
        UploadRenditionHeader noIdentity = UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setSizeBytes(bytes.size())
                .build();
        assertThatThrownBy(() -> upload(noIdentity, bytes, 4))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(failure.getStatus().getDescription()).contains("operation_id, expected_sha256");
                });
        UploadRenditionHeader wrongLength = noIdentity.toBuilder().setOperationId("short-" + address.getEntryId())
                .setSizeBytes(bytes.size() + 5).build();
        assertThatThrownBy(() -> upload(wrongLength, bytes, 4))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(revisions(address)).isZero();
    }

    @Test
    void theEntryFactsAreTypedStructuredDataKeptUntilReplaced() throws Exception {
        EntryAddress address = entry(RETAINED);
        ObjectStoreOrigin origin = ObjectStoreOrigin.newBuilder().setBucket("intake-bucket")
                .setObjectKey("2026/q3.pdf").build();
        archive.putEntry(firstPut(address).setOrigin(origin).build());
        archive.putEntry(PutEntryRequest.newBuilder().setAddress(address).setTitle("retitled").build());
        var stored = documents.getDocument(GetDocumentRequest.newBuilder()
                .setAddress(ArchiveCoordinates.address(address)).build());
        assertThat(stored.getManifest().getRevision()).isEqualTo(2);
        assertThat(stored.getDocument().getStructuredData().unpack(ArchiveEntryFacts.class))
                .isEqualTo(ArchiveEntryFacts.newBuilder().setOrigin(origin).build());

        PutEntryRequest partial = PutEntryRequest.newBuilder().setAddress(address)
                .setOrigin(ObjectStoreOrigin.newBuilder().setBucket("intake-bucket")).build();
        assertThat(code(() -> archive.putEntry(partial))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(description(() -> archive.putEntry(partial))).contains("origin.object_key");
        assertThat(revisions(address)).isEqualTo(2);
    }

    @Test
    void unaryGetEntryEnforcesMaxByteBudgetAcrossSelectedRenditions() {
        EntryAddress address = entry(RETAINED);
        archive.putEntry(firstPut(address).build());
        long smallBudget = PDF.size() + PAGE.size();
        DocumentArchive bounded = new DocumentArchive(
                documentsChannel,
                (account, name) -> Optional.of(new ArchiveBinding(
                        account, name, DATASOURCE, "", VersioningPolicy.VERSIONING_POLICY_RETAINED)),
                Clock.systemUTC(),
                smallBudget);

        // Selecting renditions within the budget succeeds.
        GetEntryResponse withinBudget = bounded.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions("original")
                .addRenditions("page")
                .build());
        assertThat(withinBudget.getRenditionsList())
                .extracting(RenditionContent::getData)
                .containsExactly(PDF, PAGE);

        // Fetching all renditions (PDF + MARKDOWN + PAGE > smallBudget) fails with RESOURCE_EXHAUSTED.
        StatusRuntimeException cumulativeExceeded = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> bounded.getEntry(GetEntryRequest.newBuilder().setAddress(address).build()));
        assertThat(cumulativeExceeded.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(cumulativeExceeded.getStatus().getDescription()).contains("ReadRendition");

        // A single rendition larger than the budget is rejected before streaming.
        DocumentArchive tiny = new DocumentArchive(
                documentsChannel,
                (account, name) -> Optional.of(new ArchiveBinding(
                        account, name, DATASOURCE, "", VersioningPolicy.VERSIONING_POLICY_RETAINED)),
                Clock.systemUTC(),
                PDF.size() - 1);
        StatusRuntimeException singleExceeded = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> tiny.getEntry(GetEntryRequest.newBuilder()
                        .setAddress(address)
                        .addRenditions("original")
                        .build()));
        assertThat(singleExceeded.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(singleExceeded.getStatus().getDescription()).contains("ReadRendition");
    }

    @Test
    void createGetAndListArchivesStoreAndProjectDefinitionsOverV2Documents() {
        String account = "acct-defs-" + UUID.randomUUID();
        ArchiveBindings v2Bindings = ArchiveBindings.overDocuments(
                documentsChannel,
                DATASOURCE,
                (acct, drive) -> "intake".equals(drive) || "pipeline".equals(drive));
        DocumentArchive defsArchive = new DocumentArchive(documentsChannel, v2Bindings, Clock.systemUTC());

        // Missing policy is refused with INVALID_ARGUMENT.
        assertThat(code(() -> defsArchive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("no-policy")
                        .setDriveName("intake"))
                .build()))).isEqualTo(Status.Code.INVALID_ARGUMENT);

        // Unprovisioned drive is refused with FAILED_PRECONDITION.
        assertThat(code(() -> defsArchive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("bad-drive")
                        .setDriveName("unknown-drive")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build()))).isEqualTo(Status.Code.FAILED_PRECONDITION);

        Archive alpha = defsArchive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("alpha")
                        .setDriveName("intake")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)
                        .setDescription("Alpha archive")
                        .putMetadata("team", "core"))
                .build()).getArchive();
        assertThat(alpha.getAccountId()).isEqualTo(account);
        assertThat(alpha.getName()).isEqualTo("alpha");
        assertThat(alpha.getDriveName()).isEqualTo("intake");
        assertThat(alpha.getVersioning()).isEqualTo(VersioningPolicy.VERSIONING_POLICY_RETAINED);
        assertThat(alpha.getDescription()).isEqualTo("Alpha archive");
        assertThat(alpha.getMetadataMap()).containsEntry("team", "core");
        assertThat(alpha.hasCreatedAt()).isTrue();

        // Duplicate archive name in the same account is refused with ALREADY_EXISTS.
        assertThat(code(() -> defsArchive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("alpha")
                        .setDriveName("intake")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build()))).isEqualTo(Status.Code.ALREADY_EXISTS);

        Archive beta = defsArchive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("beta")
                        .setDriveName("pipeline")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE))
                .build()).getArchive();

        assertThat(defsArchive.getArchive(GetArchiveRequest.newBuilder()
                .setAccountId(account).setArchive("alpha").build()).getArchive()).isEqualTo(alpha);
        assertThat(defsArchive.getArchive(GetArchiveRequest.newBuilder()
                .setAccountId(account).setArchive("beta").build()).getArchive()).isEqualTo(beta);
        assertThat(code(() -> defsArchive.getArchive(GetArchiveRequest.newBuilder()
                .setAccountId(account).setArchive("missing").build()))).isEqualTo(Status.Code.NOT_FOUND);

        ListArchivesResponse page1 = defsArchive.listArchives(ListArchivesRequest.newBuilder()
                .setAccountId(account).setLimit(1).build());
        assertThat(page1.getArchivesList()).containsExactly(alpha);
        assertThat(page1.getNextContinuationToken()).isNotEmpty();

        ListArchivesResponse page2 = defsArchive.listArchives(ListArchivesRequest.newBuilder()
                .setAccountId(account).setLimit(1)
                .setContinuationToken(page1.getNextContinuationToken()).build());
        assertThat(page2.getArchivesList()).containsExactly(beta);
        assertThat(page2.getNextContinuationToken()).isEmpty();

        // Entries can be written and read in the created archive.
        EntryAddress entryAddr = EntryAddress.newBuilder()
                .setAccountId(account).setArchive("alpha").setEntryId("doc-1").build();
        defsArchive.putEntry(firstPut(entryAddr).build());
        assertThat(defsArchive.getEntry(GetEntryRequest.newBuilder().setAddress(entryAddr).build())
                .getInfo().getTitle()).isEqualTo("Quarterly report");
    }

    @Test
    void archiveStatsAndListEntriesTotalCountTrackRetainedAndUnversionedEntries() {
        String account = "acct-stats-" + UUID.randomUUID();
        archive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("retained-docs")
                        .setDriveName("intake")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build());
        archive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("scratch-docs")
                        .setDriveName("intake")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE))
                .build());

        // Empty archive stats.
        ArchiveStats emptyStats = archive.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(account).setArchive("retained-docs").build()).getStats();
        assertThat(emptyStats.getEntries()).isZero();
        assertThat(emptyStats.getVersions()).isZero();
        assertThat(emptyStats.getCurrentBytes()).isZero();
        assertThat(emptyStats.getRetainedBytes()).isZero();

        // Write entry 1 with v1 (original + markdown) and v2 (original carried forward, markdown revised).
        EntryAddress e1 = EntryAddress.newBuilder()
                .setAccountId(account).setArchive("retained-docs").setEntryId("e1").build();
        ByteString mdV1 = ByteString.copyFromUtf8("# Report v1");
        ByteString mdV2 = ByteString.copyFromUtf8("# Report v2 revised");
        archive.putEntry(PutEntryRequest.newBuilder()
                .setAddress(e1)
                .setTitle("Entry 1")
                .addRenditions(rendition("original", "", "application/pdf", PDF))
                .addRenditions(rendition("markdown", "", "text/markdown", mdV1))
                .build());
        archive.putEntry(PutEntryRequest.newBuilder()
                .setAddress(e1)
                .setTitle("Entry 1 v2")
                .addRenditions(rendition("markdown", "", "text/markdown", mdV2))
                .build());

        // Write entry 2 with 1 version.
        EntryAddress e2 = EntryAddress.newBuilder()
                .setAccountId(account).setArchive("retained-docs").setEntryId("e2").build();
        archive.putEntry(PutEntryRequest.newBuilder()
                .setAddress(e2)
                .setTitle("Entry 2")
                .addRenditions(rendition("original", "", "application/pdf", PAGE))
                .build());

        // Check paginated ListEntries total_count and include_manifests.
        ListEntriesResponse listedPage1 = archive.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(account)
                .setArchive("retained-docs")
                .setLimit(1)
                .setIncludeManifests(true)
                .build());
        assertThat(listedPage1.getEntriesList()).hasSize(1);
        assertThat(listedPage1.getManifestsList()).hasSize(1);
        assertThat(listedPage1.getTotalCount()).isEqualTo(2);

        ArchiveStats retainedStats = archive.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(account).setArchive("retained-docs").build()).getStats();
        assertThat(retainedStats.getEntries()).isEqualTo(2);
        assertThat(retainedStats.getVersions()).isEqualTo(3);
        long expectedCurrent = PDF.size() + mdV2.size() + PAGE.size();
        long expectedRetained = PDF.size() + mdV1.size() + mdV2.size() + PAGE.size();
        assertThat(retainedStats.getCurrentBytes()).isEqualTo(expectedCurrent);
        assertThat(retainedStats.getRetainedBytes()).isEqualTo(expectedRetained);
        assertThat(retainedStats.getRenditionsList()).containsExactly(
                RenditionStats.newBuilder()
                        .setRenditionName("markdown")
                        .setObjectCount(2)
                        .setTotalBytes(mdV1.size() + mdV2.size())
                        .build(),
                RenditionStats.newBuilder()
                        .setRenditionName("original")
                        .setObjectCount(2)
                        .setTotalBytes(PDF.size() + PAGE.size())
                        .build());

        // Under VERSIONING_POLICY_NONE, only the current version is counted.
        EntryAddress s1 = EntryAddress.newBuilder()
                .setAccountId(account).setArchive("scratch-docs").setEntryId("s1").build();
        archive.putEntry(PutEntryRequest.newBuilder()
                .setAddress(s1)
                .addRenditions(rendition("original", "", "text/plain", mdV1))
                .build());
        archive.putEntry(PutEntryRequest.newBuilder()
                .setAddress(s1)
                .addRenditions(rendition("original", "", "text/plain", mdV2))
                .build());
        ArchiveStats scratchStats = archive.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(account).setArchive("scratch-docs").build()).getStats();
        assertThat(scratchStats.getEntries()).isEqualTo(1);
        assertThat(scratchStats.getVersions()).isEqualTo(1);
        assertThat(scratchStats.getCurrentBytes()).isEqualTo(mdV2.size());
        assertThat(scratchStats.getRetainedBytes()).isEqualTo(mdV2.size());
    }

    @Test
    void deleteEntryCommitsTombstoneHidesEntryFromGetAndListReplaysByOperationIdAndServesGetArchiveMutation() {
        String account = "acct-mut-" + UUID.randomUUID();
        archive.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setAccountId(account)
                        .setName("mut-docs")
                        .setDriveName("intake")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build());
        EntryAddress target = EntryAddress.newBuilder()
                .setAccountId(account)
                .setArchive("mut-docs")
                .setEntryId("doc-to-delete")
                .build();
        EntryAddress survivor = EntryAddress.newBuilder()
                .setAccountId(account)
                .setArchive("mut-docs")
                .setEntryId("doc-survivor")
                .build();

        PutEntryResponse savedTarget = archive.putEntry(firstPut(target).build());
        archive.putEntry(firstPut(survivor).build());
        assertThat(savedTarget.getVersion()).isEqualTo(1L);
        assertThat(archive.listEntries(ListEntriesRequest.newBuilder()
                        .setAccountId(account)
                        .setArchive("mut-docs")
                        .build())
                .getEntriesList())
                .extracting(EntryInfo::getAddress)
                .containsExactly(survivor, target);

        String deleteOpId = UUID.randomUUID().toString();
        ArchiveMutationRequest deleteReq = ArchiveMutationRequest.newBuilder()
                .setOperationId(deleteOpId)
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(target))
                .build();
        ArchiveMutationReceipt receipt = mutations.archiveMutation(deleteReq).getReceipt();
        assertThat(receipt.getOperationId()).isEqualTo(deleteOpId);
        assertThat(receipt.getAddress()).isEqualTo(target);
        assertThat(receipt.getKind()).isEqualTo(ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_ENTRY);
        assertThat(receipt.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
        assertThat(receipt.getEntryDeleted()).isTrue();
        assertThat(receipt.getVersionsRemoved()).isZero();
        assertThat(receipt.getVersionsTombstoned()).isZero();
        assertThat(receipt.getObjectsTargeted()).isZero();
        assertThat(receipt.getObjectsPending()).isZero();
        assertThat(receipt.getObjectsConfirmedAbsent()).isZero();
        assertThat(receipt.getStatusRevision()).isEqualTo(1L);
        assertThat(receipt.hasObservedAt()).isTrue();
        assertThat(receipt.getCommandSha256())
                .isEqualTo(ArchiveOperationKeys.mutationCommandSha256(deleteReq));

        // Tombstone revision is committed at revision 2.
        List<VersionSummary> versions = archive.listVersions(
                ListVersionsRequest.newBuilder().setAddress(target).build()).getSummariesList();
        assertThat(versions).extracting(VersionSummary::getVersion).containsExactly(2L, 1L);
        assertThat(versions.getFirst().getDeleted()).isTrue();

        // Deleted entry is no longer returned by GetEntry, GetEntryManifest, or ListEntries.
        assertThat(code(() -> archive.getEntry(
                GetEntryRequest.newBuilder().setAddress(target).build())))
                .isEqualTo(Status.Code.NOT_FOUND);
        assertThat(code(() -> archive.getEntryManifest(
                GetEntryManifestRequest.newBuilder().setAddress(target).build())))
                .isEqualTo(Status.Code.NOT_FOUND);
        ListEntriesResponse listedAfterDelete = archive.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(account)
                .setArchive("mut-docs")
                .build());
        assertThat(listedAfterDelete.getTotalCount()).isEqualTo(1L);
        assertThat(listedAfterDelete.getEntriesList())
                .extracting(EntryInfo::getAddress)
                .containsExactly(survivor);

        // Idempotent replay with the same operation_id returns the same receipt without creating a new revision.
        ArchiveMutationReceipt replay = mutations.archiveMutation(deleteReq).getReceipt();
        assertThat(replay).isEqualTo(receipt);
        assertThat(archive.listVersions(
                        ListVersionsRequest.newBuilder().setAddress(target).build())
                .getSummariesCount()).isEqualTo(2);

        // GetArchiveMutation returns the recorded receipt.
        ArchiveMutationReceipt observed = mutations.getArchiveMutation(
                GetArchiveMutationRequest.newBuilder()
                        .setAccountId(account)
                        .setOperationId(deleteOpId)
                        .build()).getReceipt();
        assertThat(observed).isEqualTo(receipt);

        // A second delete with a new operation_id against the already-tombstoned entry is ABORTED.
        ArchiveMutationRequest secondDelete = ArchiveMutationRequest.newBuilder()
                .setOperationId(UUID.randomUUID().toString())
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(target))
                .build();
        assertThat(code(() -> mutations.archiveMutation(secondDelete)))
                .isEqualTo(Status.Code.ABORTED);

        // Deleting a non-existent entry returns NOT_FOUND.
        EntryAddress missingEntry = EntryAddress.newBuilder()
                .setAccountId(account)
                .setArchive("mut-docs")
                .setEntryId("never-existed")
                .build();
        assertThat(code(() -> mutations.archiveMutation(ArchiveMutationRequest.newBuilder()
                .setOperationId(UUID.randomUUID().toString())
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(missingEntry))
                .build()))).isEqualTo(Status.Code.NOT_FOUND);

        // GetArchiveMutation for an unknown operation_id or a save operation_id returns NOT_FOUND.
        assertThat(code(() -> mutations.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                .setAccountId(account)
                .setOperationId(UUID.randomUUID().toString())
                .build()))).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(code(() -> mutations.getArchiveMutation(GetArchiveMutationRequest.newBuilder()
                .setAccountId(account)
                .setOperationId(savedTarget.getOperationId())
                .build()))).isEqualTo(Status.Code.NOT_FOUND);

        // Reusing a delete operation_id for a different entry or reusing a save operation_id for delete is ALREADY_EXISTS.
        assertThat(code(() -> mutations.archiveMutation(ArchiveMutationRequest.newBuilder()
                .setOperationId(deleteOpId)
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(survivor))
                .build()))).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(code(() -> mutations.archiveMutation(ArchiveMutationRequest.newBuilder()
                .setOperationId(savedTarget.getOperationId())
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(survivor))
                .build()))).isEqualTo(Status.Code.ALREADY_EXISTS);
    }

    @Test
    void deleteEntryChecksExpectedVersionAndRefusesDeleteRenditionAndPruneVersionsByName() {
        EntryAddress address = entry(RETAINED);
        archive.putEntry(firstPut(address).build());
        archive.putEntry(PutEntryRequest.newBuilder().setAddress(address).setTitle("v2").build());

        // Stale expectedVersion (1 when current is 2) is ABORTED and leaves the entry live at v2.
        assertThat(code(() -> archiveEngine.deleteEntry(
                UUID.randomUUID().toString(),
                DeleteEntryRequest.newBuilder().setAddress(address).build(),
                1L,
                "stale version check"))).isEqualTo(Status.Code.ABORTED);
        assertThat(archive.getEntryManifest(GetEntryManifestRequest.newBuilder()
                        .setAddress(address).build())
                .getManifest()
                .getVersion()).isEqualTo(2L);

        // DeleteRendition and PruneVersions are refused by name with FAILED_PRECONDITION over gRPC.
        ArchiveMutationRequest deleteRenditionReq = ArchiveMutationRequest.newBuilder()
                .setOperationId(UUID.randomUUID().toString())
                .setDeleteRendition(DeleteRenditionRequest.newBuilder()
                        .setAddress(address)
                        .setRendition("original")
                        .setReason("RTBF erasure"))
                .build();
        assertThat(code(() -> mutations.archiveMutation(deleteRenditionReq)))
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(description(() -> mutations.archiveMutation(deleteRenditionReq)))
                .contains("DeleteRendition");

        ArchiveMutationRequest pruneVersionsReq = ArchiveMutationRequest.newBuilder()
                .setOperationId(UUID.randomUUID().toString())
                .setPruneVersions(PruneVersionsRequest.newBuilder()
                        .setAddress(address)
                        .setKeepLatest(1))
                .build();
        assertThat(code(() -> mutations.archiveMutation(pruneVersionsReq)))
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(description(() -> mutations.archiveMutation(pruneVersionsReq)))
                .contains("PruneVersions");

        // Matching expectedVersion = 2 succeeds and commits tombstone at v3.
        String validDeleteId = UUID.randomUUID().toString();
        ArchiveMutationReceipt deleted = archiveEngine.deleteEntry(
                validDeleteId,
                DeleteEntryRequest.newBuilder().setAddress(address).build(),
                2L,
                "exact version delete").getReceipt();
        assertThat(deleted.getEntryDeleted()).isTrue();
        assertThat(revisions(address)).isEqualTo(3);
    }
}
