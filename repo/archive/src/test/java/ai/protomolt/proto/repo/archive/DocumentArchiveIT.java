package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.repo.accession.V2RepositoryFixture;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.archive.v1.ArchiveEntryFacts;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionReadHeader;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
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
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;

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
        archiveServer = InProcessServerBuilder.forName("archive-v2-it-archive")
                .addService(new DocumentArchiveGrpcService(documentsChannel, bindings, Clock.systemUTC()))
                .build().start();
        archiveChannel = InProcessChannelBuilder.forName("archive-v2-it-archive").build();
        archive = ArchiveServiceGrpc.newBlockingStub(archiveChannel);
        asyncArchive = ArchiveServiceGrpc.newStub(archiveChannel);
    }

    @AfterAll
    static void shutdown() {
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
        assertThat(again.getVersion()).isEqualTo(first.getVersion());
        assertThat(again.getManifest()).isEqualTo(first.getManifest());
        assertThat(revisions(address)).isEqualTo(1);

        PutEntryResponse changed = archive.putEntry(firstPut(address).setTitle("Renamed")
                .setOperationId("dedupe-3-" + address.getEntryId()).build());
        assertThat(changed.getDeduplicated()).isFalse();
        assertThat(changed.getVersion()).isEqualTo(2);
    }

    /**
     * Writer A saves revision 1 and loses its response; writer B saves revision 2;
     * A's retry replays revision 1 and leaves revision 2 current. Pinned for a
     * derived and a caller-supplied operation ID.
     */
    @Test
    void aLostResponseRetryReplaysItsOwnRevisionAfterAnotherWriter() {
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
                .setAccountId(ACCOUNT).setArchive(RETAINED).build()))).isEqualTo(Status.Code.UNIMPLEMENTED);
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
        var result = new CompletableFuture<UploadRenditionResponse>();
        StreamObserver<UploadRenditionRequest> requests = asyncArchive.uploadRendition(new StreamObserver<>() {
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
}
