package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.archive.v1.Archive;
import ai.pipestream.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.pipestream.proto.repo.archive.v1.ArchiveStats;
import ai.pipestream.proto.repo.archive.v1.CreateArchiveRequest;
import ai.pipestream.proto.repo.archive.v1.DeleteEntryRequest;
import ai.pipestream.proto.repo.archive.v1.DeleteEntryResponse;
import ai.pipestream.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.pipestream.proto.repo.archive.v1.DeleteRenditionResponse;
import ai.pipestream.proto.repo.archive.v1.EntryAddress;
import ai.pipestream.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.pipestream.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.pipestream.proto.repo.archive.v1.GetEntryRequest;
import ai.pipestream.proto.repo.archive.v1.GetEntryResponse;
import ai.pipestream.proto.repo.archive.v1.ListEntriesRequest;
import ai.pipestream.proto.repo.archive.v1.ListVersionsRequest;
import ai.pipestream.proto.repo.archive.v1.PruneVersionsRequest;
import ai.pipestream.proto.repo.archive.v1.PruneVersionsResponse;
import ai.pipestream.proto.repo.archive.v1.PutEntryRequest;
import ai.pipestream.proto.repo.archive.v1.PutEntryResponse;
import ai.pipestream.proto.repo.archive.v1.RenditionContent;
import ai.pipestream.proto.repo.archive.v1.RenditionDescriptor;
import ai.pipestream.proto.repo.archive.v1.RenditionManifestEntry;
import ai.pipestream.proto.repo.archive.v1.RenditionState;
import ai.pipestream.proto.repo.archive.v1.UploadRenditionHeader;
import ai.pipestream.proto.repo.archive.v1.UploadRenditionRequest;
import ai.pipestream.proto.repo.archive.v1.UploadRenditionResponse;
import ai.pipestream.proto.repo.archive.v1.VersionManifest;
import ai.pipestream.proto.repo.archive.v1.VersioningPolicy;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The archive end to end against real infrastructure (PostgreSQL + LocalStack
 * S3, the full stack through {@link RepoServices}, no mocks): entries with
 * named renditions, entry-local content addressing across retained versions,
 * all three doors in (unary, client-streaming, HTTP POST), the exact ledger
 * counters, tombstoned rendition deletion, pruning, and both versioning
 * policies.
 */
@Testcontainers(disabledWithoutDocker = true)
class ArchiveServiceIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final ObjectMapper JSON = new ObjectMapper();

    static RepoServices services;
    static ManagedChannel channel;
    static ArchiveServiceGrpc.ArchiveServiceBlockingStub archives;
    static ArchiveServiceGrpc.ArchiveServiceStub archivesAsync;
    static DriveServiceGrpc.DriveServiceBlockingStub drives;
    static UploadHttpServer http;

    @BeforeAll
    static void boot() {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                        POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-archive",
                0,
                null, null, null, null, 0, 0L);
        services = RepoServices.build(config);
        services.startInProcess("archive-it");
        channel = InProcessChannelBuilder.forName("archive-it").build();
        archives = ArchiveServiceGrpc.newBlockingStub(channel);
        archivesAsync = ArchiveServiceGrpc.newStub(channel);
        drives = DriveServiceGrpc.newBlockingStub(channel);
        http = services.startHttp(0);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static void provision(String account, VersioningPolicy policy, String archive) {
        drives.createDrive(CreateDriveRequest.newBuilder()
                .setName("archive-drive")
                .setAccountId(account)
                .setDriveType(DriveType.DRIVE_TYPE_CUSTOM)
                .build());
        archives.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setName(archive)
                        .setAccountId(account)
                        .setDriveName("archive-drive")
                        .setVersioning(policy))
                .build());
    }

    private static EntryAddress address(String account, String archive, String entryId) {
        return EntryAddress.newBuilder()
                .setAccountId(account)
                .setArchive(archive)
                .setEntryId(entryId)
                .build();
    }

    private static RenditionContent rendition(String name, String mediaType, String body) {
        return RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder()
                        .setName(name)
                        .setMediaType(mediaType))
                .setData(ByteString.copyFromUtf8(body))
                .build();
    }

    private static String sha256(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    private static UploadRenditionResponse upload(EntryAddress address, String name,
                                                  String mediaType, byte[] body,
                                                  String declaredSha) throws Exception {
        CompletableFuture<UploadRenditionResponse> done = new CompletableFuture<>();
        StreamObserver<UploadRenditionRequest> requests = archivesAsync.uploadRendition(
                new StreamObserver<>() {
                    @Override
                    public void onNext(UploadRenditionResponse response) {
                        done.complete(response);
                    }

                    @Override
                    public void onError(Throwable t) {
                        done.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
        UploadRenditionHeader.Builder header = UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(RenditionDescriptor.newBuilder()
                        .setName(name)
                        .setMediaType(mediaType))
                .setSizeBytes(body.length);
        if (declaredSha != null) {
            header.setExpectedSha256(declaredSha);
        }
        requests.onNext(UploadRenditionRequest.newBuilder().setHeader(header).build());
        // Chunked deliberately smaller than the body, so ordering and
        // reassembly are actually exercised.
        for (int at = 0; at < body.length; at += 7) {
            requests.onNext(UploadRenditionRequest.newBuilder()
                    .setChunk(ByteString.copyFrom(body, at, Math.min(7, body.length - at)))
                    .build());
        }
        requests.onCompleted();
        return done.get(30, TimeUnit.SECONDS);
    }

    private static RenditionManifestEntry entryOf(VersionManifest manifest, String name) {
        return manifest.getRenditionsList().stream()
                .filter(e -> e.getRendition().getName().equals(name))
                .findFirst().orElseThrow();
    }

    // ------------------------------------------------------------------
    // The retained archive: versions, sharing, dedupe, reads
    // ------------------------------------------------------------------

    @Test
    void aRetainedArchiveVersionsEntriesAndSharesUnchangedRenditions() throws Exception {
        String account = "acct-retained";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "docs");
        EntryAddress address = address(account, "docs", "report.pdf#2026");

        // v1: two renditions plus metadata.
        PutEntryResponse v1 = archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .setTitle("Quarterly report")
                .setFilename("report.pdf")
                .setContentType("application/pdf")
                .addRenditions(rendition("original", "application/pdf", "raw-pdf-bytes"))
                .addRenditions(rendition("markdown", "text/markdown", "# Report"))
                .build());
        assertThat(v1.getVersion()).isEqualTo(1);
        assertThat(v1.getDeduplicated()).isFalse();
        assertThat(v1.getManifest().getRenditionsList()).hasSize(2);

        // v2: markdown changes, original does not — the unchanged rendition's
        // hash, and therefore its object key, must be shared, not copied.
        PutEntryResponse v2 = archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("markdown", "text/markdown", "# Report, revised"))
                .build());
        assertThat(v2.getVersion()).isEqualTo(2);
        assertThat(entryOf(v2.getManifest(), "original").getObjectKey())
                .isEqualTo(entryOf(v1.getManifest(), "original").getObjectKey());
        assertThat(entryOf(v2.getManifest(), "markdown").getObjectKey())
                .isNotEqualTo(entryOf(v1.getManifest(), "markdown").getObjectKey());

        // Identical content dedupes the whole save: no version lands.
        PutEntryResponse again = archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("markdown", "text/markdown", "# Report, revised"))
                .build());
        assertThat(again.getDeduplicated()).isTrue();
        assertThat(again.getVersion()).isEqualTo(2);

        // The current read carries v2's bytes; the filter narrows to one name.
        GetEntryResponse current = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions("markdown")
                .build());
        assertThat(current.getRenditionsList()).hasSize(1);
        assertThat(current.getRenditions(0).getData().toStringUtf8())
                .isEqualTo("# Report, revised");
        assertThat(current.getManifest().getRenditionsList()).hasSize(2);
        assertThat(current.getInfo().getTitle()).isEqualTo("Quarterly report");

        // Time travel: version 1 still reads byte-for-byte.
        GetEntryResponse historic = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address)
                .setVersion(1)
                .addRenditions("markdown")
                .build());
        assertThat(historic.getRenditions(0).getData().toStringUtf8()).isEqualTo("# Report");

        assertThat(archives.listVersions(ListVersionsRequest.newBuilder()
                .setAddress(address).build()).getVersionsList())
                .extracting(VersionManifest::getVersion)
                .containsExactly(2L, 1L);

        // The exact counters: 2 entries' worth of objects — original (shared,
        // once) plus two markdown revisions.
        ArchiveStats stats = archives.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(account).setArchive("docs").build()).getStats();
        assertThat(stats.getEntries()).isEqualTo(1);
        assertThat(stats.getVersions()).isEqualTo(2);
        long originalBytes = "raw-pdf-bytes".length();
        long markdownBytes = "# Report".length() + "# Report, revised".length();
        assertThat(stats.getRetainedBytes()).isEqualTo(originalBytes + markdownBytes);
        assertThat(stats.getCurrentBytes())
                .isEqualTo(originalBytes + "# Report, revised".length());
    }

    @Test
    void expectedVersionGuardsConcurrentWriters() {
        String account = "acct-guard";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "guarded");
        EntryAddress address = address(account, "guarded", "doc-1");
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("original", "text/plain", "first"))
                .build());

        assertThatThrownBy(() -> archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .setExpectedVersion(7)
                .addRenditions(rendition("original", "text/plain", "second"))
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(((StatusRuntimeException) t).getStatus().getCode())
                        .isEqualTo(Status.Code.ABORTED));
    }

    // ------------------------------------------------------------------
    // The streaming door
    // ------------------------------------------------------------------

    @Test
    void theStreamingDoorLandsBytesWithAndWithoutADeclaredHash() throws Exception {
        String account = "acct-stream";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "streams");
        EntryAddress address = address(account, "streams", "big-doc");
        byte[] body = "streamed rendition content, delivered in small chunks"
                .getBytes(StandardCharsets.UTF_8);

        // With the hash declared up front, bytes land directly on their final
        // content-addressed key.
        UploadRenditionResponse declared = upload(address, "original", "text/plain",
                body, sha256(body));
        assertThat(declared.getVersion()).isEqualTo(1);
        assertThat(declared.getSha256()).isEqualTo(sha256(body));
        assertThat(declared.getObjectKey()).endsWith("/" + sha256(body));

        // Without one, the bytes stage and settle; the receipt is identical
        // in shape and the sibling rendition is re-referenced, not copied.
        byte[] parsed = "{\"parsed\":true}".getBytes(StandardCharsets.UTF_8);
        UploadRenditionResponse staged = upload(address, "parsed.json", "application/json",
                parsed, null);
        assertThat(staged.getVersion()).isEqualTo(2);
        assertThat(staged.getSha256()).isEqualTo(sha256(parsed));

        GetEntryResponse read = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address).build());
        assertThat(read.getRenditionsList()).hasSize(2);
        assertThat(entryOf(read.getManifest(), "original").getObjectKey())
                .isEqualTo(declared.getObjectKey());
    }

    @Test
    void theStreamingDoorRefusesAShortDelivery() throws Exception {
        String account = "acct-short";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "shorts");
        byte[] body = "only-half".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<UploadRenditionResponse> done = new CompletableFuture<>();
        StreamObserver<UploadRenditionRequest> requests = archivesAsync.uploadRendition(
                new StreamObserver<>() {
                    @Override
                    public void onNext(UploadRenditionResponse response) {
                        done.complete(response);
                    }

                    @Override
                    public void onError(Throwable t) {
                        done.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
        requests.onNext(UploadRenditionRequest.newBuilder()
                .setHeader(UploadRenditionHeader.newBuilder()
                        .setAddress(address(account, "shorts", "doc"))
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setSizeBytes(body.length * 10L))
                .build());
        requests.onNext(UploadRenditionRequest.newBuilder()
                .setChunk(ByteString.copyFrom(body)).build());
        requests.onCompleted();

        assertThatThrownBy(() -> done.get(30, TimeUnit.SECONDS))
                .hasCauseInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(
                        ((StatusRuntimeException) t.getCause()).getStatus().getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    // ------------------------------------------------------------------
    // The HTTP door
    // ------------------------------------------------------------------

    @Test
    void theHttpDoorStreamsARenditionInWithAReceipt() throws Exception {
        String account = "acct-http";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "uploads");
        byte[] body = "posted straight from curl, no protomolt dependency anywhere"
                .getBytes(StandardCharsets.UTF_8);

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + http.port() + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                                + "?account_id=" + account + "&archive=uploads"
                                + "&entry_id=curl-doc&rendition=original&filename=doc.txt"))
                        .header("Content-Type", "text/plain")
                        .header("X-Content-Sha256", sha256(body))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode receipt = JSON.readTree(response.body());
        assertThat(receipt.path("version").asLong()).isEqualTo(1);
        assertThat(receipt.path("sha256").asText()).isEqualTo(sha256(body));
        assertThat(receipt.path("rendition").asText()).isEqualTo("original");

        GetEntryResponse read = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address(account, "uploads", "curl-doc")).build());
        assertThat(read.getRenditions(0).getData().toByteArray()).isEqualTo(body);
        assertThat(read.getInfo().getFilename()).isEqualTo("doc.txt");
    }

    // ------------------------------------------------------------------
    // Deletion: tombstones, pruning, whole entries
    // ------------------------------------------------------------------

    @Test
    void deletingARenditionRemovesBytesEverywhereAndLeavesTombstones() throws Exception {
        String account = "acct-rtbf";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "sensitive");
        EntryAddress address = address(account, "sensitive", "subject-record");
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("original", "text/plain", "the source"))
                .addRenditions(rendition("summary", "text/plain", "a summary v1"))
                .build());
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("summary", "text/plain", "a summary v2"))
                .build());

        DeleteRenditionResponse removed = archives.deleteRendition(
                DeleteRenditionRequest.newBuilder()
                        .setAddress(address)
                        .setRendition("summary")
                        .setReason("RTBF")
                        .build());
        assertThat(removed.getObjectsDeleted()).isEqualTo(2);
        assertThat(removed.getVersionsTombstoned()).isEqualTo(2);

        // Both manifests carry the tombstone: bytes gone, size and hash
        // retained as provenance, the reason on the record.
        for (long version : new long[] {1, 2}) {
            VersionManifest manifest = archives.getEntryManifest(
                    GetEntryManifestRequest.newBuilder()
                            .setAddress(address).setVersion(version).build()).getManifest();
            RenditionManifestEntry tombstone = entryOf(manifest, "summary");
            assertThat(tombstone.getState())
                    .isEqualTo(RenditionState.RENDITION_STATE_DELETED);
            assertThat(tombstone.getDeletedReason()).isEqualTo("RTBF");
            assertThat(tombstone.getSha256()).isNotBlank();
            assertThat(tombstone.getSizeBytes()).isPositive();
        }

        // Reads simply no longer carry it; the original is untouched.
        GetEntryResponse read = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address).build());
        assertThat(read.getRenditionsList()).hasSize(1);
        assertThat(read.getRenditions(0).getRendition().getName()).isEqualTo("original");

        // A rendition nothing holds is an idempotent no-op, not an error.
        assertThat(archives.deleteRendition(DeleteRenditionRequest.newBuilder()
                .setAddress(address).setRendition("summary").setReason("RTBF").build())
                .getObjectsDeleted()).isZero();
    }

    @Test
    void pruningKeepsTheNewestVersionsAndOnlyDeletesUnsharedObjects() throws Exception {
        String account = "acct-prune";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "pruned");
        EntryAddress address = address(account, "pruned", "doc");
        for (String revision : List.of("one", "two", "three")) {
            archives.putEntry(PutEntryRequest.newBuilder()
                    .setAddress(address)
                    .addRenditions(rendition("original", "text/plain", "stable body"))
                    .addRenditions(rendition("notes", "text/plain", "notes " + revision))
                    .build());
        }

        PruneVersionsResponse pruned = archives.pruneVersions(PruneVersionsRequest.newBuilder()
                .setAddress(address)
                .setKeepLatest(1)
                .build());
        assertThat(pruned.getVersionsRemoved()).isEqualTo(2);
        // 'stable body' is shared by the kept version and must survive; only
        // the two superseded notes objects go.
        assertThat(pruned.getObjectsDeleted()).isEqualTo(2);

        GetEntryResponse read = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address).build());
        assertThat(read.getRenditionsList()).hasSize(2);
        assertThat(entryOf(read.getManifest(), "notes").getSha256()).isNotBlank();

        ArchiveStats stats = archives.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(account).setArchive("pruned").build()).getStats();
        assertThat(stats.getVersions()).isEqualTo(1);
        assertThat(stats.getRetainedBytes())
                .isEqualTo("stable body".length() + "notes three".length());
    }

    @Test
    void deletingAnEntryRemovesEveryVersionAndSettlesTheCounters() {
        String account = "acct-delete";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "doomed");
        EntryAddress address = address(account, "doomed", "doc");
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("original", "text/plain", "body one"))
                .build());
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("original", "text/plain", "body two"))
                .build());

        DeleteEntryResponse deleted = archives.deleteEntry(DeleteEntryRequest.newBuilder()
                .setAddress(address).build());
        assertThat(deleted.getDeleted()).isTrue();
        assertThat(deleted.getVersionsRemoved()).isEqualTo(2);
        assertThat(deleted.getObjectsDeleted()).isEqualTo(2);

        // Idempotent: a second delete finds nothing and says so.
        assertThat(archives.deleteEntry(DeleteEntryRequest.newBuilder()
                .setAddress(address).build()).getDeleted()).isFalse();

        ArchiveStats stats = archives.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(account).setArchive("doomed").build()).getStats();
        assertThat(stats.getEntries()).isZero();
        assertThat(stats.getVersions()).isZero();
        assertThat(stats.getRetainedBytes()).isZero();
        assertThat(stats.getCurrentBytes()).isZero();
    }

    // ------------------------------------------------------------------
    // The unversioned policy
    // ------------------------------------------------------------------

    @Test
    void anUnversionedArchiveKeepsOneStatePerEntry() {
        String account = "acct-none";
        provision(account, VersioningPolicy.VERSIONING_POLICY_NONE, "scratch");
        EntryAddress address = address(account, "scratch", "doc");
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("original", "text/plain", "first state"))
                .build());
        PutEntryResponse second = archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address)
                .addRenditions(rendition("original", "text/plain", "second state"))
                .build());

        // The counter still says the entry changed twice; the bytes do not
        // accumulate and version 1 is genuinely gone.
        assertThat(second.getVersion()).isEqualTo(2);
        assertThat(archives.listVersions(ListVersionsRequest.newBuilder()
                .setAddress(address).build()).getVersionsList())
                .extracting(VersionManifest::getVersion)
                .containsExactly(2L);
        assertThatThrownBy(() -> archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address).setVersion(1).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(((StatusRuntimeException) t).getStatus().getCode())
                        .isEqualTo(Status.Code.NOT_FOUND));

        ArchiveStats stats = archives.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(account).setArchive("scratch").build()).getStats();
        assertThat(stats.getVersions()).isEqualTo(1);
        assertThat(stats.getRetainedBytes()).isEqualTo("second state".length());
    }

    // ------------------------------------------------------------------
    // Contract edges
    // ------------------------------------------------------------------

    @Test
    void theContractRefusesWhatItNames() {
        String account = "acct-edges";
        provision(account, VersioningPolicy.VERSIONING_POLICY_RETAINED, "edges");

        // An archive on a drive nobody provisioned hard-fails by name.
        assertThatThrownBy(() -> archives.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setName("no-drive")
                        .setAccountId(account)
                        .setDriveName("missing-drive")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE))
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(((StatusRuntimeException) t).getStatus().getCode())
                        .isEqualTo(Status.Code.FAILED_PRECONDITION));

        // An unstated versioning policy is refused, never assumed.
        assertThatThrownBy(() -> archives.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setName("no-policy")
                        .setAccountId(account)
                        .setDriveName("archive-drive"))
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(((StatusRuntimeException) t).getStatus().getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT));

        // A rendition name that could escape its key path is refused.
        assertThatThrownBy(() -> archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address(account, "edges", "doc"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("../escape"))
                        .setData(ByteString.copyFromUtf8("x")))
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(((StatusRuntimeException) t).getStatus().getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Listings paginate with a stable order and an honest total.
        for (int i = 0; i < 3; i++) {
            archives.putEntry(PutEntryRequest.newBuilder()
                    .setAddress(address(account, "edges", "doc-" + i))
                    .addRenditions(rendition("original", "text/plain", "body " + i))
                    .build());
        }
        var page = archives.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(account).setArchive("edges").setLimit(2).build());
        assertThat(page.getEntriesList()).hasSize(2);
        assertThat(page.getTotalCount()).isEqualTo(3);
        var rest = archives.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(account).setArchive("edges").setLimit(2)
                .setContinuationToken(page.getNextContinuationToken()).build());
        assertThat(rest.getEntriesList()).hasSize(1);
        assertThat(rest.getNextContinuationToken()).isEmpty();
    }
}
