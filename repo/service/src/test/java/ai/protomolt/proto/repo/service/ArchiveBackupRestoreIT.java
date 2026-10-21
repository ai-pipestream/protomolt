package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationState;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.ArchiveStats;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Offline backup and cold-restore qualification of the surviving archive stack
 * (Issue #393): the {@code Archive*} ledger tables, the {@code drives} rows
 * naming each archive's bucket and prefix, the v2 {@code repo_catalog} schema,
 * and the RustFS objects the archive ledger references — no v1
 * document/publication/inventory machinery anywhere.
 *
 * <p>Phase 1 seeds through the production gRPC/HTTP doors against real
 * PostgreSQL and RustFS (retained versions sharing rendition objects, an EMPTY
 * rendition, a streamed 1 MiB rendition with a declared digest, an unversioned
 * archive, a rendition deletion driven to completion before the cut so the
 * ledger carries a durable {@code DELETED} cleanup tombstone whose provider
 * object is already gone, an admitted rendition deletion whose cleanup is
 * still pending, and a v2 parts-based document), proves the deployment
 * quiescent, then takes the offline cut: {@code pg_dump -Fc} of the whole
 * database plus a tar of the stopped RustFS data volume.
 *
 * <p>Phase 2 restores into a FRESH PostgreSQL cluster and a FRESH provider
 * volume served at the recorded endpoint (never overwriting the source), boots
 * a fresh host at the same backend generation, and verifies every archived
 * entry and every retained version byte-for-byte against the ledger SHA-256,
 * every recorded provider object by its recorded version id, the pending
 * mutation completing through the recovery lane, replay idempotence, and fresh
 * writes advancing past the restored counters.
 *
 * <p>Phases 3 and 4 restore disposable copies and prove the qualification fails
 * loudly, naming the affected entry, when one provider object version or one
 * ledger row is missing from the restored set.
 *
 * <p>Runs under the ordinary {@code :protomolt-repo-service:test} task — no
 * init script, no opt-in property. The procedure exercised here is the one
 * documented for operators in {@code docs/operations/archive-backup-and-restore.md}.
 * Evidence lands under {@code build/archive-backup-qualification/<timestamp>/}.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ArchiveBackupRestoreIT {

    private static final String POSTGRES_IMAGE = "postgres:18-alpine";
    private static final String RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-beta.11-preview.1";
    private static final String DB_NAME = "repo";
    private static final String DB_USER = "repo";
    private static final String S3_ACCESS = "abq-test";
    private static final String S3_SECRET = "abq-test-secret";
    private static final String REGION = "us-east-1";
    private static final String BUCKET_BASE = "abq-docs";
    private static final String GENERATION = "abq-original";
    private static final String REALM = "abq-realm";
    private static final String HTTP_TOKEN = "synthetic-http-operator-key";
    private static final String ALPHA = "abq-alpha";
    private static final String BETA = "abq-beta";
    /** Seed-host recovery lanes parked at one hour; the restored host sweeps every second. */
    private static final long PARKED_LANE_MS = 3_600_000L;
    private static final long ACTIVE_LANE_MS = 1_000L;

    private static final Path ROOT = Path.of("build", "archive-backup-qualification",
            java.time.Instant.now().toString().replace(':', '-')).toAbsolutePath();
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String runId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private final String dbPassword = "abq-" + UUID.randomUUID();
    private final List<String> dockerContainers = new ArrayList<>();
    private final List<String> dockerVolumes = new ArrayList<>();
    private final List<PostgreSQLContainer> postgresContainers = new ArrayList<>();

    private final Path backupDir = ROOT.resolve("backup");
    private int providerPort;
    private String sourceVolume;
    private String sourceVolumeDigest;
    private PostgreSQLContainer sourceDb;
    private Capture capture;

    // ------------------------------------------------------------------
    // Docker helpers: the offline provider snapshot is a volume-level tar,
    // the same procedure the operations guide documents.
    // ------------------------------------------------------------------

    private record DockerResult(int exitCode, String output) {}

    private static DockerResult docker(String... args) throws IOException, InterruptedException {
        var command = new ArrayList<>(List.of("docker"));
        command.addAll(List.of(args));
        return runProcess(command, 180_000L);
    }

    /**
     * Runs a child process with its combined output drained to a temp file, so the
     * deadline is enforced on {@code waitFor} itself: a child that holds its output
     * stream open without exiting cannot block the drain and is destroyed on
     * timeout (and on interruption). Reading the pipe first would hang past the
     * deadline for exactly that child shape.
     */
    static DockerResult runProcess(List<String> command, long timeoutMs)
            throws IOException, InterruptedException {
        var outputFile = Files.createTempFile("abq-process-", ".log");
        try {
            var process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(outputFile.toFile())
                    .start();
            try {
                if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    if (!process.waitFor(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException(String.join(" ", command)
                                + " timed out and did not die on destroyForcibly");
                    }
                    throw new IllegalStateException(String.join(" ", command) + " timed out");
                }
            } catch (InterruptedException interrupted) {
                process.destroyForcibly();
                throw interrupted;
            }
            return new DockerResult(process.exitValue(),
                    Files.readString(outputFile, StandardCharsets.UTF_8).strip());
        } finally {
            try {
                Files.deleteIfExists(outputFile);
            } catch (IOException ignored) {
                // evidence of the run is not the temp file; deletion is best-effort
            }
        }
    }

    private static DockerResult dockerOk(String... args) throws IOException, InterruptedException {
        var result = docker(args);
        assertThat(result.exitCode()).as("docker %s -> %s", String.join(" ", args), result.output()).isZero();
        return result;
    }

    private String volumeCreate(String name) throws IOException, InterruptedException {
        dockerOk("volume", "create", name);
        dockerVolumes.add(name);
        return name;
    }

    private void startRustFs(String name, String volume, int port) throws Exception {
        dockerOk("run", "-d", "--name", name,
                "-p", "127.0.0.1:" + port + ":9000",
                "-v", volume + ":/data",
                "-e", "RUSTFS_VOLUMES=/data",
                "-e", "RUSTFS_ADDRESS=:9000",
                "-e", "RUSTFS_CONSOLE_ENABLE=false",
                "-e", "RUSTFS_ACCESS_KEY=" + S3_ACCESS,
                "-e", "RUSTFS_SECRET_KEY=" + S3_SECRET,
                RUSTFS_IMAGE, "/data");
        dockerContainers.add(name);
        var client = HttpClient.newHttpClient();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (true) {
            try {
                var response = client.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health")).build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) return;
            } catch (Exception notReady) {
                // container still starting
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("RustFS did not become healthy on port " + port);
            }
            Thread.sleep(500);
        }
    }

    private String volumeDigest(String volume) throws IOException, InterruptedException {
        return dockerOk("run", "--rm", "-v", volume + ":/data:ro", POSTGRES_IMAGE,
                "sh", "-c", "cd /data && find . -type f | sort | xargs sha256sum | sha256sum").output();
    }

    private PostgreSQLContainer startPostgres() {
        var pg = new PostgreSQLContainer(POSTGRES_IMAGE)
                .withDatabaseName(DB_NAME).withUsername(DB_USER).withPassword(dbPassword);
        pg.start();
        postgresContainers.add(pg);
        return pg;
    }

    private static Connection connect(PostgreSQLContainer pg) throws SQLException {
        return DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    }

    @AfterAll
    void cleanup() {
        for (var pg : postgresContainers) {
            try { pg.stop(); } catch (RuntimeException ignored) { }
        }
        for (var container : dockerContainers) {
            try { docker("rm", "-f", container); } catch (Exception ignored) { }
        }
        for (var volume : dockerVolumes) {
            try { docker("volume", "rm", volume); } catch (Exception ignored) { }
        }
    }

    // ------------------------------------------------------------------
    // Host boot over the production composition
    // ------------------------------------------------------------------

    private String endpoint() {
        return "http://127.0.0.1:" + providerPort;
    }

    private record Host(RepoServices services, Server server, ManagedChannel channel,
                        ArchiveServiceGrpc.ArchiveServiceBlockingStub archives,
                        ArchiveServiceGrpc.ArchiveServiceStub archivesAsync,
                        ArchiveMutationServiceGrpc.ArchiveMutationServiceBlockingStub mutations,
                        DriveServiceGrpc.DriveServiceBlockingStub drives,
                        DocumentServiceGrpc.DocumentServiceBlockingStub documents,
                        UploadHttpServer http) implements AutoCloseable {
        @Override public void close() {
            channel.shutdownNow();
            server.shutdownNow();
            services.close();
        }
    }

    private Host bootHost(PostgreSQLContainer pg, long laneMs, String serverName) throws IOException {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword()),
                endpoint(), REGION, S3_ACCESS, S3_SECRET, BUCKET_BASE, 0,
                null, null, 0, 0L,
                true, laneMs, laneMs, false, true, PARKED_LANE_MS)
                .withManagedStorage(new ManagedStoragePolicy(GENERATION, REALM, true));
        var services = RepoServices.build(config);
        var operator = Caller.operator();
        var builder = InProcessServerBuilder.forName(serverName).intercept(new io.grpc.ServerInterceptor() {
            @Override public <Q, S> io.grpc.ServerCall.Listener<Q> interceptCall(io.grpc.ServerCall<Q, S> call,
                    io.grpc.Metadata headers, io.grpc.ServerCallHandler<Q, S> next) {
                return io.grpc.Contexts.interceptCall(io.grpc.Context.current()
                        .withValue(ai.protomolt.proto.authz.grpc.CallerContexts.CALLER, operator),
                        call, headers, next);
            }
        });
        services.services().forEach(builder::addService);
        var server = builder.build().start();
        var channel = InProcessChannelBuilder.forName(serverName).build();
        return new Host(services, server, channel,
                ArchiveServiceGrpc.newBlockingStub(channel),
                ArchiveServiceGrpc.newStub(channel),
                ArchiveMutationServiceGrpc.newBlockingStub(channel),
                DriveServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                services.startHttp(0, HTTP_TOKEN, RepositoryConsole.notConfigured()));
    }

    /** A raw provider handle for backup-side object verification and damage injection. */
    private OpenedBlobStore openRawStore() {
        return BlobStores.discover().open("s3", Map.ofEntries(
                Map.entry("endpoint", endpoint()),
                Map.entry("region", REGION),
                Map.entry("access-key", S3_ACCESS),
                Map.entry("secret-key", S3_SECRET),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")));
    }

    // ------------------------------------------------------------------
    // Seed fixtures through the production doors
    // ------------------------------------------------------------------

    private static EntryAddress address(String account, String archive, String entryId) {
        return EntryAddress.newBuilder().setAccountId(account).setArchive(archive).setEntryId(entryId).build();
    }

    private static RenditionContent rendition(String name, String mediaType, byte[] body) {
        return RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName(name).setMediaType(mediaType))
                .setData(ByteString.copyFrom(body))
                .build();
    }

    private static byte[] labeled(String label) {
        return ("archive-backup-qualification synthetic fixture: " + label + "\n").getBytes(StandardCharsets.UTF_8);
    }

    /** One MiB of deterministic, self-identifying payload for the streaming door. */
    private static byte[] largePayload(String label) {
        var line = labeled(label);
        var out = new byte[1024 * 1024];
        for (int at = 0; at < out.length; at++) out[at] = line[at % line.length];
        return out;
    }

    private static String sha256(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    private static RenditionManifestEntry entryOf(VersionManifest manifest, String name) {
        return manifest.getRenditionsList().stream()
                .filter(e -> e.getRendition().getName().equals(name)).findFirst().orElseThrow();
    }

    private static UploadRenditionResponse upload(Host host, EntryAddress address, String name,
                                                  byte[] body) throws Exception {
        CompletableFuture<UploadRenditionResponse> done = new CompletableFuture<>();
        StreamObserver<UploadRenditionRequest> requests = host.archivesAsync().uploadRendition(
                new StreamObserver<>() {
                    @Override public void onNext(UploadRenditionResponse response) { done.complete(response); }
                    @Override public void onError(Throwable t) { done.completeExceptionally(t); }
                    @Override public void onCompleted() { }
                });
        requests.onNext(UploadRenditionRequest.newBuilder().setHeader(UploadRenditionHeader.newBuilder()
                        .setAddress(address)
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName(name).setMediaType("application/octet-stream"))
                        .setSizeBytes(body.length)
                        .setExpectedSha256(sha256(body)))
                .build());
        for (int at = 0; at < body.length; at += 8192) {
            requests.onNext(UploadRenditionRequest.newBuilder()
                    .setChunk(ByteString.copyFrom(body, at, Math.min(8192, body.length - at))).build());
        }
        requests.onCompleted();
        return done.get(60, TimeUnit.SECONDS);
    }

    /** The seeded surface, recorded at the cut and re-verified after restore. */
    private static final class Capture {
        final List<RecordedEntry> entries = new ArrayList<>();
        final Map<String, ListEntriesResponse> listings = new LinkedHashMap<>();
        final Map<String, ArchiveStats> stats = new LinkedHashMap<>();
        final List<RecordedObject> objects = new ArrayList<>();
        final Map<String, String> fingerprints = new LinkedHashMap<>();
        final Map<String, Long> sequences = new LinkedHashMap<>();
        final Map<String, Long> archiveState = new LinkedHashMap<>();
        final List<String> drives = new ArrayList<>();
        ArchiveMutationReceipt mutation;
        String mutationAccount;
        String mutationOperationId;
        String completedOperationId;
        String v2Account;
        String v2Datasource;
        String v2Drive;
        String v2DocId;
        byte[] v2Bytes;
        String v2BlobId;
        ai.protomolt.proto.repo.v2.GetDocumentResponse v2Document;
        long mutationRevision = -1;
        EntryAddress notesAddress;
    }

    private record RecordedEntry(String tag, EntryAddress address, String entryUuid,
                                 List<GetEntryResponse> versions, ListVersionsResponse listing) {}

    private record RecordedObject(String objectId, String entryTag, String rendition, String bucket, String key,
                                  String sha256, String providerVersion, long size, String state) {}

    // ------------------------------------------------------------------
    // Phase 1: seed, quiesce, take the offline cut
    // ------------------------------------------------------------------

    @Test @Order(1) @Timeout(900)
    void seedQuiesceAndCaptureTheOfflineBackup() throws Exception {
        Files.createDirectories(backupDir.resolve("sql"));
        Files.createDirectories(backupDir.resolve("provider"));
        providerPort = reservePort();
        sourceDb = startPostgres();
        sourceVolume = volumeCreate("abq-" + runId + "-src");
        startRustFs("abq-" + runId + "-src-rustfs", sourceVolume, providerPort);

        capture = new Capture();
        try (var host = bootHost(sourceDb, PARKED_LANE_MS, "abq-seed-" + runId)) {
            seedAlpha(host);
            seedBeta(host);
            seedUnversioned(host);
            seedV2Document(host);
            admitCompletedDeletion(host);
        }
        // A transient host with an active recovery lane drives that deletion to
        // completion, so the cut carries a durable DELETED cleanup tombstone
        // whose provider object is already gone. It closes before the pending
        // mutation is admitted, or its lane would reclaim that one too.
        try (var sweeper = bootHost(sourceDb, ACTIVE_LANE_MS, "abq-sweeper-" + runId)) {
            var settled = awaitMutation(sweeper, ALPHA, capture.completedOperationId);
            assertThat(settled.getState())
                    .as("the pre-cut deletion completes before the backup window")
                    .isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
            assertThat(settled.getObjectsConfirmedAbsent()).isEqualTo(1);
        }
        try (var host = bootHost(sourceDb, PARKED_LANE_MS, "abq-record-" + runId)) {
            admitPendingDeletion(host);
            recordReads(host);
        }

        try (var db = connect(sourceDb)) {
            captureQuiescence(db);
            capture.fingerprints.putAll(tableFingerprints(db));
            capture.sequences.putAll(sequenceValues(db));
            capture.objects.addAll(objectInventory(db));
            capture.drives.addAll(driveRows(db));
            capture.mutationRevision = capture.sequences.get("public.archive_mutation_revision_seq");
        }
        var dump = backupDir.resolve("sql/ledger.dump");
        var dumpRun = sourceDb.execInContainer("sh", "-c",
                "pg_dump -Fc -U " + DB_USER + " -d " + DB_NAME + " > /tmp/ledger.dump");
        assertThat(dumpRun.getExitCode()).as("pg_dump stderr: %s", dumpRun.getStderr()).isZero();
        dockerOk("cp", sourceDb.getContainerId() + ":/tmp/ledger.dump", dump.toString());
        assertThat(Files.size(dump)).isGreaterThan(0L);

        // The dump window changed nothing; then both source stores go offline.
        try (var db = connect(sourceDb)) {
            assertThat(tableFingerprints(db))
                    .as("every table fingerprint is unchanged across the pg_dump window")
                    .isEqualTo(capture.fingerprints);
        }
        var unreachableUrl = sourceDb.getJdbcUrl();
        var unreachableUser = sourceDb.getUsername();
        var unreachablePassword = sourceDb.getPassword();
        sourceDb.stop();
        assertThatThrownBy(() -> DriverManager.getConnection(unreachableUrl, unreachableUser, unreachablePassword)
                        .close())
                .as("the source database is unreachable after the cut")
                .isInstanceOf(SQLException.class);
        dockerOk("stop", "abq-" + runId + "-src-rustfs");
        sourceVolumeDigest = volumeDigest(sourceVolume);
        dockerOk("run", "--rm", "-v", sourceVolume + ":/data:ro",
                "-v", backupDir.resolve("provider").toString() + ":/backup", POSTGRES_IMAGE,
                "tar", "-cf", "/backup/rustfs-data.tar", "-C", "/data", ".");

        var manifest = new LinkedHashMap<String, Object>();
        manifest.put("images", Map.of("postgres", POSTGRES_IMAGE, "rustfs", RUSTFS_IMAGE));
        manifest.put("backend", Map.of("generation", GENERATION, "realm", REALM,
                "endpoint", endpoint(), "region", REGION, "pathStyle", true));
        manifest.put("database", Map.of("name", DB_NAME, "user", DB_USER));
        manifest.put("tables", capture.fingerprints.size());
        manifest.put("sequences", capture.sequences);
        manifest.put("archiveState", capture.archiveState);
        manifest.put("objects", capture.objects.size());
        manifest.put("ledgerDumpSha256", sha256(Files.readAllBytes(dump)));
        manifest.put("providerTarSha256", sha256(Files.readAllBytes(backupDir.resolve("provider/rustfs-data.tar"))));
        manifest.put("sourceVolumeDigest", sourceVolumeDigest);
        JSON.writerWithDefaultPrettyPrinter().writeValue(backupDir.resolve("manifest.json").toFile(), manifest);
        JSON.writerWithDefaultPrettyPrinter().writeValue(ROOT.resolve("capture-objects.json").toFile(), capture.objects);
        JSON.writerWithDefaultPrettyPrinter()
                .writeValue(ROOT.resolve("capture-fingerprints.json").toFile(), capture.fingerprints);
        Files.writeString(backupDir.resolve("SEALED"), "archive-backup-qualification " + runId + "\n");
    }

    private void seedAlpha(Host host) throws Exception {
        host.drives().createDrive(CreateDriveRequest.newBuilder()
                .setName("archive-drive").setAccountId(ALPHA).setDriveType(DriveType.DRIVE_TYPE_CUSTOM).build());
        host.archives().createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setName("docs").setAccountId(ALPHA).setDriveName("archive-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());

        var report = address(ALPHA, "docs", "report.pdf#2026");
        PutEntryResponse v1 = host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(report)
                .setTitle("Quarterly report").setFilename("report.pdf").setContentType("application/pdf")
                .addRenditions(rendition("original", "application/pdf", labeled("report v1 original")))
                .addRenditions(rendition("markdown", "text/markdown", labeled("report v1 markdown")))
                .build());
        assertThat(v1.getVersion()).isEqualTo(1);
        PutEntryResponse v2 = host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(report)
                .setTitle("Revised report")
                .addRenditions(rendition("original", "application/pdf", labeled("report v1 original")))
                .addRenditions(rendition("markdown", "text/markdown", labeled("report v2 markdown")))
                .build());
        assertThat(v2.getVersion()).isEqualTo(2);
        // The unchanged rendition is re-referenced, never copied.
        assertThat(entryOf(v2.getManifest(), "original").getObjectKey())
                .isEqualTo(entryOf(v1.getManifest(), "original").getObjectKey());

        var attachment = largePayload("report v3 attachment.bin");
        UploadRenditionResponse v3 = upload(host, report, "attachment.bin", attachment);
        assertThat(v3.getVersion()).isEqualTo(3);
        assertThat(v3.getSha256()).isEqualTo(sha256(attachment));

        capture.notesAddress = address(ALPHA, "docs", "notes#1");
        PutEntryResponse notes = host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(capture.notesAddress)
                .addRenditions(rendition("original", "text/plain", labeled("notes original")))
                .addRenditions(rendition("empty", "text/plain", new byte[0]))
                .build());
        assertThat(notes.getVersion()).isEqualTo(1);
        assertThat(entryOf(notes.getManifest(), "empty").getState())
                .isEqualTo(RenditionState.RENDITION_STATE_EMPTY);
    }

    private void seedBeta(Host host) {
        host.drives().createDrive(CreateDriveRequest.newBuilder()
                .setName("records-drive").setAccountId(BETA).setDriveType(DriveType.DRIVE_TYPE_CUSTOM).build());
        host.archives().createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setName("records").setAccountId(BETA).setDriveName("records-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());

        var subject = address(BETA, "records", "subject-record");
        host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(subject)
                .addRenditions(rendition("original", "text/plain", labeled("subject original")))
                .addRenditions(rendition("scratch", "text/plain", labeled("subject v1 scratch")))
                .build());
        host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(subject)
                .addRenditions(rendition("original", "text/plain", labeled("subject original")))
                .addRenditions(rendition("scratch", "text/plain", labeled("subject v2 scratch")))
                .build());
    }

    /** Admitted and reclaimed BEFORE the cut: a durable DELETED cleanup tombstone. */
    private void admitCompletedDeletion(Host host) {
        var obsolete = address(ALPHA, "docs", "obsolete#1");
        host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(obsolete)
                .addRenditions(rendition("original", "text/plain", labeled("obsolete original")))
                .addRenditions(rendition("trash", "text/plain", labeled("obsolete trash")))
                .build());
        capture.completedOperationId = UUID.randomUUID().toString();
        var receipt = host.mutations().archiveMutation(ArchiveMutationRequest.newBuilder()
                .setOperationId(capture.completedOperationId)
                .setDeleteRendition(DeleteRenditionRequest.newBuilder()
                        .setAddress(obsolete).setRendition("trash").setReason("RETENTION"))
                .build()).getReceipt();
        assertThat(receipt.getObjectsTargeted()).isEqualTo(1);
        assertThat(receipt.getVersionsTombstoned()).isEqualTo(1);
        assertThat(receipt.getObjectsPending()).isEqualTo(1);
    }

    /** Admitted but deliberately not reclaimed at the cut: the recording host's recovery lane is parked. */
    private void admitPendingDeletion(Host host) {
        capture.mutationAccount = BETA;
        capture.mutationOperationId = UUID.randomUUID().toString();
        capture.mutation = host.mutations().archiveMutation(ArchiveMutationRequest.newBuilder()
                .setOperationId(capture.mutationOperationId)
                .setDeleteRendition(DeleteRenditionRequest.newBuilder()
                        .setAddress(address(BETA, "records", "subject-record"))
                        .setRendition("scratch").setReason("RTBF"))
                .build()).getReceipt();
        assertThat(capture.mutation.getObjectsTargeted()).isEqualTo(2);
        assertThat(capture.mutation.getVersionsTombstoned()).isEqualTo(2);
        assertThat(capture.mutation.getObjectsPending()).isEqualTo(2);
    }

    private void seedUnversioned(Host host) {
        host.archives().createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setName("scratch").setAccountId(ALPHA).setDriveName("archive-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE)).build());
        var draft = address(ALPHA, "scratch", "draft");
        host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(draft)
                .addRenditions(rendition("original", "text/plain", labeled("draft v1")))
                .build());
        PutEntryResponse second = host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(draft)
                .addRenditions(rendition("original", "text/plain", labeled("draft v2")))
                .build());
        assertThat(second.getVersion()).isEqualTo(2);
    }

    /** One parts-based v2 document through the HTTP door, so repo_catalog holds real rows. */
    private void seedV2Document(Host host) throws Exception {
        capture.v2Account = ALPHA;
        capture.v2Datasource = "ds-abq";
        capture.v2Drive = "archive-drive";
        capture.v2DocId = "ops-manual";
        capture.v2Bytes = labeled("ops-manual v2 document body");
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + host.http().port() + UploadHttpServer.UPLOAD_PATH
                                + "?account_id=" + ALPHA + "&datasource_id=" + capture.v2Datasource
                                + "&drive=" + capture.v2Drive + "&filename=ops-manual.bin"
                                + "&doc_id=" + capture.v2DocId + "&operation_id=op-abq-manual"))
                .header("api_token", HTTP_TOKEN)
                .header("X-Content-Sha256", sha256(capture.v2Bytes))
                .POST(HttpRequest.BodyPublishers.ofByteArray(capture.v2Bytes)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        assertThat(response.body()).contains("\"revision\":1", "\"sha256\":\"" + sha256(capture.v2Bytes) + "\"");
        capture.v2Document = host.documents().getDocument(GetDocumentRequest.newBuilder()
                .setAddress(v2Address()).setAllowOpaque(true).build());
        assertThat(capture.v2Document.getManifest().getRevision()).isEqualTo(1);
        assertThat(capture.v2Document.getDocument().getBlobBag().hasBlob()).isTrue();
        capture.v2BlobId = capture.v2Document.getDocument().getBlobBag().getBlob().getBlobId();
        assertThat(readV2Blob(host)).isEqualTo(capture.v2Bytes);
    }

    private NodeAddress v2Address() {
        return NodeAddress.newBuilder()
                .setAccountId(capture.v2Account).setDocId(capture.v2DocId)
                .setGraphId("intake:" + capture.v2Account).setGraphAddressId(capture.v2Datasource)
                .build();
    }

    /** The public blob byte path: header carries the whole-object SHA-256. */
    private byte[] readV2Blob(Host host) throws Exception {
        var out = new java.io.ByteArrayOutputStream();
        String headerSha = null;
        var frames = host.documents().readBlob(ai.protomolt.proto.repo.v2.ReadBlobRequest.newBuilder()
                .setAddress(v2Address()).setBlobId(capture.v2BlobId).build());
        while (frames.hasNext()) {
            var frame = frames.next();
            if (frame.hasHeader()) {
                headerSha = frame.getHeader().getSha256();
                assertThat(frame.getHeader().getTotalLength()).isEqualTo(capture.v2Bytes.length);
            } else {
                out.write(frame.getData().toByteArray());
            }
        }
        assertThat(headerSha).isEqualTo(sha256(capture.v2Bytes));
        return out.toByteArray();
    }

    /** Record every retained version's read, every listing and every stat through the gRPC door. */
    private void recordReads(Host host) {
        recordEntry(host, "report.pdf#2026", address(ALPHA, "docs", "report.pdf#2026"), 1, 3);
        recordEntry(host, "notes#1", capture.notesAddress, 1, 1);
        recordEntry(host, "subject-record", address(BETA, "records", "subject-record"), 1, 2);
        // The unversioned archive retains only the current state.
        recordEntry(host, "draft", address(ALPHA, "scratch", "draft"), 2, 2);
        // Version 1 carries the completed deletion: its "trash" rendition is
        // tombstoned and its object already reclaimed before the cut.
        recordEntry(host, "obsolete#1", address(ALPHA, "docs", "obsolete#1"), 1, 1);
        capture.listings.put(ALPHA + "/docs", host.archives().listEntries(
                ListEntriesRequest.newBuilder().setAccountId(ALPHA).setArchive("docs").build()));
        capture.listings.put(BETA + "/records", host.archives().listEntries(
                ListEntriesRequest.newBuilder().setAccountId(BETA).setArchive("records").build()));
        capture.listings.put(ALPHA + "/scratch", host.archives().listEntries(
                ListEntriesRequest.newBuilder().setAccountId(ALPHA).setArchive("scratch").build()));
        for (var pair : List.of(new String[] {ALPHA, "docs"}, new String[] {BETA, "records"},
                new String[] {ALPHA, "scratch"})) {
            capture.stats.put(pair[0] + "/" + pair[1], host.archives().getArchiveStats(
                    GetArchiveStatsRequest.newBuilder().setAccountId(pair[0]).setArchive(pair[1]).build())
                    .getStats());
        }
    }

    private void recordEntry(Host host, String tag, EntryAddress address, long firstVersion, long lastVersion) {
        var versions = new ArrayList<GetEntryResponse>();
        String entryUuid = null;
        for (long version = firstVersion; version <= lastVersion; version++) {
            var read = host.archives().getEntry(GetEntryRequest.newBuilder()
                    .setAddress(address).setVersion(version).build());
            versions.add(read);
            entryUuid = read.getManifest().getMetadataSnapshot().getEntryUuid();
        }
        capture.entries.add(new RecordedEntry(tag, address, entryUuid, versions,
                host.archives().listVersions(ListVersionsRequest.newBuilder().setAddress(address).build())));
    }

    // ------------------------------------------------------------------
    // Ledger capture helpers (direct JDBC: the backup's ground truth)
    // ------------------------------------------------------------------

    private void captureQuiescence(Connection db) throws SQLException {
        capture.archiveState.put("archiveReadPins", count(db, "SELECT count(*) FROM archive_read_pins"));
        // Durable terminal rows (LIVE uploads, DELETED cleanup tombstones) are
        // data and travel with the backup; only active in-flight work
        // (STAGING, VERIFIED, DELETING) makes a cut unsafe.
        capture.archiveState.put("uploadsInFlight",
                count(db, "SELECT count(*) FROM archive_object_uploads WHERE state NOT IN ('LIVE', 'DELETED')"));
        capture.archiveState.put("uploadsDeleted",
                count(db, "SELECT count(*) FROM archive_object_uploads WHERE state = 'DELETED'"));
        capture.archiveState.put("uploads", count(db, "SELECT count(*) FROM archive_object_uploads"));
        capture.archiveState.put("admittedMutations", count(db, "SELECT count(*) FROM archive_mutations"));
        capture.archiveState.put("pendingTargets", count(db,
                "SELECT count(*) FROM archive_mutation_targets t JOIN archive_object_uploads u USING(object_id)"
                        + " WHERE u.state = 'LIVE'"));
        capture.archiveState.put("otherBackends",
                count(db, "SELECT count(*) FROM pg_stat_activity"
                        + " WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()"));
        assertThat(capture.archiveState.get("archiveReadPins")).as("open archive read pins at the cut").isZero();
        assertThat(capture.archiveState.get("uploadsInFlight"))
                .as("uploads in flight at the cut (no upload or cleanup active)").isZero();
        assertThat(capture.archiveState.get("uploadsDeleted"))
                .as("the completed pre-cut deletion is retained as a DELETED tombstone").isEqualTo(1L);
        assertThat(capture.archiveState.get("otherBackends"))
                .as("client backends other than this capture connection after host close").isZero();
        assertThat(capture.archiveState.get("admittedMutations")).isEqualTo(2L);
        assertThat(capture.archiveState.get("pendingTargets")).isEqualTo(2L);
    }

    private static long count(Connection db, String sql) throws SQLException {
        try (var statement = db.createStatement(); var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    /** Every base table of both schemas, fingerprinted over ordered per-row hashes. */
    private static Map<String, String> tableFingerprints(Connection db) throws SQLException {
        var tables = new ArrayList<String>();
        try (var statement = db.createStatement(); var rows = statement.executeQuery(
                "SELECT table_schema || '.' || table_name FROM information_schema.tables"
                        + " WHERE table_schema IN ('public', 'repo_catalog') AND table_type = 'BASE TABLE'"
                        + " ORDER BY 1")) {
            while (rows.next()) tables.add(rows.getString(1));
        }
        var fingerprints = new LinkedHashMap<String, String>();
        for (var table : tables) {
            int dot = table.indexOf('.');
            var schema = table.substring(0, dot);
            var tableName = table.substring(dot + 1);
            try (var statement = db.createStatement(); var rows = statement.executeQuery(
                    "SELECT md5(coalesce(string_agg(r, '' ORDER BY r), '')) FROM (SELECT md5(t::text) AS r FROM \""
                            + schema + "\".\"" + tableName + "\" t) s")) {
                assertThat(rows.next()).isTrue();
                fingerprints.put(table, rows.getString(1));
            }
        }
        return fingerprints;
    }

    private static Map<String, Long> sequenceValues(Connection db) throws SQLException {
        var sequences = new LinkedHashMap<String, Long>();
        var names = new ArrayList<String>();
        try (var statement = db.createStatement(); var rows = statement.executeQuery(
                "SELECT sequence_schema || '.' || sequence_name FROM information_schema.sequences"
                        + " WHERE sequence_schema IN ('public', 'repo_catalog') ORDER BY 1")) {
            while (rows.next()) names.add(rows.getString(1));
        }
        for (var name : names) {
            try (var statement = db.createStatement();
                    var rows = statement.executeQuery("SELECT last_value FROM " + name)) {
                assertThat(rows.next()).isTrue();
                sequences.put(name, rows.getLong(1));
            }
        }
        return sequences;
    }

    private List<RecordedObject> objectInventory(Connection db) throws SQLException {
        var byUuid = new LinkedHashMap<String, RecordedEntry>();
        for (var entry : capture.entries) byUuid.put(entry.entryUuid(), entry);
        var objects = new ArrayList<RecordedObject>();
        try (var statement = db.createStatement(); var rows = statement.executeQuery(
                "SELECT b.object_id, b.entry_uuid, b.bucket, b.object_key,"
                        + " u.expected_size, u.sha256, u.provider_version, u.state"
                        + " FROM archive_object_uploads u JOIN archive_object_bindings b USING(object_id)"
                        + " ORDER BY b.object_key")) {
            while (rows.next()) {
                var entry = byUuid.get(rows.getString("entry_uuid"));
                objects.add(new RecordedObject(
                        rows.getString("object_id"),
                        entry == null ? rows.getString("entry_uuid") : entry.tag(),
                        renditionName(entry, rows.getString("object_key")),
                        rows.getString("bucket"), rows.getString("object_key"),
                        rows.getString("sha256"), rows.getString("provider_version"),
                        rows.getLong("expected_size"), rows.getString("state")));
            }
        }
        return objects;
    }

    private static String renditionName(RecordedEntry entry, String objectKey) {
        if (entry == null) return "?";
        for (var read : entry.versions()) {
            for (var rendition : read.getManifest().getRenditionsList()) {
                if (rendition.getObjectKey().equals(objectKey)) return rendition.getRendition().getName();
            }
        }
        return "?";
    }

    private static List<String> driveRows(Connection db) throws SQLException {
        var rows = new ArrayList<String>();
        try (var statement = db.createStatement(); var result = statement.executeQuery(
                "SELECT account_id, name, bucket, prefix, provider, region, status FROM drives ORDER BY 1, 2")) {
            while (result.next()) {
                rows.add(result.getString(1) + "|" + result.getString(2) + "|" + result.getString(3) + "|"
                        + result.getString(4) + "|" + result.getString(5) + "|" + result.getString(6) + "|"
                        + result.getString(7));
            }
        }
        return rows;
    }

    private static int reservePort() throws IOException {
        try (var socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    // ------------------------------------------------------------------
    // Restore procedure: fresh PostgreSQL cluster, fresh provider volume,
    // the recorded endpoint; the source is never overwritten.
    // ------------------------------------------------------------------

    private record Restored(PostgreSQLContainer db, String volume, String containerName) {}

    private Restored restore(String caseName) throws Exception {
        assertThat(Files.isRegularFile(backupDir.resolve("SEALED"))).as("the backup set is sealed").isTrue();
        var dump = backupDir.resolve("sql/ledger.dump");
        var tar = backupDir.resolve("provider/rustfs-data.tar");
        var manifest = JSON.readValue(Files.readString(backupDir.resolve("manifest.json")),
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() { });
        assertThat(sha256(Files.readAllBytes(dump))).isEqualTo(manifest.get("ledgerDumpSha256"));
        assertThat(sha256(Files.readAllBytes(tar))).isEqualTo(manifest.get("providerTarSha256"));

        var volume = volumeCreate("abq-" + runId + "-rst-" + caseName);
        dockerOk("run", "--rm", "-v", volume + ":/data",
                "-v", backupDir.resolve("provider").toString() + ":/backup", POSTGRES_IMAGE,
                "tar", "-xf", "/backup/rustfs-data.tar", "-C", "/data");
        var containerName = "abq-" + runId + "-rst-rustfs-" + caseName;
        startRustFs(containerName, volume, providerPort);
        var pg = startPostgres();
        dockerOk("cp", dump.toString(), pg.getContainerId() + ":/tmp/ledger.dump");
        var restored = pg.execInContainer("pg_restore", "--exit-on-error",
                "-U", DB_USER, "-d", DB_NAME, "/tmp/ledger.dump");
        assertThat(restored.getExitCode()).as("pg_restore stderr: %s", restored.getStderr()).isZero();
        assertThat(restored.getStderr()).doesNotContain("ERROR").doesNotContain("WARNING");
        return new Restored(pg, volume, containerName);
    }

    /** Every captured table fingerprint, sequence, drive row and entry row must be present — loudly, by name. */
    private void verifyRestoredLedger(Restored restored) throws SQLException {
        try (var db = connect(restored.db())) {
            var actual = tableFingerprints(db);
            var differing = new ArrayList<String>();
            for (var entry : capture.fingerprints.entrySet()) {
                if (!entry.getValue().equals(actual.get(entry.getKey()))) differing.add(entry.getKey());
            }
            assertThat(differing)
                    .as("tables whose restored content differs from the captured backup").isEmpty();
            assertThat(actual.keySet()).isEqualTo(capture.fingerprints.keySet());
            assertThat(sequenceValues(db)).isEqualTo(capture.sequences);
            assertThat(driveRows(db)).isEqualTo(capture.drives);
            verifyLedgerRowPresence(db);
        }
    }

    /** Row-level presence: a missing row fails by naming the affected entry and version. */
    private void verifyLedgerRowPresence(Connection db) throws SQLException {
        for (var entry : capture.entries) {
            for (var read : entry.versions()) {
                long version = read.getManifest().getVersion();
                try (var statement = db.prepareStatement(
                        "SELECT count(*) FROM archive_versions WHERE entry_uuid = ?::uuid AND version = ?")) {
                    statement.setString(1, entry.entryUuid());
                    statement.setLong(2, version);
                    var rows = statement.executeQuery();
                    rows.next();
                    if (rows.getLong(1) == 0) {
                        throw new IllegalStateException("Restored ledger is missing the archive_versions row"
                                + " for entry '" + entry.tag() + "' (account " + entry.address().getAccountId()
                                + ", archive " + entry.address().getArchive() + ", version " + version + ")");
                    }
                }
            }
        }
        for (var object : capture.objects) {
            try (var statement = db.prepareStatement(
                    "SELECT count(*) FROM archive_object_uploads WHERE object_id = ?::uuid")) {
                statement.setString(1, object.objectId());
                var rows = statement.executeQuery();
                rows.next();
                if (rows.getLong(1) == 0) {
                    throw new IllegalStateException("Restored ledger is missing the archive_object_uploads row"
                            + " for entry '" + object.entryTag() + "' rendition '" + object.rendition()
                            + "' (" + object.bucket() + "/" + object.key() + ")");
                }
            }
        }
    }

    /**
     * Every recorded LIVE object must read from the restored provider by its
     * recorded version id. A DELETED tombstone is the durable record of a
     * completed cleanup: its ledger row travels with the backup (the row-level
     * gate checks it) and its reclaimed provider object must stay absent.
     * Quiescence guarantees no other state reaches the capture.
     */
    private void verifyObjectsPresent(BlobStore store) throws Exception {
        for (var object : capture.objects) {
            if ("DELETED".equals(object.state())) {
                assertThatThrownBy(() -> store.getBounded(object.bucket(), object.key(),
                        object.providerVersion(), (int) object.size()))
                        .as("reclaimed object of entry '%s' rendition '%s' (%s/%s version %s)"
                                        + " stays absent from the restored provider",
                                object.entryTag(), object.rendition(), object.bucket(), object.key(),
                                object.providerVersion())
                        .isInstanceOf(BlobStore.BlobNotFoundException.class);
                continue;
            }
            if (!"LIVE".equals(object.state())) {
                throw new IllegalStateException("Captured object of entry '" + object.entryTag()
                        + "' rendition '" + object.rendition() + "' is in non-terminal state "
                        + object.state() + " — the quiescence gate should have refused the cut");
            }
            BlobStore.GetResult result;
            try {
                result = store.getBounded(object.bucket(), object.key(), object.providerVersion(),
                        (int) object.size());
            } catch (BlobStore.BlobNotFoundException missing) {
                throw new IllegalStateException("Restored provider is missing the archived object for entry '"
                        + object.entryTag() + "' rendition '" + object.rendition() + "' ("
                        + object.bucket() + "/" + object.key() + " version " + object.providerVersion() + ")",
                        missing);
            }
            assertThat(sha256(result.data()))
                    .as("restored bytes of entry '%s' rendition '%s' (%s/%s version %s)",
                            object.entryTag(), object.rendition(), object.bucket(), object.key(),
                            object.providerVersion())
                    .isEqualTo(object.sha256());
        }
    }

    // ------------------------------------------------------------------
    // Phase 2: restore into fresh stores; serve every entry and version
    // ------------------------------------------------------------------

    @Test @Order(2) @Timeout(900)
    void restoreIntoFreshStoresServesEveryEntryAndVersion() throws Exception {
        requireCapture();
        var restored = restore("main");
        try {
            verifyRestoredLedger(restored);
            // Provider identity survives the offline volume: every recorded object,
            // including the two pending mutation targets, reads by its recorded version id.
            try (var handle = openRawStore()) {
                verifyObjectsPresent(handle.store());
            }
            try (var host = bootHost(restored.db(), ACTIVE_LANE_MS, "abq-restored-" + runId)) {
                verifyEveryVersion(host);
                verifyListingsAndStats(host);
                verifyV2Document(host);
                verifyPendingMutationCompletes(host, restored);
                verifyReplaysFreshWriteAndLifecycle(host, restored);
            }
        } finally {
            dockerOk("stop", restored.containerName());
        }
        assertThat(volumeDigest(sourceVolume))
                .as("the source provider volume is never overwritten by a restore")
                .isEqualTo(sourceVolumeDigest);
    }

    private void verifyEveryVersion(Host host) throws Exception {
        for (var entry : capture.entries) {
            for (var recorded : entry.versions()) {
                long version = recorded.getManifest().getVersion();
                var read = host.archives().getEntry(GetEntryRequest.newBuilder()
                        .setAddress(entry.address()).setVersion(version).build());
                assertThat(read)
                        .as("entry '%s' version %d reads identically after cold restore",
                                entry.tag(), version)
                        .isEqualTo(recorded);
                for (var rendition : read.getManifest().getRenditionsList()) {
                    if (rendition.getState() != RenditionState.RENDITION_STATE_PRESENT) continue;
                    var data = read.getRenditionsList().stream()
                            .filter(r -> r.getRendition().getName().equals(rendition.getRendition().getName()))
                            .findFirst().orElseThrow();
                    assertThat(sha256(data.getData().toByteArray()))
                            .as("entry '%s' version %d rendition '%s' bytes hash to the ledger SHA-256",
                                    entry.tag(), version, rendition.getRendition().getName())
                            .isEqualTo(rendition.getSha256());
                }
            }
            assertThat(host.archives().listVersions(
                    ListVersionsRequest.newBuilder().setAddress(entry.address()).build()))
                    .as("version listing of '%s'", entry.tag())
                    .isEqualTo(entry.listing());
        }
    }

    private void verifyListingsAndStats(Host host) {
        for (var listing : capture.listings.entrySet()) {
            int slash = listing.getKey().indexOf('/');
            var account = listing.getKey().substring(0, slash);
            var archive = listing.getKey().substring(slash + 1);
            assertThat(host.archives().listEntries(ListEntriesRequest.newBuilder()
                    .setAccountId(account).setArchive(archive).build()))
                    .as("entry listing of %s", listing.getKey())
                    .isEqualTo(listing.getValue());
        }
        for (var stats : capture.stats.entrySet()) {
            int slash = stats.getKey().indexOf('/');
            var account = stats.getKey().substring(0, slash);
            var archive = stats.getKey().substring(slash + 1);
            assertThat(host.archives().getArchiveStats(GetArchiveStatsRequest.newBuilder()
                    .setAccountId(account).setArchive(archive).build()).getStats())
                    .as("archive stats of %s", stats.getKey())
                    .isEqualTo(stats.getValue());
        }
    }

    /** The v2 parts-based document reads back from the restored repo_catalog and provider. */
    private void verifyV2Document(Host host) throws Exception {
        var read = host.documents().getDocument(GetDocumentRequest.newBuilder()
                .setAddress(v2Address()).setAllowOpaque(true).build());
        assertThat(read)
                .as("v2 document '%s' reads identically after cold restore", capture.v2DocId)
                .isEqualTo(capture.v2Document);
        assertThat(readV2Blob(host))
                .as("v2 document blob bytes after restore, SHA-256 verified by the read path")
                .isEqualTo(capture.v2Bytes);
    }

    private ArchiveMutationReceipt awaitMutation(Host host, String account, String operationId)
            throws InterruptedException {
        var caller = new RepositoryCaller(Caller.operator().name(), true);
        var lookup = GetArchiveMutationRequest.newBuilder()
                .setAccountId(account).setOperationId(operationId).build();
        var operations = host.services().archiveMutationRepository();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        ArchiveMutationReceipt observed;
        do {
            observed = operations.getArchiveMutation(caller, lookup);
            if (observed.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED) return observed;
            Thread.sleep(200);
        } while (System.nanoTime() < deadline);
        return observed;
    }

    private void verifyPendingMutationCompletes(Host host, Restored restored) throws Exception {
        var observed = awaitMutation(host, capture.mutationAccount, capture.mutationOperationId);
        assertThat(observed.getState())
                .as("the mutation admitted before the cut completes through the restored host's recovery lane")
                .isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
        assertThat(observed.getObjectsTargeted()).isEqualTo(2);
        assertThat(observed.getObjectsConfirmedAbsent()).isEqualTo(2);
        assertThat(observed.getObjectsPending()).isZero();
        try (var db = connect(restored.db())) {
            assertThat(count(db, "SELECT count(*) FROM archive_object_uploads WHERE state = 'DELETED'"))
                    .as("both pending targets settled to DELETED alongside the pre-cut tombstone")
                    .isEqualTo(3);
        }
        // The reclaimed scratch objects are gone from the provider; the shared
        // original survives at both versions.
        try (var handle = openRawStore()) {
            for (var object : capture.objects) {
                if (!object.entryTag().equals("subject-record") || !object.rendition().equals("scratch")) continue;
                assertThatThrownBy(() -> handle.store().getBounded(
                        object.bucket(), object.key(), object.providerVersion(), (int) object.size()))
                        .as("reclaimed scratch object of '%s'", object.entryTag())
                        .isInstanceOf(BlobStore.BlobNotFoundException.class);
            }
        }
        var v1 = host.archives().getEntry(GetEntryRequest.newBuilder()
                .setAddress(address(BETA, "records", "subject-record")).setVersion(1)
                .addRenditions("original").build());
        assertThat(v1.getRenditions(0).getData().toByteArray()).isEqualTo(labeled("subject original"));
    }

    private void verifyReplaysFreshWriteAndLifecycle(Host host, Restored restored) throws Exception {
        // Replays return the recorded logical outcomes.
        var replay = host.mutations().archiveMutation(ArchiveMutationRequest.newBuilder()
                .setOperationId(capture.mutationOperationId)
                .setDeleteRendition(DeleteRenditionRequest.newBuilder()
                        .setAddress(address(BETA, "records", "subject-record"))
                        .setRendition("scratch").setReason("RTBF"))
                .build()).getReceipt();
        assertThat(replay.getOperationId()).isEqualTo(capture.mutationOperationId);
        assertThat(replay.getObjectsTargeted()).isEqualTo(capture.mutation.getObjectsTargeted());
        assertThat(replay.getVersionsTombstoned()).isEqualTo(capture.mutation.getVersionsTombstoned());

        var notesReplay = host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(capture.notesAddress)
                .addRenditions(rendition("original", "text/plain", labeled("notes original")))
                .addRenditions(rendition("empty", "text/plain", new byte[0]))
                .build());
        assertThat(notesReplay.getDeduplicated()).isTrue();
        assertThat(notesReplay.getVersion()).isEqualTo(1);

        // A fresh write advances past the restored counters; unchanged renditions
        // re-reference the same recorded bindings instead of copying bytes.
        var report = address(ALPHA, "docs", "report.pdf#2026");
        PutEntryResponse v4 = host.archives().putEntry(PutEntryRequest.newBuilder()
                .setAddress(report)
                .setTitle("Restored revision")
                .addRenditions(rendition("original", "application/pdf", labeled("report v1 original")))
                .addRenditions(rendition("markdown", "text/markdown", labeled("report v4 markdown")))
                .addRenditions(rendition("attachment.bin", "application/octet-stream",
                        largePayload("report v3 attachment.bin")))
                .build());
        assertThat(v4.getVersion()).isEqualTo(4);
        var recordedV3 = capture.entries.get(0).versions().get(2);
        assertThat(entryOf(v4.getManifest(), "original").getStorageObjectId())
                .isEqualTo(entryOf(recordedV3.getManifest(), "original").getStorageObjectId());
        assertThat(entryOf(v4.getManifest(), "attachment.bin").getStorageObjectId())
                .isEqualTo(entryOf(recordedV3.getManifest(), "attachment.bin").getStorageObjectId());
        for (long version = 1; version <= 3; version++) {
            // info{} tracks the entry's current state and legitimately moved to
            // v4; the per-version manifest and the rendition bytes are frozen.
            var read = host.archives().getEntry(GetEntryRequest.newBuilder()
                    .setAddress(report).setVersion(version).build());
            var recorded = capture.entries.get(0).versions().get((int) version - 1);
            assertThat(read.getManifest())
                    .as("entry 'report.pdf#2026' version %d manifest after the fresh write", version)
                    .isEqualTo(recorded.getManifest());
            for (var data : read.getRenditionsList()) {
                var recordedData = recorded.getRenditionsList().stream()
                        .filter(r -> r.getRendition().getName().equals(data.getRendition().getName()))
                        .findFirst().orElseThrow();
                assertThat(data.getData().toByteArray())
                        .as("entry 'report.pdf#2026' version %d rendition '%s' bytes after the fresh write",
                                version, data.getRendition().getName())
                        .isEqualTo(recordedData.getData().toByteArray());
            }
        }

        // An authorized lifecycle operation on the restored host: tombstone every
        // markdown version; the three distinct markdown objects are reclaimed and
        // the shared original and attachment stay LIVE and readable.
        var retire = host.mutations().archiveMutation(ArchiveMutationRequest.newBuilder()
                .setOperationId(UUID.randomUUID().toString())
                .setDeleteRendition(DeleteRenditionRequest.newBuilder()
                        .setAddress(report).setRendition("markdown").setReason("RETENTION"))
                .build()).getReceipt();
        assertThat(retire.getVersionsTombstoned()).isEqualTo(4);
        assertThat(retire.getObjectsTargeted()).isEqualTo(3);
        var settled = awaitMutation(host, ALPHA, retire.getOperationId());
        assertThat(settled.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
        assertThat(settled.getObjectsConfirmedAbsent()).isEqualTo(3);
        for (long version = 1; version <= 4; version++) {
            var read = host.archives().getEntry(GetEntryRequest.newBuilder()
                    .setAddress(report).setVersion(version).addRenditions("original").build());
            assertThat(read.getRenditions(0).getData().toByteArray()).isEqualTo(labeled("report v1 original"));
        }
        try (var db = connect(restored.db())) {
            assertThat(sequenceValues(db).get("public.archive_mutation_revision_seq"))
                    .as("the mutation revision sequence advances past the restored value")
                    .isGreaterThan(capture.mutationRevision);
        }
    }

    // ------------------------------------------------------------------
    // Phase 3: a missing provider object version fails loudly, by entry
    // ------------------------------------------------------------------

    @Test @Order(3) @Timeout(900)
    void missingProviderObjectFailsTheQualificationAndNamesTheEntry() throws Exception {
        requireCapture();
        var restored = restore("missing-object");
        try {
            verifyRestoredLedger(restored);
            var v1MarkdownKey = entryOf(capture.entries.get(0).versions().get(0).getManifest(), "markdown")
                    .getObjectKey();
            var target = capture.objects.stream()
                    .filter(o -> o.key().equals(v1MarkdownKey)).findFirst().orElseThrow();
            try (var handle = openRawStore()) {
                var outcome = handle.exactVersionReclaimer()
                        .reclaim(target.bucket(), target.key(), target.providerVersion());
                assertThat(outcome.status()).isEqualTo(ExactVersionOutcome.Status.DELETED);
                // The qualification's provider gate is the first thing that fails, naming the entry.
                assertThatThrownBy(() -> verifyObjectsPresent(handle.store()))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("report.pdf#2026")
                        .hasMessageContaining(target.key());
            }
            try (var host = bootHost(restored.db(), ACTIVE_LANE_MS, "abq-missing-object-" + runId)) {
                assertThatThrownBy(() -> host.archives().getEntry(GetEntryRequest.newBuilder()
                        .setAddress(address(ALPHA, "docs", "report.pdf#2026")).setVersion(1)
                        .addRenditions("markdown").build()))
                        .as("reading entry 'report.pdf#2026' version 1's missing markdown object")
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                failure -> assertThat(failure.getStatus().getCode())
                                        .isEqualTo(Status.Code.DATA_LOSS));
                // No substitution and no collateral damage: the sibling rendition
                // and the later versions still read byte-for-byte.
                var sibling = host.archives().getEntry(GetEntryRequest.newBuilder()
                        .setAddress(address(ALPHA, "docs", "report.pdf#2026")).setVersion(1)
                        .addRenditions("original").build());
                assertThat(sha256(sibling.getRenditions(0).getData().toByteArray()))
                        .isEqualTo(entryOf(capture.entries.get(0).versions().get(0).getManifest(), "original")
                                .getSha256());
                assertThat(host.archives().getEntry(GetEntryRequest.newBuilder()
                        .setAddress(address(ALPHA, "docs", "report.pdf#2026")).setVersion(2).build()))
                        .isEqualTo(capture.entries.get(0).versions().get(1));
            }
        } finally {
            dockerOk("stop", restored.containerName());
        }
    }

    // ------------------------------------------------------------------
    // Phase 4: a missing ledger row fails loudly, by entry
    // ------------------------------------------------------------------

    @Test @Order(4) @Timeout(900)
    void missingLedgerRowFailsTheQualificationAndNamesTheEntry() throws Exception {
        requireCapture();
        var restored = restore("missing-row");
        try {
            var subject = capture.entries.stream()
                    .filter(e -> e.tag().equals("subject-record")).findFirst().orElseThrow();
            try (var db = connect(restored.db());
                    var statement = db.prepareStatement(
                            "DELETE FROM archive_versions WHERE entry_uuid = ?::uuid AND version = 1")) {
                statement.setString(1, subject.entryUuid());
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            // The fingerprint gate names the damaged tables...
            assertThatThrownBy(() -> verifyRestoredLedger(restored))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("archive_versions");
            // ...and the row-level gate names the affected entry.
            try (var db = connect(restored.db())) {
                assertThatThrownBy(() -> verifyLedgerRowPresence(db))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("subject-record")
                        .hasMessageContaining("version 1");
            }
            try (var host = bootHost(restored.db(), ACTIVE_LANE_MS, "abq-missing-row-" + runId)) {
                assertThatThrownBy(() -> host.archives().getEntry(GetEntryRequest.newBuilder()
                        .setAddress(address(BETA, "records", "subject-record")).setVersion(1).build()))
                        .as("reading entry 'subject-record' version 1 whose ledger row was lost")
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                failure -> assertThat(failure.getStatus().getCode())
                                        .isEqualTo(Status.Code.NOT_FOUND));
                // The surviving current version is unaffected.
                assertThat(host.archives().getEntry(GetEntryRequest.newBuilder()
                        .setAddress(address(BETA, "records", "subject-record")).setVersion(2).build()))
                        .isEqualTo(capture.entries.get(2).versions().get(1));
            }
        } finally {
            dockerOk("stop", restored.containerName());
        }
    }

    // ------------------------------------------------------------------
    // Process-helper contract: the deadline bounds even a child that never
    // closes its output stream (the failure shape the temp-file drain
    // protects against). Runs without Docker; ordered last so it cannot
    // starve the container phases.
    // ------------------------------------------------------------------

    @Test @Order(5) @Timeout(120)
    void runProcessDestroysAChildThatHoldsStdoutOpenPastTheDeadline() {
        var started = System.nanoTime();
        assertThatThrownBy(() -> runProcess(List.of("sh", "-c", "sleep 600"), 1_000L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("timed out");
        assertThat(System.nanoTime() - started)
                .as("the helper enforces the deadline instead of blocking on the open stream")
                .isLessThan(TimeUnit.SECONDS.toNanos(60));
    }

    private void requireCapture() {
        if (capture == null) {
            throw new IllegalStateException("The capture phase must pass before restore phases run");
        }
    }
}
