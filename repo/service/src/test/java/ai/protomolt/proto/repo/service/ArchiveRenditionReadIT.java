package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionReadHeader;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveReadLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.engine.ArchiveObjectReader;
import ai.protomolt.proto.repo.engine.ArchiveObjectWriter;
import ai.protomolt.proto.repo.engine.ArchiveOperations;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * ReadRendition (#536) over real PostgreSQL and S3: a rendition streams in bounded
 * frames paced by the client, a range read is never reported as whole-object
 * integrity proof, and the read pin and budget window are released on every end.
 */
@Testcontainers
@Timeout(120)
class ArchiveRenditionReadIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final RepositoryCaller CALLER = new RepositoryCaller("rendition-read", true);
    private static final String BUCKET = "rendition-read";
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final long BUDGET = 256L * 1024 * 1024;
    private static LedgerDatabase database;
    private static Tx tx;
    private static ArchiveLedger ledger;
    private static DriveLedger drives;
    private static OpenedBlobStore opened;

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new ArchiveLedger(tx);
        drives = new DriveLedger(tx);
        var providers = BlobStores.discover();
        var options = Map.ofEntries(
                Map.entry("endpoint", S3.getEndpoint().toString()),
                Map.entry("region", S3.getRegion()),
                Map.entry("access-key", S3.getAccessKey()),
                Map.entry("secret-key", S3.getSecretKey()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000"));
        opened = providers.open("s3", options);
        opened.ensureNamespace(BUCKET);
        new ManagedBackendLedger(tx).bind("rendition-original", new ManagedBackendLedger.Profile(
                providers.managedIdentity("s3", options), "rendition-realm"));
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "rendition-drive";
        drive.bucket = BUCKET; drive.prefix = "archive"; drive.driveType = "CUSTOM";
        drives.insert(drive);
        managed(opened.store()).createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setAccountId("account").setName("records").setDriveName("rendition-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
    }

    @AfterAll static void close() throws Exception {
        try { if (opened != null) opened.close(); }
        finally { if (database != null) database.close(); }
    }

    @Test void aLargeRenditionStreamsWholeInBoundedFramesPacedByTheClient() throws Exception {
        long size = 24L * 1024 * 1024 + 123;
        var address = address();
        var landed = managed(opened.store()).uploadStream(CALLER, address, original(), size, "", null, null, null, null,
                new Generated(size, 536));
        var pulled = new AtomicLong();
        var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> counting(pulled));
        try (var harness = new Harness(operations(reader), new ReadByteBudget(BUDGET))) {
            var frames = new ArrayList<ReadRenditionResponse>();
            var widest = new AtomicLong();
            var received = new AtomicLong();
            var digest = MessageDigest.getInstance("SHA-256");
            harness.paced(request(address).build(), frame -> {
                frames.add(frame.hasHeader() ? frame : ReadRenditionResponse.getDefaultInstance());
                if (frame.hasData()) {
                    assertThat(frame.getData().size()).isBetween(1, ArchiveGrpcService.READ_FRAME_BYTES);
                    digest.update(frame.getData().asReadOnlyByteBuffer());
                    received.addAndGet(frame.getData().size());
                }
                // What the server pulled from the store and the client has not yet taken.
                widest.accumulateAndGet(pulled.get() - received.get(), Math::max);
            }).get(90, TimeUnit.SECONDS);

            var header = frames.getFirst().getHeader();
            assertThat(VALIDATOR.validate(header).valid()).isTrue();
            assertThat(header.getWholeObject()).isTrue();
            assertThat(header.getOffset()).isZero();
            assertThat(header.getLength()).isEqualTo(size);
            assertThat(header.getSizeBytes()).isEqualTo(size);
            assertThat(header.getSha256()).isEqualTo(landed.sha256());
            assertThat(received).hasValue(size);
            assertThat(HexFormat.of().formatHex(digest.digest())).isEqualTo(landed.sha256());
            assertThat(frames).hasSize(1 + (int) ((size + ArchiveGrpcService.READ_FRAME_BYTES - 1) / ArchiveGrpcService.READ_FRAME_BYTES));
            assertThat(widest.get()).as("bytes buffered ahead of the client").isLessThanOrEqualTo(3L * ArchiveGrpcService.READ_FRAME_BYTES);
            assertThat(harness.budget.remainingBytes()).isEqualTo(BUDGET);
            assertThat(pins()).isZero();
        }
    }

    @Test void aRangeIsServedWithoutClaimingWholeObjectIntegrity() throws Exception {
        var bytes = ByteString.copyFromUtf8("0123456789".repeat(1000));
        var address = address();
        var item = put(managed(opened.store()), address, bytes);
        try (var harness = new Harness(managed(opened.store()), new ReadByteBudget(BUDGET))) {
            var range = harness.read(request(address).setOffset(10).setLength(4000).build());
            var header = range.getFirst().getHeader();
            assertThat(VALIDATOR.validate(header).valid()).isTrue();
            assertThat(header.getWholeObject()).isFalse();
            assertThat(header.getOffset()).isEqualTo(10);
            assertThat(header.getLength()).isEqualTo(4000);
            assertThat(data(range)).isEqualTo(bytes.substring(10, 4010));

            // Corrupt the stored object outside the range: the range still reads, the whole object does not.
            var damaged = bytes.toByteArray();
            damaged[9000] ^= 1;
            opened.store().put(new BlobStore.PutSpec(BUCKET, item.getObjectKey(), "application/octet-stream", null, null), damaged);
            assertThat(data(harness.read(request(address).setOffset(10).setLength(4000).build())))
                    .isEqualTo(bytes.substring(10, 4010));
            assertThat(harness.failure(request(address).build()).getCode()).isEqualTo(Status.Code.DATA_LOSS);
            var tail = harness.read(request(address).setOffset(bytes.size() - 5).build());
            assertThat(tail.getFirst().getHeader().getLength()).isEqualTo(5);
            assertThat(tail.getFirst().getHeader().getWholeObject()).isFalse();
            assertThat(harness.budget.remainingBytes()).isEqualTo(BUDGET);
        }
    }

    @Test void legacyManifestsStreamFromTheirDrive() throws Exception {
        var bytes = ByteString.copyFromUtf8("legacy rendition bytes");
        var legacy = new ArchiveOperations(ledger, drives, opened.store());
        var address = address();
        var item = put(legacy, address, bytes);
        assertThat(item.getStorageObjectId()).isEmpty();
        try (var harness = new Harness(legacy, new ReadByteBudget(BUDGET))) {
            var whole = harness.read(request(address).build());
            assertThat(whole.getFirst().getHeader().getWholeObject()).isTrue();
            assertThat(data(whole)).isEqualTo(bytes);
            assertThat(data(harness.read(request(address).setOffset(7).setLength(9).build()))).isEqualTo(bytes.substring(7, 16));
        }
    }

    @Test void refusalsComeBeforeAnyProviderRead() throws Exception {
        var bytes = ByteString.copyFromUtf8("refusals");
        var address = address();
        put(managed(opened.store()), address, bytes);
        var opens = new AtomicInteger();
        var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> {
            opens.incrementAndGet();
            return opened.store();
        });
        try (var harness = new Harness(operations(reader), new ReadByteBudget(BUDGET))) {
            assertThat(harness.failure(request(address).setRendition("markdown").build()).getCode())
                    .isEqualTo(Status.Code.NOT_FOUND);
            assertThat(harness.failure(request(address).setSubKey("page-1").build()).getCode())
                    .isEqualTo(Status.Code.NOT_FOUND);
            assertThat(harness.failure(request(address).setVersion(9).build()).getCode())
                    .isEqualTo(Status.Code.NOT_FOUND);
            assertThat(harness.failure(request(address).setOffset(bytes.size() + 1).build()).getCode())
                    .isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(harness.failure(request(address).setOffset(2).setLength(bytes.size()).build()).getCode())
                    .isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(opens).hasValue(0);
        }
        try (var harness = new Harness(operations(reader), new ReadByteBudget(ArchiveGrpcService.READ_WINDOW_BYTES - 1))) {
            assertThat(harness.failure(request(address).build()).getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(opens).hasValue(0);
        }
        for (var outsider : List.of(new RepositoryCaller("tenant", false),
                new RepositoryCaller("tenant", false, Set.of("other-account"), Set.of())))
            assertThatThrownBy(() -> operations(reader).readRendition(outsider, request(address).build()))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
        assertThat(opens).hasValue(0);
        // A caller bound to the entry's account reads it, as GetEntry allows.
        var bound = new RepositoryCaller("tenant", false, Set.of("account"), Set.of());
        try (var read = operations(reader).readRendition(bound, request(address).build()).body()) {
            assertThat(read.readAllBytes()).isEqualTo(bytes.toByteArray());
        }
        assertThat(opens).hasValue(1);
    }

    @Test void aCancelledReadReleasesItsPinAndWindow() throws Exception {
        long size = 4L * 1024 * 1024;
        var address = address();
        managed(opened.store()).uploadStream(CALLER, address, original(), size, "", null, null, null, null,
                new Generated(size, 7));
        var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> opened.store());
        try (var harness = new Harness(operations(reader), new ReadByteBudget(BUDGET))) {
            var call = harness.channel.newCall(ArchiveServiceGrpc.getReadRenditionMethod(), CallOptions.DEFAULT);
            var firstData = new CompletableFuture<Void>();
            var ended = new CompletableFuture<Status>();
            ClientCalls.asyncServerStreamingCall(call, request(address).build(), new ClientResponseObserver<ReadRenditionRequest, ReadRenditionResponse>() {
                private ClientCallStreamObserver<ReadRenditionRequest> stream;
                @Override public void beforeStart(ClientCallStreamObserver<ReadRenditionRequest> requestStream) {
                    stream = requestStream;
                    requestStream.disableAutoRequestWithInitial(2);
                }
                @Override public void onNext(ReadRenditionResponse frame) {
                    if (frame.hasData()) {
                        firstData.complete(null);
                        stream.cancel("client walked away", null);
                    }
                }
                @Override public void onError(Throwable failure) { ended.complete(Status.fromThrowable(failure)); }
                @Override public void onCompleted() { ended.complete(Status.OK); }
            });
            firstData.get(30, TimeUnit.SECONDS);
            assertThat(ended.get(30, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.CANCELLED);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while ((pins() != 0 || harness.budget.remainingBytes() != BUDGET) && System.nanoTime() < deadline)
                Thread.sleep(10);
            assertThat(pins()).isZero();
            assertThat(harness.budget.remainingBytes()).isEqualTo(BUDGET);
        }
        reader.close();
        assertThat(reader.awaitIdle(Duration.ofSeconds(5))).isTrue();
    }

    // ------------------------------------------------------------------ helpers

    private static long pins() {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_read_pins")
                .getSingleResult()).longValue());
    }

    private static ArchiveOperations managed(BlobStore readerStore) {
        return operations(new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> {
            assertThat(generation).isEqualTo("rendition-original");
            assertThat(realm).isEqualTo("rendition-realm");
            return readerStore;
        }));
    }

    private static ArchiveOperations operations(ArchiveObjectReader reader) {
        return new ArchiveOperations(ledger, drives, opened.store(), BridgeEngine.standard(), reader,
                new ArchiveObjectWriter(new ArchiveUploadLedger(tx), opened.store(), "rendition-original",
                        opened.capabilities(), Duration.ofMinutes(5)));
    }

    private static EntryAddress address() {
        return EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
    }

    private static RenditionDescriptor original() {
        return RenditionDescriptor.newBuilder().setName("original").setMediaType("application/octet-stream").build();
    }

    private static ReadRenditionRequest.Builder request(EntryAddress address) {
        return ReadRenditionRequest.newBuilder().setAddress(address).setRendition("original");
    }

    private static RenditionManifestEntry put(ArchiveOperations operations, EntryAddress address, ByteString bytes) {
        return operations.putEntry(CALLER, PutEntryRequest.newBuilder().setAddress(address).addRenditions(
                RenditionContent.newBuilder().setRendition(original()).setData(bytes)).build()).getManifest().getRenditions(0);
    }

    private static ByteString data(List<ReadRenditionResponse> frames) {
        assertThat(frames.getFirst().hasHeader()).isTrue();
        var out = ByteString.EMPTY;
        for (var frame : frames.subList(1, frames.size())) {
            assertThat(frame.hasData()).isTrue();
            out = out.concat(frame.getData());
        }
        return out;
    }

    /** The real store with the bytes its streamed reads hand out counted as they are pulled. */
    private static BlobStore counting(AtomicLong pulled) {
        var store = opened.store();
        return (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class},
                (proxy, method, arguments) -> {
                    final Object result;
                    try {
                        result = method.invoke(store, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (!method.getName().equals("openStream")) return result;
                    return new FilterInputStream((InputStream) result) {
                        @Override public int read() throws IOException {
                            int value = super.read();
                            if (value >= 0) pulled.incrementAndGet();
                            return value;
                        }
                        @Override public int read(byte[] buffer, int offset, int length) throws IOException {
                            int read = super.read(buffer, offset, length);
                            if (read > 0) pulled.addAndGet(read);
                            return read;
                        }
                    };
                });
    }

    /** Deterministic bytes of a fixed size, generated as they are read. */
    private static final class Generated extends InputStream {
        private final SplittableRandom random;
        private long remaining;

        Generated(long size, long seed) {
            this.remaining = size;
            this.random = new SplittableRandom(seed);
        }

        @Override public int read() {
            if (remaining == 0) return -1;
            remaining--;
            return random.nextInt(256);
        }

        @Override public int read(byte[] buffer, int offset, int length) {
            if (remaining == 0) return -1;
            int count = (int) Math.min(length, remaining);
            for (int at = 0; at < count; at++) buffer[offset + at] = (byte) random.nextInt(256);
            remaining -= count;
            return count;
        }
    }

    /** An in-process ArchiveService over one repository and one read budget. */
    private static final class Harness implements AutoCloseable {
        final ReadByteBudget budget;
        final Server server;
        final ManagedChannel channel;

        Harness(ArchiveOperations operations, ReadByteBudget budget) throws IOException {
            this.budget = budget;
            var name = "rendition-read-" + UUID.randomUUID();
            server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(operations, budget)).build().start();
            channel = InProcessChannelBuilder.forName(name).build();
        }

        List<ReadRenditionResponse> read(ReadRenditionRequest request) {
            var frames = new ArrayList<ReadRenditionResponse>();
            ArchiveServiceGrpc.newBlockingStub(channel).readRendition(request).forEachRemaining(frames::add);
            return frames;
        }

        Status failure(ReadRenditionRequest request) {
            return catchThrowableOfType(StatusRuntimeException.class, () -> read(request)).getStatus();
        }

        /** Reads with one message requested at a time; each frame is handed over before the next request. */
        CompletableFuture<Void> paced(ReadRenditionRequest request, Consumer<ReadRenditionResponse> each) {
            var done = new CompletableFuture<Void>();
            var call = channel.newCall(ArchiveServiceGrpc.getReadRenditionMethod(), CallOptions.DEFAULT);
            ClientCalls.asyncServerStreamingCall(call, request, new ClientResponseObserver<ReadRenditionRequest, ReadRenditionResponse>() {
                private ClientCallStreamObserver<ReadRenditionRequest> stream;
                @Override public void beforeStart(ClientCallStreamObserver<ReadRenditionRequest> requestStream) {
                    stream = requestStream;
                    requestStream.disableAutoRequestWithInitial(1);
                }
                @Override public void onNext(ReadRenditionResponse frame) {
                    try {
                        each.accept(frame);
                    } catch (Throwable failure) {
                        done.completeExceptionally(failure);
                        stream.cancel("assertion failed", failure);
                        return;
                    }
                    stream.request(1);
                }
                @Override public void onError(Throwable failure) { done.completeExceptionally(failure); }
                @Override public void onCompleted() { done.complete(null); }
            });
            return done;
        }

        @Override public void close() throws InterruptedException {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
