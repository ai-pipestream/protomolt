package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.engine.ArchivePutAdmission;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Actual authenticated Netty archive calls backed by PostgreSQL and Redis. */
@Testcontainers
class BoundedArchiveTransportIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static final RepositoryCaller CALLER = new RepositoryCaller("bounded-transport", true);

    static final class Fixture implements AutoCloseable {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch readEntered = new CountDownLatch(1);
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final PayloadBudget budget = new PayloadBudget(16384);
        final RepoServices host;
        final ManagedChannel channel;
        final String token = UUID.randomUUID().toString();
        final ClientInterceptor credentials;
        final ArchiveServiceGrpc.ArchiveServiceBlockingStub archive;
        final ArchiveServiceGrpc.ArchiveServiceFutureStub future;
        final PutEntryRequest request;

        Fixture(boolean hold) {
            this(hold, false, 2048);
        }
        Fixture(boolean hold, boolean holdRead, int responseLimit) {
            this(hold, holdRead, responseLimit, 2048);
        }
        Fixture(boolean hold, boolean holdRead, int responseLimit, int manifestLimit) {
            var provider = new RedisBlobStoreProvider();
            var observed = new BlobStoreProvider() {
                public String id() { return "redis"; }
                public BackendIdentity managedIdentity(Map<String, String> options) { return provider.managedIdentity(options); }
                public OpenedBlobStore open(Map<String, String> options) {
                    var actual = provider.open(options);
                    var store = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                            new Class<?>[]{BlobStore.class}, (proxy, method, args) -> {
                                final Object result;
                                try { result = method.invoke(actual.store(), args); }
                                catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                                if (method.getName().equals("put")) {
                                    writes.incrementAndGet(); entered.countDown();
                                    if (hold && !release.await(20, TimeUnit.SECONDS)) throw new AssertionError("Provider not released");
                                }
                                if (method.getName().equals("getBounded")) {
                                    reads.incrementAndGet(); readEntered.countDown();
                                    if (holdRead && !release.await(20, TimeUnit.SECONDS)) throw new AssertionError("Read not released");
                                }
                                return result;
                            });
                    return new OpenedBlobStore(store, () -> { closes.incrementAndGet(); actual.close(); },
                            actual.capabilities(), actual::ensureNamespace, actual.reclaimer());
                }
            };
            var config = new RepoServiceConfig(0,
                    new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                    "http://127.0.0.1:1", "us-east-1", "unused", "unused", "bounded-transport", 0,
                    "redis", null, null, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 0, 1024)
                    .withManagedStorage(new ManagedStoragePolicy("bounded-transport", "bounded-realm", true));
            var profile = new BoundedArchiveProfile(new ArchivePutAdmission.Limits(16, 2048, 4), budget, 1,
                    new ai.protomolt.proto.repo.engine.ArchiveGetAdmission.Limits(16, responseLimit, 4, manifestLimit));
            host = new RepoServices(config, BridgeEngine.standard(), BlobStores.of(List.of(observed)), profile);
            String account = "remote-" + UUID.randomUUID();
            host.driveRepository().createDrive(CALLER, CreateDriveRequest.newBuilder().setAccountId(account).setName("storage").build());
            host.archiveRepository().createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setAccountId(account).setName("records").setDriveName("storage")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            request = PutEntryRequest.newBuilder().setAddress(EntryAddress.newBuilder().setAccountId(account)
                    .setArchive("records").setEntryId("entry")).addRenditions(RenditionContent.newBuilder()
                    .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                    .setData(ByteString.copyFromUtf8("first"))).build();
            assertThatThrownBy(() -> host.startBoundedArchiveNetty(0, " ")).isInstanceOf(IllegalArgumentException.class);
            var server = host.startBoundedArchiveNetty(0, token);
            channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
            var headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
            credentials = MetadataUtils.newAttachHeadersInterceptor(headers);
            archive = ArchiveServiceGrpc.newBlockingStub(channel).withInterceptors(credentials).withDeadlineAfter(10, TimeUnit.SECONDS);
            future = ArchiveServiceGrpc.newFutureStub(channel).withInterceptors(credentials);
        }

        void awaitBudget(long expected) throws InterruptedException {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (budget.reservedBytes() != expected && System.nanoTime() < until) Thread.sleep(5);
            assertThat(budget.reservedBytes()).isEqualTo(expected);
        }

        @Override public void close() throws Exception {
            release.countDown(); channel.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            host.close();
            assertThat(budget.reservedBytes()).isZero();
            assertThat(closes.get()).isEqualTo(1);
        }
    }

    @Test void authenticatedRemoteAndLocalArchiveOperationsShareHistoryAndRetries() throws Exception {
        try (var f = new Fixture(false)) {
            assertThatThrownBy(() -> ArchiveServiceGrpc.newBlockingStub(f.channel).putEntry(f.request))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
            f.awaitBudget(0); assertThat(f.writes.get()).isZero();
            var first = f.archive.putEntry(f.request);
            assertThat(first.getVersion()).isEqualTo(1);
            f.awaitBudget(0);
            assertThat(f.host.archiveRepository().putEntry(CALLER, f.request).getVersion()).isEqualTo(1);
            assertThat(f.writes.get()).isEqualTo(1);
            assertThat(f.archive.putEntry(f.request.toBuilder().setRenditions(0, f.request.getRenditions(0).toBuilder()
                    .setData(ByteString.copyFromUtf8("second"))).build()).getVersion()).isEqualTo(2);
            f.awaitBudget(0);
            var old = GetEntryRequest.newBuilder().setAddress(f.request.getAddress()).setVersion(1).build();
            assertThat(f.archive.getEntry(old).getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first"));
            f.awaitBudget(0);
            assertThat(f.host.archiveRepository().getEntry(CALLER, old).getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first"));
            assertThatThrownBy(() -> f.archive.bridgeEntry(BridgeEntryRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
            f.awaitBudget(0);
        }
    }

    @Test void manifestJsonLimitRefusesLocalAndRemoteReadsBeforeProviderIo() throws Exception {
        try (var f = new Fixture(false, false, 2048, 1)) {
            f.archive.putEntry(f.request); f.awaitBudget(0);
            var manifest = GetEntryManifestRequest.newBuilder().setAddress(f.request.getAddress()).build();
            var entry = GetEntryRequest.newBuilder().setAddress(f.request.getAddress()).build();
            assertThatThrownBy(() -> f.archive.getEntryManifest(manifest)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            f.awaitBudget(0);
            assertThatThrownBy(() -> f.archive.getEntry(entry)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            f.awaitBudget(0);
            assertThatThrownBy(() -> f.host.archiveRepository().getManifest(CALLER, manifest))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
            assertThat(f.reads.get()).isZero();
            assertThat(f.budget.reservedBytes()).isZero();
            // A manifest-free list remains usable under the same tiny JSON allowance.
            var list = ListEntriesRequest.newBuilder().setAccountId(f.request.getAddress().getAccountId())
                    .setArchive("records").build();
            assertThat(f.archive.listEntries(list).getEntriesCount()).isEqualTo(1);
            f.awaitBudget(0);
        }
    }

    @Test void oversizedMetadataAndListRepliesAreRefusedWithoutPoisoningSubsequentReads() throws Exception {
        try (var f = new Fixture(false, false, 64)) {
            f.archive.putEntry(f.request); f.awaitBudget(0);
            var manifest = GetEntryManifestRequest.newBuilder().setAddress(f.request.getAddress()).build();
            var listing = ListEntriesRequest.newBuilder().setAccountId(f.request.getAddress().getAccountId())
                    .setArchive("records").build();
            // Local construction is deliberately outside the transport-only metadata cap.
            assertThat(f.host.archiveRepository().getManifest(CALLER, manifest).getSerializedSize()).isGreaterThan(64);
            assertThat(f.host.archiveRepository().listEntries(CALLER, listing).getSerializedSize()).isGreaterThan(64);
            assertThatThrownBy(() -> f.archive.getEntryManifest(manifest)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            f.awaitBudget(0);
            assertThatThrownBy(() -> f.archive.listEntries(listing)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            f.awaitBudget(0);
            var empty = ListArchivesRequest.newBuilder().setAccountId("empty-" + UUID.randomUUID()).build();
            assertThat(f.archive.listArchives(empty).getArchivesCount()).isZero();
            f.awaitBudget(0);
            assertThat(f.reads.get()).isZero();
            assertThat(f.writes.get()).isEqualTo(1);
        }
    }

    @Test void cancelledRemoteReadRetainsConstructionAndTransportAllowancesUntilProviderReturns() throws Exception {
        try (var f = new Fixture(false, true, 2048)) {
            f.archive.putEntry(f.request); f.awaitBudget(0);
            var request = GetEntryRequest.newBuilder().setAddress(f.request.getAddress()).build();
            var pending = f.future.getEntry(request);
            assertThat(f.readEntered.await(5, TimeUnit.SECONDS)).isTrue();
            long held = f.budget.reservedBytes();
            assertThat(held).isGreaterThan(4096 + 2048);
            assertThat(pending.cancel(true)).isTrue();
            assertThatThrownBy(() -> f.archive.getEntry(request)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            assertThat(f.budget.reservedBytes()).isEqualTo(held);
            assertThat(f.closes.get()).isZero();
            f.release.countDown(); f.awaitBudget(0);
            assertThat(f.archive.getEntry(request).getRenditions(0).getData()).isEqualTo(f.request.getRenditions(0).getData());
            f.awaitBudget(0);
        }
    }

    @Test void managedLocalAndRemoteReadsRefuseOversizedEnvelopeBeforeProviderIo() throws Exception {
        try (var f = new Fixture(false, false, 64)) {
            f.archive.putEntry(f.request); f.awaitBudget(0);
            var request = GetEntryRequest.newBuilder().setAddress(f.request.getAddress()).build();
            assertThatThrownBy(() -> f.archive.getEntry(request)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            f.awaitBudget(0);
            assertThatThrownBy(() -> f.host.archiveRepository().getEntry(CALLER, request))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
            assertThat(f.reads.get()).isZero();
            assertThat(f.budget.reservedBytes()).isZero();
        }
    }

    @Test void shutdownTimeoutRetainsCancelledReadResourcesUntilRealProviderReturns() throws Exception {
        try (var f = new Fixture(false, true, 2048)) {
            f.archive.putEntry(f.request); f.awaitBudget(0);
            var pending = f.future.getEntry(GetEntryRequest.newBuilder().setAddress(f.request.getAddress()).build());
            assertThat(f.readEntered.await(5, TimeUnit.SECONDS)).isTrue();
            long held = f.budget.reservedBytes();
            pending.cancel(true);
            assertThatThrownBy(() -> f.host.close(java.time.Duration.ofMillis(100)))
                    .isInstanceOfSatisfying(RepositoryDrainTimeoutException.class,
                            e -> assertThat(e.phase()).isEqualTo(RepositoryDrainTimeoutException.Phase.ARCHIVE_RPC));
            assertThat(f.closes.get()).isZero();
            assertThat(f.budget.reservedBytes()).isEqualTo(held);
            f.release.countDown(); f.awaitBudget(0);
            f.host.close();
            assertThat(f.closes.get()).isEqualTo(1);
        }
    }

    @Test void cancelledRemotePutRetainsBothReservationsUntilActualRedisWorkReturns() throws Exception {
        try (var f = new Fixture(true)) {
            var pending = f.future.putEntry(f.request);
            assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
            long reserved = 4096 + f.request.getSerializedSize() + 4L * f.request.getRenditions(0).getData().size();
            assertThat(f.budget.reservedBytes()).isEqualTo(reserved);
            assertThat(pending.cancel(true)).isTrue();
            assertThatThrownBy(() -> f.archive.getArchive(GetArchiveRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            // Authentication still runs first when all request slots are occupied.
            assertThatThrownBy(() -> ArchiveServiceGrpc.newBlockingStub(f.channel).putEntry(f.request))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
            assertThat(f.budget.reservedBytes()).isEqualTo(reserved);
            assertThat(f.closes.get()).isZero(); assertThat(f.writes.get()).isEqualTo(1);
            f.release.countDown(); f.awaitBudget(0);
            assertThat(f.archive.putEntry(f.request).getVersion()).isEqualTo(1);
            f.awaitBudget(0); assertThat(f.writes.get()).isEqualTo(1);
        }
    }

    @Test void hostShutdownWaitsForCancelledTransportAndProviderToDrain() throws Exception {
        try (var f = new Fixture(true); var executor = Executors.newSingleThreadExecutor()) {
            var pending = f.future.putEntry(f.request);
            assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
            long reserved = f.budget.reservedBytes();
            var closing = executor.submit(() -> { f.host.close(); });
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            boolean closedAdmission = false;
            while (!closedAdmission && System.nanoTime() < until) {
                try { f.host.services(); }
                catch (UnsupportedOperationException stillOpen) { Thread.sleep(5); }
                catch (IllegalStateException closed) { closedAdmission = true; }
            }
            assertThat(closedAdmission).isTrue();
            assertThat(closing.isDone()).isFalse();
            assertThat(f.closes.get()).isZero();
            assertThat(f.budget.reservedBytes()).isEqualTo(reserved);
            pending.cancel(true);
            f.release.countDown();
            closing.get(10, TimeUnit.SECONDS);
            assertThat(f.closes.get()).isEqualTo(1);
            assertThat(f.budget.reservedBytes()).isZero();
        }
    }

    @Test void drainTimeoutKeepsAcceptedRpcAliveUntilRedisCompletion() throws Exception {
        try (var f = new Fixture(true); var executor = Executors.newSingleThreadExecutor()) {
            try {
                var pending = f.future.putEntry(f.request);
                assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
                long reserved = f.budget.reservedBytes();
                var closing = executor.submit(() -> f.host.close(java.time.Duration.ofMillis(100)));
                assertThatThrownBy(() -> closing.get(2, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).cause()
                        .isInstanceOfSatisfying(RepositoryDrainTimeoutException.class,
                                e -> assertThat(e.phase()).isEqualTo(RepositoryDrainTimeoutException.Phase.ARCHIVE_RPC))
                        .hasMessageContaining("Archive RPCs still active");
                assertThat(f.closes.get()).isZero();
                assertThat(f.budget.reservedBytes()).isEqualTo(reserved);
                assertThatThrownBy(() -> f.archive.getArchive(GetArchiveRequest.getDefaultInstance()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
                // Exceed the transport's former ten-second forced-cancellation window.
                assertThatThrownBy(() -> pending.get(11, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(f.closes.get()).isZero();
                assertThat(f.budget.reservedBytes()).isEqualTo(reserved);
                f.release.countDown();
                assertThat(pending.get(5, TimeUnit.SECONDS).getVersion()).isEqualTo(1);
                f.awaitBudget(0);
                f.host.close();
                assertThat(f.closes.get()).isEqualTo(1);
            } finally { f.release.countDown(); }
        }
    }

    @Test void mountedArchiveMethodsAreExplicitlyReviewedAndStreamingIsRejectedAtHeaders() throws Exception {
        var described = ArchiveServiceGrpc.getServiceDescriptor().getMethods().stream()
                .map(MethodDescriptor::getFullMethodName).collect(java.util.stream.Collectors.toSet());
        var reviewed = new java.util.HashSet<>(BoundedArchiveMethods.UNARY);
        reviewed.add(ArchiveServiceGrpc.getBridgeEntryMethod().getFullMethodName());
        reviewed.add(ArchiveServiceGrpc.getUploadRenditionMethod().getFullMethodName());
        assertThat(reviewed).isEqualTo(described);
        try (var f = new Fixture(false)) {
            var status = new CompletableFuture<Status>();
            var call = f.future.getChannel().newCall(ArchiveServiceGrpc.getUploadRenditionMethod(),
                    f.future.getCallOptions().withDeadlineAfter(5, TimeUnit.SECONDS));
            var stream = io.grpc.stub.ClientCalls.asyncClientStreamingCall(call,
                    new io.grpc.stub.StreamObserver<UploadRenditionResponse>() {
                        public void onNext(UploadRenditionResponse ignored) { status.completeExceptionally(new AssertionError("Unexpected result")); }
                        public void onError(Throwable error) { status.complete(Status.fromThrowable(error)); }
                        public void onCompleted() { status.completeExceptionally(new AssertionError("Unexpected completion")); }
                    });
            stream.onNext(UploadRenditionRequest.getDefaultInstance()); stream.onCompleted();
            assertThat(status.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
            f.awaitBudget(0); assertThat(f.writes.get()).isZero();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void oversizedArchiveInputFailsBeforeStorageAndReleasesIngress(boolean compressed) throws Exception {
        try (var f = new Fixture(false)) {
            var large = f.request.toBuilder().setRenditions(0, f.request.getRenditions(0).toBuilder()
                    .setData(ByteString.copyFrom(new byte[4096]))).build();
            var stub = compressed ? f.archive.withCompression("gzip") : f.archive;
            assertThatThrownBy(() -> stub.putEntry(large)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(compressed ? Status.Code.UNKNOWN : Status.Code.RESOURCE_EXHAUSTED));
            f.awaitBudget(0); assertThat(f.writes.get()).isZero();
            assertThat(f.archive.putEntry(f.request).getVersion()).isEqualTo(1);
        }
    }

    @Test void malformedArchiveRequestDrainsWithoutWriting() throws Exception {
        try (var f = new Fixture(false)) {
            var descriptor = MethodDescriptor.<byte[], PutEntryResponse>newBuilder()
                    .setFullMethodName(ArchiveServiceGrpc.getPutEntryMethod().getFullMethodName())
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(PutEntryResponse.getDefaultInstance()))
                    .setRequestMarshaller(new MethodDescriptor.Marshaller<byte[]>() {
                        public java.io.InputStream stream(byte[] value) { return new java.io.ByteArrayInputStream(value); }
                        public byte[] parse(java.io.InputStream input) { throw new AssertionError("Client-only request marshaller"); }
                    }).build();
            assertThatThrownBy(() -> io.grpc.stub.ClientCalls.blockingUnaryCall(f.future.getChannel(), descriptor,
                    CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), new byte[]{10, 127}))
                    .isInstanceOf(StatusRuntimeException.class);
            f.awaitBudget(0); assertThat(f.writes.get()).isZero();
            assertThat(f.archive.putEntry(f.request).getVersion()).isEqualTo(1);
        }
    }

    @Test void twoListenersShareOneHostIngressLimit() throws Exception {
        try (var f = new Fixture(true)) {
            var server = f.host.startBoundedArchiveNetty(0, f.token);
            var other = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
            try {
                var pending = f.future.putEntry(f.request);
                assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
                long reserved = f.budget.reservedBytes();
                var second = ArchiveServiceGrpc.newBlockingStub(other).withInterceptors(f.credentials)
                        .withDeadlineAfter(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> second.getArchive(GetArchiveRequest.getDefaultInstance()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
                assertThat(f.budget.reservedBytes()).isEqualTo(reserved);
                f.release.countDown();
                assertThat(pending.get(5, TimeUnit.SECONDS).getVersion()).isEqualTo(1);
                f.awaitBudget(0);
                assertThat(second.getEntry(GetEntryRequest.newBuilder().setAddress(f.request.getAddress()).build())
                        .getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first"));
            } finally {
                other.shutdownNow(); assertThat(other.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
