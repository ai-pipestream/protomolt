package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.service.DocumentHistoryGrpcService;
import ai.protomolt.proto.repo.spi.HistoricalDocumentRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;

/** Real transports over the caller's real repository; no alternate storage implementation. */
final class DocumentHistoricalTransportProbe {
    static void verifyHost(ai.protomolt.proto.repo.service.RepoServiceConfig config,
            DocumentPublishedRevision published, boolean typed) throws Exception {
        var access = new ai.protomolt.proto.repo.service.HistoricalReadAccess(caller -> {
            assertThat(caller.name()).isEqualTo("host-reader");
            assertThat(caller.unrestricted()).isFalse();
            return new RepositoryCaller(caller.name(), false, Set.of(published.getAddress().getAccountId()), Set.of());
        }, 32L * 1024 * 1024, 4);
        ai.protomolt.proto.authz.CallerResolver credentials = token -> "synthetic-host-reader-key".equals(token)
                ? java.util.Optional.of(Caller.scoped("host-reader", Set.of())) : java.util.Optional.empty();
        ReadRevisionResponse first = null;
        // New hosts/readers/providers each time, over the same retained revision.
        for (boolean serialized : new boolean[] {false, true}) {
            try (var host = ai.protomolt.proto.repo.service.RepoServices.build(config,
                    ai.protomolt.proto.asset.bridge.BridgeEngine.standard(), access)) {
                String name = "host-history-" + UUID.randomUUID();
                var server = serialized ? host.startNetty(0, "synthetic-host-operator-key", credentials)
                        : host.startInProcess(name, "synthetic-host-operator-key", credentials);
                var channel = serialized ? NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build()
                        : InProcessChannelBuilder.forName(name).build();
                try {
                    var request = ReadRevisionRequest.newBuilder().setAddress(published.getAddress()).setRevisionId(published.getRevisionId())
                            .setMode(typed ? HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED
                                    : HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW).build();
                    var stub = identified(DocumentHistoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS),
                            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), "synthetic-host-reader-key");
                    var response = stub.readRevision(request);
                    assertThat(response.getRevisionId()).isEqualTo(published.getRevisionId());
                    assertThat(response.getMutationRevision()).isEqualTo(published.getMutationRevision());
                    assertThat(response.hasValidated()).isEqualTo(typed);
                    if (first == null) first = response;
                    else assertThat(response).isEqualTo(first);
                } finally {
                    channel.shutdownNow();
                    assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                }
            }
        }
    }

    static void verifySize(HistoricalDocumentRepository history, DocumentPublishedRevision published, boolean oversized) throws Exception {
        var request = ReadRevisionRequest.newBuilder().setAddress(published.getAddress()).setRevisionId(published.getRevisionId())
                .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW).build();
        // Test-owned protobuf copies independently measure real wire framing.
        // Every individual fragment fits; manifest/framing can exceed the total cap.
        ReadRevisionResponse expected;
        try (var raw = history.readRaw(new RepositoryCaller("operator", true), published.getAddress(),
                UUID.fromString(published.getRevisionId()), ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
            var fragments = RawHistoricalDocument.newBuilder();
            for (var fragment : raw.fragments()) {
                assertThat(fragment.bytes().remaining()).isLessThan(8 * 1024 * 1024);
                fragments.addFragments(HistoricalDocumentFragment.newBuilder().setRevisionOrdinal(fragment.revisionOrdinal())
                        .setContent(com.google.protobuf.ByteString.copyFrom(fragment.bytes())));
            }
            expected = ReadRevisionResponse.newBuilder().setAddress(raw.address()).setRevisionId(raw.revision().toString())
                    .setMutationRevision(raw.publicationRevision()).setManifest(raw.manifest()).setRaw(fragments).build();
        }
        assertThat(expected.getSerializedSize() > 8 * 1024 * 1024).isEqualTo(oversized);
        var responses = new PayloadBudget(32L * 1024 * 1024);
        var service = new DocumentHistoryGrpcService(history, caller -> new RepositoryCaller(caller.name(), caller.unrestricted()), responses, 2);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).executor(executor)
                    .intercept(new ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor("synthetic-size-test-key"))
                    .addService(service).build().start();
            var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext()
                    .maxInboundMessageSize(8 * 1024 * 1024).build();
            try {
                var stub = identified(DocumentHistoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(20, TimeUnit.SECONDS),
                        Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), "synthetic-size-test-key");
                if (oversized) assertStatus(() -> stub.readRevision(request), Status.Code.RESOURCE_EXHAUSTED);
                else assertThat(stub.readRevision(request)).isEqualTo(expected);
            } finally {
                channel.shutdownNow(); server.shutdownNow();
                assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(responses.reservedBytes()).isZero();
        }
    }

    static void verify(HistoricalDocumentRepository history, DocumentPublishedRevision published, boolean typed,
            Runnable maintainReads, java.util.function.Consumer<Boolean> setReadable) throws Exception {
        for (boolean serialized : new boolean[] {false, true}) {
            var responseBudget = new PayloadBudget(32L * 1024 * 1024);
            var service = new DocumentHistoryGrpcService(history, authenticated -> switch (authenticated.name()) {
                case "owner" -> new RepositoryCaller(authenticated.name(), false, Set.of(published.getAddress().getAccountId()), Set.of());
                case "foreign" -> new RepositoryCaller(authenticated.name(), false, Set.of("different-account"), Set.of());
                case "bad-binding" -> new RepositoryCaller("changed-principal", false);
                default -> throw new AssertionError("Unexpected authenticated fixture identity");
            }, responseBudget, 4);
            String name = "historical-" + UUID.randomUUID();
            ServerBuilder<?> builder = serialized ? NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                    : InProcessServerBuilder.forName(name);
            var identity = Metadata.Key.of("test-authenticated-identity", Metadata.ASCII_STRING_MARSHALLER);
            var holdResponses = new java.util.concurrent.atomic.AtomicBoolean();
            var entered = new java.util.concurrent.CountDownLatch(4);
            var release = new java.util.concurrent.CountDownLatch(1);
            ServerInterceptor auth = new ServerInterceptor() {
                @Override public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                    var gated = new ForwardingServerCall.SimpleForwardingServerCall<Q, S>(call) {
                        @Override public void sendMessage(S response) {
                            if (holdResponses.get()) {
                                entered.countDown();
                                try {
                                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Transport fixture gate timed out");
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException("Transport fixture gate interrupted", interrupted);
                                }
                            }
                            super.sendMessage(response);
                        }
                    };
                    String principal = headers.get(identity);
                    if (principal == null) return next.startCall(gated, headers);
                    // Test transport authentication only. No production API-key lookup is claimed.
                    return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER,
                            Caller.scoped(principal, Set.of())), gated, headers, next);
                }
            };
            try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                Server server = builder.executor(executor).addService(ServerInterceptors.intercept(service, auth)).build().start();
                ManagedChannel channel = serialized ? NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext()
                        .maxInboundMessageSize(8 * 1024 * 1024).build()
                        : InProcessChannelBuilder.forName(name).maxInboundMessageSize(8 * 1024 * 1024).build();
                try {
                    var request = ReadRevisionRequest.newBuilder().setAddress(published.getAddress()).setRevisionId(published.getRevisionId())
                            .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW).build();
                    var stub = DocumentHistoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
                    assertStatus(() -> stub.readRevision(request), Status.Code.UNAUTHENTICATED);
                    assertStatus(() -> identified(stub, identity, "bad-binding").readRevision(request), Status.Code.PERMISSION_DENIED);
                    assertStatus(() -> identified(stub, identity, "foreign").readRevision(request), Status.Code.NOT_FOUND);
                    var owner = identified(stub, identity, "owner");
                    assertStatus(() -> owner.readRevision(request.toBuilder().clearMode().build()), Status.Code.INVALID_ARGUMENT);
                    try (var pressure = responseBudget.reserve(responseBudget.capacity() - 1)) {
                        assertStatus(() -> owner.readRevision(request), Status.Code.RESOURCE_EXHAUSTED);
                        assertThat(responseBudget.reservedBytes()).isEqualTo(pressure.bytes());
                    }
                    var raw = owner.readRevision(request);
                    assertThat(raw.getAddress()).isEqualTo(published.getAddress());
                    assertThat(raw.getRevisionId()).isEqualTo(published.getRevisionId());
                    assertThat(raw.getMutationRevision()).isEqualTo(published.getMutationRevision());
                    assertThat(raw.hasRaw()).isTrue();
                    try (var local = history.readRaw(new RepositoryCaller("owner", false,
                            Set.of(published.getAddress().getAccountId()), Set.of()), published.getAddress(),
                            UUID.fromString(published.getRevisionId()), ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
                        assertThat(raw.getManifest()).isEqualTo(local.manifest());
                        assertThat(raw.getRaw().getFragmentsCount()).isEqualTo(local.fragments().size());
                        for (int index = 0; index < local.fragments().size(); index++) {
                            var fragment = local.fragments().get(index);
                            assertThat(raw.getRaw().getFragments(index).getRevisionOrdinal()).isEqualTo(fragment.revisionOrdinal());
                            assertThat(raw.getRaw().getFragments(index).getContent()).isEqualTo(com.google.protobuf.ByteString.copyFrom(fragment.bytes()));
                        }
                    }
                    var validatedRequest = request.toBuilder().setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED).build();
                    if (typed) {
                        var validated = owner.readRevision(validatedRequest);
                        assertThat(validated.hasValidated()).isTrue();
                        assertThat(validated.getManifest()).isEqualTo(raw.getManifest());
                        assertThat(validated.getValidated().getDocument().getDocId()).isEqualTo(published.getAddress().getDocId());
                    } else assertStatus(() -> owner.readRevision(validatedRequest), Status.Code.FAILED_PRECONDITION);
                    var localCaller = new RepositoryCaller("owner", false, Set.of(published.getAddress().getAccountId()), Set.of());
                    var revision = UUID.fromString(published.getRevisionId());
                    var control = ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE;
                    try (var retained = typed ? history.readValidated(localCaller, published.getAddress(), revision, control)
                            : history.readRaw(localCaller, published.getAddress(), revision, control)) {
                        retained.authorizeDelivery(control);
                        try {
                            setReadable.accept(false);
                            assertStatus(() -> owner.readRevision(request), Status.Code.NOT_FOUND);
                            assertThatThrownBy(() -> retained.authorizeDelivery(control))
                                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                            error -> assertThat(error.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.NOT_FOUND));
                        } finally { setReadable.accept(true); }
                    }
                    long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    while (responseBudget.reservedBytes() != 0 && System.nanoTime() < end) Thread.sleep(5);
                    assertThat(responseBudget.reservedBytes()).isZero();
                    maintainReads.run();
                    // Pause real calls after snapshot construction, before the
                    // transport accepts the response. Cancellation must not release
                    // bytes still used by these producers or admit a fifth call.
                    holdResponses.set(true);
                    var headers = new Metadata(); headers.put(identity, "owner");
                    var async = DocumentHistoryServiceGrpc.newFutureStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
                            .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
                    var pending = new java.util.ArrayList<com.google.common.util.concurrent.ListenableFuture<ReadRevisionResponse>>();
                    try {
                        for (int index = 0; index < 4; index++) {
                            pending.add(async.readRevision(request));
                            // Fill response slots sequentially: provider-worker
                            // admission is a separate bounded resource under test elsewhere.
                            long gateDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                            while (entered.getCount() > 3 - index && System.nanoTime() < gateDeadline) Thread.sleep(5);
                            assertThat(entered.getCount()).as("transport producers: %s", pending).isEqualTo(3 - index);
                        }
                        assertThat(entered.await(5, TimeUnit.SECONDS)).as("transport producers: %s", pending).isTrue();
                        long retained = responseBudget.reservedBytes();
                        assertThat(retained).isPositive();
                        pending.forEach(future -> future.cancel(true));
                        assertThat(responseBudget.reservedBytes()).isEqualTo(retained);
                        assertStatus(() -> owner.readRevision(request), Status.Code.RESOURCE_EXHAUSTED);
                    } finally {
                        holdResponses.set(false); release.countDown();
                        pending.forEach(future -> future.cancel(true));
                    }
                    end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    while (responseBudget.reservedBytes() != 0 && System.nanoTime() < end) Thread.sleep(5);
                    assertThat(responseBudget.reservedBytes()).isZero();
                    assertThat(owner.readRevision(request).getRevisionId()).isEqualTo(published.getRevisionId());
                } finally {
                    release.countDown();
                    channel.shutdownNow(); server.shutdownNow();
                    assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                }
                assertThat(responseBudget.reservedBytes()).isZero();
            }
        }
    }

    private static DocumentHistoryServiceGrpc.DocumentHistoryServiceBlockingStub identified(
            DocumentHistoryServiceGrpc.DocumentHistoryServiceBlockingStub stub, Metadata.Key<String> key, String identity) {
        var headers = new Metadata(); headers.put(key, identity);
        return stub.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
    }
    private static void assertStatus(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, Status.Code expected) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(expected));
    }
}
