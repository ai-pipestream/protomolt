package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.StringValue;
import io.grpc.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Holds an actual Redis or Git reply while a managed historical host cancels and drains. */
public final class ManagedHistoricalShutdownProbe {
    public static void main(String[] args) throws Exception {
        for (var kind : ManagedHistoricalWorkerGate.Kind.values())
            for (boolean remote : new boolean[] {false, true}) run(Path.of(args[0]), remote, kind);
    }

    private static void run(Path bundle, boolean remote, ManagedHistoricalWorkerGate.Kind kind) throws Exception {
        var gate = new ManagedHistoricalWorkerGate(kind);
        boolean schema = kind == ManagedHistoricalWorkerGate.Kind.SCHEMA;
        boolean asynchronous = kind != ManagedHistoricalWorkerGate.Kind.PUT;
        var closes = new AtomicInteger();
        var real = new RedisBlobStoreProvider();
        BlobStoreProvider provider = new BlobStoreProvider() {
            public String id() { return "redis"; }
            public BackendIdentity managedIdentity(Map<String, String> options) { return real.managedIdentity(options); }
            public OpenedBlobStore open(Map<String, String> options) {
                var actual = real.open(options);
                return new OpenedBlobStore(gate.wrap(actual.store()), () -> { actual.close(); closes.incrementAndGet(); },
                        actual.capabilities(), actual::ensureNamespace, actual.reclaimer());
            }
        };
        var config = new RepoServiceConfig(0, new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "historical-shutdown", 0,
                "redis", null, null, System.getenv("PROTOMOLT_TEST_REDIS_URI"), 0, 1024 * 1024)
                .withManagedStorage(new ManagedStoragePolicy("historical-shutdown-" + UUID.randomUUID(), "historical-shutdown", true));
        var identity = new ReaderHostOptions(UUID.randomUUID(), "historical-shutdown", UUID.randomUUID().toString());
        var caller = new RepositoryCaller("operator", true);
        var definition = BoundedDocumentHostProbe.definition(StringValue.getDescriptor());
        var fresh = BoundedDocumentHostProbe.definition(com.google.protobuf.Timestamp.getDescriptor());
        Path directory = Files.createTempDirectory("historical-shutdown-git");
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var database = new LedgerDatabase(config.ledger())) {
            git.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
            git.putDescriptorSet(fresh.metadata().getArtifactSha256(), fresh.descriptors());
            var resolver = new RegistrySchemaResolver(gate.wrapRegistry(git, fresh.metadata().getArtifactSha256(), fresh.descriptors()),
                    new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
            ManagedSchemaAccess schemas = new ManagedSchemaAccess() {
                public DocumentSchemaAdmission.Resolution open(RepositoryCaller actual, DocumentPublicationMember member, RepositoryReadControl control) {
                    return resolver.open(occurrence -> {
                        var selected = occurrence.typeUrl().equals(fresh.metadata().getTypeUrl()) ? fresh : definition;
                        require(occurrence.typeUrl().equals(selected.metadata().getTypeUrl()), "fixture only resolves the declared schema identities");
                        return new RegistrySchemaResolver.Selected(selected.metadata(), selected.source());
                    }, control::check);
                }
                public void close() { resolver.close(); }
                public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
            };
            var serverCancelled = new CountDownLatch(1);
            var options = new ManagedPublicationOptions(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5),
                    (account, principal, operation) -> caller).withRecovery((account, principal, operation) -> caller)
                    .withHistoricalPublication(2).withTransport(new ManagedPublicationOptions.Transport(auth -> {
                        require(auth.caller().unrestricted(), "operator token authenticated independently");
                        Context.current().addListener(ignored -> serverCancelled.countDown(), Runnable::run);
                        return caller;
                    }, 32L * 1024 * 1024, 1));
            try (var host = new RepoServices(config, BridgeEngine.standard(), BlobStores.of(List.of(provider)),
                    new HistoricalReadAccess(auth -> caller, 32L * 1024 * 1024, 2), schemas, null, options.journaled(),
                    new BoundedDocumentProfile(1024 * 1024, 64L * 1024 * 1024), identity)) {
                var tx = new Tx(database.entityManagerFactory());
                var fixture = BoundedDocumentHostProbe.prepare(host, tx);
                var source = host.publicationRepository().publishDocument(caller, fixture.request(), RepositoryReadControl.NONE);
                require(source.hasCommitted(), "real typed source committed before shutdown scenario");
                var request = schema ? ManagedHistoricalHostProbe.historical(tx, caller, fixture.request(), source.getCommitted().getMembers(0),
                        ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(com.google.protobuf.Any.pack(
                                com.google.protobuf.Timestamp.newBuilder().setSeconds(5).build(), "type.test"))).build())
                        : ManagedHistoricalHostProbe.historical(tx, caller, fixture.request(), source.getCommitted().getMembers(0));
                gate.source(request.getIntent().getMembers(0).getPartsList().stream().filter(DocumentPublicationPart::hasHistoricalReuse)
                        .findFirst().orElseThrow().getHistoricalReuse().getObject());
                var publisher = host.publicationRepository();
                var cancelled = new AtomicBoolean();
                var control = new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                String name = "historical-shutdown-" + UUID.randomUUID();
                String token = UUID.randomUUID().toString();
                host.startInProcess(name, token, null);
                var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
                var headers = new Metadata();
                headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                var stub = DocumentPublicationServiceGrpc.newStub(channel)
                        .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers)).withDeadlineAfter(30, TimeUnit.SECONDS);
                var service = (DocumentPublicationGrpcService) host.services().stream()
                        .filter(value -> value instanceof DocumentPublicationGrpcService).findFirst().orElseThrow();
                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    gate.armed.set(true);
                    var rpcResult = new CompletableFuture<PublishDocumentResponse>();
                    var rpcCall = new AtomicReference<io.grpc.stub.ClientCallStreamObserver<PublishDocumentRequest>>();
                    Future<PublishDocumentResponse> publication;
                    if (remote) {
                        stub.publishDocument(request, new io.grpc.stub.ClientResponseObserver<PublishDocumentRequest, PublishDocumentResponse>() {
                            public void beforeStart(io.grpc.stub.ClientCallStreamObserver<PublishDocumentRequest> call) { rpcCall.set(call); }
                            public void onNext(PublishDocumentResponse value) { rpcResult.complete(value); }
                            public void onError(Throwable failure) { rpcResult.completeExceptionally(failure); }
                            public void onCompleted() {
                                rpcResult.completeExceptionally(new AssertionError("Historical RPC completed without a response"));
                            }
                        });
                        publication = rpcResult;
                    } else publication = executor.submit(() -> publisher.publishDocument(caller, request, control));
                    try {
                        require(gate.entered.await(10, TimeUnit.SECONDS), "historical publication reached actual selected worker reply");
                        gate.verifyBytes();
                        if (schema) require(resolver.stats().activeLoads() == 1 && resolver.stats().registryReads() == 2,
                                "fresh schema uses a real uncached Git load after the original source schema");
                        var pins = pins(tx, identity);
                        require(!pins.isEmpty(), "historical source has live pins before cancellation");
                        if (remote) {
                            rpcCall.get().cancel("cancel held historical publication", null);
                            try { publication.get(5, TimeUnit.SECONDS); throw new AssertionError("Historical RPC returned a receipt after cancellation"); }
                            catch (ExecutionException expected) { require(Status.fromThrowable(expected.getCause()).getCode() == Status.Code.CANCELLED,
                                    "client receives actual RPC CANCELLED status"); }
                            require(serverCancelled.await(5, TimeUnit.SECONDS), "historical cancellation reaches server");
                        } else cancelled.set(true);
                        try { host.close(Duration.ofMillis(200)); throw new AssertionError("Host closed before historical worker exited"); }
                        catch (RepositoryDrainTimeoutException expected) {
                            require(remote && expected.getMessage().equals("Publication RPCs still active; shared resources retained"), "RPC close retains active call");
                        } catch (IllegalStateException expected) {
                            require((!remote || asynchronous) && expected.getMessage().equals("Native publication resources still active; shared resources retained"), "close retains active worker");
                        }
                        require(closes.get() == 0 && gate.exited.getCount() == 1, "real provider stays open until worker exit");
                        if (!schema) require(pins(tx, identity).equals(pins), "timed-out close preserves exact historical source pins");
                        else require(!resolver.awaitLoads(Duration.ZERO) && resolver.stats().activeLoads() == 1,
                                "caller cancellation cannot release the actual registry worker");
                        require(hostState(tx, identity).equals("ACTIVE"), "live host cannot attest termination");
                        if (!asynchronous) {
                            if (remote) require(!service.awaitIdle(Duration.ZERO), "client cancellation does not release server producer");
                            else require(!publication.isDone(), "library worker remains held");
                        }
                        try (var connection = host.ledgerDataSource().getConnection(); var statement = connection.createStatement();
                             var result = statement.executeQuery("SELECT 1")) { require(result.next(), "SQL remains open for drain"); }
                        assertUnassessed(tx, request);
                        try { publisher.publishDocument(caller, request, RepositoryReadControl.NONE);
                            throw new AssertionError("Closed host admits new historical work");
                        } catch (RepositoryException expected) { require(expected.code() == RepositoryException.Code.UNAVAILABLE, "closed admission status"); }
                        gate.release.countDown();
                        require(gate.exited.await(5, TimeUnit.SECONDS), "held provider reply exits");
                        if (schema) require(resolver.awaitLoads(Duration.ofSeconds(5)) && resolver.stats().cachedBytes() == 0,
                                "closed resolver drains and does not cache a late schema result");
                        if (remote) require(service.awaitIdle(Duration.ofSeconds(10)), "historical RPC producer drains");
                        try { publication.get(10, TimeUnit.SECONDS); throw new AssertionError("Cancelled historical publication succeeded"); }
                        catch (ExecutionException expected) { require(remote
                                ? Status.fromThrowable(expected.getCause()).getCode() == Status.Code.CANCELLED
                                : BoundedPublicationShutdownProbe.hasCancellation(expected.getCause()), "publication reports cancellation"); }
                        assertUnassessed(tx, request);
                        gate.verifyBytes();
                        host.close(Duration.ofSeconds(5));
                        require(closes.get() == 1 && pins(tx, identity).isEmpty(), "final close releases provider and historical pins");
                        require(hostState(tx, identity).equals("FENCED"), "host fenced after real worker drain");
                        var readerStates = tx.readOnly(em -> em.createNativeQuery(
                                "SELECT state FROM repository_reader_incarnations WHERE host_execution=:id", String.class)
                                .setParameter("id", identity.execution()).getResultList());
                        require(readerStates.equals(List.of("QUIESCED")), "hosted reader attests quiescence after drainage");
                        host.close(Duration.ofSeconds(5));
                        require(closes.get() == 1, "repeated close does not close provider twice");
                    } finally { gate.release.countDown(); }
                } finally { channel.shutdownNow(); require(channel.awaitTermination(5, TimeUnit.SECONDS), "historical channel drained"); }
            } finally { schemas.close(); require(schemas.awaitIdle(Duration.ofSeconds(5)), "schema workers drained before Git close"); }
        } finally {
            try (var paths = Files.walk(directory)) { for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path); }
        }
        System.out.println("MANAGED_HISTORICAL_HELD_" + kind + "_" + (remote ? "RPC" : "LIBRARY") + "_OK");
    }

    private static List<String> pins(Tx tx, ReaderHostOptions host) {
        return tx.readOnly(em -> em.createNativeQuery("""
                SELECT p.pin_id::text FROM document_read_pins p JOIN repository_reader_incarnations r
                ON r.incarnation=p.reader_incarnation WHERE r.host_execution=:id ORDER BY p.pin_id
                """, String.class).setParameter("id", host.execution()).getResultList());
    }
    private static void assertUnassessed(Tx tx, PublishDocumentRequest request) {
        BoundedPublicationShutdownProbe.assertUncommitted(tx, request);
        long assessments = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_owners WHERE operation_id=:id")
                .setParameter("id", UUID.fromString(request.getIntent().getOperationId())).getSingleResult()).longValue());
        require(assessments == 0, "cancelled historical upload creates no assessment owner");
    }
    private static String hostState(Tx tx, ReaderHostOptions host) {
        return tx.readOnly(em -> (String) em.createNativeQuery("SELECT state FROM repository_reader_host_executions WHERE execution=:id", String.class)
                .setParameter("id", host.execution()).getSingleResult());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
