package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Separate bounded hosts qualify provider and schema worker lifetime during cancellation. */
public final class BoundedPublicationShutdownProbe {
    private enum Mode { LIBRARY, RPC, SCHEMA }
    public static void run(Path bundle) throws Exception {
        for (var mode:Mode.values()) run(bundle,mode);
    }
    private static void run(Path bundle,Mode mode) throws Exception {
        var gate=new PutGate();
        var closes=new AtomicInteger();
        var real=new RedisBlobStoreProvider();
        BlobStoreProvider provider=new BlobStoreProvider() {
            public String id() { return "redis"; }
            public BackendIdentity managedIdentity(Map<String,String> options) { return real.managedIdentity(options); }
            public OpenedBlobStore open(Map<String,String> options) {
                var actual=real.open(options);
                return new OpenedBlobStore(gate.wrap(actual.store()),() -> { actual.close(); closes.incrementAndGet(); },
                        actual.capabilities(),actual::ensureNamespace,actual.reclaimer());
            }
        };
        var config=new RepoServiceConfig(0,new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"),System.getenv("PROTOMOLT_TEST_PASSWORD")),
                "http://127.0.0.1:1","us-east-1","unused","unused","bounded-shutdown",0,
                "redis",null,null,System.getenv("PROTOMOLT_TEST_REDIS_URI"),0,1024*1024)
                .withManagedStorage(new ManagedStoragePolicy("bounded-shutdown-"+UUID.randomUUID(),"bounded-shutdown-realm",true));
        var definition=BoundedDocumentHostProbe.definition(BoundedDocumentRejectionProbe.constrainedType());
        Path directory=Files.createTempDirectory("publication-shutdown-git");
        try (var git=GitSchemaRegistryStore.builder().repositoryDir(directory).build();
                var database=new LedgerDatabase(config.ledger())) {
            git.putDescriptorSet(definition.metadata().getArtifactSha256(),definition.descriptors());
            var schemaGate=new BoundedPublicationSchemaShutdownProbe(definition.descriptors());
            var resolver=new RegistrySchemaResolver(mode==Mode.SCHEMA ? schemaGate.wrap(git) : git,
                    new DocumentSchemaArtifactCache.Limits(8_000_000,16,4_000_000),4,16);
            ManagedSchemaAccess schemas=new ManagedSchemaAccess() {
                public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller,DocumentPublicationMember member,RepositoryReadControl control) {
                    return resolver.open(occurrence -> new RegistrySchemaResolver.Selected(definition.metadata(),definition.source()),control::check);
                }
                public void close() { resolver.close(); }
                public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
            };
            var caller=new RepositoryCaller("operator",true);
            var options=new ManagedPublicationOptions(bundle,Duration.ofMinutes(5),Duration.ofSeconds(5),
                    (account,principal,operation) -> caller);
            var serverCancelled=new CountDownLatch(1);
            var observeFirst=new AtomicBoolean(true);
            if (mode==Mode.RPC) options=options.withTransport(new ManagedPublicationOptions.Transport(auth -> {
                if (observeFirst.compareAndSet(true,false))
                    io.grpc.Context.current().addListener(context -> serverCancelled.countDown(),Runnable::run);
                return caller;
            },32L*1024*1024,1));
            try (var host=new RepoServices(config,BridgeEngine.standard(),BlobStores.of(List.of(provider)),
                    new HistoricalReadAccess(auth -> caller,32L*1024*1024,2),schemas,null,options.journaled(),
                    new BoundedDocumentProfile(1024*1024,64L*1024*1024))) {
                var tx=new Tx(database.entityManagerFactory());
                if (mode==Mode.RPC) {
                    BoundedPublicationRpcCancellationProbe.run(host,tx,gate,closes,serverCancelled);
                    return;
                }
                if (mode==Mode.SCHEMA) {
                    schemaGate.run(host,tx,caller,resolver,gate,closes);
                    return;
                }
                var fixture=BoundedDocumentHostProbe.prepare(host,tx);
                var publisher=host.publicationRepository();
                var cancelled=new AtomicBoolean();
                var control=new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                gate.armed.set(true);
                try (var executor=Executors.newVirtualThreadPerTaskExecutor()) {
                    var publication=executor.submit(() -> publisher.publishDocument(caller,fixture.request(),control));
                    try {
                        require(gate.entered.await(10,TimeUnit.SECONDS),"real PUT reached shutdown gate");
                        gate.verifyBytes();
                        cancelled.set(true);
                        try { host.close(Duration.ofMillis(200)); throw new AssertionError("Closed with accepted provider work"); }
                        catch (IllegalStateException timeout) {
                            require(timeout.getMessage().equals("Native publication resources still active; shared resources retained"),
                                    "expected native drain timeout: "+timeout);
                        }
                        require(closes.get()==0 && gate.exited.getCount()==1 && !publication.isDone(),
                                "cancelled publisher still owns provider work and host resources");
                        try (var connection=host.ledgerDataSource().getConnection(); var statement=connection.createStatement();
                                var result=statement.executeQuery("SELECT 1")) { require(result.next(),"host SQL remains open"); }
                        assertUncommitted(tx,fixture.request());
                        try { publisher.publishDocument(caller,fixture.request(),RepositoryReadControl.NONE);
                            throw new AssertionError("New publication admitted after close"); }
                        catch (RepositoryException expected) { require(expected.code()==RepositoryException.Code.UNAVAILABLE,"closed admission refused"); }
                        gate.release.countDown();
                        require(gate.exited.await(5,TimeUnit.SECONDS),"real provider call exited");
                        try { publication.get(10,TimeUnit.SECONDS); throw new AssertionError("Cancelled publication succeeded"); }
                        catch (ExecutionException failed) {
                            require(hasCancellation(failed.getCause()),"cancellation remains in failure chain: "+failed.getCause());
                        }
                        assertUncommitted(tx,fixture.request());
                        gate.verifyBytes();
                        require(closes.get()==0,"physical bytes retained until worker settled and final close");
                        host.close(Duration.ofSeconds(5));
                        require(closes.get()==1,"provider closed once after publication settled");
                    } finally { gate.release.countDown(); }
                }
            } finally { schemas.close(); }
            require(closes.get()==1,"repeated host close does not release provider twice");
        } finally {
            try (var paths=Files.walk(directory)) {
                for (var path:paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
        System.out.println("BOUNDED_PUBLICATION_PUT_SHUTDOWN_OK");
    }

    static boolean hasCancellation(Throwable failure) {
        for (var cause=failure;cause!=null;cause=cause.getCause())
            if (cause instanceof CancellationException || cause instanceof RepositoryException r && r.code()==RepositoryException.Code.CANCELLED)
                return true;
        return false;
    }
    static void assertUncommitted(Tx tx,PublishDocumentRequest request) {
        for (String table:List.of("repository_operation_success","document_revision_commits")) {
            long rows=tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:id")
                    .setParameter("id",UUID.fromString(request.getIntent().getOperationId())).getSingleResult()).longValue());
            require(rows==0,"cancelled operation has no durable success or revision commit");
        }
    }
    static final class PutGate {
        final AtomicBoolean armed=new AtomicBoolean();
        final AtomicInteger puts=new AtomicInteger(),reads=new AtomicInteger();
        final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1),exited=new CountDownLatch(1);
        private BlobStore actual;
        private volatile BlobStore.PutSpec written;
        BlobStore wrap(BlobStore store) {
            actual=store;
            return (BlobStore)Proxy.newProxyInstance(BlobStore.class.getClassLoader(),new Class<?>[]{BlobStore.class},(proxy,method,args) -> {
                boolean held=method.getName().equals("put") && armed.compareAndSet(true,false);
                try {
                    var value=method.invoke(store,args);
                    if (method.getName().equals("put")) puts.incrementAndGet();
                    if (method.getName().equals("getBounded")) reads.incrementAndGet();
                    if (held) { written=(BlobStore.PutSpec)args[0]; entered.countDown(); awaitRelease(); }
                    return value;
                } catch (InvocationTargetException failure) { throw failure.getCause(); }
                finally { if (held) exited.countDown(); }
            });
        }
        void verifyBytes() {
            require(written!=null,"actual PUT completed");
            var bytes=actual.getBounded(written.bucket(),written.key(),null,1024*1024).data();
            require(DocumentPartCodec.sha256Hex(bytes).equals(written.sha256Hex()),"real Redis retains exact cancelled upload");
        }
        private void awaitRelease() {
            boolean interrupted=false;
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
            try {
                while (release.getCount()!=0) {
                    long remaining=deadline-System.nanoTime();
                    if (remaining<=0) throw new IllegalStateException("Publication PUT gate timed out");
                    try { release.await(remaining,TimeUnit.NANOSECONDS); }
                    catch (InterruptedException expected) { interrupted=true; }
                }
            } finally { if (interrupted) Thread.currentThread().interrupt(); }
        }
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
