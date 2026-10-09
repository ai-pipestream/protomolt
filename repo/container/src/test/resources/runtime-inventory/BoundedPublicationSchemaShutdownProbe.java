package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** The fixture owns Git; the host must drain its resolver before that borrowed store is released. */
final class BoundedPublicationSchemaShutdownProbe {
    private final ByteString expected;
    private final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1),exited=new CountDownLatch(1);
    BoundedPublicationSchemaShutdownProbe(ByteString expected) { this.expected=expected; }
    SchemaRegistryStore wrap(SchemaRegistryStore actual) {
        return (SchemaRegistryStore)Proxy.newProxyInstance(SchemaRegistryStore.class.getClassLoader(),
                new Class<?>[]{SchemaRegistryStore.class},(proxy,method,args) -> {
                    boolean held=method.getName().equals("descriptorSet");
                    try {
                        var value=method.invoke(actual,args);
                        if (held) {
                            require(value instanceof Optional<?> result && result.isPresent() && result.get().equals(expected),
                                    "real Git returned the exact retained descriptor bytes");
                            entered.countDown();
                            awaitRelease();
                        }
                        return value;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                    finally { if (held) exited.countDown(); }
                });
    }
    void run(RepoServices host,Tx tx,RepositoryCaller caller,RegistrySchemaResolver resolver,
            BoundedPublicationShutdownProbe.PutGate provider,AtomicInteger closes) throws Exception {
        var fixture=BoundedDocumentHostProbe.prepare(host,tx);
        var publisher=host.publicationRepository();
        var cancelled=new AtomicBoolean();
        var control=new RepositoryReadControl() {
            public boolean isCancelled() { return cancelled.get(); }
            public long remainingNanos() { return Long.MAX_VALUE; }
        };
        try (var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var publication=executor.submit(() -> publisher.publishDocument(caller,fixture.request(),control));
            try {
                require(entered.await(10,TimeUnit.SECONDS),"actual Git fetch reached held return");
                require(provider.puts.get()>0 && provider.reads.get()>0,"actual Redis staging preceded schema resolution");
                require(resolver.stats().registryReads()==1 && resolver.stats().activeLoads()==1,
                        "fresh resolver has one actual active registry load");
                cancelled.set(true);
                try { host.close(Duration.ofMillis(200)); throw new AssertionError("Host closed with live schema worker"); }
                catch (IllegalStateException timeout) {
                    require(timeout.getMessage().equals("Native publication resources still active; shared resources retained"),
                            "expected schema worker drain timeout: "+timeout);
                }
                try { publication.get(5,TimeUnit.SECONDS); throw new AssertionError("Cancelled schema admission returned publication"); }
                catch (ExecutionException failure) { require(BoundedPublicationShutdownProbe.hasCancellation(failure.getCause()),
                        "caller cancellation precedes resolver worker exit: "+failure.getCause()); }
                require(exited.getCount()==1 && !resolver.awaitLoads(Duration.ZERO) && closes.get()==0,
                        "resolver worker and provider resources survive caller cancellation");
                try (var connection=host.ledgerDataSource().getConnection(); var statement=connection.createStatement();
                        var result=statement.executeQuery("SELECT 1")) { require(result.next(),"SQL remains usable for schema worker drain"); }
                BoundedPublicationShutdownProbe.assertUncommitted(tx,fixture.request());
                try { publisher.publishDocument(caller,fixture.request(),RepositoryReadControl.NONE);
                    throw new AssertionError("Closed host admitted a new schema publication"); }
                catch (RepositoryException expectedFailure) { require(expectedFailure.code()==RepositoryException.Code.UNAVAILABLE,
                        "new publication refused after admission closes"); }
                release.countDown();
                require(exited.await(5,TimeUnit.SECONDS) && resolver.awaitLoads(Duration.ofSeconds(5)),"actual schema worker drained");
                require(resolver.stats().cachedBytes()==0,"closed resolver does not cache the late descriptor result");
                BoundedPublicationShutdownProbe.assertUncommitted(tx,fixture.request());
                host.close(Duration.ofSeconds(5));
                require(closes.get()==1,"provider closes only after resolver finishes");
                host.close(Duration.ofSeconds(5));
                require(closes.get()==1,"repeated shutdown does not release provider twice");
            } finally { release.countDown(); }
        }
        System.out.println("BOUNDED_PUBLICATION_SCHEMA_SHUTDOWN_OK");
    }
    private void awaitRelease() {
        boolean interrupted=false;
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        try {
            while (release.getCount()!=0) {
                long remaining=deadline-System.nanoTime();
                if (remaining<=0) throw new IllegalStateException("Schema shutdown gate timed out");
                try { release.await(remaining,TimeUnit.NANOSECONDS); }
                catch (InterruptedException expectedInterrupt) { interrupted=true; }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
