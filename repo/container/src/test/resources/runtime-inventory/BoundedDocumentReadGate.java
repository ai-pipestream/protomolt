package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Holds a real Redis read result inside the provider call; cancellation does not release the gate. */
final class BoundedDocumentReadGate {
    private final AtomicBoolean armed=new AtomicBoolean();
    private final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1),exited=new CountDownLatch(1);
    private final AtomicBoolean providerReadSucceeded=new AtomicBoolean();
    BlobStore wrap(BlobStore actual) {
        return (BlobStore)Proxy.newProxyInstance(BlobStore.class.getClassLoader(),new Class<?>[]{BlobStore.class},
                (proxy,method,args) -> {
                    boolean gated=method.getName().equals("getBounded") && armed.compareAndSet(true,false);
                    try {
                        Object value=method.invoke(actual,args);
                        if (gated) {
                            providerReadSucceeded.set(true);
                            entered.countDown();
                            awaitRelease();
                        }
                        return value;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                    finally { if (gated) exited.countDown(); }
                });
    }
    private void awaitRelease() {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
        boolean interrupted=false;
        try {
            while (release.getCount()!=0) {
                long remaining=deadline-System.nanoTime();
                if (remaining<=0) throw new IllegalStateException("Redis read gate timed out");
                try { release.await(remaining,TimeUnit.NANOSECONDS); }
                catch (InterruptedException expected) { interrupted=true; }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
    void verifyShutdown(RepoServices host,RepositoryCaller caller,DocumentPublishedRevision revision,
            AtomicInteger closes) throws Exception {
        var history=host.historicalRepository();
        armed.set(true);
        try (var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var read=executor.submit(() -> {
                try (var result=history.readValidated(caller,revision.getAddress(),UUID.fromString(revision.getRevisionId()),
                        RepositoryReadControl.NONE)) { return result.document(); }
            });
            try {
                require(entered.await(5,TimeUnit.SECONDS),"accepted Redis read entered provider gate");
                try { host.close(Duration.ofMillis(200)); throw new AssertionError("Shutdown completed with active provider read"); }
                catch (IllegalStateException timeout) {
                    require("Native publication resources still active; shared resources retained".equals(timeout.getMessage()),
                            "expected resource retention timeout: "+timeout);
                }
                require(closes.get()==0,"provider remains open during incomplete shutdown");
                try (var connection=host.ledgerDataSource().getConnection(); var statement=connection.createStatement();
                        var result=statement.executeQuery("SELECT 1")) { require(result.next(),"database retained for active read"); }
                require(exited.getCount()==1,"provider worker remains active after caller cancellation");
                try { read.get(5,TimeUnit.SECONDS); throw new AssertionError("Closed reader returned a document"); }
                catch (ExecutionException cancelled) {
                    require(cancelled.getCause() instanceof RepositoryException failure
                            && failure.code()==RepositoryException.Code.CANCELLED,"closed read reports cancellation");
                }
                release.countDown();
                require(exited.await(5,TimeUnit.SECONDS) && providerReadSucceeded.get(),"provider call returned after cancellation with real Redis read completed");
                require(closes.get()==0,"worker completion precedes provider release");
                host.close(Duration.ofSeconds(5));
                require(closes.get()==1,"provider closes after accepted read finishes");
            } finally { release.countDown(); }
        }
        System.out.println("BOUNDED_DOCUMENT_READ_SHUTDOWN_OK");
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
