package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Holds the first real PUT reply while another public request takes ownership. */
final class HistoricalPublicTakeoverProbe implements AutoCloseable {
    private final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    private final AtomicInteger calls = new AtomicInteger();
    private final List<BlobStore.PutSpec> writes = new CopyOnWriteArrayList<>();
    private final OpenedBlobStore observed;

    HistoricalPublicTakeoverProbe(OpenedBlobStore backend) {
        var store = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class},
                (proxy, method, arguments) -> {
                    Object result;
                    try { result = method.invoke(backend.store(), arguments); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                    if (method.getName().equals("put")) {
                        writes.add((BlobStore.PutSpec) arguments[0]);
                        if (calls.incrementAndGet() == 1) {
                            entered.countDown();
                            awaitRelease();
                        }
                    }
                    return result;
                });
        observed = new OpenedBlobStore(store, () -> {}, backend.capabilities(), backend::ensureNamespace, backend.reclaimer());
    }

    OpenedBlobStore opened() { return observed; }

    void exercise(Tx tx, DocumentPublicationRuntime runtime, DocumentPublicationRepository repository,
            RepositoryCaller caller, PublishDocumentRequest request, AtomicInteger selections, AtomicInteger resolutions) throws Exception {
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var old = executor.submit(() -> repository.publishDocument(caller, request, RepositoryReadControl.NONE));
            try {
                require(entered.await(15, TimeUnit.SECONDS), "predecessor completed a real provider PUT");
                var previous = runtime.withHistoricalAttempts(attempts -> attempts.inspectSelected(caller, command).orElseThrow());
                require(previous.borrowed() && previous.attached(), "old public call retains its generation");
                var oldAttempt = attempt(tx, writes.getFirst());
                HistoricalUploadFaultProbe.expire(tx, key);
                HistoricalPublicDispatchProbe.transport(repository, caller, request, selections, resolutions, false);
                var result = repository.publishDocument(caller, request, RepositoryReadControl.NONE);
                require(result.hasCommitted(), "successor public call commits while old provider reply is held");
                require(!old.isDone(), "old worker still has not drained after successor commit");
                require(writes.size() == 2, "one real PUT per generation, with no terminal replay PUT");
                var newAttempt = attempt(tx, writes.get(1));
                require(!oldAttempt[0].equals(newAttempt[0]) && !oldAttempt[1].equals(newAttempt[1]),
                        "successor uses a fresh upload attempt and token");
                require(!Boolean.TRUE.equals(oldAttempt[2]) && Boolean.TRUE.equals(newAttempt[2]),
                        "only the successor object is verified");
                require(count(tx, key, "document_revision_commits") == 1, "one revision committed for the operation");
                require(count(tx, key, "document_assessment_owners") == 1, "only successor reached assessment creation");
                runtime.tick();
                require(runtime.withHistoricalAttempts(attempts -> attempts.drain().unresolved()) >= 1,
                        "maintenance cannot retire the borrowed predecessor");
                release.countDown();
                try {
                    old.get(15, TimeUnit.SECONDS);
                    throw new AssertionError("Fenced predecessor returned success");
                } catch (ExecutionException failure) {
                    require(fenced(failure), "predecessor reports ownership fencing: " + failure.getCause());
                }
                require(!Boolean.TRUE.equals(attempt(tx, writes.getFirst())[2]), "late old provider reply stays unverified");
                require(repository.publishDocument(caller, request, RepositoryReadControl.NONE).equals(result),
                        "late predecessor cannot replace the successor receipt");
                runtime.tick();
                require(runtime.withHistoricalAttempts(attempts -> attempts.drain().unresolved()) == 0,
                        "both generation slots return after actual worker drainage");
            } finally {
                // Release before executor.close(), which waits for the actual provider worker.
                release.countDown();
            }
        }
    }

    private static Object[] attempt(Tx tx, BlobStore.PutSpec spec) {
        return tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT a.attempt_id,a.lease_token,o.verified FROM document_part_attempt_objects o
                JOIN document_part_attempts a USING(attempt_id)
                WHERE o.storage_namespace=:n AND o.object_key=:k
                """).setParameter("n", spec.bucket()).setParameter("k", spec.key()).getSingleResult());
    }

    private static long count(Tx tx, RepositoryOperationLedger.Key key, String table) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                .setParameter("a", key.account()).setParameter("p", key.principal())
                .setParameter("o", key.operationId()).getSingleResult()).longValue());
    }

    private static boolean fenced(Throwable failure) {
        for (var next = failure; next != null; next = next.getCause())
            if (next instanceof RepositoryExecutionClaimLedger.Fenced) return true;
        return false;
    }

    private void awaitRelease() {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new AssertionError("Public takeover provider gate timed out");
                try {
                    if (!release.await(remaining, TimeUnit.NANOSECONDS)) throw new AssertionError("Public takeover provider gate timed out");
                    return;
                } catch (InterruptedException cancellation) { interrupted = true; }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }

    @Override public void close() { release.countDown(); }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
