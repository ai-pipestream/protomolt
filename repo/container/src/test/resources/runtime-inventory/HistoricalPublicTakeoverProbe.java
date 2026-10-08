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
    int completedPuts() { return calls.get(); }
    void awaitPut() throws InterruptedException {
        require(entered.await(15, TimeUnit.SECONDS), "public call completed real PUT before authorization change");
    }
    void releasePut() { release.countDown(); }

    void exerciseStop(Tx tx, DocumentPublicationRuntime runtime, DocumentReadLedger reads,
            DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            boolean cancel) throws Exception {
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var cancelled = new AtomicBoolean();
        var control = new RepositoryReadControl() {
            public boolean isCancelled() { return cancelled.get(); }
            public long remainingNanos() { return Long.MAX_VALUE; }
        };
        // Inspection only: after close(), new external registry calls must be refused.
        var attempts = runtime.withHistoricalAttempts(value -> value);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var publication = executor.submit(() -> repository.publishDocument(caller, request, control));
            try {
                require(entered.await(15, TimeUnit.SECONDS), "accepted historical call completed real PUT before stop");
                if (cancel) cancelled.set(true);
                runtime.close();
                require(!runtime.shutdownStep(java.time.Duration.ZERO), "held accepted call prevents shutdown completion");
                require(!publication.isDone() && attempts.drain().active() == 1 && reads.outstandingReads() > 0,
                        "shutdown retains the actual producer, generation and historical read pins");
                try {
                    repository.publishDocument(caller, request, RepositoryReadControl.NONE);
                    throw new AssertionError("Closed runtime accepted another public call");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.UNAVAILABLE, "closed public admission reports unavailable");
                }
                require(count(tx, key, "document_revision_commits") == 0
                                && count(tx, key, "document_assessment_owners") == 0,
                        "held upload has not reached assessment or publication");
                release.countDown();
                if (cancel) {
                    try {
                        publication.get(15, TimeUnit.SECONDS);
                        throw new AssertionError("Cancelled historical call returned success");
                    } catch (ExecutionException failure) {
                        require(cancelled(failure), "historical producer reports cancellation: " + failure.getCause());
                    }
                    require(!Boolean.TRUE.equals(attempt(tx, writes.getFirst())[2]), "cancelled PUT reply stays unverified");
                    require(count(tx, key, "document_revision_commits") == 0
                                    && count(tx, key, "document_assessment_owners") == 0,
                            "cancelled producer never reaches assessment or publication");
                } else {
                    var result = publication.get(15, TimeUnit.SECONDS);
                    require(result.hasCommitted(), "accepted historical call can finish during orderly shutdown");
                    DocumentPublicationResponseValidator.requireValid(command, caller.principalName(), result);
                    require(count(tx, key, "document_revision_commits") == 1, "accepted shutdown call commits exactly once");
                }
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++)
                    stopped = runtime.shutdownStep(java.time.Duration.ofSeconds(1));
                require(stopped && attempts.drain().unresolved() == 0 && reads.outstandingReads() == 0,
                        "shutdown completes only after real producer and captures drain");
            } finally { release.countDown(); }
        }
    }

    private static boolean cancelled(Throwable failure) {
        for (var next = failure; next != null; next = next.getCause())
            if (next instanceof RepositoryException refusal && refusal.code() == RepositoryException.Code.CANCELLED) return true;
        return false;
    }

    void exerciseRemoteCancellation(Tx tx, DocumentPublicationRuntime runtime, DocumentReadLedger reads,
            DocumentPublicationRepository remote, RepositoryCaller caller, PublishDocumentRequest request,
            ai.protomolt.proto.repo.service.DocumentPublicationGrpcService service, PayloadBudget delivery,
            CountDownLatch serverCancelled, boolean deadline) throws Exception {
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var cancelled = new AtomicBoolean();
        var control = new RepositoryReadControl() {
            public boolean isCancelled() { return cancelled.get(); }
            public long remainingNanos() { return Long.MAX_VALUE; }
        };
        var attempts = runtime.withHistoricalAttempts(value -> value);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var call = executor.submit(() -> remote.publishDocument(caller, request, deadline ? RepositoryReadControl.NONE : control));
            try {
                require(entered.await(15, TimeUnit.SECONDS), "authenticated historical RPC reached real PUT");
                if (!deadline) cancelled.set(true);
                try {
                    call.get(15, TimeUnit.SECONDS);
                    throw new AssertionError("Cancelled historical RPC returned success");
                } catch (ExecutionException failure) {
                    require(failure.getCause() instanceof RepositoryException refusal && refusal.code()
                                    == (deadline ? RepositoryException.Code.DEADLINE_EXCEEDED : RepositoryException.Code.CANCELLED),
                            "remote client reports the expected RPC termination: " + failure.getCause());
                }
                require(serverCancelled.await(5, TimeUnit.SECONDS), "client cancellation reaches server context");
                require(!service.awaitIdle(java.time.Duration.ZERO) && delivery.reservedBytes() > 0
                                && attempts.drain().active() == 1 && reads.outstandingReads() > 0,
                        "cancelled transport retains its actual producer, delivery budget and historical captures");
                try {
                    remote.publishDocument(caller, request, RepositoryReadControl.NONE);
                    throw new AssertionError("Cancelled transport released capacity before producer exit");
                } catch (RepositoryException refusal) {
                    require(refusal.code() == RepositoryException.Code.RESOURCE_EXHAUSTED, "held producer occupies transport capacity");
                }
                runtime.close();
                require(!runtime.shutdownStep(java.time.Duration.ZERO), "cancelled RPC producer prevents premature runtime shutdown");
                require(count(tx, key, "document_revision_commits") == 0
                                && count(tx, key, "document_assessment_owners") == 0,
                        "cancelled held RPC has no assessment or commit");
                release.countDown();
                require(service.awaitIdle(java.time.Duration.ofSeconds(15)), "actual RPC producer drains after provider release");
                require(!Boolean.TRUE.equals(attempt(tx, writes.getFirst())[2])
                                && count(tx, key, "document_revision_commits") == 0
                                && count(tx, key, "document_assessment_owners") == 0,
                        "late cancelled RPC reply cannot validate or publish content");
                require(delivery.reservedBytes() == 0, "transport releases budget after actual producer exit");
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++)
                    stopped = runtime.shutdownStep(java.time.Duration.ofSeconds(1));
                require(stopped && attempts.drain().unresolved() == 0 && reads.outstandingReads() == 0,
                        "cancelled RPC shutdown returns all generations and captures");
            } finally { release.countDown(); }
        }
    }

    void exerciseTransportClose(Tx tx, DocumentPublicationRuntime runtime, DocumentReadLedger reads,
            DocumentPublicationRepository remote, RepositoryCaller caller, PublishDocumentRequest request,
            ai.protomolt.proto.repo.service.DocumentPublicationGrpcService service, PayloadBudget delivery) throws Exception {
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var attempts = runtime.withHistoricalAttempts(value -> value);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var call = executor.submit(() -> remote.publishDocument(caller, request, RepositoryReadControl.NONE));
            try {
                require(entered.await(15, TimeUnit.SECONDS), "accepted RPC completed real PUT before transport close");
                service.close();
                runtime.close();
                require(!service.awaitIdle(java.time.Duration.ZERO) && !runtime.shutdownStep(java.time.Duration.ZERO)
                                && !call.isDone() && delivery.reservedBytes() > 0
                                && attempts.drain().active() == 1 && reads.outstandingReads() > 0,
                        "orderly close retains accepted producer, delivery reservation and historical captures");
                try {
                    remote.publishDocument(caller, request, RepositoryReadControl.NONE);
                    throw new AssertionError("Closed transport accepted another RPC");
                } catch (RepositoryException refusal) {
                    require(refusal.code() == RepositoryException.Code.UNAVAILABLE, "closed transport refuses new admission");
                }
                require(count(tx, key, "document_revision_commits") == 0 && count(tx, key, "document_assessment_owners") == 0,
                        "held accepted RPC has not reached assessment or commit");
                release.countDown();
                var result = call.get(15, TimeUnit.SECONDS);
                DocumentPublicationResponseValidator.requireValid(command, caller.principalName(), result);
                require(result.hasCommitted() && count(tx, key, "document_revision_commits") == 1
                                && count(tx, key, "document_assessment_owners") == 1
                                && Boolean.TRUE.equals(attempt(tx, writes.getFirst())[2]),
                        "accepted RPC verifies its upload and commits exactly once during orderly close");
                require(service.awaitIdle(java.time.Duration.ofSeconds(15)) && delivery.reservedBytes() == 0,
                        "completed producer and delivery release transport resources");
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++)
                    stopped = runtime.shutdownStep(java.time.Duration.ofSeconds(1));
                require(stopped && attempts.drain().unresolved() == 0 && reads.outstandingReads() == 0,
                        "orderly transport close drains all historical captures and generations");
            } finally { release.countDown(); }
        }
    }

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
