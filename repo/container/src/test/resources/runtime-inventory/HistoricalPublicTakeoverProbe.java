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
        observed = new OpenedBlobStore(store, () -> {}, backend.capabilities(), backend::ensureNamespace, backend.reclaimer(), backend.exactVersionReclaimer());
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

    void exercise(Tx tx, DocumentPublicationRuntime runtime, DocumentPublicationRepository repository,
            RepositoryCaller caller, PublishDocumentRequest request, AtomicInteger selections, AtomicInteger resolutions) throws Exception {
        exercise(tx, runtime, repository, caller, request, selections, resolutions, null, null);
    }

    void exercise(Tx tx, DocumentPublicationRuntime runtime, DocumentPublicationRepository repository,
            RepositoryCaller caller, PublishDocumentRequest request, AtomicInteger selections, AtomicInteger resolutions,
            HistoricalCreateCommitFault fault, HistoricalCreateCommitFault.RecoveryPhase phase) throws Exception {
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
                UUID retained = null;
                String durableRecovery = null;
                if (fault != null) {
                    var cancelled = new AtomicBoolean();
                    fault.armRecovery(key, phase, () -> cancelled.set(true));
                    var uncertainControl = new RepositoryReadControl() {
                        public boolean isCancelled() { return cancelled.get(); }
                        public long remainingNanos() { return Long.MAX_VALUE; }
                    };
                    try { repository.publishDocument(caller, request, uncertainControl);
                        throw new AssertionError("Successor did not lose its recovery acknowledgement");
                    } catch (RuntimeException failure) { fault.requireFailure(failure); }
                    require(cancelled.get(), "matched committed recovery cancels immediate confirmation");
                    var pending = runtime.withHistoricalAttempts(attempts -> attempts.inspectSelected(caller, command).orElseThrow());
                    retained = pending.identity();
                    require(!retained.equals(previous.identity()) && !pending.attached() && !pending.borrowed(),
                            "uncertain successor releases its borrow and retains its distinct local proposal");
                    require(writes.size() == 1, "uncertain recovery has not repeated provider work");
                    durableRecovery = recoveryRow(tx, key, phase);
                    require(durableRecovery != null, "recovery transaction committed before acknowledgement loss");
                    for (String table : List.of("document_assessment_owners", "document_revision_commits", "repository_operation_success"))
                        require(count(tx, key, table) == 0, "uncertain recovery has no rows in " + table);
                    HistoricalPublicDispatchProbe.replayRefusals(repository, caller, request, selections, resolutions, this, true);
                    require(recoveryRow(tx, key, phase).equals(durableRecovery), "refused retries preserve durable recovery identity");
                }
                HistoricalPublicDispatchProbe.libraryReplay(repository, caller, request, selections, resolutions, false);
                if (fault != null) {
                    require(runtime.withHistoricalAttempts(attempts -> attempts.inspectSelected(caller, command).orElseThrow()).identity().equals(retained),
                            "public retry resumes the retained successor identity");
                    require(recoveryRow(tx, key, phase).equals(durableRecovery), "retry preserves the original durable recovery row");
                    require(count(tx, key, "repository_coordinator_expirations") == 1 && count(tx, key, "repository_successor_installs") == 1,
                            "one reservation and installation across lost acknowledgement and retry");
                }
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

    private static String recoveryRow(Tx tx, RepositoryOperationLedger.Key key, HistoricalCreateCommitFault.RecoveryPhase phase) {
        String table = phase == HistoricalCreateCommitFault.RecoveryPhase.RESERVATION
                ? "repository_coordinator_expirations" : "repository_successor_installs";
        return tx.readOnly(em -> {
            var rows = em.createNativeQuery("SELECT to_jsonb(r)::text FROM " + table + " r WHERE account_id=:a AND principal=:p AND operation_id=:o", String.class)
                    .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getResultList();
            require(rows.size() <= 1, "recovery fixture has at most one durable successor");
            return rows.isEmpty() ? null : (String) rows.getFirst();
        });
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
