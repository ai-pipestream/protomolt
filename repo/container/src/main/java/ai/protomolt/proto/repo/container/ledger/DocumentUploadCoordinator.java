package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Internal selected NEW_CONTENT staging, not typed admission or publication.
 * The host supplies qualified original backends and keeps them open until idle.
 * Share one coordinator across all backends; its operation/part limits are instance-wide.
 * An ambiguous PUT is never retried here. Failure leaves attempt keys for recovery.
 */
final class DocumentUploadCoordinator implements AutoCloseable {
    record Backend(BackendIdentity identity, OpenedBlobStore opened) {
        Backend { Objects.requireNonNull(identity); Objects.requireNonNull(opened); }
    }
    @FunctionalInterface interface Resolver {
        /** Borrowed, host-qualified handle; no current-drive fallback or provisioning. */
        Backend resolve(String generation, ManagedBackendLedger.Profile profile);
    }
    /** Snapshot only; publication must re-fence these selections in its own transaction. */
    record StagedMember(DocumentSelectedAttemptLedger.Selected selection, DocumentPartAttemptLedger.Attempt attempt) {
        StagedMember {
            if (!selection.attempt().equals(attempt.id()) || !selection.token().equals(attempt.token())
                    || !attempt.state().equals("VERIFIED"))
                throw new IllegalArgumentException("Staged selection differs from verified attempt");
        }
    }
    record Staged(List<StagedMember> members) {
        Staged { members = List.copyOf(members); }
        List<DocumentPartAttemptLedger.Attempt> attempts() { return members.stream().map(StagedMember::attempt).toList(); }
    }
    @FunctionalInterface interface Preparation<T> {
        /**
         * Trusted synchronous, bounded work; call active between steps. Do not
         * retain borrowed bytes, publish or perform semantic review here.
         */
        T prepare(Staged staged, DocumentUploadPayloads.View bytes, Runnable active);
    }
    private record Bound(DocumentSelectedAttemptLedger.Selected selection, String namespace, BlobStore store) {}

    private final DocumentOperationUploadAdmission admission;
    private final DocumentSelectedAttemptLedger selected;
    private final PayloadBudget budget;
    private final Resolver resolver;
    private final int parallelism;
    private final Duration flushAge;
    private final Semaphore operationsInFlight = new Semaphore(32);
    private final Semaphore partsInFlight = new Semaphore(32, true);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycle = new Object();
    private boolean providerStartsClosed;
    private int activeTransfers;
    private long refusedProviderStarts;

    /** Local transfer counters only; idle is not proof that an operation or remote effect settled. */
    record ProviderActivity(boolean accepting, int active, long refused) {}
    ProviderActivity providerActivity() {
        synchronized (lifecycle) { return new ProviderActivity(!providerStartsClosed, activeTransfers, refusedProviderStarts); }
    }

    DocumentUploadCoordinator(Tx tx, DriveLedger drives, PayloadBudget budget, Resolver resolver,
            int parallelism, Duration flushAge, SqlTimeouts timeouts) {
        if (parallelism < 1 || parallelism > 32) throw new IllegalArgumentException("Part parallelism must be one to 32");
        if (flushAge.compareTo(Duration.ofMillis(1)) < 0 || flushAge.compareTo(Duration.ofSeconds(1)) > 0)
            throw new IllegalArgumentException("Observation flush age must be one millisecond to one second");
        var bounded = tx.withTimeouts(timeouts);
        this.admission = new DocumentOperationUploadAdmission(bounded, drives);
        this.selected = new DocumentSelectedAttemptLedger(bounded);
        this.budget = Objects.requireNonNull(budget);
        this.resolver = Objects.requireNonNull(resolver);
        this.parallelism = parallelism;
        this.flushAge = flushAge;
    }

    Staged stage(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Runnable control) {
        return execute(caller, owner, prepared, bodies, attributes, control, Map.of(), (staged, bytes, active) -> staged, false);
    }

    <T> T stageAndPrepare(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Runnable control, Preparation<T> preparation) {
        return execute(caller, owner, prepared, bodies, attributes, control, Map.of(), Objects.requireNonNull(preparation), true);
    }

    /**
     * Transfer an independently owned candidate only after post-preparation fences and
     * worker drain succeed. If either fails, close the candidate before propagating the
     * failure. The callback still owns cleanup until it returns and must not retain the
     * borrowed view. A successful caller must close the returned owner after all uses.
     */
    <T extends AutoCloseable> T stageAndPrepareOwned(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Runnable control, Preparation<T> preparation) {
        Objects.requireNonNull(preparation);
        var pending = new AtomicReference<T>();
        try {
            var result = stageAndPrepare(caller, owner, prepared, bodies, attributes, control, (staged, bytes, active) -> {
                var candidate = Objects.requireNonNull(preparation.prepare(staged, bytes, active), "Owned preparation result");
                pending.set(candidate);
                return candidate;
            });
            pending.set(null);
            return result;
        } catch (RuntimeException | Error failure) {
            var candidate = pending.getAndSet(null);
            if (candidate != null) {
                try { candidate.close(); }
                catch (Exception | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    Staged retry(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Runnable control, Map<String, DocumentOperationSelection.Expected> replacements) {
        if (replacements.isEmpty()) throw new IllegalArgumentException("Retry requires explicit replacement members");
        return execute(caller, owner, prepared, bodies, attributes, control, Map.copyOf(replacements), (staged, bytes, active) -> staged, false);
    }

    private <T> T execute(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Runnable control, Map<String, DocumentOperationSelection.Expected> replacements,
            Preparation<T> preparation, boolean recheckPreparation) {
        var authority = new DocumentUploadAuthority() {
            public DocumentOperationUploadAdmission.Admission admit() {
                return replacements.isEmpty() ? admission.admitOrReuseVerified(caller, owner, prepared)
                        : new DocumentOperationUploadAdmission.Admission(admission.retry(caller, owner, prepared, replacements), false);
            }
            public void renewOwnerAndSelections(List<DocumentSelectedAttemptLedger.Selected> selections) {
                selected.renewOwnerAndSelections(owner, selections, prepared.lease());
            }
            public List<DocumentPartAttemptLedger.Attempt> renewSelections(List<DocumentSelectedAttemptLedger.Selected> selections) {
                return selected.renew(owner, selections, prepared.lease());
            }
            public void verify(DocumentSelectedAttemptLedger.Selected selection, List<DocumentSelectedAttemptLedger.Observation> observations) {
                selected.verifyBatch(owner, selection, observations);
            }
            public void recheckPreparation(List<DocumentSelectedAttemptLedger.Selected> selections, Runnable active) {
                admission.captureRetainedReads(caller, owner, prepared);
                active.run();
                admission.recheckInitialSelections(owner, prepared);
                active.run();
                renewOwnerAndSelections(selections);
                active.run();
            }
            public void afterDrain() { check(control); }
        };
        return execute(prepared, bodies, attributes, control, replacements, preparation, recheckPreparation, authority);
    }

    /** The caller owns the operation authority until this synchronous transfer and worker cleanup finish. */
    Staged stageAuthorized(DocumentOperationUploadAdmission.Prepared prepared,
            Map<DocumentUploadPayloads.Key, PartObject> bodies, Map<String, String> attributes,
            Runnable control, DocumentUploadAuthority authority) {
        return execute(prepared, bodies, attributes, control, Map.of(), (staged, bytes, active) -> staged, false,
                Objects.requireNonNull(authority));
    }

    private <T> T execute(DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Runnable control, Map<String, DocumentOperationSelection.Expected> replacements,
            Preparation<T> preparation, boolean recheckPreparation, DocumentUploadAuthority authority) {
        Objects.requireNonNull(control); Objects.requireNonNull(prepared);
        var metadata = Map.copyOf(attributes);
        synchronized (lifecycle) {
            if (closed.get()) throw new IllegalStateException("Upload coordinator is closed");
            if (!operationsInFlight.tryAcquire()) throw new IllegalStateException("Concurrent upload capacity exhausted");
        }
        try {
            check(control);
            var members = replacements.isEmpty()
                    ? prepared.members().stream().map(m -> m.intent().getMemberId()).collect(Collectors.toUnmodifiableSet())
                    : replacements.keySet();
            try (var payloads = prepared.preparePayloads(members, bodies, budget);
                    var use = prepared.claimPayloads(payloads, members)) {
                check(control);
                var backends = resolve(prepared, members);
                check(control);
                var admitted = authority.admit();
                var bindings = bind(prepared, admitted.attempts(), replacements, backends, admitted.reusedVerified());
                var selections = bindings.values().stream().map(Bound::selection).toList();
                check(control);
                authority.renewOwnerAndSelections(selections);
                check(control);
                var failure = new AtomicReference<Throwable>();
                Runnable active = () -> {
                    rethrow(failure.get());
                    check(control);
                };
                var flusher = selections.isEmpty() || admitted.reusedVerified() ? null : new DocumentObservationFlusher(authority::verify, selections, flushAge, active,
                        cause -> failure.compareAndSet(null, cause));
                // Both background tasks are drained before Use releases its private bytes.
                T result;
                try (var tasks = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                    var stopHeartbeat = new java.util.concurrent.CountDownLatch(1);
                    try {
                        tasks.submit(() -> {
                            try {
                                while (!stopHeartbeat.await(Math.max(1, prepared.lease().toMillis() / 3), TimeUnit.MILLISECONDS)) {
                                    active.run();
                                    authority.renewOwnerAndSelections(selections);
                                    active.run();
                                }
                            } catch (InterruptedException interrupted) {
                                failure.compareAndSet(null, interrupted);
                                Thread.currentThread().interrupt();
                            } catch (Throwable cause) { failure.compareAndSet(null, cause); }
                        });
                        if (flusher != null) {
                            var flushing = tasks.submit(flusher::run);
                            var entries = use.entries();
                            var transferred = DocumentPartWorkers.run(entries.size(), parallelism, partsInFlight, active, (index, workerCheck) -> {
                                var entry = entries.get(index);
                                var binding = bindings.get(entry.attempt());
                                if (binding == null) throw new IllegalStateException("Payload attempt was not admitted");
                                final DocumentPartTransfer.Verified observation;
                                try (var permit = beginTransfer()) {
                                    // A queued refusal must not poison permitted siblings or their observation flusher.
                                    if (permit == null) return Boolean.FALSE;
                                    observation = DocumentPartTransfer.upload(binding.store(), binding.namespace(), entry.upload().object(),
                                            entry.body(), metadata, workerCheck, workerCheck);
                                }
                                flusher.add(binding.selection(), observation);
                                return Boolean.TRUE;
                            }, cause -> failure.compareAndSet(null, cause));
                            flusher.finish();
                            await(flushing, failure);
                            if (transferred.contains(Boolean.FALSE))
                                throw new ai.protomolt.proto.repo.spi.RepositoryException(
                                        ai.protomolt.proto.repo.spi.RepositoryException.Code.UNAVAILABLE,
                                        "Provider start admission stopped; incomplete attempts require reconciliation");
                        }
                        active.run();
                        var verified = selections.isEmpty() ? List.<DocumentPartAttemptLedger.Attempt>of()
                                : authority.renewSelections(selections);
                        if (verified.stream().anyMatch(a -> !a.state().equals("VERIFIED")))
                            throw new IllegalStateException("Selected upload did not verify every declared part");
                        var byId = verified.stream().collect(Collectors.toMap(DocumentPartAttemptLedger.Attempt::id, a -> a));
                        var staged = new Staged(selections.stream().sorted(java.util.Comparator.comparing(DocumentSelectedAttemptLedger.Selected::member))
                                .map(selection -> new StagedMember(selection, byId.get(selection.attempt()))).toList());
                        active.run();
                        try (var view = use.view()) {
                            result = preparation.prepare(staged, view, active);
                        }
                        active.run();
                        if (recheckPreparation) {
                            authority.recheckPreparation(selections, active);
                            active.run();
                        }
                        active.run();
                    } catch (RuntimeException | Error cause) {
                        failure.compareAndSet(null, cause);
                        throw cause;
                    } finally {
                        if (flusher != null) flusher.finish();
                        stopHeartbeat.countDown();
                        // Wake its wait without interrupting an in-flight SQL renewal; drain before release.
                    }
                }
                active.run();
                authority.afterDrain();
                return result;
            }
        } finally { operationsInFlight.release(); }
    }

    private Map<String, Backend> resolve(DocumentOperationUploadAdmission.Prepared prepared, Set<String> members) {
        var resolved = new HashMap<String, Backend>();
        for (var member : prepared.members()) {
            if (!members.contains(member.intent().getMemberId()) || member.attempt().isEmpty()) continue;
            var placement = member.placement();
            resolved.computeIfAbsent(placement.generation(), generation -> {
                var found = Objects.requireNonNull(resolver.resolve(generation, placement.profile()), "Original upload backend is unavailable");
                if (!found.identity().equals(placement.profile().identity()) || !found.opened().capabilities().containsAll(
                        Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION, BlobCapability.BOUNDED_READ)))
                    throw new IllegalArgumentException("Original upload backend identity or capabilities differ from requirements");
                return found;
            });
        }
        return Map.copyOf(resolved);
    }

    private Map<UUID, Bound> bind(DocumentOperationUploadAdmission.Prepared prepared,
            List<DocumentPartAttemptLedger.Attempt> admitted, Map<String, DocumentOperationSelection.Expected> replacements,
            Map<String, Backend> backends, boolean reusedVerified) {
        var attempts = admitted.stream().collect(Collectors.toMap(DocumentPartAttemptLedger.Attempt::id, a -> a));
        var expected = prepared.members().stream().filter(m -> m.attempt().isPresent()
                        && (replacements.isEmpty() || replacements.containsKey(m.intent().getMemberId())))
                .map(m -> m.attempt().orElseThrow().id()).collect(Collectors.toSet());
        if (!expected.equals(attempts.keySet())) throw new IllegalStateException("Admitted upload set differs from preparation");
        var result = new HashMap<UUID, Bound>();
        for (var member : prepared.members()) {
            if (member.attempt().isEmpty()) continue;
            var plan = member.attempt().orElseThrow();
            var attempt = attempts.get(plan.id());
            if (attempt == null) continue;
            var placement = member.placement();
            var backend = backends.get(placement.generation());
            if (!attempt.location().equals(plan.location()) || !attempt.storageRealm().equals(placement.profile().storageRealm())
                    || attempt.plannedCount() != plan.uploads().size() || !attempt.planKind().equals("NEW_CONTENT")
                    || !attempt.state().equals(reusedVerified ? "VERIFIED" : "STAGING"))
                throw new IllegalStateException("Committed admission differs from prepared upload");
            String memberId = member.intent().getMemberId();
            long revision = replacements.isEmpty() ? 1 : Math.addExact(replacements.get(memberId).revision(), 1);
            result.put(attempt.id(), new Bound(new DocumentSelectedAttemptLedger.Selected(memberId, revision, attempt.id(), attempt.token()),
                    attempt.location().namespace(), backend.opened().store()));
        }
        if (result.size() != attempts.size()) throw new IllegalStateException("Unbound admitted upload attempt");
        return Map.copyOf(result);
    }

    private void check(Runnable control) {
        if (closed.get() || Thread.currentThread().isInterrupted()) throw new CancellationException("Upload staging stopped");
        control.run();
    }

    private static void await(java.util.concurrent.Future<?> task, AtomicReference<Throwable> failure) {
        try { task.get(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            var cancelled = new CancellationException("Upload staging caller interrupted");
            failure.compareAndSet(null, cancelled);
            throw cancelled;
        } catch (java.util.concurrent.ExecutionException failed) {
            failure.compareAndSet(null, failed.getCause());
            rethrow(failed.getCause());
        }
    }

    private static void rethrow(Throwable cause) {
        if (cause instanceof Error error) throw error;
        if (cause instanceof RuntimeException runtime) throw runtime;
        if (cause != null) throw new IllegalStateException("Upload background task failed", cause);
    }

    /** Close only transfer admission; already permitted PUT/read-back calls may settle normally. */
    void stopProviderStarts() { synchronized (lifecycle) { providerStartsClosed = true; } }

    /**
     * Transfer-only wait. Does not cover queued workers, SQL observation settlement,
     * retained payloads, remote effects after SDK timeout, or full runtime shutdown.
     */
    boolean awaitProviderIdle(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Provider idle timeout must not be negative");
        long budget = timeout.toNanos(), start = System.nanoTime();
        synchronized (lifecycle) {
            if (!providerStartsClosed) throw new IllegalStateException("Stop provider starts before awaiting idle");
            while (activeTransfers != 0) {
                long remaining = budget - (System.nanoTime() - start);
                if (remaining <= 0) return false;
                TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
            }
            return true;
        }
    }

    private Transfer beginTransfer() {
        synchronized (lifecycle) {
            if (providerStartsClosed) { refusedProviderStarts++; return null; }
            activeTransfers++;
            return new Transfer();
        }
    }

    private final class Transfer implements AutoCloseable {
        private boolean ended;
        @Override public void close() {
            synchronized (lifecycle) {
                if (ended) return;
                ended = true;
                activeTransfers--;
                lifecycle.notifyAll();
            }
        }
    }

    @Override public void close() {
        synchronized (lifecycle) { providerStartsClosed = true; closed.set(true); }
    }

    /** False means a provider/database call is still running and borrowed resources must remain open. */
    boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (!closed.get()) throw new IllegalStateException("Close the coordinator before awaiting idle");
        if (timeout.isNegative()) throw new IllegalArgumentException("Idle timeout must not be negative");
        if (!operationsInFlight.tryAcquire(32, timeout.toNanos(), TimeUnit.NANOSECONDS)) return false;
        operationsInFlight.release(32);
        return true;
    }
}
