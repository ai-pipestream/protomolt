package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/** Private ownership after installation. Does not own reservation/install or enable public recovery. */
final class RepositoryInstalledHistoricalAttempts implements AutoCloseable {
    record Drain(int active, int unresolved) {}
    enum Retirement { NOT_PROVEN, RETAINED, RETIRED }
    private enum RetirementProof { NONE, TERMINAL, FENCED }
    private final Tx tx;
    private final PayloadBudget budget;
    private final DriveLedger drives;
    private final int capacity;
    private final Map<RepositoryOperationLedger.Key, Entry> entries = new HashMap<>();
    private boolean closed;
    private boolean detaching;
    private int active;

    private static final class Entry {
        final RepositoryCaller caller;
        final RepositorySuccessorInstall.Plan plan;
        final DocumentPublicationPreparationRecord retention;
        final DocumentSuccessorFingerprint fingerprint;
        final ByteString retentionDigest;
        final PayloadBudget.Lease bytes;
        final DocumentPublicationScopeCalls scopes = new DocumentPublicationScopeCalls();
        final DocumentPublicationScopeCalls.Call parent = scopes.enter();
        boolean borrowed;
        RetirementProof retirement = RetirementProof.NONE;
        DocumentHistoricalAssessmentSources sources;
        DocumentHistoricalAssessmentSources.Work work;
        List<DocumentReadLedger.PinnedHistory> histories = List.of();
        RepositoryHistoricalSuccessorActivation activation;
        DocumentHistoricalExecution execution;
        DocumentPublicationAssessment.Historical assessment;
        DocumentAssessmentCreation.Created stage;
        Entry(RepositoryCaller caller, RepositorySuccessorInstall.Plan plan,
                DocumentPublicationPreparationRecord retention, DocumentSuccessorFingerprint fingerprint,
                ByteString retentionDigest, PayloadBudget.Lease bytes) {
            this.caller = caller; this.plan = plan; this.retention = retention;
            this.fingerprint = fingerprint; this.retentionDigest = retentionDigest; this.bytes = bytes;
        }
    }

    RepositoryInstalledHistoricalAttempts(Tx tx, PayloadBudget budget, DriveLedger drives, int capacity) {
        this.tx = Objects.requireNonNull(tx); this.budget = Objects.requireNonNull(budget);
        this.drives = Objects.requireNonNull(drives);
        if (capacity < 1) throw new IllegalArgumentException("Historical attempt capacity must be positive");
        this.capacity = capacity;
    }

    /** No SQL; reserves capacity before capture transfer or activation registration. */
    synchronized Attempt beginInstalled(RepositoryCaller caller, RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention) {
        if (closed) throw unavailable();
        Objects.requireNonNull(caller); Objects.requireNonNull(plan); Objects.requireNonNull(retention);
        var key = plan.next().key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!retention.key().equals(key) || !retention.command().canonical().equals(plan.next().command().canonical())
                || retention.predecessorGeneration() >= plan.next().predecessorGeneration())
            throw new IllegalArgumentException("Historical retention differs from installed plan");
        var existing = entries.get(key);
        if (existing != null && existing.borrowed) throw conflict("Historical attempt is in use");
        if (active >= capacity || existing == null && entries.size() >= capacity)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical attempt capacity exhausted");
        if (existing != null && existing.caller.equals(caller) && existing.plan.equals(plan)
                && existing.retention.equals(retention)) {
            existing.borrowed = true; active++;
            return new Attempt(existing);
        }
        // Scratch is temporary; retained accounting uses the actual bounded encoded sizes.
        try (var scratch = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES
                + DocumentPublicationModesJournal.MAX_BYTES)) {
            var previous = DocumentPublicationPreparationCodec.encode(plan.previous());
            var next = DocumentPublicationPreparationCodec.encode(plan.next());
            var retained = DocumentPublicationPreparationCodec.encode(retention);
            var modes = ByteString.copyFromUtf8(RepositorySuccessorInstall.encodeModes(plan));
            var fingerprint = new DocumentSuccessorFingerprint(plan.reservation(), digest(previous), digest(next), digest(modes));
            var retentionDigest = digest(retained);
            if (existing != null) {
                if (!existing.caller.equals(caller) || !existing.fingerprint.equals(fingerprint)
                        || !existing.retentionDigest.equals(retentionDigest))
                    throw conflict("Historical retry identity changed");
            } else {
                var bytes = budget.reserve((long) previous.size() + next.size() + retained.size() + modes.size());
                try {
                    existing = new Entry(caller, plan, retention, fingerprint, retentionDigest, bytes);
                    entries.put(key, existing);
                } catch (RuntimeException | Error failure) { bytes.close(); throw failure; }
            }
            existing.borrowed = true; active++;
            return new Attempt(existing);
        }
    }

    /** Resume before discovery or allocating another capture. The exact entry's plan remains fixed. */
    synchronized Optional<Attempt> resume(RepositoryCaller caller, DocumentPublicationCommand command) {
        if (closed) throw unavailable();
        Objects.requireNonNull(caller); Objects.requireNonNull(command);
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        var entry = entries.get(key);
        if (entry == null) return Optional.empty();
        if (!entry.caller.equals(caller) || !entry.plan.next().command().canonical().equals(command.canonical()))
            throw conflict("Historical retry identity changed");
        if (entry.borrowed) throw conflict("Historical attempt is in use");
        if (active >= capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                "Historical call capacity exhausted");
        entry.borrowed = true; active++;
        return Optional.of(new Attempt(entry));
    }

    final class Attempt implements AutoCloseable {
        private final Entry entry;
        private boolean ended;
        private Attempt(Entry entry) { this.entry = entry; }
        private void requireActive(RepositoryReadControl control) {
            if (ended) throw new IllegalStateException("Historical attempt call is closed");
            Objects.requireNonNull(control).check();
        }
        private void authorize(RepositoryReadControl control) {
            requireMutable(control);
            if (entry.work == null) throw new IllegalStateException("Historical sources are not attached");
            entry.work.requireCaller(entry.caller); entry.work.authorize(control);
        }

        private void requireMutable(RepositoryReadControl control) {
            requireActive(control);
            if (entry.retirement != RetirementProof.NONE)
                throw conflict("Historical attempt is retained for disposal only");
        }

        /** Success transfers sources, root Work and exact histories. Failure transfers nothing. */
        synchronized void attachSources(DocumentHistoricalAssessmentSources sources,
                DocumentHistoricalAssessmentSources.Work work, RepositoryReadControl control) {
            requireMutable(control);
            if (entry.sources != null) throw conflict("Historical sources are already attached");
            Objects.requireNonNull(sources); Objects.requireNonNull(work);
            var histories = work.histories(sources);
            work.requireCaller(entry.caller);
            work.references(entry.plan.next().command(), control::check);
            work.authorize(control);
            var activation = new RepositoryHistoricalSuccessorActivation(tx, budget, entry.plan, entry.retention, sources, drives);
            control.check();
            entry.sources = sources; entry.work = work; entry.histories = histories; entry.activation = activation;
        }

        /** Retains the same handle, including acknowledged START and sticky mutation flags, across calls. */
        synchronized void openExecution(RepositoryCaller coordinator, RepositoryReadControl control) {
            authorize(control);
            RepositoryCoordinatorReservation.require(coordinator, entry.plan.reservation(), control);
            if (entry.execution == null) entry.execution = entry.activation.openAcceptedExecution(coordinator, entry.caller,
                    entry.work, entry.scopes, entry.parent, control);
        }
        private DocumentHistoricalExecution execution(RepositoryReadControl control) {
            authorize(control);
            if (entry.execution == null) throw new IllegalStateException("Historical execution is not attached");
            return entry.execution;
        }
        synchronized DocumentAssessmentStartJournal.Started start(Duration retention, RepositoryReadControl control) {
            return execution(control).start(entry.caller, retention, control);
        }
        synchronized DocumentOperationUploadAdmission.Admission admitUploads(RepositoryReadControl control) {
            return execution(control).admitUploads(entry.caller, control);
        }
        synchronized void prepareAssessment(DocumentSchemaPolicies.Selection policy,
                Map<String, Map<Integer, ByteString>> fragments, Optional<DocumentSchemaAdmission.Definition> container,
                DocumentPublicationCandidate.Resolver resolver, DocumentRevisionAssembly.Limits limits,
                Instant evaluatedAt, RepositoryReadControl control) throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.assessment != null) throw conflict("Historical assessment is already retained");
            entry.assessment = execution.prepareAssessment(entry.caller, policy, fragments, container, resolver,
                    limits, evaluatedAt, control);
        }
        /** Synchronous borrowed access only. Workers must retain their own existing source/scope permits. */
        synchronized <T> T withAssessment(Function<DocumentPublicationAssessment.Historical, T> action,
                RepositoryReadControl control) {
            execution(control);
            if (entry.assessment == null) throw new IllegalStateException("Historical assessment is not prepared");
            return Objects.requireNonNull(action).apply(entry.assessment);
        }
        synchronized DocumentAssessmentCreation.Created createAssessment(
                Map<String, DocumentSelectedAttemptLedger.Selected> selections,
                DocumentAssessmentRuntimeObserver.Observation observation, RepositorySchemaArtifacts storage,
                DocumentAssessmentStartJournal.Started started, RepositoryReadControl control) throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.assessment == null) throw new IllegalStateException("Historical assessment is not prepared");
            if (entry.stage != null) throw conflict("Historical CREATE already acknowledged; continue publication");
            entry.stage = execution.createAssessment(entry.caller, entry.assessment, selections, observation, storage, started, control);
            return entry.stage;
        }
        synchronized Optional<DocumentAssessmentCreation.Created> reconcileAssessment(
                Map<String, DocumentSelectedAttemptLedger.Selected> selections,
                DocumentAssessmentRuntimeObserver.Observation observation, RepositoryReadControl control)
                throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.assessment == null) throw new IllegalStateException("Historical assessment is not prepared");
            var verified = execution.reconcileAssessment(entry.caller, entry.assessment, selections, observation, control);
            if (verified.isPresent()) entry.stage = verified.orElseThrow();
            return verified;
        }
        synchronized ai.protomolt.proto.repo.v1.DocumentPublicationResult publishAssessment(
                Map<String, DocumentSelectedAttemptLedger.Selected> selections,
                DocumentAssessmentRuntimeObserver.Observation observation, RepositorySchemaArtifacts storage,
                DocumentPublicationCommit publication, RepositoryReadControl control) throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.stage == null) throw new IllegalStateException("Historical CREATE is not acknowledged");
            return execution.publishAssessment(entry.caller, entry.assessment, selections, observation,
                    storage, publication, entry.stage, control);
        }
        /** Terminal replay authorizes cleanup only, never receipt delivery or replacement execution. */
        synchronized Retirement retireTerminal(RepositoryCaller coordinator, Duration timeout,
                RepositoryReadControl control) throws InterruptedException {
            return retire(coordinator, timeout, control, true);
        }

        /** A lease expiring is not proof that the retained claim can never execute again. */
        synchronized Retirement retireFenced(RepositoryCaller coordinator, Duration timeout,
                RepositoryReadControl control) throws InterruptedException {
            return retire(coordinator, timeout, control, false);
        }

        private Retirement retire(RepositoryCaller coordinator, Duration timeout,
                RepositoryReadControl control, boolean terminal) throws InterruptedException {
            requireActive(control);
            long nanos = checkedNanos(timeout), start = System.nanoTime();
            RepositoryCoordinatorReservation.require(coordinator, entry.plan.reservation(), control);
            if (entry.retirement == RetirementProof.NONE) {
                var command = entry.plan.next().command();
                if (terminal) {
                    // Observe the global terminal outcome, which may belong to a later generation.
                    var observed = new DocumentPublicationReplay(tx).observe(entry.caller, command);
                    if (observed.state() != DocumentPublicationReplay.State.COMMITTED
                            && observed.state() != DocumentPublicationReplay.State.TERMINATED) {
                        control.check();
                        return Retirement.NOT_PROVEN;
                    }
                    entry.retirement = RetirementProof.TERMINAL;
                } else {
                    var reservation = entry.plan.reservation();
                    var identity = new RepositoryCoordinatorDrain.Identity(entry.plan.next().key(), command.sha256(),
                            reservation.predecessor().epoch() + 1, reservation.successorToken(), reservation.successorIncarnation());
                    if (!RepositoryClaimRetirement.fenced(tx, command, List.of(identity), control)) return Retirement.NOT_PROVEN;
                    entry.retirement = RetirementProof.FENCED;
                }
            }
            control.check();
            if (!disposeEntry(entry, coordinator, nanos, start, control)) return Retirement.RETAINED;
            control.check();
            synchronized (RepositoryInstalledHistoricalAttempts.this) {
                if (!entries.remove(entry.plan.next().key(), entry))
                    throw new IllegalStateException("Historical entry lost exclusive retirement ownership");
                entry.bytes.close();
                ended = true; entry.borrowed = false; active--;
                RepositoryInstalledHistoricalAttempts.this.notifyAll();
            }
            return Retirement.RETIRED;
        }

        @Override public synchronized void close() {
            if (ended) return;
            ended = true;
            synchronized (RepositoryInstalledHistoricalAttempts.this) {
                entry.borrowed = false; active--; RepositoryInstalledHistoricalAttempts.this.notifyAll();
            }
        }
    }

    @Override public synchronized void close() { closed = true; }
    synchronized Drain drain() { return new Drain(active, entries.size()); }
    synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (!closed) throw new IllegalStateException("Close historical admission before waiting");
        long remaining = checkedNanos(timeout), start = System.nanoTime();
        while (active != 0) {
            if (remaining <= 0) return false;
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - start);
        }
        return true;
    }

    /** Local shutdown only. Timeout/failure leaves the entry retained; never fences a shared reader. */
    boolean detachClosed(Duration timeout, Function<RepositoryOperationLedger.Key, RepositoryCaller> authority,
            RepositoryReadControl control) throws InterruptedException {
        long nanos = checkedNanos(timeout), start = System.nanoTime();
        Objects.requireNonNull(authority); Objects.requireNonNull(control).check();
        if (!await(this::awaitIdle, nanos, start, control)) return false;
        final List<Entry> retained;
        synchronized (this) {
            if (detaching) return false;
            detaching = true; retained = List.copyOf(entries.values());
        }
        try {
            for (var entry : retained) {
                control.check();
                var coordinator = Objects.requireNonNull(authority.apply(entry.plan.next().key()));
                RepositoryCoordinatorReservation.require(coordinator, entry.plan.reservation(), control);
                if (!disposeEntry(entry, coordinator, nanos, start, control)) return false;
                control.check();
                entry.bytes.close();
                synchronized (this) { entries.remove(entry.plan.next().key(), entry); }
            }
            return true;
        } finally { synchronized (this) { detaching = false; notifyAll(); } }
    }
    /** No owner-map monitor or SQL transaction spans the local drainage waits. */
    private boolean disposeEntry(Entry entry, RepositoryCaller coordinator, long nanos, long start,
            RepositoryReadControl control) throws InterruptedException {
        entry.scopes.close();
        if (entry.assessment != null) entry.assessment.close();
        if (entry.execution != null) entry.execution.close();
        entry.parent.close();
        if (!await(entry.scopes::awaitIdle, nanos, start, control)) return false;
        if (entry.work != null) entry.work.close();
        if (entry.activation != null) {
            var disposed = entry.activation.disposeCapture(coordinator, remaining(nanos, start), control);
            if (disposed.isEmpty()) return false;
            if (disposed.orElseThrow() == RepositoryHistoricalSuccessorActivation.Disposal.NO_CAPTURE
                    && !releaseUnregistered(entry, nanos, start, control)) return false;
        }
        return true;
    }
    private static boolean releaseUnregistered(Entry entry, long nanos, long start, RepositoryReadControl control)
            throws InterruptedException {
        entry.sources.close();
        entry.histories.forEach(DocumentReadLedger.PinnedHistory::close);
        if (!await(entry.sources::awaitDrained, nanos, start, control)) return false;
        for (var history : entry.histories) {
            control.check();
            if (!await(history::awaitDrained, nanos, start, control)) return false;
            history.release();
        }
        control.check();
        return entry.histories.stream().allMatch(DocumentReadLedger.PinnedHistory::isReleased);
    }
    private static long checkedNanos(Duration timeout) {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative historical shutdown wait");
        return timeout.toNanos();
    }
    @FunctionalInterface private interface DrainWait { boolean await(Duration timeout) throws InterruptedException; }
    private static boolean await(DrainWait wait, long nanos, long start, RepositoryReadControl control)
            throws InterruptedException {
        while (true) {
            control.check();
            long slice = Math.min(remaining(nanos, start).toNanos(),
                    Math.min(100_000_000L, Math.max(0, control.remainingNanos())));
            boolean drained = wait.await(Duration.ofNanos(slice));
            control.check();
            if (drained) return true;
            if (System.nanoTime() - start >= nanos) return false;
        }
    }
    private static ByteString digest(ByteString bytes) {
        return ByteString.copyFrom(DocumentPublicationPreparationJournal.digest(bytes));
    }
    private static Duration remaining(long nanos, long start) {
        return Duration.ofNanos(Math.max(0, nanos - (System.nanoTime() - start)));
    }
    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Historical attempt admission is closed");
    }
    private static RepositoryException conflict(String message) {
        return new RepositoryException(RepositoryException.Code.CONFLICT, message);
    }
}
