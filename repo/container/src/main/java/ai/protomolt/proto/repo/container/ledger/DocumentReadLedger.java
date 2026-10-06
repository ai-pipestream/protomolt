package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.LinkedHashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Owns one fresh document-reader incarnation and its local protected lifetimes.
 * Pins do not expire. The host must preserve each Use through actual provider
 * completion and every returned batch, including after cancellation.
 * This ledger borrows its Tx and owns no executor or provider client.
 */
public final class DocumentReadLedger {
    private enum Completion { RELEASE, RECOVER, CONFIRM_RELEASED }
    private final Tx tx;
    private final UUID incarnation;
    private final Object lifetime = new Object();
    private boolean admissionClosed;
    private int activeLifetimes;
    private boolean fenced;
    private boolean quiesced;
    private boolean shutdownClosing;
    private final int maxOutstandingReads;
    private int outstandingReads;
    private final LinkedHashSet<PinnedRead<?>> retainedReads = new LinkedHashSet<>();

    /** Duplicate identities fail; a restarted owner must register a fresh UUID. */
    public DocumentReadLedger(Tx tx, UUID incarnation) {
        this(tx, incarnation, 32);
    }

    /** Bounds captures, active batches and drained handles awaiting successful SQL release together. */
    public DocumentReadLedger(Tx tx, UUID incarnation, int maxOutstandingReads) {
        if (maxOutstandingReads < 1) throw new IllegalArgumentException("Outstanding read limit must be positive");
        this.maxOutstandingReads = maxOutstandingReads;
        this.tx = Objects.requireNonNull(tx);
        this.incarnation = Objects.requireNonNull(incarnation);
        ReaderRegistration.register(tx, incarnation);
    }

    /** Counts capture itself, so fencing cannot attest quiescence during SQL admission. */
    PinnedPlan capture(DocumentOperationUploadAdmission admission, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentOperationUploadAdmission.Prepared prepared) {
        beginCapture();
        var pending = new java.util.concurrent.atomic.AtomicReference<DocumentReadPins.Captured<DocumentRetainedReadPlan>>();
        boolean handedOff = false;
        try {
            var result = new PinnedPlan(admission.capturePinnedReads(caller, owner, prepared, incarnation, pending::set));
            register(result);
            handedOff = true;
            return result;
        } finally {
            if (!handedOff) finishFailedCapture(pending.get());
        }
    }

    /**
     * Captures native typed or opaque history under current READ policy. The host
     * must reauthorize delivery after provider I/O; pins protect retention only.
     */
    public PinnedHistory captureHistorical(RepositoryCaller caller,
            ai.protomolt.proto.repo.v1.NodeAddress address, UUID revision) {
        Objects.requireNonNull(address); Objects.requireNonNull(revision);
        beginCapture();
        var pending = new java.util.concurrent.atomic.AtomicReference<DocumentReadPins.Captured<DocumentHistoricalReadPlan>>();
        boolean handedOff = false;
        try {
            var captured = tx.inTransaction(em -> {
                em.createNativeQuery("SELECT require_active_repository_reader(:reader)")
                        .setParameter("reader", incarnation).getSingleResult();
                DocumentAdmissionAuthorization.authorizeHistory(em, caller, address);
                var plan = DocumentHistoricalReadRows.capture(em, address, revision);
                var protectedRead = DocumentReadPins.acquireHistorical(em, plan, incarnation);
                pending.set(protectedRead);
                return protectedRead;
            });
            var result = new PinnedHistory(captured, caller);
            register(result);
            handedOff = true;
            return result;
        } finally {
            if (!handedOff) finishFailedCapture(pending.get());
        }
    }

    /**
     * A bounded admission-time maintenance pass. Does no SQL while capacity is
     * available, and never releases a handle with a live use. Concurrent callers
     * still compete for capacity; this does not reserve a capture slot.
     */
    public int releaseDrainedAtCapacity(int limit) {
        if (limit < 1) throw new IllegalArgumentException("Release limit must be positive");
        synchronized (lifetime) {
            if (admissionClosed || outstandingReads < maxOutstandingReads) return 0;
        }
        return releaseDrained(limit);
    }

    private void beginCapture() {
        synchronized (lifetime) {
            if (admissionClosed) throw new IllegalStateException("Reader admission is closed");
            if (outstandingReads == maxOutstandingReads)
                throw new ai.protomolt.proto.repo.spi.RepositoryException(
                        ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Outstanding document read capacity exhausted");
            outstandingReads++; activeLifetimes++;
        }
    }

    /** Internal retained bindings; provider reads require an open Use and current delivery authorization. */
    PinnedAssessment captureAssessment(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            ai.protomolt.proto.repo.spi.DocumentPublicationCommand command,
            java.util.Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selections,
            UUID assessment, String manifestSha, java.time.Instant deadline,
            ai.protomolt.proto.repo.blob.spi.PayloadBudget budget, Runnable control) {
        var protection = new DocumentAssessmentReadProtection<>(
                new DocumentAssessmentCreation.Created(assessment, manifestSha, deadline), assessment, incarnation, UUID.randomUUID());
        beginCapture();
        boolean handedOff = false;
        var commitReady = new java.util.concurrent.atomic.AtomicBoolean();
        try {
            var plan = new DocumentAssessmentReconciliation(tx).captureRetained(caller, owner, command, selections,
                    assessment, manifestSha, deadline, budget, control, incarnation, protection.session(),
                    () -> commitReady.set(true));
            var result = new PinnedAssessment(new DocumentAssessmentReadProtection<>(plan, assessment, incarnation, protection.session()),
                    caller, new DocumentAssessmentReadAuthority.LiveOwner(owner), command);
            register(result);
            handedOff = true;
            try {
                if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Assessment capture interrupted");
                control.run();
                if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Assessment capture interrupted");
                return result;
            } catch (RuntimeException | Error failure) {
                result.close(); // Commit is acknowledged; ordinary drained release can finish this handle.
                throw failure;
            }
        } finally {
            if (!handedOff) finishFailedCapture(commitReady.get() ? protection : null);
        }
    }

    /** Receipt-authorized evidence read; no write-owner nonce, lease or current schema policy. */
    PinnedAssessment captureRejectedAssessment(RepositoryCaller caller,
            ai.protomolt.proto.repo.spi.DocumentPublicationCommand command,
            ai.protomolt.proto.repo.blob.spi.PayloadBudget budget, ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        UUID session = UUID.randomUUID();
        var pending = new java.util.concurrent.atomic.AtomicReference<DocumentAssessmentReadProtection<DocumentAssessmentReadPlan>>();
        beginCapture();
        boolean handedOff = false;
        try {
            var captured = DocumentRejectedAssessmentReads.capture(tx, caller, command, budget, incarnation, session, control,
                    plan -> pending.set(new DocumentAssessmentReadProtection<>(plan, plan.stage().assessment(), incarnation, session)));
            var result = new PinnedAssessment(new DocumentAssessmentReadProtection<>(captured.plan(), captured.plan().stage().assessment(), incarnation, session),
                    caller, new DocumentAssessmentReadAuthority.Rejected(captured.receipt()), command);
            register(result);
            handedOff = true;
            try { control.check(); return result; }
            catch (RuntimeException | Error failure) { result.close(); throw failure; }
        } finally {
            if (!handedOff) finishFailedCapture(pending.get());
        }
    }

    private void failedCapture() {
        synchronized (lifetime) { outstandingReads--; activeLifetimes--; lifetime.notifyAll(); }
    }

    /** No provider use escaped; keep exact identities if commit may have been attempted. */
    private <P> void finishFailedCapture(DocumentReadProtection<P> pending) {
        if (pending == null) { failedCapture(); return; }
        var retained = new PinnedRead<>(pending, true) {};
        register(retained);
        retained.close();
    }

    private void register(PinnedRead<?> read) {
        synchronized (lifetime) {
            retainedReads.add(read);
            if (shutdownClosing) read.close();
        }
    }

    /** Stops new uses, including on captures completing concurrently. Existing uses still protect work. */
    void closeForShutdown() {
        synchronized (lifetime) {
            admissionClosed = true; shutdownClosing = true;
            retainedReads.forEach(PinnedRead::close);
        }
        fence();
    }

    boolean awaitLocalDrain(Duration timeout) throws InterruptedException {
        if (timeout.isNegative()) throw new IllegalArgumentException("Drain timeout must not be negative");
        long budget = timeout.toNanos(), start = System.nanoTime();
        synchronized (lifetime) {
            if (!shutdownClosing) throw new IllegalStateException("Shutdown has not stopped reader admission");
            while (activeLifetimes != 0) {
                long remaining = budget - (System.nanoTime() - start);
                if (remaining <= 0) return false;
                TimeUnit.NANOSECONDS.timedWait(lifetime, remaining);
            }
            return true;
        }
    }

    int recoverQuiescedPins(int limit) {
        int pins = new DocumentReadRecovery(tx).recoverBatch(incarnation, limit);
        if (pins == limit) return pins;
        return pins + new DocumentAssessmentReadRecovery(tx).recoverBatch(incarnation, limit - pins);
    }

    /** Includes in-progress captures and drained handles whose SQL release has not succeeded. */
    public int outstandingReads() { synchronized (lifetime) { return outstandingReads; } }

    /**
     * Retry at most limit drained handles without holding the lifetime monitor over
     * SQL. Never closes active plans or treats cancellation as drain. Failures stay
     * owned and move behind other candidates; all failures in this pass are reported.
     * The host supplies bounded SQL timeouts and schedules retries/shutdown itself.
     */
    public int releaseDrained(int limit) {
        return finishDrained(limit, Completion.RELEASE);
    }

    /**
     * Confirm exact local handles after durable recovery or an uncertain release
     * acknowledgment. Requires local drain and durable QUIESCED state. Does not
     * delete pins, close plans or recover an unknown capture; durable discovery
     * owns those missing handles. Remaining pins keep their capacity reservation.
     * The return value counts retired local handles only: zero is not proof of
     * incarnation quiescence or an empty durable pin set.
     */
    public int reconcileDrained(int limit) {
        return finishDrained(limit, Completion.CONFIRM_RELEASED);
    }

    private int finishDrained(int limit, Completion completion) {
        if (limit < 1) throw new IllegalArgumentException("Release batch limit must be positive");
        final java.util.List<PinnedRead<?>> ready;
        synchronized (lifetime) {
            ready = retainedReads.stream().filter(PinnedRead::isDrained).limit(limit).toList();
        }
        int completed = 0;
        IllegalStateException failures = null;
        for (var read : ready) {
            try {
                if (read.finish(completion)) completed++;
                else rotatePending(read);
            }
            catch (RuntimeException failure) {
                rotatePending(read);
                if (failures == null) failures = new IllegalStateException("Document read pin completion failed; handles retained for retry", failure);
                else failures.addSuppressed(failure);
            }
        }
        if (failures != null) throw failures;
        return completed;
    }

    private void rotatePending(PinnedRead<?> read) {
        synchronized (lifetime) {
            if (retainedReads.remove(read)) retainedReads.add(read);
        }
    }

    /** Stops new capture and new Uses permanently; existing Uses still own their work. */
    public synchronized void fence() {
        if (fenced) return;
        synchronized (lifetime) { admissionClosed = true; }
        tx.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT fence_repository_reader(:id)")
                    .setParameter("id", incarnation).getSingleResult()))
                throw new IllegalStateException("Reader fence was not acknowledged");
        });
        fenced = true;
    }

    /** Local lifetime proof only; failed SQL releases may still leave recoverable pins. */
    public synchronized void attestLocalQuiescence() {
        if (quiesced) return;
        synchronized (lifetime) {
            if (!fenced || !admissionClosed || activeLifetimes != 0)
                throw new IllegalStateException("Reader must be fenced with all local lifetimes completed");
        }
        tx.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)")
                    .setParameter("id", incarnation).getSingleResult()))
                throw new IllegalStateException("Reader quiescence was not acknowledged");
        });
        quiesced = true;
    }

    /**
     * Close stops new uses, without waiting or releasing SQL pins. A coordinator
     * must await drain and explicitly release (or recover) this handle. Retain it
     * after a failed release for retry. No callback runs on the last provider worker.
     */
    public final class PinnedPlan extends PinnedRead<DocumentRetainedReadPlan> {
        private PinnedPlan(DocumentReadPins.Captured<DocumentRetainedReadPlan> captured) { super(captured); }
    }

    public final class PinnedAssessment extends PinnedRead<DocumentAssessmentReadPlan> {
        private final DocumentAssessmentReadProtection<DocumentAssessmentReadPlan> assessment;
        private final RepositoryCaller caller;
        private final DocumentAssessmentReadAuthority authority;
        private final ai.protomolt.proto.repo.spi.DocumentPublicationCommand command;
        private PinnedAssessment(DocumentAssessmentReadProtection<DocumentAssessmentReadPlan> captured,
                RepositoryCaller caller, DocumentAssessmentReadAuthority authority,
                ai.protomolt.proto.repo.spi.DocumentPublicationCommand command) {
            super(captured);
            this.assessment = captured; this.caller = caller; this.authority = authority; this.command = command;
        }

        /** The provider/batch owner supplies its existing Use; this check creates no new lifetime. */
        public void authorizeDelivery(PinnedRead<?>.Use use, ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
            if (Objects.requireNonNull(use).plan() != assessment.plan())
                throw new IllegalArgumentException("Delivery use belongs to another assessment capture");
            authority.check(tx, caller, command, assessment, control);
            use.plan(); // Refuse delivery if the caller ended its lifetime while SQL waited.
        }

        /** Internal owned evidence scope; provider fragments and replay remain separate. */
        DocumentAssessmentReplayInputs loadReplayInputs(ai.protomolt.proto.repo.blob.spi.PayloadBudget budget,
                ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
            return DocumentAssessmentReplayInputs.load(tx, this, command, budget, control);
        }
    }

    public final class PinnedHistory extends PinnedRead<DocumentHistoricalReadPlan> {
        private final RepositoryCaller caller;
        private final DocumentHistoricalReadPlan plan;
        private final ai.protomolt.proto.repo.v1.NodeAddress address;
        private final UUID revision;
        private PinnedHistory(DocumentReadPins.Captured<DocumentHistoricalReadPlan> captured, RepositoryCaller caller) {
            super(captured);
            this.plan = captured.plan();
            this.caller = Objects.requireNonNull(caller);
            this.address = captured.plan().address();
            this.revision = captured.plan().revision();
        }

        /**
         * Selects an exact retained binding under current READ authorization. The
         * caller keeps its existing Use through provider work and reference commit.
         * This issues neither a current admission verdict nor publication authority.
         */
        public java.util.List<DocumentHistoricalReadPlan.Entry> selectRetained(PinnedRead<?>.Use use,
                java.util.List<ai.protomolt.proto.repo.v1.PublicationHistoricalReuse> selections,
                ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
            Objects.requireNonNull(control).check();
            if (Objects.requireNonNull(use).plan() != plan)
                throw new IllegalArgumentException("Selection use belongs to another historical capture");
            Objects.requireNonNull(selections);
            if (selections.isEmpty() || selections.size() > ai.protomolt.proto.repo.spi.DocumentPublicationCommand.MAX_PARTS)
                throw new IllegalArgumentException("Historical selector count exceeds bounds");
            selections = java.util.List.copyOf(selections);
            long bytes = 0;
            for (var selection : selections) {
                control.check();
                bytes += selection.getSerializedSize();
                if (bytes > ai.protomolt.proto.repo.spi.DocumentPublicationCommand.MAX_COMMAND_BYTES)
                    throw new IllegalArgumentException("Historical selectors exceed command byte bound");
            }
            authorizeDelivery(control);
            var selected = new java.util.ArrayList<DocumentHistoricalReadPlan.Entry>(selections.size());
            for (var selection : selections) {
                control.check();
                selected.add(DocumentHistoricalSelection.select(plan, selection));
            }
            control.check();
            use.plan(); // Refuse exposure if ownership ended while authorization waited.
            return java.util.List.copyOf(selected);
        }

        /**
         * Replays exact supplied fragments using retained definitions only. The
         * caller owns and budgets fragment copies and must hold its provider batch
         * open. This method reserves retained SQL byte copies before loading them.
         */
        public DocumentHistoricalValidation validateFragments(
                java.util.Map<Integer, com.google.protobuf.ByteString> fragments,
                ai.protomolt.proto.repo.blob.spi.PayloadBudget budget,
                ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
            Objects.requireNonNull(fragments); Objects.requireNonNull(budget); Objects.requireNonNull(control);
            var leases = new java.util.ArrayList<ai.protomolt.proto.repo.blob.spi.PayloadBudget.Lease>();
            boolean handedOff = false;
            try {
                var proof = new DocumentHistoricalSchemas(tx).check(caller, address, revision, fragments, control::check, bytes -> {
                    try { leases.add(budget.reserve(bytes)); }
                    catch (ai.protomolt.proto.repo.blob.spi.PayloadBudget.CapacityExceededException exhausted) {
                        throw new ai.protomolt.proto.repo.spi.RepositoryException(
                                ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED,
                                "Historical schema capacity exhausted", exhausted);
                    }
                });
                authorizeDelivery(control);
                var validation = new DocumentHistoricalValidation(proof, leases);
                handedOff = true;
                return validation;
            } catch (RuntimeException failure) {
                // Error reauthorization may itself wait on SQL. Do not return
                // detailed replay failures after cancellation or expiry during that wait.
                control.check();
                throw failure;
            } finally {
                if (!handedOff) leases.forEach(ai.protomolt.proto.repo.blob.spi.PayloadBudget.Lease::close);
            }
        }

        /**
         * Decodes one recorded typed occurrence, without rerunning admission validation.
         * Caller supplies a stable exact provider fragment; this method copies it and
         * retains its own pin use until result close. Host budgets parsed heap separately.
         */
        public DocumentHistoricalMaterialization materializeFragment(int ordinal, com.google.protobuf.ByteString fragment,
                ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Selection selection,
                ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Limits limits,
                ai.protomolt.proto.repo.blob.spi.PayloadBudget budget,
                ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
            Objects.requireNonNull(fragment); Objects.requireNonNull(selection); Objects.requireNonNull(limits);
            Objects.requireNonNull(budget); Objects.requireNonNull(control);
            var pin = use();
            ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Result decoded = null;
            boolean handedOff = false;
            try {
                var value = new DocumentHistoricalMaterializer(tx).read(caller, address, revision, ordinal,
                        fragment, selection, limits, budget, control);
                decoded = value;
                var result = new DocumentHistoricalMaterialization(this, pin, value);
                handedOff = true;
                return result;
            } finally {
                if (!handedOff) {
                    if (decoded != null) decoded.close();
                    pin.close();
                }
            }
        }

        /** Rechecks current policy for the exact caller bound at capture; grants no new read lifetime. */
        public void authorizeDelivery(ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
            Objects.requireNonNull(control).check();
            tx.inTransaction(em -> {
                DocumentAdmissionAuthorization.authorizeHistory(em, caller, address);
                control.check();
            });
            control.check();
        }
    }

    /** Shared ownership of a ledger-issued plan; only this ledger can create handles. */
    public abstract class PinnedRead<P> implements AutoCloseable {
        private final DocumentReadProtection<P> captured;
        private final boolean uncertainCapture;
        private final CountDownLatch drained = new CountDownLatch(1);
        private final Object releaseLock = new Object();
        private int uses;
        private boolean closed;
        private boolean released;

        private PinnedRead(DocumentReadProtection<P> captured) { this(captured, false); }
        private PinnedRead(DocumentReadProtection<P> captured, boolean uncertainCapture) {
            this.captured = captured;
            this.uncertainCapture = uncertainCapture;
        }

        public Use use() {
            synchronized (lifetime) {
                if (admissionClosed || closed) throw new IllegalStateException("Read plan admission is closed");
                var use = new Use();
                uses++;
                return use;
            }
        }

        @Override public void close() {
            synchronized (lifetime) {
                if (closed) return;
                closed = true;
                finishIfDrained();
            }
        }

        // Called only at close or when an open Use closes. The transition is once-only.
        private void finishIfDrained() {
            if (closed && uses == 0) {
                activeLifetimes--;
                drained.countDown();
                lifetime.notifyAll();
            }
        }

        public boolean isDrained() { return drained.getCount() == 0; }

        /** Timeout or interruption leaves all pins intact. Zero timeout is a readiness check. */
        public boolean awaitDrained(Duration timeout) throws InterruptedException {
            Objects.requireNonNull(timeout);
            if (timeout.isNegative()) throw new IllegalArgumentException("Drain timeout must not be negative");
            return drained.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        /** Does SQL only after actual local drain; failures propagate and remain retryable. */
        public void release() { finish(Completion.RELEASE); }

        /** Requires local drain and V46's durable QUIESCED proof; does not attest that proof. */
        public void recover() { finish(Completion.RECOVER); }

        private boolean finish(Completion completion) {
            if (!isDrained()) throw new IllegalStateException("Read plan must be closed and drained before release");
            synchronized (releaseLock) {
                if (released) return false;
                // A failed commit acknowledgment is not permission to treat missing
                // pins as released while the original transaction could still finish.
                if (uncertainCapture && completion == Completion.RELEASE) return false;
                switch (completion) {
                    case RELEASE -> captured.release(tx);
                    case RECOVER -> captured.recover(tx);
                    case CONFIRM_RELEASED -> {
                        if (!captured.confirmReleased(tx)) return false;
                    }
                }
                released = true;
                synchronized (lifetime) {
                    if (retainedReads.remove(this)) outstandingReads--;
                }
                return true;
            }
        }

        /** A single owner's lifetime. Closing asserts that this owner no longer uses its bytes. */
        public final class Use implements AutoCloseable {
            private boolean ended;
            private Use() {}

            public P plan() {
                synchronized (lifetime) {
                    if (ended) throw new IllegalStateException("Read plan use has ended");
                    return captured.plan();
                }
            }

            /**
             * Moves an already admitted lifetime from setup to a batch. It remains
             * valid after plan/host fencing; it neither starts a new use nor leaves
             * an unprotected gap. Closing the old handle is then a no-op.
             */
            public Use transfer() {
                synchronized (lifetime) {
                    if (ended) throw new IllegalStateException("Read plan use has ended");
                    var next = new Use();
                    ended = true;
                    return next;
                }
            }

            @Override public void close() {
                synchronized (lifetime) {
                    if (ended) return;
                    ended = true;
                    uses--;
                    finishIfDrained();
                }
            }
        }
    }
}
