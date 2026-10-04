package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Owns one fresh document-reader incarnation and its local protected lifetimes.
 * Pins do not expire. The host must preserve each Use through actual provider
 * completion and every returned batch, including after cancellation.
 * This ledger borrows its Tx and owns no executor or provider client.
 */
public final class DocumentReadLedger {
    private final Tx tx;
    private final UUID incarnation;
    private final Object lifetime = new Object();
    private boolean admissionClosed;
    private int activeLifetimes;
    private boolean fenced;
    private boolean quiesced;

    /** Duplicate identities fail; a restarted owner must register a fresh UUID. */
    public DocumentReadLedger(Tx tx, UUID incarnation) {
        this.tx = Objects.requireNonNull(tx);
        this.incarnation = Objects.requireNonNull(incarnation);
        tx.inTransaction(em -> {
            em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:id,'ACTIVE')")
                    .setParameter("id", incarnation).executeUpdate();
        });
    }

    /** Counts capture itself, so fencing cannot attest quiescence during SQL admission. */
    PinnedPlan capture(DocumentOperationUploadAdmission admission, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentOperationUploadAdmission.Prepared prepared) {
        synchronized (lifetime) {
            if (admissionClosed) throw new IllegalStateException("Reader admission is closed");
            activeLifetimes++;
        }
        boolean handedOff = false;
        try {
            var result = new PinnedPlan(admission.capturePinnedReads(caller, owner, prepared, incarnation));
            handedOff = true;
            return result;
        } finally {
            if (!handedOff) synchronized (lifetime) { activeLifetimes--; }
        }
    }

    /**
     * Captures native typed or opaque history under current READ policy. The host
     * must reauthorize delivery after provider I/O; pins protect retention only.
     */
    public PinnedHistory captureHistorical(RepositoryCaller caller,
            ai.protomolt.proto.repo.v1.NodeAddress address, UUID revision) {
        Objects.requireNonNull(address); Objects.requireNonNull(revision);
        synchronized (lifetime) {
            if (admissionClosed) throw new IllegalStateException("Reader admission is closed");
            activeLifetimes++;
        }
        boolean handedOff = false;
        try {
            var captured = tx.inTransaction(em -> {
                em.createNativeQuery("SELECT require_active_repository_reader(:reader)")
                        .setParameter("reader", incarnation).getSingleResult();
                DocumentAdmissionAuthorization.authorizeHistory(em, caller, address);
                var plan = DocumentHistoricalReadRows.capture(em, address, revision);
                return DocumentReadPins.acquireHistorical(em, plan, incarnation);
            });
            var result = new PinnedHistory(captured, caller);
            handedOff = true;
            return result;
        } finally {
            if (!handedOff) synchronized (lifetime) { activeLifetimes--; }
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

    public final class PinnedHistory extends PinnedRead<DocumentHistoricalReadPlan> {
        private final RepositoryCaller caller;
        private final ai.protomolt.proto.repo.v1.NodeAddress address;
        private PinnedHistory(DocumentReadPins.Captured<DocumentHistoricalReadPlan> captured, RepositoryCaller caller) {
            super(captured);
            this.caller = Objects.requireNonNull(caller);
            this.address = captured.plan().address();
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
        private final DocumentReadPins.Captured<P> captured;
        private final CountDownLatch drained = new CountDownLatch(1);
        private final Object releaseLock = new Object();
        private int uses;
        private boolean closed;
        private boolean released;

        private PinnedRead(DocumentReadPins.Captured<P> captured) { this.captured = captured; }

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
        public void release() { finish(false); }

        /** Requires local drain and V46's durable QUIESCED proof; does not attest that proof. */
        public void recover() { finish(true); }

        private void finish(boolean recovery) {
            if (!isDrained()) throw new IllegalStateException("Read plan must be closed and drained before release");
            synchronized (releaseLock) {
                if (released) return;
                if (recovery) DocumentReadPins.recover(tx, captured);
                else DocumentReadPins.release(tx, captured);
                released = true;
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
