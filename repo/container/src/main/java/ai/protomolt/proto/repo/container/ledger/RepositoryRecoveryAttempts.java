package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;

/** Private host retry ownership before session activation. Closing admission never discards uncertain work. */
final class RepositoryRecoveryAttempts implements AutoCloseable {
    enum Phase { PROPOSED, RESERVED, INSTALLED, ACTIVATED, RETIRED }
    record Drain(int active, int unresolved) {}
    private final Tx tx;
    private final PayloadBudget budget;
    private final DocumentPublicationSessions sessions;
    private final Duration lease;
    private final SqlTimeouts timeouts;
    private final int capacity;
    private final Map<RepositoryOperationLedger.Key,Entry> entries=new HashMap<>();
    private boolean closed;
    private boolean detaching;
    private int activeCalls;

    private static final class Entry {
        final DocumentPublicationCommand command;
        final boolean processCaller;
        final Optional<RepositoryCredentialBinding> credential;
        RepositoryCoordinatorReservation.Proposal proposal;
        DocumentPublicationSessions.SuccessorTarget target;
        Pending pending;
        final PayloadBudget.Lease commandBytes;
        boolean active;
        Phase phase=Phase.PROPOSED;
        RepositoryReservedPreparation.Loaded loaded;
        PayloadBudget.Lease nextBytes;
        RepositorySuccessorInstall.Plan plan;
        DocumentSuccessorFingerprint submitted;
        Entry(DocumentPublicationCommand command, RepositoryCaller caller,
                RepositoryCoordinatorReservation.Proposal proposal, DocumentPublicationSessions.SuccessorTarget target,
                PayloadBudget.Lease bytes) {
            this.command=command; processCaller=caller.processAuthority(); credential=caller.credentialBinding();
            this.proposal=proposal; this.target=target; commandBytes=bytes;
        }
        void releasePreparation() {
            if (loaded!=null) loaded.close();
            if (nextBytes!=null) nextBytes.close();
            loaded=null; nextBytes=null; plan=null;
        }
        void release() {
            releasePreparation();
            commandBytes.close();
        }
    }

    private record Pending(RepositoryCoordinatorReservation.SupersededUnactivated proposal,
            DocumentPublicationSessions.SuccessorTarget target) {}

    RepositoryRecoveryAttempts(Tx tx, PayloadBudget budget, DocumentPublicationSessions sessions,
            Duration lease, SqlTimeouts timeouts, int capacity) {
        this.tx=Objects.requireNonNull(tx).withTimeouts(Objects.requireNonNull(timeouts)); this.budget=Objects.requireNonNull(budget);
        this.sessions=Objects.requireNonNull(sessions); this.lease=Objects.requireNonNull(lease);
        this.timeouts=Objects.requireNonNull(timeouts);
        if (capacity<1) throw new IllegalArgumentException("Recovery capacity must be positive");
        if (lease.compareTo(Duration.ofSeconds(1))<0 || lease.compareTo(Duration.ofDays(1))>0)
            throw new IllegalArgumentException("Recovery lease requires one second to one day");
        sessions.coordinatorIdentity(); this.capacity=capacity;
    }

    /** No SQL here. An existing entry wins over a newer observation of its own reservation. */
    synchronized Attempt begin(RepositoryCaller caller, DocumentPublicationCommand command,
            RepositoryCoordinatorRecoveryDiscovery.Observation observed) {
        var retained=resume(caller,command);
        if (retained.isPresent()) return retained.orElseThrow();
        Objects.requireNonNull(observed);
        var key=new RepositoryOperationLedger.Key(command.intent().getAccountId(),caller.principalName(),command.operationId());
        if (entries.size()>=capacity || activeCalls>=capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Recovery capacity exhausted");
        var predecessor=observed.candidate().map(RepositoryCoordinatorRecoveryDiscovery.Candidate::predecessor)
                .or(() -> observed.unactivated().map(RepositoryCoordinatorRecoveryDiscovery.UnactivatedCandidate::predecessor))
                .orElseThrow(() -> new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Recovery state is not eligible: "+observed.status()));
        var target=sessions.successorTarget(key,command.sha256(),predecessor.incarnation());
        var proposal=proposal(observed,target.incarnation());
        if (!proposal.predecessor().key().equals(key) || !proposal.predecessor().commandSha256().equals(command.sha256()))
            throw conflict("Recovery observation differs from command");
        var bytes=budget.reserve((long)command.canonical().size()+command.intent().getSerializedSize());
        var entry=new Entry(command,caller,proposal,target,bytes); entries.put(key,entry);
        return borrow(key,entry);
    }

    /** Reopen only a local retained attempt, without SQL, discovery or a newly minted identity. */
    synchronized Optional<Attempt> resume(RepositoryCaller caller, DocumentPublicationCommand command) {
        if (closed) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE,"Recovery admission is closed");
        Objects.requireNonNull(caller); Objects.requireNonNull(command);
        var key=new RepositoryOperationLedger.Key(command.intent().getAccountId(),caller.principalName(),command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller,key,key.account());
        var entry=entries.get(key);
        if (entry==null) return Optional.empty();
        requireCaller(entry,caller);
        if (!entry.command.canonical().equals(command.canonical())) throw conflict("Recovery command changed");
        if (entry.active) throw conflict("Recovery attempt is in use");
        if (activeCalls>=capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Recovery call capacity exhausted");
        return Optional.of(borrow(key,entry));
    }

    private Attempt borrow(RepositoryOperationLedger.Key key, Entry entry) {
        entry.active=true;
        activeCalls++;
        return new Attempt(key,entry);
    }

    private RepositoryCoordinatorReservation.Proposal proposal(RepositoryCoordinatorRecoveryDiscovery.Observation observed, UUID incarnation) {
        var token=UUID.randomUUID();
        if (observed.status()==RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND) {
            var source=observed.candidate().orElseThrow();
            return new RepositoryCoordinatorReservation.ExpiredUnquiesced(source.predecessor(),token,incarnation,lease,source.owner());
        }
        if (observed.unactivated().isPresent()) {
            var source=observed.unactivated().orElseThrow();
            return new RepositoryCoordinatorReservation.SupersededUnactivated(source.predecessor(),token,incarnation,lease,
                    source.owner(),source.preparationSha256(),source.installation());
        }
        throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,"Recovery state is not eligible: "+observed.status());
    }

    final class Attempt implements AutoCloseable {
        private final RepositoryOperationLedger.Key key;
        private final Entry entry;
        private boolean ended;
        private Map<String,DocumentPublicationCandidate.Mode> activatedModes;
        private Attempt(RepositoryOperationLedger.Key key,Entry entry) { this.key=key; this.entry=entry; }

        /** One bounded protocol phase. Both authorities are supplied anew; no cached permission grants. */
        synchronized Phase advance(RepositoryCaller authority, RepositoryCaller caller,
                Map<String, DocumentPublicationCandidate.Mode> expectedModes, RepositoryReadControl control) {
            if (ended) throw new IllegalStateException("Recovery attempt call is closed");
            Objects.requireNonNull(control).check();
            RepositoryCoordinatorReservation.require(authority,entry.proposal,control);
            DocumentAdmissionAuthorization.requireCaller(caller,key,key.account()); requireCaller(entry,caller);
            if (entry.pending!=null) throw conflict("Pending supersession must be confirmed before advancing");
            Objects.requireNonNull(expectedModes);
            if (expectedModes.size()>10000) throw new IllegalArgumentException("Invalid publication mode count");
            var requestedModes=Map.copyOf(expectedModes);
            DocumentPublicationModesJournal.encode(entry.command,requestedModes);
            control.check();
            switch (entry.phase) {
                case PROPOSED -> {
                    switch (entry.proposal) {
                        case RepositoryCoordinatorReservation.ExpiredUnquiesced p -> RepositoryCoordinatorExpiration.reserve(tx,authority,p,control);
                        case RepositoryCoordinatorReservation.SupersededUnactivated p -> RepositoryCoordinatorSupersession.reserve(tx,authority,p,control);
                        default -> throw new IllegalStateException("Unsupported recovery reservation kind");
                    }
                    entry.phase=Phase.RESERVED;
                }
                case RESERVED -> {
                    if (entry.loaded==null) entry.loaded=new RepositoryReservedPreparation(tx,budget,timeouts).load(
                            authority,caller,entry.proposal,RepositoryCoordinatorReservation.owner(entry.proposal).orElseThrow(),control);
                    requireModes(entry.loaded.modes(),requestedModes);
                    if (entry.plan==null) {
                        var bytes=budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES);
                        try {
                            entry.plan=RepositorySuccessorInstall.prepare(entry.proposal,entry.loaded.record(),lease,entry.loaded.modes());
                            entry.nextBytes=bytes;
                        } catch (RuntimeException | Error failure) { bytes.close(); throw failure; }
                    }
                    RepositorySuccessorInstall.install(tx,budget,authority,entry.plan,control);
                    entry.phase=Phase.INSTALLED;
                }
                case INSTALLED -> {
                    requireModes(entry.loaded.modes(),requestedModes);
                    // A previous failed activation may still occupy this operation's local slot.
                    // False is not permission to replace it: activation still checks its fingerprint.
                    sessions.retireSuperseded(caller,entry.command,control);
                    sessions.activateSuccessor(entry.target,authority,caller,entry.plan,control,
                            fingerprint -> entry.submitted=fingerprint);
                    activatedModes=entry.loaded.modes();
                    entry.phase=Phase.ACTIVATED;
                    synchronized (RepositoryRecoveryAttempts.this) { entries.remove(key,entry); }
                    entry.release();
                }
                case ACTIVATED -> requireModes(activatedModes,requestedModes);
                case RETIRED -> throw conflict("Recovery attempt is retired");
            }
            return entry.phase;
        }

        /** One explicit supersession, not an automatic retry loop or a quiescence assertion. */
        synchronized Phase supersedeExpired(RepositoryCaller authority, RepositoryCaller caller,
                RepositoryReadControl control) {
            if (ended) throw new IllegalStateException("Recovery attempt call is closed");
            Objects.requireNonNull(control).check();
            RepositoryCoordinatorReservation.require(authority,entry.proposal,control);
            DocumentAdmissionAuthorization.requireCaller(caller,key,key.account()); requireCaller(entry,caller);
            if (entry.phase==Phase.ACTIVATED || entry.phase==Phase.RETIRED)
                throw conflict("Completed local recovery requires its session reconciliation path");
            if (entry.pending==null) {
                var observed=new RepositoryCoordinatorRecoveryDiscovery(tx,timeouts)
                        .inspect(authority,key,entry.command.sha256(),control);
                var source=observed.unactivated().orElseThrow(() -> new RepositoryException(
                        RepositoryException.Code.FAILED_PRECONDITION,"Recovery successor is not expired and unactivated"));
                var expected=new RepositoryCoordinatorDrain.Identity(key,entry.command.sha256(),
                        entry.proposal.predecessor().epoch()+1,entry.proposal.successorToken(),entry.proposal.successorIncarnation());
                if (!expected.equals(source.predecessor())) throw conflict("Recovery successor differs from retained attempt");
                var target=sessions.successorTarget(key,entry.command.sha256(),expected.incarnation());
                var proposal=new RepositoryCoordinatorReservation.SupersededUnactivated(expected,UUID.randomUUID(),
                        target.incarnation(),lease,source.owner(),source.preparationSha256(),source.installation());
                entry.pending=new Pending(proposal,target);
            }
            var pending=entry.pending;
            RepositoryCoordinatorSupersession.reserve(tx,authority,pending.proposal(),control);
            // Commit confirmation fences the old claim. It does not settle old provider work.
            entry.releasePreparation();
            entry.proposal=pending.proposal(); entry.target=pending.target();
            entry.phase=Phase.RESERVED; entry.pending=null;
            return entry.phase;
        }

        /** Releases this owner's memory only; separately retained sessions and provider work remain owned. */
        synchronized boolean retireFenced(RepositoryCaller authority, RepositoryCaller caller, RepositoryReadControl control) {
            if (ended) throw new IllegalStateException("Recovery attempt call is closed");
            Objects.requireNonNull(control).check();
            RepositoryCoordinatorReservation.require(authority,entry.proposal,control);
            DocumentAdmissionAuthorization.requireCaller(caller,key,key.account()); requireCaller(entry,caller);
            if (entry.phase==Phase.RETIRED) return true;
            if (entry.phase==Phase.ACTIVATED) throw conflict("Activated recovery is owned by its session");
            var identities=new ArrayList<RepositoryCoordinatorDrain.Identity>(2);
            identities.add(successorIdentity(entry.proposal));
            if (entry.pending!=null) identities.add(successorIdentity(entry.pending.proposal()));
            boolean fenced=RepositoryClaimRetirement.fenced(tx,entry.command,identities,control);
            if (!fenced) return false;
            entry.phase=Phase.RETIRED; entry.pending=null;
            synchronized (RepositoryRecoveryAttempts.this) { entries.remove(key,entry); }
            entry.release();
            return true;
        }

        synchronized RepositoryCoordinatorReservation.Proposal proposal() {
            if (ended) throw new IllegalStateException("Recovery attempt call is closed");
            return entry.proposal;
        }

        @Override public synchronized void close() {
            if (ended) return;
            ended=true;
            activatedModes=null;
            synchronized (RepositoryRecoveryAttempts.this) {
                entry.active=false;
                activeCalls--;
                RepositoryRecoveryAttempts.this.notifyAll();
            }
        }
    }

    private static RepositoryCoordinatorDrain.Identity successorIdentity(RepositoryCoordinatorReservation.Proposal proposal) {
        var predecessor=proposal.predecessor();
        return new RepositoryCoordinatorDrain.Identity(predecessor.key(),predecessor.commandSha256(),predecessor.epoch()+1,
                proposal.successorToken(),proposal.successorIncarnation());
    }

    private static void requireCaller(Entry entry,RepositoryCaller caller) {
        if (entry.processCaller!=caller.processAuthority() || !entry.credential.equals(caller.credentialBinding()))
            throw conflict("Recovery caller identity changed");
    }
    private static RepositoryException conflict(String message) { return new RepositoryException(RepositoryException.Code.CONFLICT,message); }
    private static void requireModes(Map<String,DocumentPublicationCandidate.Mode> fixed,
            Map<String,DocumentPublicationCandidate.Mode> requested) {
        if (!Objects.requireNonNull(fixed).equals(requested)) throw new RepositoryException(
                RepositoryException.Code.FAILED_PRECONDITION,"Retry publication modes differ from fixed modes");
    }

    /** Reports unresolved identities separately from active calls; neither field attests provider quiescence. */
    synchronized Drain drain() {
        if (!closed) throw new IllegalStateException("Close recovery admission before inspecting drain");
        return new Drain(activeCalls,entries.size());
    }

    /** Handle completion only; unresolved entries and session/provider work remain separately owned. */
    synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (!closed) throw new IllegalStateException("Close recovery admission before awaiting idle");
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative recovery wait");
        long remaining=timeout.toNanos(), started=System.nanoTime();
        while (activeCalls!=0) {
            if (remaining<=0) return false;
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this,remaining);
            remaining=timeout.toNanos()-(System.nanoTime()-started);
        }
        return true;
    }

    /**
     * Dispose local recovery copies only after closed admission and exact activation ownership.
     * This neither abandons SQL state nor proves session, provider or schema-worker quiescence.
     */
    boolean detachClosed(Duration wait, java.util.function.Function<RepositoryOperationLedger.Key,RepositoryCaller> authority,
            RepositoryReadControl control) throws InterruptedException {
        Objects.requireNonNull(wait); Objects.requireNonNull(authority); Objects.requireNonNull(control).check();
        if (wait.isNegative()) throw new IllegalArgumentException("Negative recovery wait");
        long started=System.nanoTime(), nanos=wait.toNanos();
        if (!awaitIdle(wait)) return false;
        control.check();
        final List<Entry> retained;
        synchronized (this) {
            if (detaching) return false;
            detaching=true;
            retained=List.copyOf(entries.values());
        }
        try {
            if (!sessions.captureShutdownRegistrations(Duration.ofNanos(Math.max(0,nanos-(System.nanoTime()-started))),control))
                return false;
            for (var entry : retained) {
                control.check();
                var key=entry.proposal.predecessor().key();
                var caller=Objects.requireNonNull(authority.apply(key),"Private recovery disposal authority");
                RepositoryCoordinatorReservation.require(caller,entry.proposal,control);
                if (entry.submitted!=null && !sessions.ownsShutdownActivation(entry.submitted)) {
                    // Closed, idle owners cannot retry. Keep command/proposal/fingerprint evidence,
                    // but release duplicate preparation graphs before bounded terminal inspection.
                    entry.releasePreparation();
                    var identity=successorIdentity(entry.submitted.reservation());
                    if (!RepositoryClaimRetirement.fenced(tx,entry.command,List.of(identity),control)
                            && !RepositorySuccessorShutdown.terminal(tx,budget,caller,entry.submitted,control)) return false;
                }
                control.check();
                entry.release();
                synchronized (this) {
                    entries.remove(key,entry);
                    entry.phase=Phase.RETIRED;
                    entry.pending=null;
                    entry.submitted=null;
                }
            }
            control.check();
            return true;
        } finally {
            synchronized (this) { detaching=false; notifyAll(); }
        }
    }
    @Override public synchronized void close() { closed=true; }
}
