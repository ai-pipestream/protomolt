package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;

/** Private host retry ownership before session activation. Closing admission never discards uncertain work. */
final class RepositoryRecoveryAttempts implements AutoCloseable {
    enum Phase { PROPOSED, RESERVED, INSTALLED, ACTIVATED }
    record Drain(int active, int unresolved) {}
    private final Tx tx;
    private final PayloadBudget budget;
    private final DocumentPublicationSessions sessions;
    private final Duration lease;
    private final SqlTimeouts timeouts;
    private final int capacity;
    private final Map<RepositoryOperationLedger.Key,Entry> entries=new HashMap<>();
    private boolean closed;
    private int activeCalls;

    private static final class Entry {
        final DocumentPublicationCommand command;
        final boolean processCaller;
        final Optional<RepositoryCredentialBinding> credential;
        final RepositoryCoordinatorReservation.Proposal proposal;
        final PayloadBudget.Lease commandBytes;
        boolean active;
        Phase phase=Phase.PROPOSED;
        RepositoryReservedPreparation.Loaded loaded;
        PayloadBudget.Lease nextBytes;
        RepositorySuccessorInstall.Plan plan;
        Entry(DocumentPublicationCommand command, RepositoryCaller caller,
                RepositoryCoordinatorReservation.Proposal proposal, PayloadBudget.Lease bytes) {
            this.command=command; processCaller=caller.processAuthority(); credential=caller.credentialBinding();
            this.proposal=proposal; commandBytes=bytes;
        }
        void release() {
            if (loaded!=null) loaded.close();
            if (nextBytes!=null) nextBytes.close();
            loaded=null; nextBytes=null; plan=null;
            commandBytes.close();
        }
    }

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
        if (closed) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE,"Recovery admission is closed");
        Objects.requireNonNull(caller); Objects.requireNonNull(command); Objects.requireNonNull(observed);
        var key=new RepositoryOperationLedger.Key(command.intent().getAccountId(),caller.principalName(),command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller,key,key.account());
        var entry=entries.get(key);
        if (entry!=null) {
            requireCaller(entry,caller);
            if (!entry.command.canonical().equals(command.canonical())) throw conflict("Recovery command changed");
            if (entry.active) throw conflict("Recovery attempt is in use");
            if (activeCalls>=capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Recovery call capacity exhausted");
        } else {
            if (entries.size()>=capacity || activeCalls>=capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Recovery capacity exhausted");
            var proposal=proposal(observed);
            if (!proposal.predecessor().key().equals(key) || !proposal.predecessor().commandSha256().equals(command.sha256()))
                throw conflict("Recovery observation differs from command");
            var bytes=budget.reserve((long)command.canonical().size()+command.intent().getSerializedSize());
            entry=new Entry(command,caller,proposal,bytes); entries.put(key,entry);
        }
        entry.active=true;
        activeCalls++;
        return new Attempt(key,entry);
    }

    private RepositoryCoordinatorReservation.Proposal proposal(RepositoryCoordinatorRecoveryDiscovery.Observation observed) {
        var token=UUID.randomUUID(); var incarnation=sessions.coordinatorIdentity();
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
        private Attempt(RepositoryOperationLedger.Key key,Entry entry) { this.key=key; this.entry=entry; }

        /** One bounded protocol phase. Both authorities are supplied anew; no cached permission grants. */
        synchronized Phase advance(RepositoryCaller authority, RepositoryCaller caller, RepositoryReadControl control) {
            if (ended) throw new IllegalStateException("Recovery attempt call is closed");
            Objects.requireNonNull(control).check();
            RepositoryCoordinatorReservation.require(authority,entry.proposal,control);
            DocumentAdmissionAuthorization.requireCaller(caller,key,key.account()); requireCaller(entry,caller);
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
                    sessions.activateSuccessor(authority,caller,entry.plan,control);
                    entry.phase=Phase.ACTIVATED;
                    synchronized (RepositoryRecoveryAttempts.this) { entries.remove(key,entry); }
                    entry.release();
                }
                case ACTIVATED -> { }
            }
            return entry.phase;
        }

        synchronized RepositoryCoordinatorReservation.Proposal proposal() {
            if (ended) throw new IllegalStateException("Recovery attempt call is closed");
            return entry.proposal;
        }

        @Override public synchronized void close() {
            if (ended) return;
            ended=true;
            synchronized (RepositoryRecoveryAttempts.this) { entry.active=false; activeCalls--; }
        }
    }

    private static void requireCaller(Entry entry,RepositoryCaller caller) {
        if (entry.processCaller!=caller.processAuthority() || !entry.credential.equals(caller.credentialBinding()))
            throw conflict("Recovery caller identity changed");
    }
    private static RepositoryException conflict(String message) { return new RepositoryException(RepositoryException.Code.CONFLICT,message); }

    /** Reports unresolved identities separately from active calls; neither field attests provider quiescence. */
    synchronized Drain drain() {
        if (!closed) throw new IllegalStateException("Close recovery admission before inspecting drain");
        return new Drain(activeCalls,entries.size());
    }
    @Override public synchronized void close() { closed=true; }
}
