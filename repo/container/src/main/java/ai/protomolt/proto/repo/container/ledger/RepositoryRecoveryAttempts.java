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
            var predecessor=observed.candidate().map(RepositoryCoordinatorRecoveryDiscovery.Candidate::predecessor)
                    .or(() -> observed.unactivated().map(RepositoryCoordinatorRecoveryDiscovery.UnactivatedCandidate::predecessor))
                    .orElseThrow(() -> new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                            "Recovery state is not eligible: "+observed.status()));
            var target=sessions.successorTarget(key,command.sha256(),predecessor.incarnation());
            var proposal=proposal(observed,target.incarnation());
            if (!proposal.predecessor().key().equals(key) || !proposal.predecessor().commandSha256().equals(command.sha256()))
                throw conflict("Recovery observation differs from command");
            var bytes=budget.reserve((long)command.canonical().size()+command.intent().getSerializedSize());
            entry=new Entry(command,caller,proposal,target,bytes); entries.put(key,entry);
        }
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
        private Attempt(RepositoryOperationLedger.Key key,Entry entry) { this.key=key; this.entry=entry; }

        /** One bounded protocol phase. Both authorities are supplied anew; no cached permission grants. */
        synchronized Phase advance(RepositoryCaller authority, RepositoryCaller caller, RepositoryReadControl control) {
            if (ended) throw new IllegalStateException("Recovery attempt call is closed");
            Objects.requireNonNull(control).check();
            RepositoryCoordinatorReservation.require(authority,entry.proposal,control);
            DocumentAdmissionAuthorization.requireCaller(caller,key,key.account()); requireCaller(entry,caller);
            if (entry.pending!=null) throw conflict("Pending supersession must be confirmed before advancing");
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
                    // A previous failed activation may still occupy this operation's local slot.
                    // False is not permission to replace it: activation still checks its fingerprint.
                    sessions.retireSuperseded(caller,entry.command,control);
                    sessions.activateSuccessor(entry.target,authority,caller,entry.plan,control);
                    entry.phase=Phase.ACTIVATED;
                    synchronized (RepositoryRecoveryAttempts.this) { entries.remove(key,entry); }
                    entry.release();
                }
                case ACTIVATED -> { }
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
            boolean fenced=tx.inTransaction(em -> {
                control.check();
                em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
                var rows=em.createNativeQuery("""
                        SELECT command_sha256,claim_epoch,claim_token FROM repository_execution_claims
                        WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                        """).setParameter("a",key.account()).setParameter("p",key.principal())
                        .setParameter("o",key.operationId()).getResultList();
                if (rows.isEmpty()) return false;
                var row=(Object[])rows.getFirst();
                if (!HexFormat.of().formatHex((byte[])row[0]).equals(entry.command.sha256()))
                    throw conflict("Recovery claim command changed");
                RepositoryOperationLedger.requireCommand(em,key,entry.command);
                long epoch=((Number)row[1]).longValue(); var token=(UUID)row[2];
                return excludes(epoch,token,entry.proposal)
                        && (entry.pending==null || excludes(epoch,token,entry.pending.proposal()));
            });
            control.check();
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
            synchronized (RepositoryRecoveryAttempts.this) { entry.active=false; activeCalls--; }
        }
    }

    /** V78 forbids claim deletion, epoch reversal and token changes within an epoch. */
    private static boolean excludes(long epoch, UUID token, RepositoryCoordinatorReservation.Proposal proposal) {
        long proposedEpoch=proposal.predecessor().epoch()+1;
        return epoch>proposedEpoch || epoch==proposedEpoch && !token.equals(proposal.successorToken());
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
