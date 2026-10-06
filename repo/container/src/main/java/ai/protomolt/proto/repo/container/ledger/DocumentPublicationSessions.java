package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Bounded host-owned retry identities. No expiry, implicit takeover, or uncertain-outcome eviction. */
final class DocumentPublicationSessions implements AutoCloseable {
    private final Tx tx;
    private final DocumentPublicationExecution execution;
    private final DocumentPublicationReplay replay;
    private final Duration lease;
    private final int capacity;
    private final long maxCommandBytes;
    private final ai.protomolt.proto.repo.blob.spi.PayloadBudget journalBudget;
    private final UUID coordinator;
    private long commandBytes;
    private boolean closed;
    private int activeCalls;
    private final DocumentPublicationScopeCalls registrations = new DocumentPublicationScopeCalls();
    private final Map<RepositoryOperationLedger.Key, Entry> entries = new HashMap<>();
    // Captured only after registration admission closes and its accepted calls drain.
    // Never refresh from the cache: accepted calls may subsequently evict terminal entries.
    private java.util.List<ShutdownEntry> drainIdentities;

    private static final class ShutdownEntry {
        final RepositoryCoordinatorDrain.Identity identity;
        final DocumentSuccessorFingerprint successor;
        // Monotonic local outcome; an exact late activation cannot reopen this host.
        volatile boolean detachedUnactivated;
        ShutdownEntry(RepositoryCoordinatorDrain.Identity identity, DocumentSuccessorFingerprint successor) {
            this.identity = identity;
            this.successor = successor;
            if (successor != null) {
                var p = successor.reservation();
                if (!identity.key().equals(p.predecessor().key())
                        || !identity.commandSha256().equals(p.predecessor().commandSha256())
                        || identity.epoch() != p.predecessor().epoch()+1
                        || !identity.token().equals(p.successorToken())
                        || !identity.incarnation().equals(p.successorIncarnation()))
                    throw new IllegalStateException("Shutdown successor differs from retained identity");
            }
        }
    }

    private static final class Entry {
        final DocumentPublicationCommand command;
        final long commandBytes;
        DocumentPublicationSession session;
        DocumentPublicationRestoration restoration;
        RepositoryOperationLedger.Owner restorationOwner;
        DocumentSuccessorFingerprint successor;
        int users = 1;
        boolean terminal;
        boolean recovering;
        Entry(DocumentPublicationCommand command) {
            this.command = command;
            commandBytes = (long) command.canonical().size() + command.intent().getSerializedSize();
        }
    }

    DocumentPublicationSessions(Tx tx, DocumentPublicationExecution execution, Duration lease, int capacity, long maxCommandBytes) {
        this(tx, execution, lease, capacity, maxCommandBytes, null);
    }

    /** Explicit private opt-in; no host selects durable registration by default. */
    static DocumentPublicationSessions journaled(Tx tx, DocumentPublicationExecution execution, Duration lease,
            int capacity, long maxCommandBytes, ai.protomolt.proto.repo.blob.spi.PayloadBudget budget) {
        return new DocumentPublicationSessions(tx, execution, lease, capacity, maxCommandBytes, Objects.requireNonNull(budget));
    }

    private DocumentPublicationSessions(Tx tx, DocumentPublicationExecution execution, Duration lease, int capacity,
            long maxCommandBytes, ai.protomolt.proto.repo.blob.spi.PayloadBudget journalBudget) {
        this.journalBudget = journalBudget;
        coordinator = journalBudget == null ? null : UUID.randomUUID();
        this.tx = Objects.requireNonNull(tx); this.execution = Objects.requireNonNull(execution);
        this.lease = Objects.requireNonNull(lease);
        if (capacity < 1) throw new IllegalArgumentException("Publication session capacity must be positive");
        if (maxCommandBytes < 1) throw new IllegalArgumentException("Publication command byte capacity must be positive");
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Operation lease requires one second to one day");
        this.capacity = capacity; this.maxCommandBytes = maxCommandBytes; replay = new DocumentPublicationReplay(tx);
    }

    /** Trusted host identity for preparing a handoff to this manager. */
    UUID coordinatorIdentity() {
        if (coordinator == null) throw new IllegalStateException("Session manager does not own journaled registrations");
        return coordinator;
    }

    /**
     * Host-private target identity, not a proposal authorization. The retained
     * successor fingerprint and SQL checks bind the complete proposal and plan.
     * Retaining this capability grants no SQL claim or execution authority.
     */
    static final class SuccessorTarget {
        private final DocumentPublicationSessions manager;
        private final RepositoryOperationLedger.Key key;
        private final String commandSha256;
        private final UUID incarnation;
        private SuccessorTarget(DocumentPublicationSessions manager, RepositoryOperationLedger.Key key,
                String commandSha256, UUID incarnation) {
            this.manager = manager; this.key = key; this.commandSha256 = commandSha256; this.incarnation = incarnation;
        }
        UUID incarnation() { return incarnation; }
        @Override public String toString() { return "SuccessorTarget[private]"; }
    }

    /** The bounded recovery entry owns this capability; the manager keeps no extra target registry. */
    synchronized SuccessorTarget successorTarget(RepositoryOperationLedger.Key key, String commandSha256,
            UUID predecessorIncarnation) {
        if (closed) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Publication sessions are closed");
        coordinatorIdentity();
        Objects.requireNonNull(key); Objects.requireNonNull(predecessorIncarnation);
        if (commandSha256 == null || !commandSha256.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Expected command SHA-256");
        UUID incarnation;
        do { incarnation = UUID.randomUUID(); }
        while (incarnation.equals(predecessorIncarnation) || incarnation.equals(coordinator));
        return new SuccessorTarget(this, key, commandSha256, incarnation);
    }

    void activateSuccessor(SuccessorTarget target, RepositoryCaller coordinatorCaller, RepositoryCaller executionCaller,
            RepositorySuccessorInstall.Plan plan, RepositoryReadControl control) {
        Objects.requireNonNull(target); Objects.requireNonNull(plan);
        if (target.manager != this || !target.key.equals(plan.next().key())
                || !target.commandSha256.equals(plan.next().command().sha256())
                || !target.incarnation.equals(plan.reservation().successorIncarnation()))
            throw new IllegalArgumentException("Successor target differs from manager or plan");
        activateSuccessor(coordinatorCaller, executionCaller, plan, target.incarnation, control);
    }

    /**
     * Retain the exact successor before activation can commit. A failed or cancelled
     * attachment leaves that identity available for retry and shutdown reconciliation.
     * Both callers come from the trusted host; coordinator rights never replace the
     * execution caller's current authorization. This method performs no provider I/O.
     */
    void activateSuccessor(RepositoryCaller coordinatorCaller, RepositoryCaller executionCaller,
            RepositorySuccessorInstall.Plan plan, RepositoryReadControl control) {
        activateSuccessor(coordinatorCaller, executionCaller, plan, coordinatorIdentity(), control);
    }

    private void activateSuccessor(RepositoryCaller coordinatorCaller, RepositoryCaller executionCaller,
            RepositorySuccessorInstall.Plan plan, UUID incarnation, RepositoryReadControl control) {
        try (var call = beginCall(); var registration = registrations.enter()) {
            Objects.requireNonNull(plan); Objects.requireNonNull(control).check();
            if (!incarnation.equals(plan.reservation().successorIncarnation()))
                throw new IllegalArgumentException("Successor incarnation differs from session manager");
            var key = plan.next().key();
            DocumentAdmissionAuthorization.requireCaller(coordinatorCaller, key, key.account());
            DocumentAdmissionAuthorization.requireCaller(executionCaller, key, key.account());
            if (!coordinatorCaller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                    "Successor activation requires private process authority");
            final Entry entry;
            // All preparation is side-effect free and outside the shared monitor.
            // Reserving encoded bytes does not account for the complete parsed heap.
            try (var encoded = journalBudget.reserve(2L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
                var fingerprint = DocumentSuccessorFingerprint.of(plan);
                var session = DocumentPublicationSession.successor(tx, execution.drives(), executionCaller, plan, journalBudget, incarnation, registrations);
                control.check();
                entry = reserveSuccessor(plan.next().command(), key, fingerprint, session);
            }
            try {
                RepositorySuccessorExecution.activate(tx, journalBudget, coordinatorCaller, executionCaller, plan, control, execution.drives());
                entry.session.admit(executionCaller, control).orElseThrow(() -> new IllegalStateException("Successor returned no owner"));
            } finally {
                synchronized (this) {
                    entry.recovering = false;
                    release(key, entry, false);
                }
            }
        }
    }

    private synchronized Entry reserveSuccessor(DocumentPublicationCommand command, RepositoryOperationLedger.Key key,
            DocumentSuccessorFingerprint fingerprint, DocumentPublicationSession session) {
        var entry = entries.get(key);
        if (entry == null) {
            entry = new Entry(command);
            if (entries.size() >= capacity || entry.commandBytes > maxCommandBytes - commandBytes)
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Publication session capacity exhausted");
            // Publish the fully constructed session and its drain identity together,
            // before any activation SQL. No observable successor placeholder exists.
            entry.successor = fingerprint;
            entry.session = session;
            entries.put(key, entry);
            commandBytes += entry.commandBytes;
        } else {
            requireCommand(entry, command);
            if (!fingerprint.equals(entry.successor)) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                    "Retained successor proposal changed");
            if (entry.users != 0 || entry.recovering) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                    "Publication session is in use");
            entry.users = 1;
        }
        entry.recovering = true;
        return entry;
    }

    /**
     * Placements are already qualified by the host; existing sessions retain their
     * original selection. Borrowed bodies and resolvers are never stored here.
     * Count and serialized-command estimates combine with existing member/part
     * limits; they are not a measurement or reservation of all parsed Java heap.
     */
    DocumentPublicationResult execute(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements,
            Map<DocumentUploadPayloads.Key, PartObject> bodies, Map<String, String> attributes,
            Map<String, DocumentPublicationCandidate.Mode> modes,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            RepositoryReadControl control) throws InvalidProtocolBufferException {
        try (var call = beginCall()) {
            return executeOpen(caller, command, placements, bodies, attributes, modes, container, resolver, control);
        }
    }

    private DocumentPublicationResult executeOpen(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements,
            Map<DocumentUploadPayloads.Key, PartObject> bodies, Map<String, String> attributes,
            Map<String, DocumentPublicationCandidate.Mode> modes,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            RepositoryReadControl control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(control).check();
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        var entry = existing(key, command);
        if (entry == null) {
            // Replay needs no current placement, including after terminal eviction.
            var observed = replay.observe(caller, command);
            control.check();
            observed.requireNotTerminated();
            if (observed.result().isPresent()) return observed.result().orElseThrow();
            entry = create(key, caller, command, placements);
        }
        boolean terminal = false;
        try {
            var result = execution.execute(caller, entry.session, bodies, attributes, modes, container, resolver, control);
            terminal = true;
            return result;
        } catch (DocumentPublicationReplay.Terminated terminated) {
            terminal = true; // Authorized terminal replay, independent of provider cleanup.
            throw terminated;
        } catch (RuntimeException failure) {
            // A marker may commit after the pre-creation observation. Preserve the
            // primary error; only fresh authorized evidence permits local eviction.
            if (journalBudget != null) {
                try {
                    control.check();
                    var observed = replay.observe(caller, command);
                    control.check();
                    terminal = observed.state() == DocumentPublicationReplay.State.ABANDONED;
                } catch (RuntimeException confirmation) {
                    if (confirmation != failure) failure.addSuppressed(confirmation);
                }
            }
            throw failure;
        } finally {
            release(key, entry, terminal);
        }
    }

    /** Explicit private cancellation; absence is not proof of a durable outcome. */
    boolean abandonRetained(RepositoryCaller caller, DocumentPublicationCommand command, RepositoryReadControl control) {
        try (var call = beginCall()) {
            Objects.requireNonNull(command); Objects.requireNonNull(control).check();
            if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                    "Authenticated repository caller is required");
            var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(),
                    caller.principalName(), command.operationId());
            DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
            if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                    "Registration abandonment requires private process authority");
            if (journalBudget == null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Session manager does not own journaled registrations");
            final Entry entry;
            synchronized (this) {
                entry = entries.get(key);
                if (entry == null) return false;
                requireCommand(entry, command);
                if (entry.users != 0 || entry.recovering || entry.session == null)
                    throw new RepositoryException(RepositoryException.Code.CONFLICT, "Publication session is in use");
                entry.users = 1; entry.recovering = true;
            }
            boolean confirmed = false;
            try {
                // Operation-wide evidence also covers a replacement session refused
                // by SQL after its earlier NOT_OBSERVED replay raced abandonment.
                var observed = replay.observe(caller, command);
                control.check();
                if (observed.state() != DocumentPublicationReplay.State.ABANDONED)
                    entry.session.abandonRegistration(caller, control);
                control.check();
                confirmed = true;
                return true;
            } finally {
                synchronized (this) {
                    entry.recovering = false;
                    release(key, entry, confirmed);
                }
            }
        }
    }

    private synchronized Entry existing(RepositoryOperationLedger.Key key, DocumentPublicationCommand command) {
        var entry = entries.get(key);
        if (entry == null) return null;
        requireCommand(entry, command);
        if (entry.session == null || entry.recovering) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                "Publication session preparation or recovery is already in progress");
        entry.users = Math.incrementExact(entry.users);
        return entry;
    }

    private Entry create(RepositoryOperationLedger.Key key, RepositoryCaller caller,
            DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements) {
        final Entry reserved;
        synchronized (this) {
            var found = existing(key, command);
            if (found != null) return found;
            reserved = new Entry(command);
            if (entries.size() >= capacity || reserved.commandBytes > maxCommandBytes - commandBytes) throw new RepositoryException(
                    RepositoryException.Code.RESOURCE_EXHAUSTED, "Publication session capacity exhausted");
            entries.put(key, reserved);
            commandBytes += reserved.commandBytes;
        }
        try {
            // Bounded preparation can still be substantial; keep it outside the shared lock.
            var session = journalBudget == null ? new DocumentPublicationSession(tx, caller, command, placements, lease)
                    : DocumentPublicationSession.journaled(tx, execution.drives(), caller, command, placements, lease, journalBudget, coordinator, registrations);
            synchronized (this) { reserved.session = session; }
            return reserved;
        } catch (RuntimeException | Error failure) {
            // Construction performs no admission SQL, so no uncertain identity is lost.
            synchronized (this) { remove(key, reserved); }
            throw failure;
        }
    }

    private synchronized void release(RepositoryOperationLedger.Key key, Entry entry, boolean terminal) {
        entry.terminal |= terminal;
        entry.users--;
        if (entry.users == 0 && !entry.recovering && (entry.terminal
                || entry.session != null && entry.session.discardableBeforeRegistration())) remove(key, entry);
    }

    private void remove(RepositoryOperationLedger.Key key, Entry entry) {
        if (entries.remove(key, entry)) {
            if (entry.restoration != null) entry.restoration.close();
            commandBytes -= entry.commandBytes;
        }
    }

    /** Trusted original-owner reconciliation only; no claim lookup, takeover or replacement payloads. */
    DocumentPublicationResult resumeStarted(RepositoryCaller caller, DocumentPublicationCommand command,
            RepositoryOperationLedger.Owner owner, RepositoryReadControl control) {
        try (var call = beginCall()) {
            Objects.requireNonNull(command); Objects.requireNonNull(owner); Objects.requireNonNull(control).check();
            DocumentAdmissionAuthorization.requireCaller(caller, owner.key(), command.intent().getAccountId());
            if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                    "Publication restoration requires private process authority");
            if (!owner.key().account().equals(command.intent().getAccountId())
                    || !owner.key().operationId().equals(command.operationId())
                    || !owner.executionClaim().orElseThrow(() -> new IllegalArgumentException("Restoration requires claim"))
                            .commandSha256().equals(command.sha256()))
                throw new IllegalArgumentException("Restoration owner differs from command");
            var observed = replay.observe(caller, command);
            control.check();
            if (observed.rejection().isPresent() || observed.result().isPresent()) {
                completed(owner.key(), command);
                observed.requireNotTerminated();
                return observed.result().orElseThrow();
            }
            final Entry entry;
            // Drain cannot snapshot between accepting restoration authority and retaining it.
            try (var registration = registrations.enter()) {
                tx.inTransaction(em -> {
                    RepositoryCoordinatorBinding.requireResume(em, owner.executionClaim().orElseThrow(), coordinator);
                    return null;
                });
                entry = reserveRestoration(owner, command);
            }
            boolean terminal = false;
            try {
                if (entry.restoration == null) {
                    // SQL and bounded decoding stay outside the shared manager monitor.
                    var restored = execution.restoreStarted(caller, owner, control);
                    synchronized (this) { entry.restoration = restored; }
                }
                var result = execution.resumeStarted(caller, entry.restoration, control);
                terminal = true;
                return result;
            } catch (DocumentPublicationReplay.Terminated rejected) {
                terminal = true;
                throw rejected;
            } finally {
                synchronized (this) {
                    entry.recovering = false;
                    release(owner.key(), entry, terminal);
                    if (journalBudget == null && entry.restoration == null && entry.users == 0) remove(owner.key(), entry);
                }
            }
        }
    }

    private synchronized Entry reserveRestoration(RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command) {
        var entry = entries.get(owner.key());
        if (entry == null) {
            entry = new Entry(command);
            if (entries.size() >= capacity || entry.commandBytes > maxCommandBytes - commandBytes)
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Publication session capacity exhausted");
            entry.restorationOwner = owner;
            entries.put(owner.key(), entry);
            commandBytes += entry.commandBytes;
        } else {
            requireCommand(entry, command);
            if (entry.users != 0 || entry.recovering || entry.session != null || entry.restorationOwner == null)
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Publication session is in use");
            if (!owner.equals(entry.restorationOwner))
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Retained restoration owner changed");
            entry.users = 1;
        }
        entry.recovering = true;
        return entry;
    }

    synchronized int retainedSessions() { return entries.size(); }
    synchronized long retainedCommandBytes() { return commandBytes; }

    /** Releases only local capacity after durable ownership has permanently fenced this nonce. */
    boolean retireSuperseded(RepositoryCaller caller, DocumentPublicationCommand command, RepositoryReadControl control) {
        try (var call = beginCall()) {
            return retireSupersededOpen(caller, command, control, false);
        }
    }

    /** Pre-close local cache retirement, not drain attestation or permission to close worker resources. */
    boolean retireClaimFenced(RepositoryCaller caller, DocumentPublicationCommand command, RepositoryReadControl control) {
        try (var call = beginCall()) {
            return retireSupersededOpen(caller, command, control, true);
        }
    }

    private boolean retireSupersededOpen(RepositoryCaller caller, DocumentPublicationCommand command,
            RepositoryReadControl control, boolean claimFence) {
        Objects.requireNonNull(command); Objects.requireNonNull(control).check();
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (claimFence && !caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Claim-fenced retirement requires private process authority");
        final Entry entry;
        synchronized (this) {
            entry = entries.get(key);
            if (entry == null) return false;
            requireCommand(entry, command);
            if (entry.users != 0 || entry.recovering || entry.session == null)
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Publication session is in use");
            entry.users = 1;
            entry.recovering = true;
        }
        boolean superseded = false;
        try {
            if (claimFence) {
                var identity = entry.session.drainIdentity();
                superseded = identity.isPresent() && RepositoryClaimRetirement.fenced(tx, entry.command,
                        java.util.List.of(identity.orElseThrow()), control);
            } else superseded = entry.session.isSuperseded(caller, control);
            return superseded;
        } finally {
            synchronized (this) {
                entry.recovering = false;
                release(key, entry, false);
                if (superseded && entry.users == 0) remove(key, entry);
            }
        }
    }

    /**
     * Explicit, host-authorized takeover preparation. Empty means takeover returned
     * ownership, not publication. Advancing one further generation requires database
     * confirmation that the retained recovery nonce became that expired owner.
     * Never infer that decision from an exception or timeout.
     */
    Optional<DocumentPublicationResult> recover(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements, long predecessorGeneration,
            Map<String, DocumentPublicationCandidate.Mode> modes, RepositoryReadControl control) {
        try (var call = beginCall()) {
            if (journalBudget != null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Journaled registration requires explicit claim recovery; unjournaled replacement is disabled");
            return recoverOpen(caller, command, placements, predecessorGeneration, modes, control);
        }
    }

    private Optional<DocumentPublicationResult> recoverOpen(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements, long predecessorGeneration,
            Map<String, DocumentPublicationCandidate.Mode> modes, RepositoryReadControl control) {
        Objects.requireNonNull(command); Objects.requireNonNull(control).check();
        if (predecessorGeneration < 1 || predecessorGeneration == Long.MAX_VALUE)
            throw new IllegalArgumentException("Recovery requires a replaceable predecessor generation");
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        var observed = replay.observe(caller, command);
        control.check();
        if (observed.rejection().isPresent()) {
            completed(key, command);
            observed.requireNotTerminated();
        }
        if (observed.result().isPresent()) {
            completed(key, command);
            return observed.result();
        }
        var entry = reserveRecovery(key, command);
        boolean terminal = false;
        try {
            var previous = entry.session;
            boolean advance = previous != null && previous.predecessorGeneration() != 0
                    && previous.predecessorGeneration() != predecessorGeneration;
            if (previous != null) {
                try (var scope = previous.begin(caller, control)) { scope.checkModes(modes); }
                if (advance) previous.requireRecoveryAdvance(caller, predecessorGeneration, control);
            }
            if (previous == null || previous.predecessorGeneration() == 0 || advance) {
                var replacement = DocumentPublicationSession.recovering(tx, caller, entry.command, placements, lease, predecessorGeneration, modes);
                // Publish the private identities before SQL; every uncertain retry must find them.
                synchronized (this) { entry.session = replacement; }
            }
            entry.session.admit(caller, control).orElseThrow(() -> new IllegalStateException("Recovery returned no owner"));
            return Optional.empty();
        } catch (RepositoryOperationLedger.TerminalOperationException completed) {
            var result = replay.observe(caller, command);
            control.check();
            if (result.rejection().isPresent()) {
                terminal = true;
                result.requireNotTerminated();
            }
            if (result.result().isEmpty()) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                    "Terminal operation has no replayable success");
            terminal = true;
            return result.result();
        } finally {
            synchronized (this) {
                entry.recovering = false;
                release(key, entry, terminal);
                if (entry.session == null && entry.users == 0) remove(key, entry);
            }
        }
    }

    private synchronized Entry reserveRecovery(RepositoryOperationLedger.Key key, DocumentPublicationCommand command) {
        var entry = entries.get(key);
        if (entry == null) {
            entry = new Entry(command);
            if (entries.size() >= capacity || entry.commandBytes > maxCommandBytes - commandBytes)
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Publication session capacity exhausted");
            entries.put(key, entry);
            commandBytes += entry.commandBytes;
        } else {
            requireCommand(entry, command);
            if (entry.users != 0 || entry.recovering || entry.session == null)
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Publication session is in use");
            entry.users = 1;
        }
        entry.recovering = true;
        return entry;
    }

    private synchronized void completed(RepositoryOperationLedger.Key key, DocumentPublicationCommand command) {
        var entry = entries.get(key);
        if (entry == null) return;
        requireCommand(entry, command);
        entry.terminal = true;
        if (entry.users == 0) remove(key, entry);
    }

    private static void requireCommand(Entry entry, DocumentPublicationCommand command) {
        if (!entry.command.canonical().equals(command.canonical()))
            throw new RepositoryException(RepositoryException.Code.CONFLICT, "Publication operation command changed");
    }

    private synchronized Call beginCall() {
        if (closed) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE,
                "Publication sessions are closed");
        activeCalls = Math.incrementExact(activeCalls);
        return new Call();
    }

    private final class Call implements AutoCloseable {
        private boolean released;
        @Override public void close() {
            synchronized (DocumentPublicationSessions.this) {
                if (released) return;
                released = true;
                activeCalls--;
                releaseClosedRestorations();
                DocumentPublicationSessions.this.notifyAll();
            }
        }
    }

    /** Refuse new calls without cancelling accepted work or discarding uncertain identities. */
    @Override public synchronized void close() { registrations.close(); closed = true; releaseClosedRestorations(); }

    /**
     * Registration barrier/SQL markers for retained nonterminal registrations only.
     * Entries already evicted after durable terminal proof are excluded. This is neither
     * LOCAL_DRAINED nor permission to close providers.
     */
    record DrainProgress(boolean registrationsIdle, int confirmed, int unresolved, int fenced, int detached) {
        DrainProgress(boolean registrationsIdle, int confirmed, int unresolved) {
            this(registrationsIdle, confirmed, unresolved, 0, 0);
        }
        DrainProgress(boolean registrationsIdle, int confirmed, int unresolved, int fenced) {
            this(registrationsIdle, confirmed, unresolved, fenced, 0);
        }
    }

    DrainProgress drainRegistrations(Duration wait, java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> authority,
            RepositoryReadControl control) throws InterruptedException {
        Objects.requireNonNull(wait); Objects.requireNonNull(authority); Objects.requireNonNull(control).check();
        if (wait.isNegative()) throw new IllegalArgumentException("Negative registration wait");
        if (journalBudget == null) throw new IllegalStateException("Session manager does not own journaled registrations");
        execution.stopProviderStarts();
        close();
        if (!registrations.awaitIdle(wait)) return new DrainProgress(false, 0, 0);
        final java.util.List<ShutdownEntry> identities;
        synchronized (this) {
            if (drainIdentities == null) drainIdentities = entries.values().stream()
                    .flatMap(entry -> drainIdentity(entry).stream().map(identity -> new ShutdownEntry(identity, entry.successor))).toList();
            identities = drainIdentities;
        }
        int confirmed = 0, unresolved = 0, fenced = 0, detached = 0;
        for (var retained : identities) {
            var identity = retained.identity;
            control.check();
            var caller = Objects.requireNonNull(authority.apply(identity.key()), "Private operation authority");
            if (retained.detachedUnactivated) {
                if (confirmDetached(retained, caller, control)) detached++;
                else unresolved++;
                continue;
            }
            var state = retained.successor == null ? RepositoryShutdownClaim.inspect(tx, caller, identity, control)
                    : RepositoryShutdownClaim.inspectDetached(tx, caller, identity, control);
            if (state.fenced()) fenced++;
            else if (state == RepositoryShutdownClaim.State.UNRESOLVED) unresolved++;
            else {
                if (retained.successor != null) {
                    var successorState = RepositorySuccessorShutdown.inspect(tx, journalBudget, caller, retained.successor, control);
                    if (successorState == RepositorySuccessorShutdown.State.UNACTIVATED) {
                        retained.detachedUnactivated = true;
                        detached++;
                        continue;
                    }
                    if (successorState != RepositorySuccessorShutdown.State.ACTIVATED) {
                        unresolved++;
                        continue;
                    }
                }
                if (RepositoryCoordinatorDrain.beginRetained(tx, caller, identity, control)) confirmed++;
                else unresolved++;
            }
        }
        control.check();
        return new DrainProgress(true, confirmed, unresolved, fenced, detached);
    }

    private boolean confirmDetached(ShutdownEntry retained, RepositoryCaller caller, RepositoryReadControl control) {
        var state = RepositoryShutdownClaim.inspectDetached(tx, caller, retained.identity, control);
        if (state.fenced()) return true;
        if (state != RepositoryShutdownClaim.State.CURRENT) return false;
        var successor = RepositorySuccessorShutdown.inspect(tx, journalBudget, caller, retained.successor, control);
        return successor == RepositorySuccessorShutdown.State.UNACTIVATED
                || successor == RepositorySuccessorShutdown.State.ACTIVATED;
    }

    private void releaseClosedRestorations() {
        if (!closed || activeCalls != 0) return;
        for (var item : Map.copyOf(entries).entrySet()) {
            var entry = item.getValue();
            if (entry.restoration == null) continue;
            if (journalBudget == null) remove(item.getKey(), entry);
            else {
                // Release borrowed resources without discarding nonterminal claim identity.
                entry.restoration.close();
                entry.restoration = null;
            }
        }
    }

    private Optional<RepositoryCoordinatorDrain.Identity> drainIdentity(Entry entry) {
        if (entry.session != null) return entry.session.drainIdentity();
        if (entry.restorationOwner == null) return Optional.empty();
        var claim = entry.restorationOwner.executionClaim().orElseThrow();
        return Optional.of(new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(),
                claim.epoch(), claim.token(), coordinator));
    }

    /** Host has drained provider/read/schema workers; partial SQL success remains exactly retryable. */
    boolean attestLocalDrain(java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> authority,
            RepositoryReadControl control) {
        Objects.requireNonNull(authority); Objects.requireNonNull(control).check();
        final java.util.List<ShutdownEntry> identities;
        synchronized (this) {
            if (journalBudget == null || !closed || activeCalls != 0 || drainIdentities == null)
                throw new IllegalStateException("Journaled registration and session drain must complete before attestation");
            identities = drainIdentities;
        }
        for (var retained : identities) {
            var identity = retained.identity;
            control.check();
            var caller = Objects.requireNonNull(authority.apply(identity.key()), "Private operation authority");
            if (retained.detachedUnactivated) {
                if (!confirmDetached(retained, caller, control)) return false;
                continue;
            }
            if (RepositoryCoordinatorLocalDrain.confirm(tx, caller, identity, control).isPresent()) continue;
            var state = retained.successor == null ? RepositoryShutdownClaim.inspect(tx, caller, identity, control)
                    : RepositoryShutdownClaim.inspectDetached(tx, caller, identity, control);
            if (state.fenced()) continue;
            if (state == RepositoryShutdownClaim.State.UNRESOLVED) return false;
            RepositoryCoordinatorLocalDrain.record(tx, caller, identity, control);
        }
        control.check();
        return true;
    }

    /**
     * Includes SQL admission, receipt replay and recovery, even before a session exists.
     * Idle calls do not prove provider-worker quiescence. The host must separately drain
     * upload/read workers and release their pins before closing providers or SQL.
     */
    synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (!closed) throw new IllegalStateException("Close publication sessions before awaiting idle");
        if (timeout.isNegative()) throw new IllegalArgumentException("Idle timeout must not be negative");
        long remaining = timeout.toNanos();
        long started = System.nanoTime();
        while (activeCalls != 0) {
            if (remaining <= 0) return false;
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - started);
        }
        return true;
    }
}
