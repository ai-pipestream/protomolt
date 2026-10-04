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
final class DocumentPublicationSessions {
    private final Tx tx;
    private final DocumentPublicationExecution execution;
    private final DocumentPublicationReplay replay;
    private final Duration lease;
    private final int capacity;
    private final long maxCommandBytes;
    private long commandBytes;
    private final Map<RepositoryOperationLedger.Key, Entry> entries = new HashMap<>();

    private static final class Entry {
        final DocumentPublicationCommand command;
        final long commandBytes;
        DocumentPublicationSession session;
        int users = 1;
        boolean committed;
        boolean recovering;
        Entry(DocumentPublicationCommand command) {
            this.command = command;
            commandBytes = (long) command.canonical().size() + command.intent().getSerializedSize();
        }
    }

    DocumentPublicationSessions(Tx tx, DocumentPublicationExecution execution, Duration lease, int capacity, long maxCommandBytes) {
        this.tx = Objects.requireNonNull(tx); this.execution = Objects.requireNonNull(execution);
        this.lease = Objects.requireNonNull(lease);
        if (capacity < 1) throw new IllegalArgumentException("Publication session capacity must be positive");
        if (maxCommandBytes < 1) throw new IllegalArgumentException("Publication command byte capacity must be positive");
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Operation lease requires one second to one day");
        this.capacity = capacity; this.maxCommandBytes = maxCommandBytes; replay = new DocumentPublicationReplay(tx);
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
            if (observed.result().isPresent()) return observed.result().orElseThrow();
            entry = create(key, caller, command, placements);
        }
        boolean committed = false;
        try {
            var result = execution.execute(caller, entry.session, bodies, attributes, modes, container, resolver, control);
            committed = true;
            return result;
        } finally {
            release(key, entry, committed);
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
            var session = new DocumentPublicationSession(tx, caller, command, placements, lease);
            synchronized (this) { reserved.session = session; }
            return reserved;
        } catch (RuntimeException | Error failure) {
            // Construction performs no admission SQL, so no uncertain identity is lost.
            synchronized (this) { remove(key, reserved); }
            throw failure;
        }
    }

    private synchronized void release(RepositoryOperationLedger.Key key, Entry entry, boolean committed) {
        entry.committed |= committed;
        entry.users--;
        if (entry.committed && entry.users == 0) remove(key, entry);
    }

    private void remove(RepositoryOperationLedger.Key key, Entry entry) {
        if (entries.remove(key, entry)) commandBytes -= entry.commandBytes;
    }

    synchronized int retainedSessions() { return entries.size(); }
    synchronized long retainedCommandBytes() { return commandBytes; }

    /**
     * Explicit, host-authorized takeover preparation. Empty means takeover returned
     * ownership, not publication. A retained recovery transition accepts only its
     * original predecessor generation; advancing another generation needs separate
     * host reconciliation. Never infer that decision from an exception or timeout.
     */
    Optional<DocumentPublicationResult> recover(RepositoryCaller caller, DocumentPublicationCommand command,
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
        if (observed.result().isPresent()) {
            committed(key, command);
            return observed.result();
        }
        var entry = reserveRecovery(key, command);
        boolean terminal = false;
        try {
            var previous = entry.session;
            if (previous != null) {
                if (previous.predecessorGeneration() != 0 && previous.predecessorGeneration() != predecessorGeneration)
                    throw new RepositoryException(RepositoryException.Code.CONFLICT, "Retained recovery predecessor changed");
                try (var scope = previous.begin(caller, control)) { scope.checkModes(modes); }
            }
            if (previous == null || previous.predecessorGeneration() == 0) {
                var replacement = DocumentPublicationSession.recovering(tx, caller, entry.command, placements, lease, predecessorGeneration, modes);
                // Publish the private identities before SQL; every uncertain retry must find them.
                synchronized (this) { entry.session = replacement; }
            }
            try {
                entry.session.admit(caller, control).orElseThrow(() -> new IllegalStateException("Recovery returned no owner"));
                return Optional.empty();
            } catch (RepositoryOperationLedger.TerminalOperationException completed) {
                var result = replay.observe(caller, command);
                control.check();
                if (result.result().isEmpty()) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                        "Terminal operation has no replayable success");
                terminal = true;
                return result.result();
            }
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

    private synchronized void committed(RepositoryOperationLedger.Key key, DocumentPublicationCommand command) {
        var entry = entries.get(key);
        if (entry == null) return;
        requireCommand(entry, command);
        entry.committed = true;
        if (entry.users == 0) remove(key, entry);
    }

    private static void requireCommand(Entry entry, DocumentPublicationCommand command) {
        if (!entry.command.canonical().equals(command.canonical()))
            throw new RepositoryException(RepositoryException.Code.CONFLICT, "Publication operation command changed");
    }
}
