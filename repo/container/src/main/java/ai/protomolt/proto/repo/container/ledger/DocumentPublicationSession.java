package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/** Host-private identities minted before operation admission SQL and retained across uncertain outcomes. */
final class DocumentPublicationSession {
    private final RepositoryOperationLedger operations;
    private final RepositoryOperationLedger.Key key;
    private final DocumentPublicationCommand command;
    private final UUID ownerNonce;
    private final Duration lease;
    private final long predecessorGeneration;
    private final DocumentOperationUploadAdmission.Prepared prepared;
    private final AtomicBoolean executing = new AtomicBoolean();
    private Map<String, DocumentPublicationCandidate.Mode> modes;

    DocumentPublicationSession(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements, Duration lease) {
        this(tx, caller, command, placements, lease, 0);
    }

    /** Explicit host recovery; never selected automatically by ordinary admission. */
    static DocumentPublicationSession recovering(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements, Duration lease, long predecessorGeneration,
            Map<String, DocumentPublicationCandidate.Mode> modes) {
        if (predecessorGeneration < 1 || predecessorGeneration == Long.MAX_VALUE)
            throw new IllegalArgumentException("Recovery requires a replaceable predecessor generation");
        var session = new DocumentPublicationSession(tx, caller, command, placements, lease, predecessorGeneration);
        session.modes = session.checkedModes(modes);
        return session;
    }

    private DocumentPublicationSession(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements, Duration lease, long predecessorGeneration) {
        this.command = Objects.requireNonNull(command); this.lease = Objects.requireNonNull(lease);
        this.predecessorGeneration = predecessorGeneration;
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Operation lease requires one second to one day");
        var attempts = new HashMap<String, UUID>();
        for (var member : command.intent().getMembersList()) {
            if (member.getPartsList().stream().anyMatch(part -> part.hasUpload()))
                attempts.put(member.getMemberId(), UUID.randomUUID());
        }
        prepared = DocumentOperationUploadAdmission.prepare(command, placements, attempts, lease);
        ownerNonce = UUID.randomUUID();
        operations = new RepositoryOperationLedger(Objects.requireNonNull(tx));
    }

    /**
     * Exact retry only: never renews or changes owner/attempt identities. A normal
     * session admits; an explicitly constructed recovery session retries only its
     * fixed predecessor generation and next nonce through command-bound takeover.
     * Empty means no executable owner was granted. A terminal operation must use
     * authorized result replay instead. Failure after SQL, including cancellation,
     * leaves this session intact for reconciliation; the host must retain it.
     */
    Optional<RepositoryOperationLedger.Owner> admit(RepositoryCaller caller, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        var owner = predecessorGeneration == 0 ? operations.admit(key, command, ownerNonce, lease).owner()
                : Optional.of(operations.takeOver(key, command, predecessorGeneration, ownerNonce, lease));
        control.check();
        return owner;
    }

    DocumentOperationUploadAdmission.Prepared prepared() { return prepared; }
    long predecessorGeneration() { return predecessorGeneration; }

    private Map<String, DocumentPublicationCandidate.Mode> checkedModes(Map<String, DocumentPublicationCandidate.Mode> requested) {
        var copy = Map.copyOf(requested);
        var members = command.intent().getMembersList().stream()
                .map(member -> member.getMemberId()).collect(java.util.stream.Collectors.toSet());
        if (!copy.keySet().equals(members)) throw new IllegalArgumentException("Admission modes differ from command members");
        return copy;
    }

    /** Fail fast rather than queue borrowed payloads behind another execution. */
    Execution begin(RepositoryCaller caller, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!executing.compareAndSet(false, true)) throw new RepositoryException(
                RepositoryException.Code.CONFLICT, "Publication session already has an active execution");
        var execution = new Execution();
        try {
            control.check();
            return execution;
        } catch (RuntimeException | Error failure) {
            execution.close();
            throw failure;
        }
    }

    final class Execution implements AutoCloseable {
        private final AtomicBoolean closed = new AtomicBoolean();

        /** Host choices remain fixed even when admission or publication fails. */
        synchronized Map<String, DocumentPublicationCandidate.Mode> bindModes(Map<String, DocumentPublicationCandidate.Mode> requested) {
            var copy = checkModes(requested);
            if (modes == null) modes = copy;
            return modes;
        }

        /** Validate replacement choices without mutating a session that preparation may leave intact. */
        synchronized Map<String, DocumentPublicationCandidate.Mode> checkModes(Map<String, DocumentPublicationCandidate.Mode> requested) {
            if (closed.get()) throw new IllegalStateException("Publication execution is closed");
            var copy = checkedModes(requested);
            if (modes != null && !modes.equals(copy)) throw new IllegalArgumentException("Publication session admission modes changed");
            return copy;
        }

        @Override public synchronized void close() {
            if (closed.compareAndSet(false, true)) executing.set(false);
        }
    }
}
