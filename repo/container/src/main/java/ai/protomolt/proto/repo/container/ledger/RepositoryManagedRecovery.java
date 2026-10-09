package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/** Exact-key host retry routing. No fleet scan, repeated takeover loop or request-supplied process rights. */
final class RepositoryManagedRecovery {
    private final RepositoryRecoveryAttempts attempts;
    private final RepositoryCoordinatorRecoveryDiscovery discovery;
    private final DocumentPublicationReplay replay;
    private final Tx tx;
    private final PayloadBudget budget;
    private final DocumentPublicationSessions sessions;
    private final DocumentPublicationRuntime.RecoveryAuthority authority;
    final RepositoryPublicationCalls calls;

    RepositoryManagedRecovery(Tx tx, PayloadBudget budget, DocumentPublicationSessions sessions, Duration lease,
            SqlTimeouts timeouts, int capacity, DocumentPublicationRuntime.RecoveryAuthority authority) {
        this.sessions=Objects.requireNonNull(sessions);
        this.tx=tx.withTimeouts(timeouts);
        this.budget=Objects.requireNonNull(budget);
        this.authority=Objects.requireNonNull(authority);
        attempts=new RepositoryRecoveryAttempts(tx,budget,sessions,lease,timeouts,capacity);
        discovery=new RepositoryCoordinatorRecoveryDiscovery(tx,timeouts);
        replay=new DocumentPublicationReplay(tx.withTimeouts(timeouts));
        calls=new RepositoryPublicationCalls(capacity);
    }

    /** Caller holds the runtime scope and same-key call guard through subsequent ordinary publication. */
    DocumentRecoveryPayloads prepare(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<DocumentUploadPayloads.Key,PartObject> bodies, Map<String,DocumentPublicationCandidate.Mode> modes,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        var key=new RepositoryOperationLedger.Key(command.intent().getAccountId(),caller.principalName(),command.operationId());
        // Authorize before local routing can disclose retained/busy state as well.
        var outcome=replay.observe(caller,command);
        control.check();
        switch (outcome.state()) {
            case COMMITTED, TERMINATED, ABANDONED -> { return null; }
            default -> { }
        }
        // A retained proposal wins over discovery of its own uncertain committed reservation.
        var retained=attempts.resume(caller,command);
        if (retained.isPresent()) {
            try (var attempt=retained.orElseThrow()) {
                if (attempt.retireTerminal(authority(key),caller,control)
                        !=RepositoryRecoveryAttempts.TerminalDisposal.NOT_TERMINAL) return null;
                var snapshot=inputs(command,bodies,modes,control);
                try {
                    if (!attempt.reconcileExpiredBound(authority(key),caller,modes,control))
                        attempt.reconcileUnactivated(authority(key),caller,modes,control);
                    advance(attempt,key,caller,modes,control);
                    return snapshot;
                } catch (RuntimeException | Error failure) { snapshot.close(); throw failure; }
            }
        }
        sessions.requireIdleForRouting(caller,command);
        if (outcome.state()==DocumentPublicationReplay.State.NOT_OBSERVED && absent(key,control)) return null;
        // Discovery has process rights; authorize the current read set before using
        // its result to route a caller's request. Ownerless partial state stays private.
        var observed=discovery.inspect(authority(key),key,command.sha256(),control);
        switch (observed.status()) {
            case ABSENT, LIVE, TERMINAL, ABANDONED -> { return null; }
            case EXPIRED_BOUND, RESERVED_NOT_INSTALLED, INSTALLED_NOT_ACTIVATED -> {
                if (observed.candidate().isEmpty() && observed.unactivated().isEmpty())
                    throw unsupported();
                var snapshot=inputs(command,bodies,modes,control);
                try (var attempt=attempts.begin(caller,command,observed)) {
                    advance(attempt,key,caller,modes,control);
                    return snapshot;
                } catch (RuntimeException | Error failure) { snapshot.close(); throw failure; }
            }
            default -> throw unsupported();
        }
    }

    private void advance(RepositoryRecoveryAttempts.Attempt attempt, RepositoryOperationLedger.Key key,
            RepositoryCaller caller, Map<String,DocumentPublicationCandidate.Mode> modes, RepositoryReadControl control) {
        for (int phase=0;phase<3;phase++) {
            if (attempt.advance(authority(key),caller,modes,control)==RepositoryRecoveryAttempts.Phase.ACTIVATED) return;
        }
        throw new IllegalStateException("Recovery did not activate within its bounded phases");
    }

    private RepositoryCaller authority(RepositoryOperationLedger.Key key) {
        return Objects.requireNonNull(authority.forOperation(key.account(),key.principal(),key.operationId()),
                "Private recovery authority");
    }

    /** Staleable routing fact only; atomic initial admission still arbitrates concurrent inserts. */
    private boolean absent(RepositoryOperationLedger.Key key, RepositoryReadControl control) {
        control.check();
        boolean absent=tx.inTransaction(em -> (Boolean) em.createNativeQuery("""
                SELECT NOT EXISTS(SELECT 1 FROM repository_execution_claims
                  WHERE account_id=:a AND principal=:p AND operation_id=:o)
                  AND NOT EXISTS(SELECT 1 FROM repository_operations
                  WHERE account_id=:a AND principal=:p AND operation_id=:o)
                  AND NOT EXISTS(SELECT 1 FROM repository_publication_preparations
                  WHERE account_id=:a AND principal=:p AND operation_id=:o)
                """).setParameter("a",key.account()).setParameter("p",key.principal())
                .setParameter("o",key.operationId()).getSingleResult());
        control.check();
        return absent;
    }

    /** Reuse modes and complete resubmitted payload declarations; never infer absent bodies from old uploads. */
    private DocumentRecoveryPayloads inputs(DocumentPublicationCommand command, Map<DocumentUploadPayloads.Key,PartObject> bodies,
            Map<String,DocumentPublicationCandidate.Mode> modes, RepositoryReadControl control) {
        DocumentPublicationModesJournal.encode(command,modes);
        return DocumentRecoveryPayloads.prepare(command,bodies,budget,control);
    }

    private static RepositoryException unsupported() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,"Operation is not eligible for managed retry");
    }

    /** Called only after the runtime's complete publication-call barrier has drained. */
    boolean detach(Duration wait, RepositoryReadControl control) throws InterruptedException {
        attempts.close();
        return attempts.detachClosed(wait,this::authority,control);
    }
}
