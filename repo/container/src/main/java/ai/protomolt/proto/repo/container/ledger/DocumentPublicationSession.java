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

/** Host-private identities minted before operation admission SQL and retained across uncertain outcomes. */
final class DocumentPublicationSession {
    private final RepositoryOperationLedger operations;
    private final RepositoryOperationLedger.Key key;
    private final DocumentPublicationCommand command;
    private final UUID ownerNonce;
    private final Duration lease;
    private final DocumentOperationUploadAdmission.Prepared prepared;

    DocumentPublicationSession(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements, Duration lease) {
        this.command = Objects.requireNonNull(command); this.lease = Objects.requireNonNull(lease);
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
     * Exact retry only: never renews, takes over, or changes owner/attempt identities.
     * Empty means no executable owner was granted. A terminal operation must use
     * authorized result replay instead. Failure after SQL, including cancellation,
     * leaves this session intact for reconciliation; the host must retain it.
     */
    Optional<RepositoryOperationLedger.Owner> admit(RepositoryCaller caller, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        var admitted = operations.admit(key, command, ownerNonce, lease);
        control.check();
        return admitted.owner();
    }

    DocumentOperationUploadAdmission.Prepared prepared() { return prepared; }
}
