package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Permanent claim fencing for local cache retirement only; never provider or reader quiescence. */
final class RepositoryClaimRetirement {
    private RepositoryClaimRetirement() {}

    /** Caller supplies its bounded transaction view and exclusively held, internally retained identities. */
    static boolean fenced(Tx tx, DocumentPublicationCommand command,
            List<RepositoryCoordinatorDrain.Identity> retained, RepositoryReadControl control) {
        Objects.requireNonNull(tx); Objects.requireNonNull(command); Objects.requireNonNull(control).check();
        var identities=List.copyOf(retained);
        if (identities.isEmpty() || identities.size()>2) throw new IllegalArgumentException("Expected one or two retained claims");
        var key=identities.getFirst().key();
        if (!key.account().equals(command.intent().getAccountId()) || !key.operationId().equals(command.operationId())
                || identities.stream().anyMatch(i -> !i.key().equals(key) || !i.commandSha256().equals(command.sha256())))
            throw new IllegalArgumentException("Retirement identity differs from command");
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
            if (!HexFormat.of().formatHex((byte[])row[0]).equals(command.sha256()))
                throw new RepositoryException(RepositoryException.Code.CONFLICT,"Retirement claim command changed");
            RepositoryOperationLedger.requireCommand(em,key,command);
            long epoch=((Number)row[1]).longValue(); var token=(UUID)row[2];
            // V78 forbids deletion, epoch reversal and changing a committed token within its epoch.
            return identities.stream().allMatch(i -> epoch>i.epoch() || epoch==i.epoch() && !token.equals(i.token()));
        });
        control.check();
        return fenced;
    }
}
