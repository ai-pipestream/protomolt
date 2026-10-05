package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.gson.JsonObject;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Set;
import java.util.stream.Collectors;

/** Checks derived commit modes without transferring private journal state or opening another transaction. */
final class DocumentPublicationModeBinding {
    private DocumentPublicationModeBinding() {}

    /** Caller already fenced owner/command and acquired current document authorization in this transaction. */
    static void require(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Set<String> typedMembers, Runnable control) {
        control.run();
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Mode binding requires a writable commit transaction");
        if (owner.executionClaim().isEmpty()) return; // V81 preparation requires a claimed operation scope.
        var members = command.intent().getMembersList();
        if (members.isEmpty() || members.size()>64 || !members.stream().map(member -> member.getMemberId())
                .collect(Collectors.toSet()).containsAll(typedMembers))
            throw new IllegalArgumentException("Committed mode set differs from command");
        // Command validation bounds member names; at most 64 small entries are sent to SQL.
        var modes = new JsonObject();
        members.forEach(member -> modes.addProperty(member.getMemberId(), typedMembers.contains(member.getMemberId()) ? "TYPED" : "OPAQUE"));
        String encoded = modes.toString();
        if (encoded.getBytes(StandardCharsets.UTF_8).length>128*1024)
            throw new IllegalArgumentException("Committed mode encoding exceeds bound");
        boolean matched = Boolean.TRUE.equals(em.createNativeQuery("""
                SELECT NOT EXISTS (
                  SELECT 1 FROM repository_publication_preparations WHERE account_id=:a AND principal=:p
                    AND operation_id=:o AND predecessor_generation=:g
                ) OR EXISTS (
                  SELECT 1 FROM repository_publication_preparations preparation
                  JOIN repository_publication_modes modes USING(account_id,principal,operation_id,predecessor_generation)
                  WHERE preparation.account_id=:a AND preparation.principal=:p AND preparation.operation_id=:o
                    AND preparation.predecessor_generation=:g AND preparation.owner_nonce=:owner
                    AND modes.owner_nonce=:owner AND preparation.command_sha256=:digest
                    AND modes.modes=CAST(:observed AS jsonb)
                )
                """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()-1)
                .setParameter("owner", owner.token()).setParameter("digest", HexFormat.of().parseHex(command.sha256()))
                .setParameter("observed", encoded).getSingleResult());
        control.run();
        if (!matched) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Committed publication modes differ from fixed modes");
    }
}
