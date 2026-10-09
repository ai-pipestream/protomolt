package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.spi.RepositoryException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.Objects;

/** Internal durable key authority. No creation grant or transport endpoint is supplied here. */
final class RepositoryCredentialAuthorities {
    private final Tx tx;
    RepositoryCredentialAuthorities(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Exact retries cannot change principal/generation or revive a revoked key. */
    void register(RepositoryCaller administrator, RepositoryCredentialBinding binding, String principal) {
        administrator(administrator); Objects.requireNonNull(binding); principal(principal);
        tx.inTransaction(em -> {
            bind(em.createNativeQuery("""
                    INSERT INTO repository_credential_authorities(issuer,credential_id,generation,principal)
                    VALUES (:issuer,:id,:generation,:principal) ON CONFLICT(issuer,credential_id) DO NOTHING
                    """), binding).setParameter("generation", binding.generation()).setParameter("principal", principal).executeUpdate();
            var row = read(em, binding);
            requireIdentity(row, binding, principal);
            if ((Boolean) row[2]) throw denied();
        });
    }

    /** A missing/exchanged generation is not reported as successfully revoked. */
    void revoke(RepositoryCaller administrator, RepositoryCredentialBinding binding, String principal) {
        administrator(administrator); Objects.requireNonNull(binding); principal(principal);
        tx.inTransaction(em -> {
            int changed = bind(em.createNativeQuery("""
                    UPDATE repository_credential_authorities SET revoked=true
                    WHERE issuer=:issuer AND credential_id=:id AND generation=:generation AND principal=:principal
                    """), binding).setParameter("generation", binding.generation()).setParameter("principal", principal).executeUpdate();
            if (changed != 1) throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Credential generation is unavailable");
        });
    }

    /** Compare-and-set rotation; stale or repeated requests conflict and never transfer a grant. */
    RepositoryCredentialBinding rotate(RepositoryCaller administrator, RepositoryCredentialBinding previous, String principal) {
        administrator(administrator); Objects.requireNonNull(previous); principal(principal);
        if (previous.generation() == Long.MAX_VALUE) throw new IllegalArgumentException("Credential generation is exhausted");
        var next = new RepositoryCredentialBinding(previous.issuer(), previous.credentialId(), previous.generation()+1);
        return tx.inTransaction(em -> {
            int changed = bind(em.createNativeQuery("""
                    UPDATE repository_credential_authorities SET generation=:next,revoked=false
                    WHERE issuer=:issuer AND credential_id=:id AND generation=:generation AND principal=:principal
                    """), previous).setParameter("next", next.generation()).setParameter("generation", previous.generation())
                    .setParameter("principal", principal).executeUpdate();
            if (changed != 1) throw new RepositoryException(RepositoryException.Code.CONFLICT, "Credential generation changed or is unavailable");
            return next;
        });
    }

    /**
     * Invoke inside the existing admission transaction, after its claim/owner/revision locks.
     * Shared row locks allow parallel readers; a completed revoke fences later checks.
     * This does not check account grants and must never be treated as creation authority.
     */
    static void requireLive(EntityManager em, RepositoryCaller caller) {
        if (!em.getTransaction().isActive()) throw new IllegalStateException("Credential checks require a transaction");
        if (caller == null || caller.processAuthority() || caller.credentialBinding().isEmpty()) throw denied();
        var binding = caller.credentialBinding().orElseThrow();
        var row = read(em, binding);
        if (row == null || ((Number) row[0]).longValue() != binding.generation()
                || !caller.principalName().equals(row[1]) || (Boolean) row[2]) throw denied();
    }

    private static Object[] read(EntityManager em, RepositoryCredentialBinding binding) {
        var rows = bind(em.createNativeQuery("""
                SELECT generation,principal,revoked FROM lock_repository_credential_authority(:issuer,:id)
                """), binding).getResultList();
        return rows.isEmpty() ? null : (Object[]) rows.getFirst();
    }

    private static void requireIdentity(Object[] row, RepositoryCredentialBinding binding, String principal) {
        if (row == null || ((Number) row[0]).longValue() != binding.generation() || !principal.equals(row[1]))
            throw new RepositoryException(RepositoryException.Code.CONFLICT, "Credential authority differs from requested identity");
    }

    private static Query bind(Query query, RepositoryCredentialBinding binding) {
        return query.setParameter("issuer", binding.issuer()).setParameter("id", binding.credentialId());
    }

    private static void administrator(RepositoryCaller caller) {
        if (caller == null || !caller.processAuthority())
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED, "Credential provisioning requires process authority");
    }

    private static void principal(String principal) {
        if (principal == null || principal.isBlank() || principal.length()>200 || principal.codePoints().anyMatch(Character::isISOControl))
            throw new IllegalArgumentException("Credential principal must be nonblank, at most 200 characters and contain no controls");
    }

    private static RepositoryException denied() {
        return new RepositoryException(RepositoryException.Code.UNAUTHENTICATED, "Repository credential is unavailable");
    }
}
