package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.persistence.EntityManager;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Internal catalog adapter. Its caller must authorize administration and policy reads. */
final class DocumentSchemaPolicies {
    record Selection(String account, long revision, DocumentAdmissionPolicy policy) {
        Selection {
            Objects.requireNonNull(account); Objects.requireNonNull(policy);
            if (revision < 1 || !account.equals(policy.definition().getAccountId()))
                throw new IllegalArgumentException("Invalid account policy selection");
        }
    }
    static final class StalePolicy extends IllegalStateException {
        StalePolicy(String message) { super(message); }
    }
    private final Tx tx;
    DocumentSchemaPolicies(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Trusted administration only. Expected zero creates the first pointer; every update advances its revision. */
    Selection activate(DocumentAdmissionPolicy policy, long expectedRevision, Runnable control) {
        Objects.requireNonNull(policy); Objects.requireNonNull(control); active(control);
        if (expectedRevision < 0 || expectedRevision == Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid expected policy revision");
        var account = policy.definition().getAccountId();
        var digest = HexFormat.of().parseHex(policy.sha256());
        return tx.inTransaction(em -> {
            active(control);
            em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
            em.createNativeQuery("""
                    INSERT INTO document_schema_policies(account_id,policy_sha256,policy_codec,policy_version,policy_bytes)
                    VALUES(:account,:sha,:codec,:version,:bytes) ON CONFLICT(account_id,policy_sha256) DO NOTHING
                    """).setParameter("account", account).setParameter("sha", digest)
                    .setParameter("codec", DocumentAdmissionPolicy.CODEC).setParameter("version", DocumentAdmissionPolicy.VERSION)
                    .setParameter("bytes", policy.bytes().toByteArray()).executeUpdate();
            var stored = (byte[]) em.createNativeQuery("""
                    SELECT policy_bytes FROM document_schema_policies WHERE account_id=:account AND policy_sha256=:sha
                    """).setParameter("account", account).setParameter("sha", digest).getSingleResult();
            if (!policy.bytes().equals(ByteString.copyFrom(stored)))
                throw new IllegalStateException("Policy digest collision or corrupt stored bytes");
            active(control);
            int changed;
            if (expectedRevision == 0) {
                changed = em.createNativeQuery("""
                        INSERT INTO document_schema_policy_current(account_id,policy_revision,policy_sha256)
                        VALUES(:account,1,:sha) ON CONFLICT(account_id) DO NOTHING
                        """).setParameter("account", account).setParameter("sha", digest).executeUpdate();
            } else {
                changed = em.createNativeQuery("""
                        UPDATE document_schema_policy_current SET policy_revision=policy_revision+1,policy_sha256=:sha
                        WHERE account_id=:account AND policy_revision=:expected
                        """).setParameter("account", account).setParameter("sha", digest)
                        .setParameter("expected", expectedRevision).executeUpdate();
            }
            if (changed != 1) throw new StalePolicy("Active schema policy changed during administration");
            active(control);
            return new Selection(account, expectedRevision + 1, policy);
        });
    }

    /** Prepare outside the write transaction; the returned revision must be rechecked at publication. */
    Selection read(String account, Runnable control) {
        Objects.requireNonNull(account); Objects.requireNonNull(control); active(control);
        return tx.inTransaction(em -> {
            em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
            return select(em, account, control);
        });
    }

    /**
     * After operation fencing, before document/drive locks. Hold through terminal
     * commit. Parallel writers share this pointer lock; activation UPDATE waits.
     * No registry, provider or other external I/O belongs under this lock.
     */
    static Selection lockCurrent(EntityManager em, Selection expected, Runnable control) {
        Objects.requireNonNull(em); Objects.requireNonNull(expected); Objects.requireNonNull(control); active(control);
        if (!em.getTransaction().isActive()) throw new IllegalStateException("Policy lock requires a transaction");
        em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
        // Lock the pointer first. A concurrent activation can change its digest
        // while this SELECT waits; read the referenced immutable body afterwards.
        var pointers = em.createNativeQuery("""
                SELECT policy_revision,policy_sha256 FROM document_schema_policy_current
                WHERE account_id=:account FOR SHARE
                """).setParameter("account", expected.account()).getResultList();
        if (pointers.isEmpty()) throw new StalePolicy("No active schema policy for account");
        var pointer = (Object[]) pointers.getFirst();
        if (((Number) pointer[0]).longValue() != expected.revision()
                || !HexFormat.of().formatHex((byte[]) pointer[1]).equals(expected.policy().sha256()))
            throw new StalePolicy("Prepared schema policy is no longer active");
        var actual = select(em, expected.account(), control);
        if (actual.revision() != expected.revision() || !actual.policy().bytes().equals(expected.policy().bytes()))
            throw new IllegalStateException("Locked policy content differs from its expected identity");
        active(control);
        return actual;
    }

    private static Selection select(EntityManager em, String account, Runnable control) {
        active(control);
        var rows = em.createNativeQuery("""
                SELECT c.policy_revision,p.policy_codec,p.policy_version,p.policy_bytes,p.policy_sha256
                FROM document_schema_policy_current c JOIN document_schema_policies p
                 ON p.account_id=c.account_id AND p.policy_sha256=c.policy_sha256
                WHERE c.account_id=:account
                """).setParameter("account", account).getResultList();
        if (rows.isEmpty()) throw new StalePolicy("No active schema policy for account");
        var row = (Object[]) rows.getFirst();
        final DocumentAdmissionPolicy policy;
        try {
            policy = DocumentAdmissionPolicy.decode((String) row[1], ((Number) row[2]).intValue(),
                    ByteString.copyFrom((byte[]) row[3]), HexFormat.of().formatHex((byte[]) row[4]), () -> active(control));
        } catch (InvalidProtocolBufferException invalid) {
            throw new IllegalStateException("Invalid retained schema policy", invalid);
        }
        active(control);
        return new Selection(account, ((Number) row[0]).longValue(), policy);
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Schema policy operation interrupted");
        control.run();
    }
}
