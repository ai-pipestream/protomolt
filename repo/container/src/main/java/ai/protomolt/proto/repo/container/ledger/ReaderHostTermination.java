package ai.protomolt.proto.repo.container.ledger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Trusted host evidence boundary. No transport endpoint or default verifier is installed. */
public final class ReaderHostTermination {
    public record Identity(UUID execution, String host, String boot) {
        public Identity {
            Objects.requireNonNull(execution); requireText(host, 512); requireText(boot, 512);
        }
    }

    /** Proof bytes are verifier-specific; neither the bytes nor their digest authorize cleanup alone. */
    public record Evidence(String verifier, int format, UUID attestation, byte[] bytes) {
        public Evidence {
            requireText(verifier, 128); Objects.requireNonNull(attestation); Objects.requireNonNull(bytes);
            if (format < 1 || bytes.length < 1 || bytes.length > 65536)
                throw new IllegalArgumentException("Termination evidence requires a positive format and 1 to 65536 bytes");
            bytes = bytes.clone();
        }
        @Override public byte[] bytes() { return bytes.clone(); }
    }

    /**
     * Configured by trusted host composition, never selected from client code.
     * Must independently verify exact execution/host/boot, attestation and proof
     * format, including termination of every worker that can perform its reads.
     * Reject unsupported or unproven evidence by throwing; do not use a no-op.
     * Called before any ledger transaction, including on receipt replay.
     */
    @FunctionalInterface public interface Verifier {
        void verify(Identity identity, Evidence evidence);
    }

    public record Receipt(UUID id, Identity identity, String verifier, int format,
                          UUID attestation, String evidenceSha256) {}

    private final Tx tx;
    private final Map<String, Verifier> verifiers;

    public ReaderHostTermination(Tx tx, Map<String, Verifier> verifiers) {
        this.tx = Objects.requireNonNull(tx);
        this.verifiers = Map.copyOf(verifiers);
    }

    /** Verify first, then atomically record one immutable receipt and terminate the fenced execution. */
    public Receipt record(Identity identity, Evidence evidence) {
        Objects.requireNonNull(identity); Objects.requireNonNull(evidence);
        var verifier = verifiers.get(evidence.verifier());
        if (verifier == null) throw new IllegalArgumentException("No trusted verifier configured for " + evidence.verifier());
        verifier.verify(identity, evidence);
        String digest = digest(evidence.bytes());
        return tx.inTransaction(em -> {
            var hosts = em.createNativeQuery("""
                    SELECT host_identity,boot_identity,state FROM repository_reader_host_executions
                    WHERE execution=:execution FOR UPDATE
                    """).setParameter("execution", identity.execution()).getResultList();
            if (hosts.size() != 1) throw new IllegalStateException("Unknown host execution");
            var host = (Object[]) hosts.getFirst();
            if (!identity.host().equals(host[0]) || !identity.boot().equals(host[1]))
                throw new IllegalStateException("Host execution identity mismatch");
            var receipts = em.createNativeQuery("""
                    SELECT receipt_id,verifier,proof_format,attestation_id,evidence_sha256
                    FROM repository_reader_host_terminations WHERE execution=:execution
                    """).setParameter("execution", identity.execution()).getResultList();
            if (!receipts.isEmpty()) {
                var stored = (Object[]) receipts.getFirst();
                if (!"TERMINATED".equals(host[2]) || !evidence.verifier().equals(stored[1])
                        || evidence.format() != ((Number) stored[2]).intValue()
                        || !evidence.attestation().equals(stored[3]) || !digest.equals(stored[4]))
                    throw new IllegalStateException("Conflicting host termination evidence");
                return new Receipt((UUID) stored[0], identity, evidence.verifier(), evidence.format(), evidence.attestation(), digest);
            }
            if (!"FENCED".equals(host[2])) throw new IllegalStateException("Host termination requires a fenced execution");
            var id = UUID.randomUUID();
            em.createNativeQuery("""
                    INSERT INTO repository_reader_host_terminations
                    (execution,receipt_id,host_identity,boot_identity,verifier,proof_format,attestation_id,evidence_sha256)
                    VALUES(:execution,:receipt,:host,:boot,:verifier,:format,:attestation,:digest)
                    """).setParameter("execution", identity.execution()).setParameter("receipt", id)
                    .setParameter("host", identity.host()).setParameter("boot", identity.boot())
                    .setParameter("verifier", evidence.verifier()).setParameter("format", evidence.format())
                    .setParameter("attestation", evidence.attestation()).setParameter("digest", digest).executeUpdate();
            int updated = em.createNativeQuery("""
                    UPDATE repository_reader_host_executions SET state='TERMINATED'
                    WHERE execution=:execution AND state='FENCED'
                    """).setParameter("execution", identity.execution()).executeUpdate();
            if (updated != 1) throw new IllegalStateException("Host termination transition was not acknowledged");
            return new Receipt(id, identity, evidence.verifier(), evidence.format(), evidence.attestation(), digest);
        });
    }

    private static String digest(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException("SHA-256 unavailable", failure); }
    }
    private static void requireText(String value, int maximum) {
        Objects.requireNonNull(value);
        if (value.isBlank() || value.codePointCount(0, value.length()) > maximum)
            throw new IllegalArgumentException("Identity text is empty or exceeds its bound");
    }
}
