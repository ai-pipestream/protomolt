package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Internal byte staging, not schema validation, path evidence or historical retention. */
final class RepositorySchemaArtifacts {
    static final int MAX_ARTIFACT_BYTES = 16 * 1024 * 1024;
    static final int MAX_ARTIFACTS = 64;
    static final long MAX_BATCH_BYTES = 64L * 1024 * 1024;
    private final Tx tx;

    RepositorySchemaArtifacts(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Host supplies immutable bytes after schema verification and accounts for JDBC
     * copies. A live owner does not grant document authorization. Claims protect only
     * these staged bytes; publication needs exact member/path evidence separately.
     * Lowercase fixed-length hex order matches PostgreSQL BYTEA hash order.
     */
    List<String> stage(RepositoryOperationLedger.Owner owner, List<ByteString> artifacts, Runnable control) {
        Objects.requireNonNull(owner); Objects.requireNonNull(artifacts); Objects.requireNonNull(control);
        active(control);
        if (artifacts.isEmpty() || artifacts.size() > MAX_ARTIFACTS) {
            throw new IllegalArgumentException("schema staging requires 1 to 64 artifacts");
        }
        var input = List.copyOf(artifacts);
        long total = 0;
        for (var bytes : input) {
            active(control);
            if (bytes.isEmpty() || bytes.size() > MAX_ARTIFACT_BYTES) {
                throw new IllegalArgumentException("schema artifact exceeds byte limits");
            }
            total += bytes.size();
            if (total > MAX_BATCH_BYTES) throw new IllegalArgumentException("schema staging exceeds batch byte limit");
        }
        var sorted = new TreeMap<String, ByteString>();
        for (var bytes : input) {
            active(control);
            String hash = sha256(bytes);
            var prior = sorted.putIfAbsent(hash, bytes);
            if (prior != null && !prior.equals(bytes)) throw new IllegalArgumentException("schema artifact digest collision");
        }
        var identities = List.copyOf(sorted.keySet());
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            for (var entry : sorted.entrySet()) {
                active(control);
                em.createNativeQuery("""
                        SELECT stage_repository_schema_artifact(:account,:principal,:operation,:generation,:sha,:bytes)
                        """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                        .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                        .setParameter("sha", HexFormat.of().parseHex(entry.getKey()))
                        .setParameter("bytes", entry.getValue().toByteArray()).getSingleResult();
            }
            active(control);
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
        });
        return identities;
    }

    private static String sha256(ByteString bytes) {
        try {
            var hash = MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) hash.update(buffer);
            return HexFormat.of().formatHex(hash.digest());
        } catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 unavailable", e); }
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("schema staging interrupted");
        control.run();
    }
}
