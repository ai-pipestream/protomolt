package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
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
        return stage(owner, artifacts, control, em -> {});
    }

    /** Publication staging additionally binds the exact durable command in the staging transaction. */
    List<String> stage(RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            List<ByteString> artifacts, Runnable control) {
        Objects.requireNonNull(command);
        if (!owner.key().account().equals(command.intent().getAccountId()) || !owner.key().operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Schema staging command differs from owner scope");
        return stage(owner, artifacts, control, em -> RepositoryOperationLedger.requireCommand(em, owner.key(), command));
    }

    private List<String> stage(RepositoryOperationLedger.Owner owner, List<ByteString> artifacts, Runnable control,
            java.util.function.Consumer<jakarta.persistence.EntityManager> commandCheck) {
        return stage(owner, artifacts, control, commandCheck, em -> {});
    }

    /** Internal composition: the ordered authority fence runs in the same transaction as artifact claims. */
    List<String> stageAuthorized(RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            List<ByteString> artifacts, Runnable control,
            java.util.function.Consumer<jakarta.persistence.EntityManager> authorityFence) {
        Objects.requireNonNull(command); Objects.requireNonNull(authorityFence);
        if (!owner.key().account().equals(command.intent().getAccountId()) || !owner.key().operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Schema staging command differs from owner scope");
        return stage(owner, artifacts, control,
                em -> RepositoryOperationLedger.requireCommand(em, owner.key(), command), authorityFence);
    }

    private List<String> stage(RepositoryOperationLedger.Owner owner, List<ByteString> artifacts, Runnable control,
            java.util.function.Consumer<jakarta.persistence.EntityManager> commandCheck,
            java.util.function.Consumer<jakarta.persistence.EntityManager> authorityFence) {
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
            authorityFence.accept(em);
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            commandCheck.accept(em);
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

    /**
     * Reads bytes already claimed by this operation, including an earlier generation.
     * Ownership and claim visibility are checked at the database read, without locking
     * the owner across descriptor decoding. The returned bytes belong to the caller.
     * This is retry preparation, not document authorization or a historical read API.
     * A later publication must recheck ownership and all admission requirements.
     */
    ByteString readRetained(RepositoryOperationLedger.Owner owner, String artifactSha256, Runnable control) {
        Objects.requireNonNull(owner); Objects.requireNonNull(artifactSha256); Objects.requireNonNull(control);
        active(control);
        if (!artifactSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("schema artifact identity requires lowercase SHA-256");
        }
        byte[] bytes = tx.readOnly(em -> {
            var rows = em.createNativeQuery("""
                    SELECT a.artifact_bytes FROM repository_operation_owners o
                    JOIN repository_schema_artifacts a ON a.account_id=o.account_id AND a.artifact_sha256=:sha
                    WHERE o.account_id=:account AND o.principal=:principal AND o.operation_id=:operation
                     AND o.owner_generation=:generation AND o.owner_token=:token AND o.lease_until>clock_timestamp()
                     AND EXISTS(SELECT 1 FROM repository_schema_artifact_claims c
                      WHERE c.account_id=o.account_id AND c.principal=o.principal AND c.operation_id=o.operation_id
                       AND c.artifact_sha256=a.artifact_sha256)
                    """).setParameter("sha", HexFormat.of().parseHex(artifactSha256))
                    .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                    .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                    .setParameter("token", owner.token()).getResultList();
            if (rows.isEmpty()) throw new IllegalStateException("Retained schema is unavailable to the live operation owner");
            return (byte[]) rows.getFirst();
        });
        active(control);
        if (bytes.length == 0 || bytes.length > MAX_ARTIFACT_BYTES) throw new IllegalStateException("Invalid retained schema size");
        var retained = ByteString.copyFrom(bytes);
        if (!sha256(retained).equals(artifactSha256)) throw new IllegalStateException("Retained schema digest mismatch");
        active(control);
        return retained;
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
