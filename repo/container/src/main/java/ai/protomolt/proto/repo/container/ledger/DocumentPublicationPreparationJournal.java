package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Private shared preparation storage. No restored session or provider execution is enabled here. */
final class DocumentPublicationPreparationJournal {
    private final Tx tx;
    private final PayloadBudget budget;
    DocumentPublicationPreparationJournal(Tx tx, PayloadBudget budget) {
        this.tx = Objects.requireNonNull(tx); this.budget = Objects.requireNonNull(budget);
    }

    /** Claim and immutable initial seeds become visible together, including uncertain commit responses. */
    RepositoryExecutionClaimLedger.Claim acquireInitial(RepositoryCaller caller,
            DocumentPublicationPreparationRecord record, UUID token, RepositoryReadControl control) {
        return acquireInitial(caller, record, token, null, control);
    }

    /** Optional manager identity binds only a newly created claim or its exact bound retry. */
    RepositoryExecutionClaimLedger.Claim acquireInitial(RepositoryCaller caller,
            DocumentPublicationPreparationRecord record, UUID token, UUID coordinator, RepositoryReadControl control) {
        Objects.requireNonNull(record); Objects.requireNonNull(token); require(caller, record.key(), control);
        if (record.predecessorGeneration() != 0)
            throw new IllegalArgumentException("Initial registration requires no predecessor");
        try (var reservation = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
            // Serialize before acquiring SQL locks; the immutable record is the exact retry identity.
            var encoded = DocumentPublicationPreparationCodec.encode(record); var digest = digest(encoded);
            control.check();
            var claim = tx.inTransaction(em -> {
                var acquisition = RepositoryExecutionClaimLedger.acquireInitialInTransaction(
                        em, record.key(), record.command(), token, record.lease());
                var acquired = acquisition.claim();
                if (coordinator != null) RepositoryCoordinatorBinding.bindInitial(em, acquisition, coordinator);
                else RepositoryCoordinatorBinding.requireUnbound(em, acquired);
                control.check();
                insert(em, acquired, record, encoded, digest);
                control.check();
                return acquired;
            });
            control.check();
            return claim;
        }
    }

    void save(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            DocumentPublicationPreparationRecord record, RepositoryReadControl control) {
        Objects.requireNonNull(record); require(caller, claim.key(), control);
        if (!record.key().equals(claim.key()) || !record.command().sha256().equals(claim.commandSha256()))
            throw new IllegalArgumentException("Preparation differs from execution claim");
        // Bound encoded work before allocation; this counts bytes, not total parsed JVM heap.
        try (var reservation = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
            var encoded = DocumentPublicationPreparationCodec.encode(record); var digest = digest(encoded);
            control.check();
            tx.inTransaction(em -> {
                insert(em, claim, record, encoded, digest);
                control.check(); return null;
            });
            control.check();
        }
    }

    private static void insert(EntityManager em, RepositoryExecutionClaimLedger.Claim claim,
            DocumentPublicationPreparationRecord record, ByteString encoded, byte[] digest) {
        // Exact claim retries must re-establish the current-transaction fence for V81's guard.
        RepositoryExecutionClaimLedger.lockLive(em, claim);
        bind(em.createNativeQuery("""
                INSERT INTO repository_publication_preparations(account_id,principal,operation_id,predecessor_generation,
                  owner_nonce,command_codec,command_version,command_bytes,command_sha256,preparation_bytes,preparation_sha256)
                VALUES (:a,:p,:o,:g,:owner,:codec,:version,:command,:commandDigest,:bytes,:digest)
                ON CONFLICT(account_id,principal,operation_id,predecessor_generation) DO NOTHING
                """), claim.key(), record.predecessorGeneration()).setParameter("owner", record.seeds().ownerNonce())
                .setParameter("codec", DocumentPublicationCommand.CODEC).setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                .setParameter("command", record.command().canonical().toByteArray())
                .setParameter("commandDigest", HexFormat.of().parseHex(record.command().sha256()))
                .setParameter("bytes", encoded.toByteArray()).setParameter("digest", digest).executeUpdate();
    }

    /** Trusted process-only bootstrap before claim acquisition; never returns private seeds or placement. */
    Optional<DocumentPublicationCommand> readCommand(RepositoryCaller caller, RepositoryOperationLedger.Key key,
            long predecessor, RepositoryReadControl control) {
        require(caller, key, control); generation(predecessor);
        if (!caller.processAuthority())
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                    "Private recovery bootstrap requires trusted process authority");
        try (var reservation = budget.reserve(DocumentPublicationCommand.MAX_COMMAND_BYTES)) {
            var row = tx.readOnly(em -> {
                var rows = bind(em.createNativeQuery("""
                        SELECT command_codec,command_version,
                          CASE WHEN octet_length(command_bytes)<=1048576 THEN command_bytes END,command_sha256
                        FROM repository_publication_preparations
                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                        """), key, predecessor).getResultList();
                return rows.isEmpty() ? null : (Object[]) rows.getFirst();
            });
            control.check();
            if (row == null) return Optional.empty();
            try {
                if (row[2]==null) throw corrupt();
                var bytes = ByteString.copyFrom((byte[]) row[2]);
                if (!DocumentPublicationCommand.CODEC.equals(row[0]) || ((Number) row[1]).intValue()!=DocumentPublicationCommand.ENCODING_VERSION
                        || bytes.size()>DocumentPublicationCommand.MAX_COMMAND_BYTES || !Arrays.equals(digest(bytes), (byte[]) row[3])) throw corrupt();
                var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(bytes).toBuilder()
                        .setOperationId(key.operationId().toString()).build());
                if (!command.intent().getAccountId().equals(key.account()) || !command.canonical().equals(bytes)) throw corrupt();
                control.check(); return Optional.of(command);
            } catch (com.google.protobuf.InvalidProtocolBufferException | IllegalArgumentException malformed) { throw corrupt(malformed); }
        }
    }

    /** Holds a byte reservation through the caller's use of the decoded private preparation. */
    Optional<Loaded> load(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            long predecessor, RepositoryReadControl control) {
        require(caller, claim.key(), control); generation(predecessor);
        PayloadBudget.Lease[] reservation = {null}; boolean transferred = false;
        try {
            var row = tx.inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                var sizes = bind(em.createNativeQuery("""
                        SELECT octet_length(preparation_bytes) FROM repository_publication_preparations
                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                        """), claim.key(), predecessor).getResultList();
                if (sizes.isEmpty()) return null;
                int size = ((Number) sizes.getFirst()).intValue();
                if (size < 1 || size > DocumentPublicationPreparationCodec.MAX_BYTES) throw corrupt();
                reservation[0] = budget.reserve(size);
                control.check();
                return (Object[]) bind(em.createNativeQuery("""
                        SELECT preparation_bytes,preparation_sha256,owner_nonce,command_sha256 FROM repository_publication_preparations
                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                        """), claim.key(), predecessor).getSingleResult();
            });
            control.check();
            if (row == null) return Optional.empty();
            var bytes = ByteString.copyFrom((byte[]) row[0]);
            if (bytes.size()!=reservation[0].bytes() || !Arrays.equals(digest(bytes), (byte[]) row[1])
                    || !claim.commandSha256().equals(HexFormat.of().formatHex((byte[]) row[3]))) throw corrupt();
            DocumentPublicationPreparationRecord record;
            try { record = DocumentPublicationPreparationCodec.decode(bytes, claim.key(), claim.commandSha256()); }
            catch (IllegalArgumentException malformed) { throw corrupt(malformed); }
            if (record.predecessorGeneration()!=predecessor || !record.seeds().ownerNonce().equals((UUID) row[2])) throw corrupt();
            control.check();
            // Decoding happens outside SQL locks. Reauthorize delivery after that work;
            // the borrowed record itself grants no authority for later mutations.
            tx.inTransaction(em -> { RepositoryExecutionClaimLedger.lockLive(em, claim); return null; });
            control.check();
            var loaded = new Loaded(record, reservation[0]); transferred = true;
            return Optional.of(loaded);
        } finally { if (!transferred && reservation[0]!=null) reservation[0].close(); }
    }

    /** Borrowed value: finish all uses before close. It grants no operation or claim authority. */
    static final class Loaded implements AutoCloseable {
        private DocumentPublicationPreparationRecord record;
        private final PayloadBudget.Lease lease;
        private Loaded(DocumentPublicationPreparationRecord record, PayloadBudget.Lease lease) { this.record=record; this.lease=lease; }
        synchronized DocumentPublicationPreparationRecord record() {
            if (record==null) throw new IllegalStateException("Preparation load is closed");
            return record;
        }
        @Override public synchronized void close() { record=null; lease.close(); }
        @Override public String toString() { return "LoadedPreparation[private]"; }
    }

    private static Query bind(Query query, RepositoryOperationLedger.Key key, long predecessor) {
        return query.setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).setParameter("g", predecessor);
    }
    private static void require(RepositoryCaller caller, RepositoryOperationLedger.Key key, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
    }
    private static void generation(long value) {
        if (value<0 || value==Long.MAX_VALUE) throw new IllegalArgumentException("Invalid preparation predecessor");
    }
    private static byte[] digest(ByteString bytes) {
        try { var digest=MessageDigest.getInstance("SHA-256"); digest.update(bytes.asReadOnlyByteBuffer()); return digest.digest(); }
        catch (NoSuchAlgorithmException missing) { throw new IllegalStateException("SHA-256 unavailable", missing); }
    }
    private static RepositoryException corrupt() { return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Private preparation integrity check failed"); }
    private static RepositoryException corrupt(Throwable cause) { return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Private preparation integrity check failed", cause); }
}
