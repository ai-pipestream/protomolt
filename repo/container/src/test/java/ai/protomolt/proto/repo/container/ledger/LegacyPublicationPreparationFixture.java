package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.HexFormat;
import java.util.UUID;

/** Seeds the actual V81 preparation format for migration tests, before V103 projections existed. */
final class LegacyPublicationPreparationFixture {
    private LegacyPublicationPreparationFixture() {}

    /** First schema version whose preparation writer also indexes historical sources (V103). */
    static final int HISTORY_SETS_VERSION = 103;

    static int schemaVersion(Tx tx) {
        return tx.readOnly(em -> schemaVersion(em));
    }

    static int schemaVersion(jakarta.persistence.EntityManager em) {
        return ((Number) em.createNativeQuery(
                "SELECT max(CAST(version AS integer)) FROM flyway_schema_history WHERE success AND version IS NOT NULL")
                .getSingleResult()).intValue();
    }

    /**
     * Version-aware initial acquisition for migration tests: a pre-V103 schema gets the
     * writer semantics of its own version, a current schema gets the real journal. The
     * returned claim is the same identity either way, so the test then migrates with the
     * real Flyway chain and exercises the current API against rows an older writer left.
     */
    static RepositoryExecutionClaimLedger.Claim acquireInitial(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            DocumentPublicationPreparationRecord record, UUID token, UUID coordinator, RepositoryReadControl control) {
        return schemaVersion(tx) < HISTORY_SETS_VERSION
                ? acquire(tx, record, token, coordinator)
                : new DocumentPublicationPreparationJournal(tx, budget).acquireInitial(caller, record, token, coordinator, control);
    }

    static RepositoryExecutionClaimLedger.Claim acquire(Tx tx, DocumentPublicationPreparationRecord record,
            UUID token, UUID coordinator) {
        return tx.inTransaction(em -> {
            var version = schemaVersion(em);
            if (version < 89 || version >= HISTORY_SETS_VERSION) throw new IllegalArgumentException("Legacy fixture requires schema V89 through V102");
            var acquired = RepositoryExecutionClaimLedger.acquireInitialInTransaction(
                    em, record.key(), record.command(), token, record.lease());
            RepositoryCoordinatorBinding.bindInitial(em, acquired, coordinator);
            insert(em, record);
            return acquired.claim();
        });
    }

    static void save(Tx tx, RepositoryExecutionClaimLedger.Claim claim, DocumentPublicationPreparationRecord record) {
        tx.inTransaction(em -> {
            var version = schemaVersion(em);
            if (version < 81 || version >= HISTORY_SETS_VERSION) throw new IllegalArgumentException("Legacy fixture requires schema V81 through V102");
            if (!claim.key().equals(record.key()) || !claim.commandSha256().equals(record.command().sha256()))
                throw new IllegalArgumentException("Preparation differs from execution claim");
            RepositoryExecutionClaimLedger.lockLive(em, claim);
            insert(em, record);
        });
    }

    private static void insert(jakarta.persistence.EntityManager em, DocumentPublicationPreparationRecord record) {
        var bytes = DocumentPublicationPreparationCodec.encode(record);
        em.createNativeQuery("""
                    INSERT INTO repository_publication_preparations(account_id,principal,operation_id,predecessor_generation,
                     owner_nonce,command_codec,command_version,command_bytes,command_sha256,preparation_bytes,preparation_sha256)
                    VALUES(:a,:p,:o,:g,:owner,:codec,:version,:command,:commandDigest,:bytes,:digest)
                    """).setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                    .setParameter("o", record.key().operationId()).setParameter("g", record.predecessorGeneration())
                    .setParameter("owner", record.seeds().ownerNonce())
                    .setParameter("codec", ai.protomolt.proto.repo.spi.DocumentPublicationCommand.CODEC)
                    .setParameter("version", ai.protomolt.proto.repo.spi.DocumentPublicationCommand.ENCODING_VERSION)
                    .setParameter("command", record.command().canonical().toByteArray())
                    .setParameter("commandDigest", HexFormat.of().parseHex(record.command().sha256()))
                    .setParameter("bytes", bytes.toByteArray())
                    .setParameter("digest", DocumentPublicationPreparationJournal.digest(bytes)).executeUpdate();
    }
}
