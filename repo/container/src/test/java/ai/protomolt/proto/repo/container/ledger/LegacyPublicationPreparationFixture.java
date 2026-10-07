package ai.protomolt.proto.repo.container.ledger;

import java.util.HexFormat;
import java.util.UUID;

/** Seeds the actual V81 preparation format for migration tests, before V103 projections existed. */
final class LegacyPublicationPreparationFixture {
    private LegacyPublicationPreparationFixture() {}

    static RepositoryExecutionClaimLedger.Claim acquire(Tx tx, DocumentPublicationPreparationRecord record,
            UUID token, UUID coordinator) {
        var bytes = DocumentPublicationPreparationCodec.encode(record);
        return tx.inTransaction(em -> {
            var version = ((Number) em.createNativeQuery(
                    "SELECT max(CAST(version AS integer)) FROM flyway_schema_history WHERE success AND version IS NOT NULL")
                    .getSingleResult()).intValue();
            if (version < 89 || version >= 103) throw new IllegalArgumentException("Legacy fixture requires schema V89 through V102");
            var acquired = RepositoryExecutionClaimLedger.acquireInitialInTransaction(
                    em, record.key(), record.command(), token, record.lease());
            RepositoryCoordinatorBinding.bindInitial(em, acquired, coordinator);
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
            return acquired.claim();
        });
    }
}
