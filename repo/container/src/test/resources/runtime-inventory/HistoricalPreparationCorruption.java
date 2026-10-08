package ai.protomolt.proto.repo.container.ledger;

import java.util.Arrays;

/** Privileged corruption in an isolated test database; normal SQL cannot write this state. */
final class HistoricalPreparationCorruption implements AutoCloseable {
    private static final String WHERE = " WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=0";
    private final Tx tx;
    private final RepositoryOperationLedger.Key key;
    private final byte[] original;
    private final String damagedRow;

    HistoricalPreparationCorruption(Tx tx, RepositoryOperationLedger.Key key) {
        this.tx = tx;
        this.key = key;
        original = bytes();
        byte[] damaged = original.clone();
        damaged[0] ^= 1;
        tx.inTransaction(em -> {
            em.createNativeQuery("ALTER TABLE repository_publication_preparations DISABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
            em.createNativeQuery("ALTER TABLE repository_publication_preparations DROP CONSTRAINT repository_preparation_digest").executeUpdate();
            require(bind(em.createNativeQuery("UPDATE repository_publication_preparations SET preparation_bytes=:bytes" + WHERE))
                    .setParameter("bytes", damaged).executeUpdate() == 1, "corrupt exactly the original preparation");
            // Enforce the digest for subsequent writes while retaining this deliberately damaged row.
            em.createNativeQuery("ALTER TABLE repository_publication_preparations ADD CONSTRAINT repository_preparation_digest CHECK(preparation_sha256=sha256(preparation_bytes)) NOT VALID").executeUpdate();
            em.createNativeQuery("ALTER TABLE repository_publication_preparations ENABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
        });
        damagedRow = row();
    }

    void requireNoPublication() {
        for (String table : new String[] {"repository_successor_installs", "document_assessment_owners",
                "document_revision_commits", "repository_operation_success"}) {
            long count = tx.readOnly(em -> ((Number) bind(em.createNativeQuery("SELECT count(*) FROM " + table
                    + " WHERE account_id=:a AND principal=:p AND operation_id=:o")).getSingleResult()).longValue());
            require(count == 0, "corrupt preparation cannot create " + table);
        }
        require(!Arrays.equals(original, bytes()), "refusal does not repair or replace the damaged journal");
        require(damagedRow.equals(row()), "refusal preserves the exact damaged row including its stored digest");
    }

    private String row() {
        return tx.readOnly(em -> (String) bind(em.createNativeQuery(
                "SELECT to_jsonb(p)::text FROM repository_publication_preparations p" + WHERE)).getSingleResult());
    }

    private byte[] bytes() {
        return tx.readOnly(em -> (byte[]) bind(em.createNativeQuery(
                "SELECT preparation_bytes FROM repository_publication_preparations" + WHERE)).getSingleResult());
    }

    private jakarta.persistence.Query bind(jakarta.persistence.Query query) {
        return query.setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId());
    }

    @Override public void close() {
        tx.inTransaction(em -> {
            em.createNativeQuery("ALTER TABLE repository_publication_preparations DISABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
            require(bind(em.createNativeQuery("UPDATE repository_publication_preparations SET preparation_bytes=:bytes" + WHERE))
                    .setParameter("bytes", original).executeUpdate() == 1, "restore exactly the original preparation");
            em.createNativeQuery("ALTER TABLE repository_publication_preparations VALIDATE CONSTRAINT repository_preparation_digest").executeUpdate();
            em.createNativeQuery("ALTER TABLE repository_publication_preparations ENABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
        });
        require(Arrays.equals(original, bytes()), "original preparation restored");
    }

    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
