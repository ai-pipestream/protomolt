package ai.protomolt.proto.repo.container.ledger;

import java.sql.SQLException;
import java.util.*;

/** Real PostgreSQL rollback after the selected reader's native pin and mirror have been deleted. */
final class HistoricalPinReleaseFault implements AutoCloseable {
    private static final String MESSAGE = "injected historical pin release failure after mirror removal";
    private final Tx tx;
    private final UUID reader;
    private final RepositoryOperationLedger.Key key;
    private final String trigger = "zz_test_pin_release_" + UUID.randomUUID().toString().replace("-", "");
    private final List<UUID> ids;
    private final List<String> before;

    HistoricalPinReleaseFault(Tx tx, UUID reader, RepositoryOperationLedger.Key key) {
        this.tx = tx; this.reader = reader; this.key = key;
        ids = tx.readOnly(em -> List.copyOf(em.createNativeQuery(
                "SELECT pin_id FROM document_read_pins WHERE reader_incarnation=:r ORDER BY pin_id", UUID.class)
                .setParameter("r", reader).getResultList()));
        before = nativePins();
        require(!ids.isEmpty() && before.equals(mirrors()), "exact native pins and mirrors exist before release fault");
        // Identifiers contain only the fixed prefix and generated UUID hex. The reader is a generated UUID.
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION " + trigger + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + "IF OLD.reader_incarnation='" + reader + "'::uuid THEN "
                    + "IF EXISTS(SELECT 1 FROM repository_object_references WHERE owner_kind='DOCUMENT_READER' AND owner_id=OLD.pin_id) THEN "
                    + "RAISE EXCEPTION 'Pin mirror was not removed before fault'; END IF; "
                    + "RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='" + MESSAGE + "'; END IF; RETURN OLD; END $$").executeUpdate();
            em.createNativeQuery("CREATE TRIGGER " + trigger + " AFTER DELETE ON document_read_pins FOR EACH ROW EXECUTE FUNCTION "
                    + trigger + "()").executeUpdate();
        });
    }

    void requireFailure(RuntimeException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof SQLException sql && "P0001".equals(sql.getSQLState()) && sql.getMessage().contains(MESSAGE)) return;
        throw new AssertionError("Expected the exact SQL pin-release rollback", failure);
    }

    void requireRetained() {
        require(nativePins().equals(before) && mirrors().equals(before), "rollback restores every exact pin and mirror");
        require(drains() == 0, "failed SQL release cannot produce a capture-drain receipt");
    }

    void requireReleased() {
        require(nativePins().isEmpty() && mirrors().isEmpty(), "retry removes exact native pins and mirrors");
        require(drains() == 1, "successful retry records one capture-drain receipt");
    }

    private List<String> nativePins() {
        return tx.readOnly(em -> List.copyOf(em.createNativeQuery(
                "SELECT pin_id::text || ':' || object_id::text FROM document_read_pins WHERE reader_incarnation=:r ORDER BY pin_id", String.class)
                .setParameter("r", reader).getResultList()));
    }

    private List<String> mirrors() {
        return tx.readOnly(em -> List.copyOf(em.createNativeQuery("""
                SELECT owner_id::text || ':' || object_id::text FROM repository_object_references
                WHERE owner_kind='DOCUMENT_READER' AND owner_id IN (:pins) ORDER BY owner_id
                """, String.class).setParameter("pins", ids).getResultList()));
    }

    private long drains() {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM repository_preparation_capture_drains
                WHERE account_id=:a AND principal=:p AND operation_id=:o
                """).setParameter("a", key.account()).setParameter("p", key.principal())
                .setParameter("o", key.operationId()).getSingleResult()).longValue());
    }

    @Override public void close() {
        tx.inTransaction(em -> {
            em.createNativeQuery("DROP TRIGGER " + trigger + " ON document_read_pins").executeUpdate();
            em.createNativeQuery("DROP FUNCTION " + trigger + "()").executeUpdate();
        });
    }

    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
