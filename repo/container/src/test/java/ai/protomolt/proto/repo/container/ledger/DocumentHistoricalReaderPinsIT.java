package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Historical pins share the ordinary durable reader lifecycle and mirror protocol. */
@Testcontainers
class DocumentHistoricalReaderPinsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final String GENERATION = "historical-reader-pins";
    private static final ManagedBackendLedger.Profile PROFILE = new ManagedBackendLedger.Profile(
            new BackendIdentity("s3", "s3/v1", Map.of("endpoint", "http://synthetic.invalid", "region", "us-east-1", "path-style", "true")),
            "historical-reader-realm");
    private static LedgerDatabase database;
    private static Tx tx;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        new ManagedBackendLedger(tx).bind(GENERATION, PROFILE);
    }

    @AfterAll static void close() { if (database != null) database.close(); }

    record Source(DriveRecord drive, NodeAddress address, ManagedDocumentFixture old, ManagedDocumentFixture current) {
        UUID oldObject() { return UUID.fromString(old.identities().getFirst().getObjectId()); }
        UUID currentObject() { return UUID.fromString(current.identities().getFirst().getObjectId()); }
    }

    static Source supersededSource() {
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "historical-pins-" + drive.driveId; drive.driveType = "PIPELINE";
        drive.bucket = "synthetic";
        new DriveLedger(tx).insert(drive);
        var address = NodeAddress.newBuilder().setAccountId("account").setDocId(UUID.randomUUID().toString())
                .setGraphId("graph").setGraphAddressId("historical-source").build();
        // ManagedDocumentFixture records synthetic SQL verification observations; no provider I/O is claimed.
        var old = ManagedDocumentFixture.publish(tx, drive, GENERATION, PROFILE, address,
                DocumentSecurity.getDefaultInstance(), 1, 1, "old-version");
        var current = ManagedDocumentFixture.publish(tx, drive, GENERATION, PROFILE, address,
                DocumentSecurity.getDefaultInstance(), 1, 1, "current-version");
        return new Source(drive, address, old, current);
    }

    static UUID reader() {
        UUID reader = UUID.randomUUID();
        tx.inTransaction(em -> {
            em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:id,'ACTIVE')")
                    .setParameter("id", reader).executeUpdate();
        });
        return reader;
    }

    static void insert(UUID pin, UUID reader, Source source, UUID actualRevision, UUID recordedRevision,
            UUID object, UUID node, String scope) {
        tx.inTransaction(em -> {
            var query = em.createNativeQuery(scope == null ? """
                    INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision)
                    SELECT :pin,:reader,:object,:node,:recorded,publication_revision
                    FROM document_revision_publications WHERE revision_id=:actual
                    """ : """
                    INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision,read_scope)
                    SELECT :pin,:reader,:object,:node,:recorded,publication_revision,:scope
                    FROM document_revision_publications WHERE revision_id=:actual
                    """).setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object)
                    .setParameter("node", node).setParameter("recorded", recordedRevision).setParameter("actual", actualRevision);
            if (scope != null) query.setParameter("scope", scope);
            if (query.executeUpdate() != 1) throw new IllegalArgumentException("pin fixture could not select publication revision");
        });
    }

    static void insertHistorical(UUID pin, UUID reader, Source source) {
        insert(pin, reader, source, source.old().attempt(), source.old().attempt(), source.oldObject(),
                source.old().row().nodeId, "HISTORICAL");
    }

    static void release(UUID pin, UUID reader, UUID object) {
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT release_document_read_pin(:pin,:reader,:object)")
                    .setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object).getSingleResult();
        });
    }

    static void quiesce(UUID reader) {
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", reader).getSingleResult();
            em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)").setParameter("id", reader).getSingleResult();
        });
    }

    static void recover(UUID pin, UUID reader, UUID object) {
        String claims = "[{\"pin\":\"" + pin + "\",\"object\":\"" + object + "\"}]";
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT recover_quiesced_document_read_pins(:reader,CAST(:claims AS jsonb))")
                    .setParameter("reader", reader).setParameter("claims", claims).getSingleResult();
        });
    }

    static long mirrorCount(UUID pin) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='DOCUMENT_READER' AND owner_id=:pin")
                .setParameter("pin", pin).getSingleResult()).longValue());
    }

    static long pinCount(UUID pin) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_read_pins WHERE pin_id=:pin")
                .setParameter("pin", pin).getSingleResult()).longValue());
    }

    @Test void supersededRevisionPinsOnlyWithHistoricalScopeAndMirrorsExactObject() {
        var source = supersededSource(); var reader = reader(); var pin = UUID.randomUUID();
        insertHistorical(pin, reader, source);
        Object[] stored = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT read_scope,object_id,source_node,source_revision,publication_revision
                FROM document_read_pins WHERE pin_id=:pin
                """).setParameter("pin", pin).getSingleResult());
        assertThat(stored[0]).isEqualTo("HISTORICAL");
        assertThat(stored[1]).isEqualTo(source.oldObject());
        assertThat(stored[2]).isEqualTo(source.old().row().nodeId);
        assertThat(stored[3]).isEqualTo(source.old().attempt());
        assertThat(stored[3]).isNotEqualTo(source.current().attempt());
        assertThat(mirrorCount(pin)).isEqualTo(1);
        Object[] mirror = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT object_id,owner_kind,owner_id,owner_revision FROM repository_object_references
                WHERE owner_kind='DOCUMENT_READER' AND owner_id=:pin
                """).setParameter("pin", pin).getSingleResult());
        assertThat(mirror[0]).isEqualTo(source.oldObject());
        assertThat(mirror[1]).isEqualTo("DOCUMENT_READER");
        assertThat(mirror[2]).isEqualTo(pin);
        assertThat(((Number) mirror[3]).longValue()).isEqualTo(((Number) stored[4]).longValue());

        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_read_pins SET read_scope='CURRENT' WHERE pin_id=:pin")
                    .setParameter("pin", pin).executeUpdate();
        })).hasStackTraceContaining("Document read pin identity is immutable");
        assertThat(mirrorCount(pin)).isEqualTo(1);
        release(pin, reader, source.oldObject());
        assertThat(pinCount(pin)).isZero();
        assertThat(mirrorCount(pin)).isZero();
        release(pin, reader, source.oldObject());
    }

    @Test void defaultAndExplicitCurrentScopeRejectSupersededRevision() {
        var source = supersededSource(); var reader = reader();
        UUID omitted = UUID.randomUUID(), explicit = UUID.randomUUID();
        assertThatThrownBy(() -> insert(omitted, reader, source, source.old().attempt(), source.old().attempt(),
                source.oldObject(), source.old().row().nodeId, null))
                .hasStackTraceContaining("Document read pin requires an open retained current source");
        assertThatThrownBy(() -> insert(explicit, reader, source, source.old().attempt(), source.old().attempt(),
                source.oldObject(), source.old().row().nodeId, "CURRENT"))
                .hasStackTraceContaining("Document read pin requires an open retained current source");
        assertThat(pinCount(omitted)).isZero(); assertThat(pinCount(explicit)).isZero();
        assertThat(mirrorCount(omitted)).isZero(); assertThat(mirrorCount(explicit)).isZero();
    }

    @Test void retiringHistoricalObjectRefusesNewHistoricalPins() {
        var source = supersededSource(); var reader = reader(); var pin = UUID.randomUUID();
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE repository_object_retention SET retiring=true WHERE object_id=:id")
                    .setParameter("id", source.oldObject()).executeUpdate();
        });
        assertThatThrownBy(() -> insertHistorical(pin, reader, source))
                .hasStackTraceContaining("Document read pin requires an open retained historical source");
        assertThat(pinCount(pin)).isZero();
        assertThat(mirrorCount(pin)).isZero();
    }

    @Test void historicalPinRejectsWrongNodeRevisionOrObject() {
        var source = supersededSource(); var other = supersededSource(); var reader = reader();
        UUID wrongNode = UUID.randomUUID(), wrongRevision = UUID.randomUUID(), wrongObject = UUID.randomUUID();
        assertHistoricalRejected(wrongNode, reader, source, source.old().attempt(), source.old().attempt(),
                source.oldObject(), other.old().row().nodeId);
        assertHistoricalRejected(wrongRevision, reader, source, source.old().attempt(), UUID.randomUUID(),
                source.oldObject(), source.old().row().nodeId);
        assertHistoricalRejected(wrongObject, reader, source, source.old().attempt(), source.old().attempt(),
                source.currentObject(), source.old().row().nodeId);
    }

    @Test void historicalPinsUseFenceReleaseRecoveryAndRollbackGates() {
        var source = supersededSource(); var reader = reader();
        UUID releasePin = UUID.randomUUID(), recoveryPin = UUID.randomUUID();
        insertHistorical(releasePin, reader, source); insertHistorical(recoveryPin, reader, source);
        assertThatThrownBy(() -> tx.inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
            em.createNativeQuery("SELECT release_document_read_pin(:pin,:reader,:object)")
                    .setParameter("pin", releasePin).setParameter("reader", reader)
                    .setParameter("object", source.oldObject()).getSingleResult();
            throw new IllegalStateException("rollback release test");
        })).hasMessageContaining("rollback release test");
        assertThat(pinCount(releasePin)).isEqualTo(1); assertThat(mirrorCount(releasePin)).isEqualTo(1);
        release(releasePin, reader, source.oldObject());

        assertThatThrownBy(() -> recover(recoveryPin, reader, source.oldObject())).isInstanceOf(RuntimeException.class);
        var wrongReader = reader(); quiesce(wrongReader);
        assertThatThrownBy(() -> recover(recoveryPin, wrongReader, source.oldObject())).isInstanceOf(RuntimeException.class);
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", reader).getSingleResult();
        });
        assertThatThrownBy(() -> insertHistorical(UUID.randomUUID(), reader, source)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> recover(recoveryPin, reader, source.oldObject())).isInstanceOf(RuntimeException.class);
        quiesce(reader);
        recover(recoveryPin, reader, source.oldObject());
        recover(recoveryPin, reader, source.oldObject());
        assertThat(pinCount(recoveryPin)).isZero(); assertThat(mirrorCount(recoveryPin)).isZero();
        assertThat(mirrorCount(releasePin)).isZero();
    }

    private static void assertHistoricalRejected(UUID pin, UUID reader, Source source, UUID actualRevision,
            UUID recordedRevision, UUID object, UUID node) {
        assertThatThrownBy(() -> insert(pin, reader, source, actualRevision, recordedRevision, object, node, "HISTORICAL"))
                .isInstanceOf(RuntimeException.class);
        assertThat(pinCount(pin)).isZero(); assertThat(mirrorCount(pin)).isZero();
    }

    @ParameterizedTest @ValueSource(strings = {"CURRENT", "HISTORICAL"})
    void exclusiveDocumentFenceRejectsSqlHistoricalCaptureWithoutPartialPins(String scope) throws Exception {
        var source = supersededSource(); var reader = reader(); var pin = UUID.randomUUID();
        var node = source.old().row().nodeId;
        var revision = scope.equals("CURRENT") ? source.current().attempt() : source.old().attempt();
        var object = scope.equals("CURRENT") ? source.currentObject() : source.oldObject();
        try (var connection = java.sql.DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.setAutoCommit(false);
            try (var lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
                lock.setLong(1, node.getMostSignificantBits() ^ node.getLeastSignificantBits());
                lock.execute();
            }
            // Separate database session; no provider activity or timing-only race.
            var failure = catchThrowable(() -> insert(pin, reader, source, revision, revision, object, node, scope));
            assertThat(failure).hasStackTraceContaining("Historical source acquisition conflicts with document mutation");
            Throwable cause = failure;
            while (cause != null && !(cause instanceof java.sql.SQLException)) cause = cause.getCause();
            assertThat(cause).isInstanceOf(java.sql.SQLException.class);
            assertThat(((java.sql.SQLException) cause).getSQLState()).isEqualTo("40001");
            assertThat(pinCount(pin)).isZero(); assertThat(mirrorCount(pin)).isZero();
            connection.rollback();
            insert(pin, reader, source, revision, revision, object, node, scope);
            assertThat(pinCount(pin)).isEqualTo(1); assertThat(mirrorCount(pin)).isEqualTo(1);
            release(pin, reader, object);
        }
    }

    @Test void sqlCaptureHoldsDocumentFenceUntilRollbackAndRollsBackItsMirror() throws Exception {
        var source = supersededSource(); var reader = reader(); var pin = UUID.randomUUID();
        var node = source.old().row().nodeId;
        long key = node.getMostSignificantBits() ^ node.getLeastSignificantBits();
        try (var connection = java.sql.DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.setAutoCommit(false);
            try (var insert = connection.prepareStatement("""
                    INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,
                        source_revision,publication_revision,read_scope)
                    SELECT ?,?,?,node_id,revision_id,publication_revision,'HISTORICAL'
                    FROM document_revision_publications WHERE revision_id=?
                    """)) {
                insert.setObject(1, pin); insert.setObject(2, reader);
                insert.setObject(3, source.oldObject()); insert.setObject(4, source.old().attempt());
                assertThat(insert.executeUpdate()).isEqualTo(1);
            }
            assertThat(exclusiveAvailable(key)).isFalse();
            connection.rollback();
            assertThat(exclusiveAvailable(key)).isTrue();
            assertThat(pinCount(pin)).isZero(); assertThat(mirrorCount(pin)).isZero();
        }
    }

    @Test void sourceFenceMatchesJavaSignedUuidKeysAndHashCollisions() throws Exception {
        for (String value : java.util.List.of("00000000-0000-0000-0000-000000000000",
                "80000000-0000-0000-0000-000000000001", "00000000-0000-0001-8000-000000000000",
                "ffffffff-ffff-ffff-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff")) {
            var node = UUID.fromString(value);
            long key = node.getMostSignificantBits() ^ node.getLeastSignificantBits();
            try (var connection = java.sql.DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
                connection.setAutoCommit(false);
                try (var acquire = connection.prepareStatement("SELECT require_document_source_acquisition(?)")) {
                    acquire.setObject(1, node);
                    try (var result = acquire.executeQuery()) { assertThat(result.next()).isTrue(); assertThat(result.getBoolean(1)).isTrue(); }
                }
                assertThat(exclusiveAvailable(key)).as(value).isFalse();
                // Swapping UUID halves preserves the Java XOR key, including signed keys.
                var collision = new UUID(node.getLeastSignificantBits(), node.getMostSignificantBits());
                assertThat(exclusiveAvailable(collision.getMostSignificantBits() ^ collision.getLeastSignificantBits())).isFalse();
                assertThat(exclusiveAvailable(key ^ 1)).isTrue();
                connection.commit();
                assertThat(exclusiveAvailable(key)).isTrue();
            }
        }
    }

    private static boolean exclusiveAvailable(long key) {
        return tx.inTransaction(em -> (Boolean) em.createNativeQuery("SELECT pg_try_advisory_xact_lock(:key)")
                .setParameter("key", key).getSingleResult());
    }
}
