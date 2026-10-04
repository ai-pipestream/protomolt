package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.v1.*;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL native/mirror guards. Synthetic source evidence is not provider qualification. */
@Testcontainers
class DocumentReaderPinsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    static LedgerDatabase database;
    static Tx tx;
    static final String GENERATION = "reader-pins";
    static final ManagedBackendLedger.Profile PROFILE = new ManagedBackendLedger.Profile(
            new BackendIdentity("s3", "s3/v1", Map.of("endpoint", "http://synthetic.invalid", "region", "us-east-1", "path-style", "true")), "reader-realm");

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        new ManagedBackendLedger(tx).bind(GENERATION, PROFILE);
    }
    @AfterAll static void close() { if (database != null) database.close(); }
    record Source(ManagedDocumentFixture fixture, DriveRecord drive, NodeAddress address) {
        UUID object() { return UUID.fromString(fixture.identities().getFirst().getObjectId()); }
    }
    static Source source() {
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "pin-" + drive.driveId; drive.driveType = "PIPELINE"; drive.bucket = "synthetic";
        new DriveLedger(tx).insert(drive);
        var address = NodeAddress.newBuilder().setAccountId("account").setDocId(UUID.randomUUID().toString())
                .setGraphId("graph").setGraphAddressId("source").build();
        return new Source(ManagedDocumentFixture.publish(tx, drive, GENERATION, PROFILE, address,
                DocumentSecurity.getDefaultInstance(), 1, 1, "v1"), drive, address);
    }
    static UUID reader() {
        UUID reader = UUID.randomUUID();
        tx.inTransaction(em -> { em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:id,'ACTIVE')")
                .setParameter("id", reader).executeUpdate(); });
        return reader;
    }
    static void pin(UUID pin, UUID reader, Source source, UUID revision) {
        tx.inTransaction(em -> { insert(em, pin, reader, source, revision); });
    }
    static void insert(jakarta.persistence.EntityManager em, UUID pin, UUID reader, Source source, UUID revision) {
        em.createNativeQuery("""
                INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision)
                SELECT :pin,:reader,:object,:node,:revision,publication_revision
                FROM document_revision_publications WHERE revision_id=:actual
                """).setParameter("pin", pin).setParameter("reader", reader).setParameter("object", source.object())
                .setParameter("node", source.fixture.row().nodeId).setParameter("revision", revision)
                .setParameter("actual", source.fixture.attempt()).executeUpdate();
    }
    static void release(UUID pin, UUID reader, UUID object) {
        tx.inTransaction(em -> { em.createNativeQuery("SELECT release_document_read_pin(:pin,:reader,:object)")
                .setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object).getSingleResult(); });
    }
    static long references(UUID pin) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_object_references WHERE owner_kind='DOCUMENT_READER' AND owner_id=:pin")
                .setParameter("pin", pin).getSingleResult()).longValue());
    }

    @Test void nativePinMirrorsAndExactReleaseIsRetryable() {
        var source = source(); UUID reader = reader(), pin = UUID.randomUUID();
        pin(pin, reader, source, source.fixture.attempt());
        assertThat(references(pin)).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_read_pins SET source_revision=:revision WHERE pin_id=:pin")
                    .setParameter("revision", UUID.randomUUID()).setParameter("pin", pin).executeUpdate();
        })).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> release(pin, UUID.randomUUID(), source.object())).isInstanceOf(RuntimeException.class);
        assertThat(references(pin)).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("DELETE FROM repository_object_references WHERE owner_kind='DOCUMENT_READER' AND owner_id=:pin")
                    .setParameter("pin", pin).executeUpdate();
        })).isInstanceOf(RuntimeException.class);
        release(pin, reader, source.object());
        release(pin, reader, source.object());
        assertThat(references(pin)).isZero();
    }

    @Test void fencedReaderCannotAdmitButCanReleaseItsExistingPin() {
        var source = source(); UUID reader = reader(), pin = UUID.randomUUID();
        pin(pin, reader, source, source.fixture.attempt());
        tx.inTransaction(em -> { em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", reader).getSingleResult(); });
        UUID rejected = UUID.randomUUID();
        assertThatThrownBy(() -> pin(rejected, reader, source, source.fixture.attempt())).isInstanceOf(RuntimeException.class);
        assertThat(references(rejected)).isZero();
        assertThat(references(pin)).isEqualTo(1);
        release(pin, reader, source.object());
    }

    @Test void forgedRevisionAndDetachedMirrorAreRejected() {
        var source = source(); UUID reader = reader(), pin = UUID.randomUUID();
        assertThatThrownBy(() -> pin(pin, reader, source, UUID.randomUUID())).isInstanceOf(RuntimeException.class);
        assertThat(references(pin)).isZero();
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("INSERT INTO repository_object_references VALUES(:object,'DOCUMENT_READER',:pin,1)")
                    .setParameter("object", source.object()).setParameter("pin", pin).executeUpdate();
        })).isInstanceOf(RuntimeException.class);
    }

    @Test void sourceReplacementDoesNotReleaseReaderProtection() {
        var source = source(); UUID reader = reader(), pin = UUID.randomUUID();
        pin(pin, reader, source, source.fixture.attempt());
        ManagedDocumentFixture.publish(tx, source.drive, GENERATION, PROFILE, source.address,
                DocumentSecurity.getDefaultInstance(), 1, 1, "v2");
        assertThat(references(pin)).isEqualTo(1);
        UUID rejected = UUID.randomUUID();
        assertThatThrownBy(() -> pin(rejected, reader, source, source.fixture.attempt())).isInstanceOf(RuntimeException.class);
        assertThat(references(rejected)).isZero();
        release(pin, reader, source.object());
        assertThat(references(pin)).isZero();
    }

    @Test void retiringSourceRefusesNewPins() {
        var source = source(); UUID reader = reader(), pin = UUID.randomUUID();
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE repository_object_retention SET retiring=true WHERE object_id=:id")
                .setParameter("id", source.object()).executeUpdate(); });
        assertThatThrownBy(() -> pin(pin, reader, source, source.fixture.attempt())).isInstanceOf(RuntimeException.class);
        assertThat(references(pin)).isZero();
    }

    @Test void twoReadersAcquireSharedObjectWithoutSerializingTheirTransactions() throws Exception {
        var source = source(); UUID firstReader = reader(), secondReader = reader();
        UUID first = UUID.randomUUID(), second = UUID.randomUUID();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var tasks = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var firstTask = tasks.submit(() -> tx.inTransaction(em -> {
                insert(em, first, firstReader, source, source.fixture.attempt());
                entered.countDown();
                try {
                    if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("Pin test gate expired");
                } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
            }));
            try {
                assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var secondTask = tasks.submit(() -> pin(second, secondReader, source, source.fixture.attempt()));
                secondTask.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(references(second)).isEqualTo(1);
            } finally { release.countDown(); }
            firstTask.get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        release(first, firstReader, source.object());
        release(second, secondReader, source.object());
    }
}
