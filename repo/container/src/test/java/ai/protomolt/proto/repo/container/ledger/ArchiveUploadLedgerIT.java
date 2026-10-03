package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchiveUploadLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void verifiesOnlyTheLiveAttemptAndPreservesItsExactByteIdentity() throws Exception {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var uploads = new ArchiveUploadLedger(tx);
            var admission = uploads.begin(location(tx), 7, "text/plain", Duration.ofMinutes(1));
            var upload = admission.upload();
            try (var connection = java.sql.DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                    var query = connection.prepareStatement("SELECT lease_until FROM archive_object_uploads WHERE object_id=?")) {
                query.setObject(1, upload.objectId());
                try (var rows = query.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(upload.leaseUntil()).isEqualTo(rows.getObject(1, java.time.OffsetDateTime.class).toInstant());
                }
            }
            assertThat(upload.state()).isEqualTo("STAGING");
            assertThatThrownBy(() -> uploads.verify(upload.objectId(), UUID.randomUUID(), 7, "a".repeat(64), null, null))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            assertThatThrownBy(() -> uploads.verify(upload.objectId(), upload.leaseToken(), 8, "a".repeat(64), null, null))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            var verified = uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), "v1", "etag");
            assertThat(verified.state()).isEqualTo("VERIFIED");
            assertThat(uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), "v1", "etag"))
                    .isEqualTo(verified);
            assertThatThrownBy(() -> uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), "v2", "etag"))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            assertThat(uploads.renew(upload.objectId(), upload.leaseToken(), Duration.ofMinutes(2)).leaseUntil())
                    .isAfter(verified.leaseUntil());
            for (String set : new String[] {"expected_size=8", "sha256='" + "b".repeat(64) + "'", "state='STAGING'"}) {
                assertThatThrownBy(() -> tx.inTransaction(em -> {
                    em.createNativeQuery("UPDATE archive_object_uploads SET " + set + " WHERE object_id=:id")
                            .setParameter("id", upload.objectId()).executeUpdate();
                })).hasStackTraceContaining("immutable");
            }
        }
    }

    @Test void expiredLeasesCannotBeRenewedOrVerifiedAndBareBindingsAreNotUploads() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var uploads = new ArchiveUploadLedger(tx);
            var upload = uploads.begin(location(tx), 0, "application/octet-stream", Duration.ofMinutes(1)).upload();
            tx.inTransaction(em -> {
                em.createNativeQuery("UPDATE archive_object_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=:id")
                        .setParameter("id", upload.objectId()).executeUpdate();
            });
            assertThatThrownBy(() -> uploads.renew(upload.objectId(), upload.leaseToken(), Duration.ofMinutes(1)))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            assertThatThrownBy(() -> uploads.verify(upload.objectId(), upload.leaseToken(), 0, "a".repeat(64), null, null))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            var bare = new ArchiveObjectLedger(tx).register(location(tx));
            assertThatThrownBy(() -> uploads.verify(bare.objectId(), UUID.randomUUID(), 0, "a".repeat(64), null, null))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class).hasMessageContaining("missing");
        }
    }

    @Test void failedUploadAdmissionRollsBackThePhysicalReservation() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var location = location(tx);
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION reject_archive_admission() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'injected upload admission failure'; END; $$
                        """).executeUpdate();
                em.createNativeQuery("""
                        CREATE TRIGGER reject_archive_admission BEFORE INSERT ON archive_object_uploads
                        FOR EACH ROW WHEN (NEW.expected_size=777) EXECUTE FUNCTION reject_archive_admission()
                        """).executeUpdate();
            });
            try {
                assertThatThrownBy(() -> new ArchiveUploadLedger(tx).begin(location, 777, "text/plain", Duration.ofMinutes(1)))
                        .hasStackTraceContaining("injected upload admission failure");
                // Reusing the exact coordinates succeeds only if the first reservation rolled back.
                assertThat(new ArchiveObjectLedger(tx).register(location).location()).isEqualTo(location);
            } finally {
                tx.inTransaction(em -> {
                    em.createNativeQuery("DROP TRIGGER reject_archive_admission ON archive_object_uploads").executeUpdate();
                    em.createNativeQuery("DROP FUNCTION reject_archive_admission()").executeUpdate();
                });
            }
        }
    }

    private static LedgerDatabase database() {
        return new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    private static ArchiveObjectLedger.Location location(Tx tx) {
        String generation = "upload-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                "s3", "https://storage.example", "us-east-1", true, generation));
        return new ArchiveObjectLedger.Location(UUID.randomUUID(), "account", "records", generation, "bucket", "unique-key");
    }
}
