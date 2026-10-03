package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.container.archive.*;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchiveCleanupLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void claimsFenceUploadAndCompletionButKeepTombstonesForLateWrites() {
        try (var database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()))) {
            var tx = new Tx(database.entityManagerFactory());
            var uploads = new ArchiveUploadLedger(tx);
            var cleanup = new ArchiveCleanupLedger(tx);
            String generation = "cleanup-" + UUID.randomUUID();
            new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                    S3BackendIdentity.of("https://storage.example", "us-east-1", true), generation));
            var admission = uploads.begin(new ArchiveObjectLedger.Location(UUID.randomUUID(), "account", "archive", generation,
                    "bucket", "key"), 7, "text/plain", Duration.ofMinutes(1));
            UUID id = admission.upload().objectId();
            Instant cutoff = Instant.now().plusSeconds(60);
            assertThat(cleanup.claim(id, cutoff)).isEmpty();
            assertThat(cleanup.candidates(cutoff, 1000)).doesNotContain(id);
            tx.inTransaction(em -> {
                em.createNativeQuery("UPDATE archive_object_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=:id")
                        .setParameter("id", id).executeUpdate();
            });
            assertThat(cleanup.candidates(cutoff, 1000)).contains(id);
            var first = cleanup.claim(id, cutoff).orElseThrow();
            assertThat(first.binding()).isEqualTo(admission.binding());
            assertThatThrownBy(() -> uploads.verify(id, admission.upload().leaseToken(), 7, "a".repeat(64), null, null))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            assertThat(cleanup.claim(id, Instant.EPOCH)).isEmpty();
            assertThat(cleanup.candidates(cutoff, 1000)).contains(id);
            var second = cleanup.claim(id, cutoff).orElseThrow();
            assertThat(cleanup.succeeded(id, first.token())).isFalse();
            assertThat(cleanup.failed(id, second.token(), "PROVIDER_UNAVAILABLE")).isTrue();
            var third = cleanup.claim(id, cutoff).orElseThrow();
            assertThat(cleanup.succeeded(id, third.token())).isTrue();
            assertThat(cleanup.candidates(cutoff, 1000)).contains(id);
            var reconciliation = cleanup.claim(id, cutoff).orElseThrow();
            assertThat(reconciliation.token()).isNotEqualTo(third.token());
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery("INSERT INTO archive_version_object_refs(entry_uuid,version,object_id) VALUES (:entry,1,:id)")
                        .setParameter("entry", admission.binding().location().entryUuid()).setParameter("id", id).executeUpdate();
            })).hasStackTraceContaining("requires a live object");
            assertThat(cleanup.succeeded(id, reconciliation.token())).isTrue();
        }
    }
}
