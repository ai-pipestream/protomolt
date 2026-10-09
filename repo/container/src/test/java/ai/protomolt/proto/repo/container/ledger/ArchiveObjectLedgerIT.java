package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchiveObjectLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void concurrentRegistrationsCannotAssignOnePhysicalObjectToTwoEntries() throws Exception {
        try (var database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            var objects = new ArchiveObjectLedger(tx);
            String generation = "race-" + UUID.randomUUID();
            new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                    ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of("https://storage.example", "us-east-1", true), generation));
            var start = new java.util.concurrent.CountDownLatch(1);
            var futures = new java.util.ArrayList<java.util.concurrent.Future<ArchiveObjectLedger.Binding>>();
            for (int i = 0; i < 2; i++) {
                var location = new ArchiveObjectLedger.Location(UUID.randomUUID(), "account", "records",
                        generation, "bucket", "same-physical-key");
                futures.add(executor.submit(() -> {
                    assertThat(start.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    return objects.register(location);
                }));
            }
            start.countDown();
            int registered = 0;
            int conflicted = 0;
            for (var future : futures) {
                try {
                    var binding = future.get(10, java.util.concurrent.TimeUnit.SECONDS);
                    assertThat(objects.find(binding.objectId())).contains(binding);
                    registered++;
                } catch (java.util.concurrent.ExecutionException failure) {
                    assertThat(failure.getCause()).hasStackTraceContaining("uq_archive_physical_object");
                    conflicted++;
                }
            }
            assertThat(registered).isEqualTo(1);
            assertThat(conflicted).isEqualTo(1);
        }
    }

    @Test void storageCoordinatesRemainBoundBeforeCreationAndAfterEntryDeletion() {
        try (var database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()))) {
            var tx = new Tx(database.entityManagerFactory());
            var profiles = new ManagedBackendLedger(tx);
            String generation = "archive-" + UUID.randomUUID();
            profiles.bind(generation, new ManagedBackendLedger.Profile(ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of("https://storage.example", "us-east-1", true), "archive"));
            var objects = new ArchiveObjectLedger(tx);
            UUID entry = UUID.randomUUID();
            var location = new ArchiveObjectLedger.Location(entry, "account", "records", generation, "original-bucket", "unique-key");
            var binding = objects.register(location);
            assertThat(objects.find(binding.objectId())).contains(binding);
            assertThatThrownBy(() -> objects.register(location))
                    .hasStackTraceContaining("uq_archive_physical_object");
            String rotated = "archive-" + UUID.randomUUID();
            profiles.bind(rotated, profiles.find(generation).orElseThrow());
            assertThatThrownBy(() -> objects.register(new ArchiveObjectLedger.Location(entry,
                    "account", "records", rotated, "original-bucket", "unique-key")))
                    .hasStackTraceContaining("uq_archive_physical_object");
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version)
                        VALUES (:id,'account','records','later-entry',1)
                        """).setParameter("id", entry).executeUpdate();
                em.createNativeQuery("DELETE FROM archive_entries WHERE entry_uuid=:id")
                        .setParameter("id", entry).executeUpdate();
            });
            assertThat(objects.find(binding.objectId())).contains(binding);
            for (String sql : new String[] {
                    "UPDATE archive_object_bindings SET bucket='redirected' WHERE object_id=:id",
                    "DELETE FROM archive_object_bindings WHERE object_id=:id"}) {
                assertThatThrownBy(() -> tx.inTransaction(em -> {
                    em.createNativeQuery(sql).setParameter("id", binding.objectId()).executeUpdate();
                })).hasStackTraceContaining("Archive object storage bindings are immutable");
            }
            assertThat(objects.find(binding.objectId())).contains(binding);
            assertThatThrownBy(() -> objects.register(new ArchiveObjectLedger.Location(entry,
                    "account", "records", "unregistered", "original-bucket", "other-key")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Archive backend generation is not registered");
        }
    }
}
