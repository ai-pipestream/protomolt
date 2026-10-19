package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveVersionRecord;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchiveRetainedRevisionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void movingAVersionInvalidatesBothOwnersAndRollbackPreservesTheirRevisions() {
        try (var database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()))) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            UUID source = UUID.randomUUID();
            UUID destination = UUID.randomUUID();
            tx.inTransaction(em -> {
                for (UUID id : new UUID[] {source, destination}) {
                    em.createNativeQuery("""
                            INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version)
                            VALUES (:id,'account','records',:name,1)
                            """).setParameter("id", id).setParameter("name", id.toString()).executeUpdate();
                }
                em.createNativeQuery("""
                        INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes)
                        VALUES (:id,1,'{}','original',0)
                        """).setParameter("id", source).executeUpdate();
            });
            long sourceBefore = ledger.findEntry(source).orElseThrow().mutationRevision;
            long destinationBefore = ledger.findEntry(destination).orElseThrow().mutationRevision;
            var rollback = new IllegalStateException("abort after trigger");
            assertThatThrownBy(() -> tx.inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
                em.createNativeQuery("UPDATE archive_versions SET entry_uuid=:destination WHERE entry_uuid=:source")
                        .setParameter("destination", destination).setParameter("source", source).executeUpdate();
                throw rollback;
            })).isSameAs(rollback);
            assertThat(ledger.findEntry(source).orElseThrow().mutationRevision).isEqualTo(sourceBefore);
            assertThat(ledger.findEntry(destination).orElseThrow().mutationRevision).isEqualTo(destinationBefore);
            assertThat(ledger.findVersion(source, 1)).isPresent();
            tx.inTransaction(em -> {
                em.createNativeQuery("UPDATE archive_versions SET entry_uuid=:destination WHERE entry_uuid=:source")
                        .setParameter("destination", destination).setParameter("source", source).executeUpdate();
            });
            assertThat(ledger.findEntry(source).orElseThrow().mutationRevision).isGreaterThan(sourceBefore);
            assertThat(ledger.findEntry(destination).orElseThrow().mutationRevision).isGreaterThan(destinationBefore);
            assertThat(ledger.findVersion(source, 1)).isEmpty();
            assertThat(ledger.findVersion(destination, 1)).isPresent();
            tx.inTransaction(em -> {
                em.createNativeQuery("DELETE FROM archive_entries WHERE entry_uuid=:id")
                        .setParameter("id", destination).executeUpdate();
            });
            assertThat(ledger.findEntry(destination)).isEmpty();
            assertThat(ledger.findVersion(destination, 1)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"rewrite", "prune", "insert"})
    void retainedSetChangesRejectPreviouslyPreparedSavesAndMetadata(String change) {
        try (var database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()))) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            UUID id = UUID.randomUUID();
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version)
                        VALUES (:id,'account','records',:name,1)
                        """).setParameter("id", id).setParameter("name", id.toString()).executeUpdate();
                em.createNativeQuery("""
                        INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes)
                        VALUES (:id,1,'{}','original',0)
                        """).setParameter("id", id).executeUpdate();
            });
            var stale = ledger.findEntry(id).orElseThrow();
            tx.inTransaction(em -> {
                String sql = switch (change) {
                    case "rewrite" -> "UPDATE archive_versions SET root_checksum='rewritten' WHERE entry_uuid=:id";
                    case "prune" -> "DELETE FROM archive_versions WHERE entry_uuid=:id";
                    default -> """
                            INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes)
                            VALUES (:id,2,'{}','additional',0)
                            """;
                };
                em.createNativeQuery(sql).setParameter("id", id).executeUpdate();
            });
            stale.title = "Derived from obsolete manifest";
            assertThatThrownBy(() -> ledger.mergeEntry(stale))
                    .isInstanceOf(ArchiveLedger.VersionConflictException.class);
            stale.currentVersion = 3;
            var candidate = new ArchiveVersionRecord();
            candidate.entryUuid = id;
            candidate.version = 3;
            candidate.manifest = "{}";
            candidate.rootChecksum = "stale-derived";
            candidate.createdAt = Instant.now();
            assertThatThrownBy(() -> ledger.commitSave(stale, 1, candidate, 0, ArchiveLedger.StatsDelta.none()))
                    .isInstanceOf(ArchiveLedger.VersionConflictException.class);
            assertThat(ledger.findVersion(id, 3)).isEmpty();
            var current = ledger.findEntry(id).orElseThrow();
            assertThat(current.currentVersion).isEqualTo(1);
            assertThat(current.title).isNull();
            assertThat(current.mutationRevision).isGreaterThan(stale.mutationRevision);
        }
    }
}
