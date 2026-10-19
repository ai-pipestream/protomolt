package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveVersionRecord;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchiveMetadataRevisionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void staleMetadataCannotOverwriteANewerEntryEvenWhenVersionNumberIsUnchanged() {
        try (var database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()))) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            UUID id = UUID.randomUUID();
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version,title)
                        VALUES (:id,'account','records','entry',1,'Initial')
                        """).setParameter("id", id).executeUpdate();
            });
            var first = ledger.findEntry(id).orElseThrow();
            var stale = ledger.findEntry(id).orElseThrow();
            long before = first.mutationRevision;
            first.title = "Newer metadata";
            ledger.mergeEntry(first);
            stale.title = "Stale metadata";
            assertThatThrownBy(() -> ledger.mergeEntry(stale)).isInstanceOf(ArchiveLedger.VersionConflictException.class);
            stale.currentVersion = 2;
            var next = new ArchiveVersionRecord();
            next.entryUuid = id;
            next.version = 2;
            next.manifest = "{}";
            next.rootChecksum = "unused";
            next.createdAt = Instant.now();
            assertThatThrownBy(() -> ledger.commitSave(stale, 1, next, 0, ArchiveLedger.StatsDelta.none()))
                    .isInstanceOf(ArchiveLedger.VersionConflictException.class);
            assertThat(ledger.findVersion(id, 2)).isEmpty();
            var retained = ledger.findEntry(id).orElseThrow();
            assertThat(retained.title).isEqualTo("Newer metadata");
            assertThat(retained.currentVersion).isEqualTo(1);
            assertThat(retained.mutationRevision).isGreaterThan(before);
        }
    }
}
