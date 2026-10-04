package ai.protomolt.proto.repo.container.ledger;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentAdmissionSnapshotIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "America/New_York", "Asia/Kathmandu"})
    void prewriteSnapshotMatchesPersistedBodyAndMetadataAcrossTimezones(String timezone) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var frozen = new AtomicReference<DocumentAdmissionSnapshot>();
            var preparedRow = new AtomicReference<DocumentRecord>();
            UUID revision = DocumentSchemaRetentionFixture.publishWithSnapshot(c, f, (em, candidate) -> {
                em.createNativeQuery("SELECT set_config('TimeZone',:zone,true)").setParameter("zone", timezone).getSingleResult();
                candidate.row().createdAt = Instant.parse("2026-10-04T12:34:56.123456789Z");
                candidate.row().updatedAt = Instant.parse("2026-10-04T13:34:56.999999789Z");
                assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM documents").getSingleResult()).intValue()).isZero();
                preparedRow.set(candidate.row());
                frozen.set(DocumentAdmissionSnapshot.prepare(em, candidate.row()));
                assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM documents").getSingleResult()).intValue()).isZero();
            });
            boolean matches = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                    SELECT p.body=CAST(:body AS jsonb) AND c.metadata_snapshot=CAST(:metadata AS jsonb)
                    FROM document_revision_publications p JOIN document_revision_commits c USING(revision_id)
                    WHERE revision_id=:revision
                    """).setParameter("body", frozen.get().body()).setParameter("metadata", frozen.get().metadata())
                    .setParameter("revision", revision).getSingleResult());
            assertThat(matches).as("prewrite snapshot equals persisted snapshot in %s", timezone).isTrue();
            // Change metadata on the detached candidate, then snapshot before merge.
            // Existing immutable revision metadata must retain its earlier value.
            var updated = preparedRow.get();
            updated.connectorId = "connector"; updated.crawlId = "crawl";
            updated.filename = "updated title"; updated.deleteSourceBlobsOnSettle = true;
            updated.sourceBlobDeleteReason = "source retained elsewhere";
            updated.updatedAt = Instant.parse("2026-10-04T14:34:56.123456789Z");
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT set_config('TimeZone',:zone,true)").setParameter("zone", timezone).getSingleResult();
                var expected = DocumentAdmissionSnapshot.prepare(em, updated);
                em.merge(updated); em.flush();
                Object[] actual = (Object[]) em.createNativeQuery("""
                        SELECT document_publication_body(d)=CAST(:body AS jsonb),
                               document_revision_metadata_v1(d)=CAST(:metadata AS jsonb),
                               c.metadata_snapshot=CAST(:original AS jsonb)
                        FROM documents d JOIN document_revision_commits c USING(node_id) WHERE c.revision_id=:revision
                        """).setParameter("body", expected.body()).setParameter("metadata", expected.metadata())
                        .setParameter("original", frozen.get().metadata()).setParameter("revision", revision).getSingleResult();
                assertThat(actual).containsExactly(true, true, true);
                var managed = em.find(DocumentRecord.class, updated.nodeId);
                managed.filename = "pending managed change";
                var pending = DocumentAdmissionSnapshot.prepare(em, managed);
                String storedFilename = (String) em.createNativeQuery("SELECT filename FROM documents WHERE node_id=:node")
                        .setFlushMode(jakarta.persistence.FlushModeType.COMMIT).setParameter("node", updated.nodeId).getSingleResult();
                assertThat(storedFilename).as("snapshot query must not auto-flush dirty entities").isEqualTo("updated title");
                em.flush();
                boolean pendingMatches = (Boolean) em.createNativeQuery("""
                        SELECT document_revision_metadata_v1(d)=CAST(:metadata AS jsonb) FROM documents d WHERE node_id=:node
                        """).setParameter("metadata", pending.metadata()).setParameter("node", updated.nodeId).getSingleResult();
                assertThat(pendingMatches).isTrue();

            });

        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectsOversizedSnapshotBeforeReturningItsText(boolean body) throws Exception {
        try (var c = context(POSTGRES)) {
            var row = new DocumentRecord();
            row.createdAt = Instant.parse("2026-10-04T12:00:00Z"); row.updatedAt = row.createdAt;
            // Synthetic JSON stresses the storage bound; it makes no validation claim.
            String padding = "{\"padding\":\"" + "x".repeat(body ? 16_777_216 : 1_048_576) + "\"}";
            if (body) row.partManifest = padding; else row.security = padding;
            assertThatThrownBy(() -> c.tx().readOnly(em -> DocumentAdmissionSnapshot.prepare(em, row)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("snapshot exceeds");
            assertThat(count(c, "documents")).isZero();
        }
    }

    @Test void preservesBigintPrecisionAndExplicitNullsWithoutWritingARow() throws Exception {
        try (var c = context(POSTGRES)) {
            var row = new DocumentRecord();
            row.sizeBytes = 9_007_199_254_740_993L;
            row.createdAt = Instant.parse("2026-10-04T12:00:00Z"); row.updatedAt = row.createdAt;
            var snapshot = c.tx().readOnly(em -> DocumentAdmissionSnapshot.prepare(em, row));
            Object[] result = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT CAST(:body AS jsonb)->>'size_bytes',
                           CAST(:metadata AS jsonb)->'connector_id'='null'::jsonb,
                           (SELECT count(*) FROM documents)
                    """).setParameter("body", snapshot.body()).setParameter("metadata", snapshot.metadata()).getSingleResult());
            assertThat(result[0]).isEqualTo("9007199254740993");
            assertThat(result[1]).isEqualTo(true);
            assertThat(((Number) result[2]).longValue()).isZero();
        }
    }
}
