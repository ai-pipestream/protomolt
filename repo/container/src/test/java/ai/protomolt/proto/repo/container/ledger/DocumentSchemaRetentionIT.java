package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real admission proofs and PostgreSQL retention rows, using synthetic provider observations. */
@Testcontainers
class DocumentSchemaRetentionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void writesTheCompleteProofArtifactAssetAndRootRowsForTheExactRevision() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var proof = f.batch().proofs().get("member");
            assertThat(proof).isNotNull();
            UUID revision = DocumentSchemaRetentionFixture.publish(c, f, (em, id) ->
                    f.retention().write(em, f.owner(), id, () -> {}));

            var expectedArtifacts = new ArrayList<>(f.batch().artifacts().keySet());
            expectedArtifacts.sort(String::compareTo);
            var storedArtifacts = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT encode(artifact_sha256,'hex') FROM document_revision_schema_artifacts
                    WHERE revision_id=:revision ORDER BY artifact_sha256
                    """).setParameter("revision", revision).getResultList());
            assertThat(storedArtifacts).isEqualTo(expectedArtifacts);
            var retainedBytes = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT encode(r.artifact_sha256,'hex'),a.artifact_bytes FROM document_revision_schema_artifacts r
                    JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
                    WHERE r.revision_id=:revision
                    """).setParameter("revision", revision).getResultList());
            assertThat(retainedBytes).hasSize(proof.artifacts().size());
            for (Object raw : retainedBytes) {
                var row = (Object[]) raw;
                assertThat((byte[]) row[1]).containsExactly(proof.artifacts().get(row[0]).toByteArray());
            }

            var assets = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT type_url,encode(descriptor_sha256,'hex'),encode(metadata_sha256,'hex'),metadata_codec,
                           metadata_version,encode(source_sha256,'hex')
                    FROM document_revision_schema_assets WHERE revision_id=:revision ORDER BY type_url
                    """).setParameter("revision", revision).getResultList());
            assertThat(assets).hasSize(proof.references().size());
            assertThat(proof.references()).anySatisfy(reference -> assertThat(reference.sourceSha256()).isPresent());
            assertThat(proof.references()).anySatisfy(reference -> assertThat(reference.sourceSha256()).isEmpty());
            var expectedUrls = proof.references().stream().map(DocumentSchemaAdmission.Reference::typeUrl).sorted().toList();
            assertThat(assets.stream().map(row -> (String) ((Object[]) row)[0]).toList()).isEqualTo(expectedUrls);
            for (Object raw : assets) {
                Object[] row = (Object[]) raw;
                var reference = proof.references().stream().filter(r -> r.typeUrl().equals(row[0])).findFirst().orElseThrow();
                assertThat(row[1]).isEqualTo(reference.descriptorSha256());
                assertThat(row[2]).isEqualTo(reference.metadataSha256());
                assertThat(row[3]).isEqualTo("repository-schema-asset");
                assertThat(((Number) row[4]).intValue()).isEqualTo(1);
                assertThat(row[5]).isEqualTo(reference.sourceSha256().orElse(null));
            }

            var evidence = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT revision_ordinal,encode(root_locator_sha256,'hex'),encode(fragment_sha256,'hex'),fragment_size,
                           evidence_codec,evidence_version,evidence_bytes,encode(evidence_sha256,'hex')
                    FROM document_revision_schema_evidence WHERE revision_id=:revision
                    ORDER BY revision_ordinal,root_locator_sha256
                    """).setParameter("revision", revision).getResultList());
            assertThat(evidence).hasSize(proof.roots().size());
            for (int i = 0; i < evidence.size(); i++) {
                Object[] row = (Object[]) evidence.get(i);
                var root = proof.roots().get(i);
                var part = proof.member().getParts(root.ordinal());
                assertThat(((Number) row[0]).intValue()).isEqualTo(root.ordinal());
                assertThat(row[1]).isEqualTo(root.locatorSha256());
                assertThat(row[2]).isEqualTo(part.getUpload().getSha256());
                assertThat(((Number) row[3]).longValue()).isEqualTo(part.getUpload().getSizeBytes());
                assertThat(row[4]).isEqualTo(root.encoded().codec());
                assertThat(((Number) row[5]).intValue()).isEqualTo(root.encoded().version());
                assertThat((byte[]) row[6]).containsExactly(root.encoded().bytes().toByteArray());
                assertThat(row[7]).isEqualTo(root.encoded().sha256());
            }
        }
    }

    @Test void aWrongRevisionFailsBeforeAnyEvidenceIsRetained() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publish(c, f, (em, revision) ->
                    f.retention().write(em, f.owner(), UUID.randomUUID(), () -> {})))
                    .hasStackTraceContaining("Schema retention requires the current unsealed member revision");
            assertNoRevisionRows(c, f.command().operationId().toString());
        }
    }

    @Test void missingPhysicalFragmentRejectsTheProofBeforeWritingRetentionRows() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publish(c, f, true, (em, revision) ->
                    f.retention().write(em, f.owner(), revision, () -> {})))
                    .hasStackTraceContaining("Schema proof fragment set differs from revision parts");
            assertNoRevisionRows(c, f.command().operationId().toString());
        }
    }

    @Test void caughtRetentionFailureStillPreventsTheOuterTransactionFromCommitting() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var caught = new AtomicBoolean();
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publish(c, f, (em, revision) -> {
                f.retention().write(em, f.owner(), revision, () -> {});
                try {
                    f.retention().write(em, f.owner(), UUID.randomUUID(), () -> {});
                    fail("Expected a wrong-revision failure");
                } catch (IllegalArgumentException expected) {
                    assertThat(expected).hasMessageContaining("current unsealed member revision");
                    assertThat(em.getTransaction().getRollbackOnly()).isTrue();
                    caught.set(true);
                }
            })).isInstanceOf(RuntimeException.class);
            assertThat(caught).isTrue();
            assertNoRevisionRows(c, f.command().operationId().toString());
        }
    }

    @Test void cancellationAfterTheArtifactBatchRollsBackPublicationAndRetentionRows() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var sawInsertedArtifacts = new AtomicBoolean();
            var cancelled = new CancellationException("cancel after artifact batch");
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publish(c, f, (em, revision) -> {
                f.retention().write(em, f.owner(), revision, () -> {
                    long inserted = ((Number) em.createNativeQuery("""
                            SELECT count(*) FROM document_revision_schema_artifacts WHERE revision_id=:revision
                            """).setParameter("revision", revision).getSingleResult()).longValue();
                    if (inserted > 0) {
                        sawInsertedArtifacts.set(true);
                        throw cancelled;
                    }
                });
            })).isSameAs(cancelled);
            assertThat(sawInsertedArtifacts).isTrue();
            assertNoRevisionRows(c, f.command().operationId().toString());
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(f.batch().artifacts().size());
            assertThat(count(c, "repository_schema_artifact_claims")).isEqualTo(f.batch().artifacts().size());
        }
    }

    @Test void aCommittedRevisionRejectsLateRetentionWrites() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            UUID revision = DocumentSchemaRetentionFixture.publish(c, f, (em, id) ->
                    f.retention().write(em, f.owner(), id, () -> {}));
            long artifacts = count(c, "document_revision_schema_artifacts");
            long assets = count(c, "document_revision_schema_assets");
            long evidence = count(c, "document_revision_schema_evidence");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                f.retention().write(em, f.owner(), revision, () -> {});
            }))
                    .isInstanceOf(RuntimeException.class).hasStackTraceContaining("terminal");
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(artifacts);
            assertThat(count(c, "document_revision_schema_assets")).isEqualTo(assets);
            assertThat(count(c, "document_revision_schema_evidence")).isEqualTo(evidence);
        }
    }

    private static void assertNoRevisionRows(Context c, String operationId) {
        assertThat(countWhere(c, "document_revision_commits", operationId)).isZero();
        assertThat(countWhere(c, "repository_operation_success", operationId)).isZero();
        assertThat(count(c, "documents")).isZero();
        for (String table : List.of("document_revision_schema_artifacts", "document_revision_schema_assets",
                "document_revision_schema_evidence")) {
            long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table)
                    .getSingleResult()).longValue());
            assertThat(count).as(table).isZero();
        }
    }

    private static long countWhere(Context c, String table, String operationId) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM " + table + " WHERE operation_id=:operation")
                .setParameter("operation", UUID.fromString(operationId)).getSingleResult()).longValue());
    }
}
