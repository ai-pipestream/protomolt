package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** A real SQL failure during typed retention rolls back the member and remains safely retryable. */
@Testcontainers
class DocumentTypedPublicationRecoveryIT {
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void retentionFailureRollsBackAdmissionAndDocumentThenSamePreparedOperationRetries() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var policies = new DocumentSchemaPolicies(c.tx());
            var activated = policies.activate(f.batch().policy().policy(), 0, () -> {});
            assertThat(activated.revision()).isEqualTo(1);
            installInjectedEvidenceFailure(c);

            var publisher = new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), false, false);
            assertThatThrownBy(() -> publisher.commit(CALLER, f.owner(), f.prepared(), Map.of(),
                    Map.of("member", f.selected()), f.batch(), () -> {}))
                    .hasStackTraceContaining("injected retention failure");
            assertThat(count(c, "documents")).isZero();
            assertThat(count(c, "document_revision_schema_admissions")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "repository_operation_success")).isZero();
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
            assertThat(count(c, "document_revision_schema_assets")).isZero();
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(f.batch().artifacts().size());
            assertThat(count(c, "repository_schema_artifact_claims")).isEqualTo(f.batch().artifacts().size());
            assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, f.command()).state())
                    .isEqualTo(DocumentPublicationReplay.State.PENDING);

            removeInjectedEvidenceFailure(c);
            var result = publisher.commit(CALLER, f.owner(), f.prepared(), Map.of(), Map.of("member", f.selected()),
                    f.batch(), () -> {});
            assertThat(result.getMembersCount()).isEqualTo(1);
            assertThat(count(c, "documents")).isEqualTo(1);
            assertThat(count(c, "document_revision_schema_admissions")).isEqualTo(1);
            assertThat(count(c, "document_revision_commits")).isEqualTo(1);
            assertThat(count(c, "repository_operation_success")).isEqualTo(1);
            assertThat(count(c, "document_revision_schema_evidence")).isEqualTo(f.batch().proofs().get("member").roots().size());
            assertThat(count(c, "document_revision_schema_assets")).isEqualTo(f.batch().proofs().get("member").references().size());
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(f.batch().artifacts().size());
            assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, f.command()).result()).contains(result);
        }
    }

    private static void installInjectedEvidenceFailure(Context c) {
        c.tx().inTransaction(em -> {
            em.createNativeQuery("""
                    CREATE FUNCTION test_reject_schema_evidence_after_admission() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF NOT EXISTS (
                        SELECT 1 FROM document_revision_commits c
                        JOIN document_revision_publications r USING(revision_id)
                        JOIN document_revision_schema_admissions a USING(revision_id)
                        JOIN documents d ON d.node_id=c.node_id
                        WHERE c.revision_id=NEW.revision_id AND c.creation_xid=pg_current_xact_id()
                          AND r.projection_xid=pg_current_xact_id() AND NOT r.projection_sealed
                          AND a.creation_xid=pg_current_xact_id() AND d.node_id=c.node_id
                      ) THEN
                        RAISE EXCEPTION 'retention injection ran before admission, document, or current revision existed';
                      END IF;
                      RAISE EXCEPTION 'injected retention failure';
                    END;
                    $$
                    """).executeUpdate();
            em.createNativeQuery("""
                    CREATE TRIGGER test_reject_schema_evidence BEFORE INSERT ON document_revision_schema_evidence
                    FOR EACH ROW EXECUTE FUNCTION test_reject_schema_evidence_after_admission()
                    """).executeUpdate();
        });
    }

    private static void removeInjectedEvidenceFailure(Context c) {
        c.tx().inTransaction(em -> {
            em.createNativeQuery("DROP TRIGGER test_reject_schema_evidence ON document_revision_schema_evidence").executeUpdate();
            em.createNativeQuery("DROP FUNCTION test_reject_schema_evidence_after_admission()").executeUpdate();
        });
    }
}
