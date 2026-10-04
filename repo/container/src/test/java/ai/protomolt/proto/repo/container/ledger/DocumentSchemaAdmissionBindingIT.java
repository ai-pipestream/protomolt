package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** SQL admission uses real runtime proofs; physical observations remain synthetic fixtures. */
@Testcontainers
class DocumentSchemaAdmissionBindingIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void commitsExactTypedOrExplicitOpaqueDecisionUnderActivePolicy(boolean typed) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c, typed);
            activate(c, f);
            UUID revision = publish(c, f);
            Object[] stored = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT a.decision,c.admission_mode,a.policy_revision,
                           a.body=r.body,a.metadata=c.metadata_snapshot,
                           a.manifest=document_schema_retention_manifest_v1(a.revision_id),r.projection_sealed
                    FROM document_revision_schema_admissions a JOIN document_revision_commits c USING(revision_id)
                    JOIN document_revision_publications r USING(revision_id) WHERE a.revision_id=:revision
                    """).setParameter("revision", revision).getSingleResult());
            String decision = typed ? "TYPED" : "OPAQUE";
            assertThat(stored[0]).isEqualTo(decision); assertThat(stored[1]).isEqualTo(decision);
            assertThat(((Number) stored[2]).longValue()).isEqualTo(1);
            for (int i = 3; i < stored.length; i++) assertThat(stored[i]).isEqualTo(true);
            assertThat(count(c, "repository_operation_success")).isEqualTo(1);
            // An activation does not rewrite the historical selected revision/hash.
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 1, () -> {});
            long retained = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT policy_revision FROM document_revision_schema_admissions WHERE revision_id=:id")
                    .setParameter("id", revision).getSingleResult()).longValue());
            assertThat(retained).isEqualTo(1);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE documents SET filename='later metadata' WHERE node_id=(SELECT node_id FROM document_revision_commits WHERE revision_id=:id)")
                        .setParameter("id", revision).executeUpdate();
            });
            boolean historical = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                    SELECT c.metadata_snapshot=a.metadata AND c.metadata_snapshot->>'filename'<>'later metadata'
                    FROM document_revision_commits c JOIN document_revision_schema_admissions a USING(revision_id) WHERE revision_id=:id
                    """).setParameter("id", revision).getSingleResult());
            assertThat(historical).isTrue();

        }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void changedBodyOrMetadataAfterBindingCannotPublish(boolean body) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {
                if (body) candidate.row().checksum = "0".repeat(64);
                else candidate.row().filename = "not the checked snapshot";
            }, (em, revision, manifest) -> f.retention().write(em, f.owner(), revision, () -> {})))
                    .hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertNoPublication(c);
        }
    }

    @Test void missingRetainedEvidenceRejectsSealingAndRollsBackBinding() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                    (em, revision, manifest) -> {})).hasStackTraceContaining("admission retained sets differ");
            assertNoPublication(c);
        }
    }

    @Test void unusedAdmissionCannotSatisfyDeferredCompletion() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {
                em.createNativeQuery("SET CONSTRAINTS document_schema_admission_terminal IMMEDIATE").executeUpdate();
            }, (em, revision, manifest) -> {})).hasStackTraceContaining("admission requires its native revision");
            assertNoPublication(c);
        }
    }

    @Test void changedPolicyRevisionRejectsPreviouslyPreparedProofEvenWithSamePolicyBytes() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 1, () -> {});
            assertThatThrownBy(() -> publish(c, f)).isInstanceOf(DocumentSchemaPolicies.StalePolicy.class);
            assertNoPublication(c);
        }
    }

    @Test void policyChangedWithinPublicationTransactionCannotUseEarlierBinding() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {
                em.createNativeQuery("UPDATE document_schema_policy_current SET policy_revision=policy_revision+1 WHERE account_id='account'")
                        .executeUpdate();
            }, (em, revision, manifest) -> f.retention().write(em, f.owner(), revision, () -> {})))
                    .hasStackTraceContaining("admission policy changed after binding");
            assertNoPublication(c);
        }
    }

    @Test void oneAdmissionCannotBeReusedForAnotherMutationInItsTransaction() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                    (em, revision, manifest) -> {
                        f.retention().write(em, f.owner(), revision, () -> {});
                        em.createNativeQuery("UPDATE documents SET filename=filename").executeUpdate();
                    })).hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertNoPublication(c);
        }
    }

    @Test void admissionRowsCannotBeUpdatedOrDeletedAfterPublication() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f); UUID revision = publish(c, f);
            for (String sql : new String[]{"UPDATE document_revision_schema_admissions SET policy_revision=policy_revision+1 WHERE revision_id=:id",
                    "DELETE FROM document_revision_schema_admissions WHERE revision_id=:id"}) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    em.createNativeQuery(sql).setParameter("id", revision).executeUpdate();
                })).hasStackTraceContaining("Document schema admission is immutable");
            }
            assertThat(count(c, "document_revision_schema_admissions")).isEqualTo(1);
        }
    }

    private static void activate(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
    }
    private static UUID publish(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        return DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {}, (em, revision, manifest) -> {
            if (f.retention() != null) f.retention().write(em, f.owner(), revision, () -> {});
        });
    }
    private static void assertNoPublication(Context c) {
        assertThat(count(c, "documents")).isZero();
        assertThat(count(c, "document_revision_schema_admissions")).isZero();
        assertThat(count(c, "document_revision_commits")).isZero();
        assertThat(count(c, "repository_operation_success")).isZero();
    }
}
