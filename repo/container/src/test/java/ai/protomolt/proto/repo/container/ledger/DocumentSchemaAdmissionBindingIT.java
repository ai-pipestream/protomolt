package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
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
            Object[] containerRole = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT container_type_url_sha256,container_descriptor_sha256
                    FROM document_revision_schema_admissions WHERE revision_id=:revision
                    """).setParameter("revision", revision).getSingleResult());
            if (typed) {
                var role = f.batch().proofs().get("member").containerReference();
                assertThat(containerRole[0]).isEqualTo(sha256(role.typeUrl().getBytes(StandardCharsets.UTF_8)));
                assertThat(containerRole[1]).isEqualTo(HexFormat.of().parseHex(role.descriptorSha256()));
                long associations = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                        SELECT count(*) FROM document_revision_schema_assets
                        WHERE revision_id=:revision AND type_url_sha256=:url AND descriptor_sha256=:descriptor
                        """).setParameter("revision", revision).setParameter("url", containerRole[0])
                        .setParameter("descriptor", containerRole[1]).getSingleResult()).longValue());
                assertThat(associations).isEqualTo(1);
            } else {
                assertThat(containerRole).containsExactly(null, null);
            }
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

    @Test void typedAdmissionRequiresContainerRoleAtInsert() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            installRoleMutation(c, "NULL_PAIR");
            try {
                assertThatThrownBy(() -> publish(c, f))
                        .hasStackTraceContaining("Document schema admission requires its exact container role");
            } finally { removeRoleMutation(c); }
            assertNoPublication(c);
        }
    }

    @Test void opaqueAdmissionRejectsAnySuppliedContainerRole() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c, false); activate(c, f);
            installRoleMutation(c, "SUPPLY_PAIR");
            try {
                assertThatThrownBy(() -> publish(c, f))
                        .hasStackTraceContaining("Document schema admission requires its exact container role");
            } finally { removeRoleMutation(c); }
            assertNoPublication(c);
        }
    }

    @Test void oneSidedContainerRolePairIsRejected() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            installRoleMutation(c, "HALF_PAIR");
            try {
                assertThatThrownBy(() -> publish(c, f))
                        .hasStackTraceContaining("Document schema admission requires its exact container role");
            } finally { removeRoleMutation(c); }
            assertNoPublication(c);
        }
    }

    @Test void containerRoleForeignKeyRejectsAnUnretainedAssociation() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            installRoleMutation(c, "UNRETAINED_PAIR");
            try {
                assertThatThrownBy(() -> publish(c, f))
                        .hasStackTraceContaining("document_schema_container_association");
            } finally { removeRoleMutation(c); }
            assertNoPublication(c);
        }
    }

    /** Mutates the production admission insert before V61/V62 guards, keeping every other row real. */
    private static void installRoleMutation(Context c, String mutation) {
        String assignments = switch (mutation) {
            case "NULL_PAIR" -> "NEW.container_type_url_sha256 := NULL; NEW.container_descriptor_sha256 := NULL;";
            case "HALF_PAIR" -> "NEW.container_type_url_sha256 := NULL; NEW.container_descriptor_sha256 := decode(repeat('22',32),'hex');";
            case "SUPPLY_PAIR" -> "NEW.container_type_url_sha256 := decode(repeat('11',32),'hex'); NEW.container_descriptor_sha256 := decode(repeat('22',32),'hex');";
            case "UNRETAINED_PAIR" -> "NEW.container_type_url_sha256 := decode(repeat('33',32),'hex'); NEW.container_descriptor_sha256 := decode(repeat('44',32),'hex');";
            default -> throw new IllegalArgumentException("unknown test mutation");
        };
        c.tx().inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION test_mutate_document_schema_container_role() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + assignments + " RETURN NEW; END; $$").executeUpdate();
            em.createNativeQuery("CREATE TRIGGER a_test_mutate_document_schema_container_role BEFORE INSERT ON document_revision_schema_admissions "
                    + "FOR EACH ROW EXECUTE FUNCTION test_mutate_document_schema_container_role()").executeUpdate();
        });
    }

    private static void removeRoleMutation(Context c) {
        c.tx().inTransaction(em -> {
            em.createNativeQuery("DROP TRIGGER a_test_mutate_document_schema_container_role ON document_revision_schema_admissions").executeUpdate();
            em.createNativeQuery("DROP FUNCTION test_mutate_document_schema_container_role()").executeUpdate();
        });
    }

    private static byte[] sha256(byte[] value) {
        try { return MessageDigest.getInstance("SHA-256").digest(value); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
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
