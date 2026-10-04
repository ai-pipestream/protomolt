package ai.protomolt.proto.repo.container.ledger;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** V62 preserves committed V61 admissions without manufacturing container provenance. */
@Testcontainers
class DocumentSchemaContainerRoleMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void committedV61TypedAndOpaqueAdmissionsUpgradeWithUnknownContainerRole(boolean typed) throws Exception {
        try (var c = context(POSTGRES, "61")) {
            var f = DocumentSchemaRetentionFixture.prepare(c, typed);
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishV61(c, f);
            Object[] before = admissionSnapshot(c, revision);
            assertThat(before[0]).isEqualTo(typed ? "TYPED" : "OPAQUE");
            assertThat(before[2]).isEqualTo(typed ? 1L : 0L); // retained root evidence rows
            assertThat(before[3]).isEqualTo(typed ? f.batch().proofs().get("member").references().size() : 0L);

            String schema = c.tx().readOnly(em -> (String) em.createNativeQuery("SELECT current_schema()").getSingleResult());
            Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("62").load().migrate();

            Object[] after = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT a.decision,a.manifest::text,a.container_type_url_sha256,a.container_descriptor_sha256,
                     (SELECT count(*) FROM document_revision_schema_evidence e WHERE e.revision_id=a.revision_id),
                     (SELECT count(*) FROM document_revision_schema_assets s WHERE s.revision_id=a.revision_id),
                     (SELECT string_agg(encode(e.evidence_bytes,'hex'),'' ORDER BY e.revision_ordinal,e.root_locator_sha256)
                      FROM document_revision_schema_evidence e WHERE e.revision_id=a.revision_id),
                     document_schema_retention_manifest_v1(a.revision_id)::text
                    FROM document_revision_schema_admissions a WHERE a.revision_id=:revision
                    """).setParameter("revision", revision).getSingleResult());
            assertThat(after[0]).isEqualTo(typed ? "TYPED" : "OPAQUE");
            assertThat(after[1]).isEqualTo(before[1]);
            assertThat(after[2]).isNull();
            assertThat(after[3]).isNull();
            assertThat(((Number) after[4]).longValue()).isEqualTo(((Number) before[2]).longValue());
            assertThat(((Number) after[5]).longValue()).isEqualTo(((Number) before[3]).longValue());
            assertThat(after[4]).isEqualTo(typed ? 1L : 0L);
            assertThat(after[5]).isEqualTo(typed ? f.batch().proofs().get("member").references().size() : 0L);
            assertThat(after[6]).isEqualTo(before[4]);
            assertThat(after[7]).isEqualTo(before[5]);
            assertThat(count(c, "repository_operation_success")).isEqualTo(1);
        }
    }

    private static Object[] admissionSnapshot(Context c, java.util.UUID revision) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT decision,manifest::text,
                 (SELECT count(*) FROM document_revision_schema_evidence e WHERE e.revision_id=a.revision_id),
                 (SELECT count(*) FROM document_revision_schema_assets s WHERE s.revision_id=a.revision_id),
                 (SELECT string_agg(encode(e.evidence_bytes,'hex'),'' ORDER BY e.revision_ordinal,e.root_locator_sha256)
                  FROM document_revision_schema_evidence e WHERE e.revision_id=a.revision_id),
                 document_schema_retention_manifest_v1(a.revision_id)::text
                FROM document_revision_schema_admissions a WHERE revision_id=:revision
                """).setParameter("revision", revision).getSingleResult());
    }
}
