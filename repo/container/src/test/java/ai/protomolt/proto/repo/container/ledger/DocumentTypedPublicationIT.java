package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Exercises the shared native committer, with real proofs and synthetic physical observations. */
@Testcontainers
class DocumentTypedPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @ParameterizedTest @CsvSource({"true,false", "false,false", "true,true"})
    void nativeCommitPublishesAndReplaysCheckedTypedOrExplicitOpaqueMember(boolean typed, boolean explicitSchema) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c, typed, explicitSchema);
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
            Map<String,DocumentCommandContent> checked = typed ? Map.of() : Map.of("member", f.content());
            var result = new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), typed, false).commit(
                    CALLER, f.owner(), f.prepared(), checked, Map.of("member", f.selected()), f.batch(), () -> {});
            assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, f.command()).result()).contains(result);
            Object[] persisted = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT c.admission_mode,c.structured_resolution,a.manifest=document_schema_retention_manifest_v1(c.revision_id),
                           (SELECT count(*) FROM document_revision_schema_evidence e WHERE e.revision_id=c.revision_id)
                    FROM document_revision_commits c JOIN document_revision_schema_admissions a USING(revision_id)
                    WHERE c.operation_id=:operation
                    """).setParameter("operation", f.command().operationId()).getSingleResult());
            assertThat(persisted[0]).isEqualTo(typed ? "TYPED" : "OPAQUE");
            assertThat(persisted[2]).isEqualTo(true);
            if (typed) {
                assertThat(persisted[1]).as("typed occurrence evidence replaces the opaque observation").isNull();
                assertThat(((Number) persisted[3]).longValue()).isEqualTo(f.batch().proofs().get("member").roots().size());
            } else {
                assertThat(persisted[1]).isNotNull();
                assertThat(((Number) persisted[3]).longValue()).isZero();
            }
        }
    }

    @Test void stricterHostRequirementCannotDowngradeAnOpaquePolicyDecision() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c, false);
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
            assertThatThrownBy(() -> new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), true, false).commit(
                    CALLER, f.owner(), f.prepared(), Map.of("member", f.content()), Map.of("member", f.selected()), f.batch(), () -> {}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Required typed publication proof is absent");
            assertThat(count(c, "documents")).isZero();
            assertThat(count(c, "document_revision_schema_admissions")).isZero();
            assertThat(count(c, "repository_operation_success")).isZero();
        }
    }

    @Test void anOpaqueMemberStillRequiresItsCheckedContent() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c, false);
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
            assertThatThrownBy(() -> new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), false, false).commit(
                    CALLER, f.owner(), f.prepared(), Map.of(), Map.of("member", f.selected()), f.batch(), () -> {}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("complete command member set");
            assertThat(count(c, "documents")).isZero();
            assertThat(count(c, "document_revision_schema_admissions")).isZero();
        }
    }
}
