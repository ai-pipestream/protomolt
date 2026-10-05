package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Normalized byte ownership on PostgreSQL; fixture bytes are not validated schemas or provider evidence. */
@Testcontainers
@Timeout(60)
class DocumentAssessmentArtifactsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentAssessmentRetentionFixture fixture;
    private static RepositorySchemaArtifacts artifacts;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); fixture = new DocumentAssessmentRetentionFixture(tx);
        artifacts = new RepositorySchemaArtifacts(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void migrationPreservesAnExistingPhysicalOnlyAssessmentAndItsRelease() throws Exception {
        try (var context = DocumentNativePublicationFixture.context(POSTGRES, "66")) {
            var prior = new DocumentAssessmentRetentionFixture(context.tx());
            var c = prior.candidate(120);
            context.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
                // V66 has no expected_artifacts column. Preserve the actual old
                // insertion shape instead of simulating a legacy row at V67.
                em.createNativeQuery("""
                        INSERT INTO document_assessment_owners(assessment_id,account_id,principal,operation_id,owner_generation,
                            command_codec,command_version,command_sha256,manifest_bytes,manifest_sha256,expected_slots,retain_until,creation_xid)
                        SELECT :id,account_id,principal,operation_id,1,command_codec,command_version,command_sha256,
                            decode('01','hex'),sha256(decode('01','hex')),2,clock_timestamp()+interval '1 second','0'::xid8
                        FROM repository_operations WHERE account_id='account' AND principal='principal' AND operation_id=:op
                        """).setParameter("id", c.assessment()).setParameter("op", c.owner().key().operationId()).executeUpdate();
                prior.insertSlots(em, c, "revision_ordinal", 1); prior.seal(em, c);
            });
            String before = context.tx().readOnly(em -> (String) em.createNativeQuery(
                    "SELECT to_jsonb(o)::text FROM document_assessment_owners o WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult());
            String schema = context.pool().getSchema();
            org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
            String after = context.tx().readOnly(em -> (String) em.createNativeQuery(
                    "SELECT (to_jsonb(o)-'expected_artifacts')::text FROM document_assessment_owners o WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult());
            assertThat(after).isEqualTo(before);
            Object[] counts = context.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT expected_artifacts,(SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id),
                        (SELECT count(*) FROM document_assessment_artifacts WHERE assessment_id=:id)
                    FROM document_assessment_owners WHERE assessment_id=:id
                    """).setParameter("id", c.assessment()).getSingleResult());
            assertThat(((Number) counts[0]).intValue()).isZero();
            assertThat(((Number) counts[1]).intValue()).isEqualTo(2);
            assertThat(((Number) counts[2]).intValue()).isZero();
            prior.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
            assertThat(prior.release(c)).isTrue();
        }
    }

    @Test void sharedArtifactIsStoredOnceAndReleasingOneAssessmentPreservesTheOther() {
        var first = fixture.candidate(120); var second = fixture.candidate(120);
        var bytes = ByteString.copyFromUtf8("shared normalized bytes " + UUID.randomUUID());
        String sha = artifacts.stage(first.owner(), List.of(bytes), () -> {}).getFirst();
        assertThat(artifacts.stage(second.owner(), List.of(bytes), () -> {})).containsExactly(sha);
        stage(first, sha, 1, 1); stage(second, sha, 120, 1);
        assertThat(catalogCount(sha)).isEqualTo(1);
        assertThat(refs(first.assessment())).isEqualTo(1);
        assertThat(refs(second.assessment())).isEqualTo(1);
        fixture.expire("document_assessment_owners", "assessment_id", first.assessment(), "retain_until");
        assertThat(fixture.release(first)).isTrue();
        assertThat(refs(first.assessment())).isZero();
        assertThat(refs(second.assessment())).isEqualTo(1);
        assertThat(artifacts.readRetained(second.owner(), sha, () -> {})).isEqualTo(bytes);
        assertThat(catalogCount(sha)).isEqualTo(1);
    }

    @Test void laterGenerationMustRestageBeforeAcquisitionAndOldAssessmentSurvivesClaimCleanup() {
        var owner = fixture.operations.admit(new RepositoryOperationLedger.Key("account", "principal", UUID.randomUUID()),
                new RepositoryOperationLedger.EncodedCommand("document-publication", 1, ByteString.copyFromUtf8("SQL fixture")),
                UUID.randomUUID(), Duration.ofSeconds(2)).owner().orElseThrow();
        var first = fixture.candidate(owner, 120);
        var bytes = ByteString.copyFromUtf8("generation retention " + UUID.randomUUID());
        String sha = artifacts.stage(owner, List.of(bytes), () -> {}).getFirst();
        stage(first, sha, 120, 1);
        fixture.expire("repository_operation_owners", "operation_id", owner.key().operationId(), "lease_until");
        var next = fixture.operations.takeOver(owner.key(), owner.generation(), UUID.randomUUID(), Duration.ofMinutes(2));
        var second = fixture.candidate(next, 120);
        assertThatThrownBy(() -> stage(second, sha, 120, 1)).hasStackTraceContaining("exact current-generation artifact claim");
        assertThat(refs(second.assessment())).isZero();
        artifacts.stage(next, List.of(bytes), () -> {});
        stage(second, sha, 120, 1);
        long removed = tx.inTransaction(em -> { return ((Number) em.createNativeQuery(
                "SELECT release_repository_replaced_schema_claims('account','principal',:op,64)")
                .setParameter("op", owner.key().operationId()).getSingleResult()).longValue(); });
        assertThat(removed).isEqualTo(1);
        assertThat(refs(first.assessment())).isEqualTo(1);
        assertThat(refs(second.assessment())).isEqualTo(1);
        assertThat(catalogCount(sha)).isEqualTo(1);
    }

    @Test void anotherOperationCannotBorrowAnArtifactByDigestAlone() {
        var source = fixture.candidate(120); var target = fixture.candidate(120);
        String sha = artifacts.stage(source.owner(), List.of(ByteString.copyFromUtf8("private claim " + UUID.randomUUID())), () -> {}).getFirst();
        assertThatThrownBy(() -> stage(target, sha, 120, 1)).hasStackTraceContaining("exact current-generation artifact claim");
        assertThat(refs(target.assessment())).isZero();
    }

    @Test void missingDeclaredArtifactsRollbackPhysicalAndSchemaOwnershipTogether() {
        var c = fixture.candidate(120);
        String sha = artifacts.stage(c.owner(), List.of(ByteString.copyFromUtf8("incomplete " + UUID.randomUUID())), () -> {}).getFirst();
        assertThatThrownBy(() -> stage(c, sha, 120, 2)).hasStackTraceContaining("artifact set is incomplete");
        assertThat(refs(c.assessment())).isZero();
        long physical = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue());
        assertThat(physical).isZero();
        long owners = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_owners WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue());
        assertThat(owners).isZero();
    }

    @Test void wrongAccountAndPrematureOrLateArtifactInsertionAreRejected() {
        var c = fixture.candidate(120);
        String sha = artifacts.stage(c.owner(), List.of(ByteString.copyFromUtf8("scope " + UUID.randomUUID())), () -> {}).getFirst();
        for (boolean seal : new boolean[]{false, true}) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
                fixture.insertOwner(em, c, 120, 2, 1); fixture.insertSlots(em, c, "revision_ordinal", 1);
                if (seal) fixture.seal(em, c);
                attach(em, c, sha, seal ? "other-account" : "account");
            })).hasStackTraceContaining("scoped physical seal in the creation transaction");
        }
        fixture.stage(c, 120);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner()); attach(em, c, sha, "account");
        })).hasStackTraceContaining("creation transaction");
    }

    @Test void rawArtifactReleaseNeedsRecoveryAndCannotCommitPartially() {
        var c = fixture.candidate(120);
        String sha = artifacts.stage(c.owner(), List.of(ByteString.copyFromUtf8("release " + UUID.randomUUID())), () -> {}).getFirst();
        stage(c, sha, 1, 1);
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery("DELETE FROM document_assessment_artifacts WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("recovery fence");
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("SELECT fence_repository_operation_recovery('account','principal',:op)")
                    .setParameter("op", c.owner().key().operationId()).getSingleResult();
            em.createNativeQuery("UPDATE document_assessment_owners SET release_xid=pg_current_xact_id() WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate();
            em.createNativeQuery("DELETE FROM document_assessment_artifacts WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate();
        })).hasStackTraceContaining("Assessment artifact set is incomplete or exceeds byte budget");
        assertThat(refs(c.assessment())).isEqualTo(1);
        assertThat(fixture.release(c)).isTrue();
        assertThat(catalogCount(sha)).isEqualTo(1);
    }

    private static void stage(DocumentAssessmentRetentionFixture.Candidate c, String sha, int seconds, int expected) {
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
            fixture.insertOwner(em, c, seconds, 2, expected); fixture.insertSlots(em, c, "revision_ordinal", 1);
            fixture.seal(em, c); attach(em, c, sha, "account");
        });
    }
    private static void attach(EntityManager em, DocumentAssessmentRetentionFixture.Candidate c, String sha, String account) {
        em.createNativeQuery("INSERT INTO document_assessment_artifacts(assessment_id,account_id,artifact_sha256) VALUES(:id,:account,:sha)")
                .setParameter("id", c.assessment()).setParameter("account", account).setParameter("sha", HexFormat.of().parseHex(sha)).executeUpdate();
    }
    private static long refs(UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_assessment_artifacts WHERE assessment_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
    private static long catalogCount(String sha) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_schema_artifacts WHERE account_id='account' AND artifact_sha256=:sha")
                .setParameter("sha", HexFormat.of().parseHex(sha)).getSingleResult()).longValue());
    }
}
