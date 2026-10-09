package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL storage lifecycle; synthetic bytes do not establish semantic admission. */
@Testcontainers
@Timeout(60)
class DocumentAssessmentRootsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentAssessmentRetentionFixture fixture;
    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); fixture = new DocumentAssessmentRetentionFixture(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void migrationPreservesExistingSchemaAndPhysicalOwnershipWithoutInventingRoots() throws Exception {
        try (var context = DocumentNativePublicationFixture.context(POSTGRES, "67")) {
            var prior = new DocumentAssessmentRetentionFixture(context.tx());
            var c = prior.candidate(120);
            String sha = new RepositorySchemaArtifacts(context.tx()).stage(c.owner(), java.util.List.of(
                    com.google.protobuf.ByteString.copyFromUtf8("synthetic migration artifact")), () -> {}).getFirst();
            context.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
                prior.insertOwner(em, c, 1, 2, 1); prior.insertSlots(em, c, "revision_ordinal", 1); prior.seal(em, c);
                em.createNativeQuery("INSERT INTO document_assessment_artifacts VALUES(:id,'account',decode(:sha,'hex'))")
                        .setParameter("id", c.assessment()).setParameter("sha", sha).executeUpdate();
            });
            String before = context.tx().readOnly(em -> (String) em.createNativeQuery(
                    "SELECT to_jsonb(o)::text FROM document_assessment_owners o WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult());
            String schema = context.pool().getSchema();
            org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
            Object[] after = context.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT (to_jsonb(o)-'expected_roots')::text,expected_roots,
                        (SELECT count(*) FROM document_assessment_roots WHERE assessment_id=:id),
                        (SELECT count(*) FROM document_assessment_artifacts WHERE assessment_id=:id),
                        (SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id)
                    FROM document_assessment_owners o WHERE assessment_id=:id
                    """).setParameter("id", c.assessment()).getSingleResult());
            assertThat(after[0]).isEqualTo(before);
            assertThat(((Number) after[1]).intValue()).isZero();
            assertThat(((Number) after[2]).intValue()).isZero();
            assertThat(((Number) after[3]).intValue()).isEqualTo(1);
            assertThat(((Number) after[4]).intValue()).isEqualTo(2);
            prior.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
            assertThat(prior.release(c)).isTrue();
        }
    }

    @Test void sparseOrdinalEvidenceSurvivesUntilExplicitRecovery() {
        var c = fixture.candidate(120);
        stage(c, 1, 1, 1, 1);
        assertThat(roots(c)).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery(
                "UPDATE document_assessment_roots SET fragment_size=2 WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("root evidence is immutable");
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery(
                "DELETE FROM document_assessment_roots WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("recovery fence");
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        assertThat(roots(c)).isEqualTo(1);
        assertThat(fixture.release(c)).isTrue();
        assertThat(roots(c)).isZero();
        assertThat(fixture.release(c)).isFalse();
    }

    @Test void incompleteAndExcessRootSetsRollbackAllPhysicalOwnership() {
        for (int expected : new int[]{0, 2}) {
            var c = fixture.candidate(120);
            assertThatThrownBy(() -> stage(c, expected, 1, 1, 120)).hasStackTraceContaining("root set is incomplete");
            assertThat(roots(c)).isZero();
            long references = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult()).longValue());
            assertThat(references).isZero();
        }
    }

    @Test void evidenceRequiresTheExactSealedSlotAndFragment() {
        for (String variant : new String[]{"ordinal", "member", "size", "digest", "codec", "version", "checksum", "empty"}) {
            var c = fixture.candidate(120);
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                begin(em, c, 1, 120, true);
                root(em, c, 1, 1, variant);
            })).as(variant).hasStackTraceContaining(switch (variant) {
                case "ordinal", "member", "size", "digest" -> "differs from its retained verified fragment";
                case "codec" -> "document_assessment_roots_evidence_codec_check";
                case "version" -> "document_assessment_roots_evidence_version_check";
                case "empty" -> "document_assessment_roots_evidence_bytes_check";
                default -> "document_assessment_root_evidence_checksum";
            });
            assertThat(roots(c)).isZero();
        }
        var early = fixture.candidate(120);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            begin(em, early, 1, 120, false); root(em, early, 1, 1, "valid");
        })).hasStackTraceContaining("physical seal in the creation transaction");
        var late = fixture.candidate(120); fixture.stage(late, 120);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, late.owner()); root(em, late, 1, 1, "valid");
        })).hasStackTraceContaining("creation transaction");
    }

    @Test void rootBudgetIsSeparateFromTheSixtyFourSchemaArtifactLimit() {
        var c = fixture.candidate(120);
        stage(c, 65, 65, 1, 120);
        assertThat(roots(c)).isEqualTo(65);
        int artifactCount = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT expected_artifacts FROM document_assessment_owners WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).intValue());
        assertThat(artifactCount).isZero();
    }

    @Test void zeroByteFragmentCanCarryEvidence() {
        var c = new DocumentAssessmentRetentionFixture(tx, 0).candidate(120);
        tx.inTransaction(em -> { begin(em, c, 1, 120, true); root(em, c, 1, 1, "zero"); });
        assertThat(roots(c)).isEqualTo(1);
    }

    @Test void rootCountDeclarationIsBoundedAndImmutable() {
        for (int count : new int[]{-1, 4097}) {
            var c = fixture.candidate(120);
            assertThatThrownBy(() -> tx.inTransaction(em -> { begin(em, c, count, 120, true); }))
                    .hasStackTraceContaining("document_assessment_owners_expected_roots_check");
        }
        var c = fixture.candidate(120); stage(c, 1, 1, 1, 120);
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery(
                "UPDATE document_assessment_owners SET expected_roots=0 WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("identity and evidence are immutable");
    }

    @Test void aggregateAndIndividualByteLimitsRejectOversizedEvidence() {
        var total = fixture.candidate(120);
        assertThatThrownBy(() -> stage(total, 17, 17, 4 * 1024 * 1024, 120))
                .hasStackTraceContaining("root set is incomplete or exceeds byte budget");
        assertThat(roots(total)).isZero();
        var individual = fixture.candidate(120);
        assertThatThrownBy(() -> stage(individual, 1, 1, 4 * 1024 * 1024 + 1, 120))
                .hasStackTraceContaining("violates check constraint");
        assertThat(roots(individual)).isZero();
    }

    @Test void partialRecoveryRollsBackRootDeletion() {
        var c = fixture.candidate(120); stage(c, 1, 1, 1, 1);
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("SELECT fence_repository_operation_recovery('account','principal',:op)")
                    .setParameter("op", c.owner().key().operationId()).getSingleResult();
            em.createNativeQuery("UPDATE document_assessment_owners SET release_xid=pg_current_xact_id() WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate();
            em.createNativeQuery("DELETE FROM document_assessment_roots WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate();
        })).hasStackTraceContaining("Assessment transaction must finish sealed or fully released");
        assertThat(roots(c)).isEqualTo(1);
        assertThat(fixture.release(c)).isTrue();
    }

    private static void stage(DocumentAssessmentRetentionFixture.Candidate c, int expected, int actual, int bytes, int seconds) {
        tx.inTransaction(em -> {
            begin(em, c, expected, seconds, true);
            for (int i = 0; i < actual; i++) root(em, c, i + 1, bytes, "valid");
        });
    }
    private static void begin(EntityManager em, DocumentAssessmentRetentionFixture.Candidate c, int expected, int seconds, boolean seal) {
        RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
        em.createNativeQuery("""
                INSERT INTO document_assessment_owners(assessment_id,account_id,principal,operation_id,owner_generation,
                    command_codec,command_version,command_sha256,manifest_bytes,manifest_sha256,expected_slots,retain_until,creation_xid,expected_roots)
                SELECT :id,account_id,principal,operation_id,:generation,command_codec,command_version,command_sha256,
                    decode('01','hex'),sha256(decode('01','hex')),2,clock_timestamp()+(:seconds * interval '1 second'),'0'::xid8,:roots
                FROM repository_operations WHERE account_id='account' AND principal='principal' AND operation_id=:op
                """).setParameter("id", c.assessment()).setParameter("generation", c.owner().generation())
                .setParameter("seconds", seconds).setParameter("roots", expected).setParameter("op", c.owner().key().operationId()).executeUpdate();
        fixture.insertSlots(em, c, "revision_ordinal", 1);
        if (seal) fixture.seal(em, c);
    }
    private static void root(EntityManager em, DocumentAssessmentRetentionFixture.Candidate c, int locator, int bytes, String variant) {
        em.createNativeQuery("""
                INSERT INTO document_assessment_roots(assessment_id,member_id,revision_ordinal,root_locator_sha256,
                    fragment_sha256,fragment_size,evidence_codec,evidence_version,evidence_bytes,evidence_sha256)
                SELECT :id,:member,:ordinal,sha256(convert_to(CAST(:locator AS text),'UTF8')),decode(:fragment,'hex'),:size,
                    :codec,:version,payload,CASE WHEN :badChecksum THEN decode(repeat('b',64),'hex') ELSE sha256(payload) END
                FROM (SELECT decode(repeat('01',:bytes),'hex') AS payload) p
                """).setParameter("id", c.assessment()).setParameter("member", variant.equals("member") ? "other" : "member")
                .setParameter("ordinal", variant.equals("ordinal") ? 0 : 2).setParameter("locator", locator)
                .setParameter("fragment", (variant.equals("digest") ? "b" : "a").repeat(64))
                .setParameter("size", variant.equals("zero") ? 0 : variant.equals("size") ? 2 : 1)
                .setParameter("codec", variant.equals("codec") ? "unknown" : "document-root-schema-evidence")
                .setParameter("version", variant.equals("version") ? 2 : 1)
                .setParameter("badChecksum", variant.equals("checksum")).setParameter("bytes", variant.equals("empty") ? 0 : bytes).executeUpdate();
    }
    private static long roots(DocumentAssessmentRetentionFixture.Candidate c) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_assessment_roots WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue());
    }
}
