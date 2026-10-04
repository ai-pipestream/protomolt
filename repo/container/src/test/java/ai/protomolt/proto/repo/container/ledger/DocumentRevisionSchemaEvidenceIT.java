package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** SQL storage guards with synthetic bytes; this does not decode or qualify typed evidence. */
@Testcontainers
class DocumentRevisionSchemaEvidenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final int FOUR_MIB = 4 * 1024 * 1024;

    @Test void migrationPreservesExistingPublicationsWithoutInventingEvidence() {
        try (var c = context(POSTGRES, "55")) {
            var p = prepare(c, 1);
            publish(c, p, Fault.NONE, em -> {});
            var before = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id").getResultList());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            var after = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id").getResultList());
            assertThat(after).isEqualTo(before);
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
        }
    }

    @Test void insertsSharedOperationRevisionsAndRejectsImmutableOrLateWrites() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            publish(c, p, Fault.NONE, false, (em, revision) -> insert(em, revision, p.owner(), 0, bytes(8)), em -> {});
            assertThat(count(c, "document_revision_schema_evidence")).isEqualTo(2);
            var revision = UUID.fromString((String)c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT revision_id::text FROM document_revision_commits ORDER BY member_ordinal LIMIT 1").getSingleResult()));
            for (String sql : List.of("DELETE FROM document_revision_schema_evidence", "UPDATE document_revision_schema_evidence SET principal=principal")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("immutable");
            }
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em, revision, p.owner(), 0, bytes(8)); }))
                    .hasStackTraceContaining("terminal");
        }
    }

    @Test void rawFragmentHashSizeOrdinalAndSupportedPartAreChecked() {
        for (int fault = 0; fault < 7; fault++) {
            try (var c = context(POSTGRES)) {
                var p = prepare(c, 1);
                final int selected = fault;
                assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                    if (selected == 1) insert(em, revision, p.owner(), 1, bytes(8)); // CHUNKS sub-key is not a root fragment.
                    else insertVariant(em, revision, p.owner(), selected, bytes(8));
                }, em -> {})).hasStackTraceContaining(selected >= 4 ? "live owner write fence"
                        : "Revision schema evidence differs from a supported verified fragment");
                assertThat(count(c, "document_revision_schema_evidence")).isZero();
            }
        }
    }

    @Test void sealedProjectionRefusesEvidenceInsertion() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                em.createNativeQuery("UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=:r")
                        .setParameter("r", revision).executeUpdate();
                insert(em, revision, p.owner(), 0, bytes(8));
            }, em -> {})).hasStackTraceContaining("current unsealed native projection");
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
        }
    }

    @Test void metadataChecksumCodecAndPerBundleSizeAreConstrained() {
        for (int fault = 0; fault < 4; fault++) {
            try (var c = context(POSTGRES)) {
                var p = prepare(c, 1);
                final int selected = fault;
                assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                    var raw = fragment(em, revision, 0);
                    byte[] payload = selected == 3 ? bytes(FOUR_MIB + 1) : bytes(8);
                    var q = em.createNativeQuery("""
                            INSERT INTO document_revision_schema_evidence(revision_id,revision_ordinal,account_id,principal,
                             operation_id,owner_generation,root_locator_sha256,fragment_sha256,fragment_size,evidence_codec,
                             evidence_version,evidence_bytes,evidence_sha256)
                            VALUES(:r,:o,:a,:p,:op,:g,:loc,:fh,:fs,:codec,:v,:b,:eh)
                            """);
                    q.setParameter("r", revision).setParameter("o", 0).setParameter("a", p.owner().key().account())
                            .setParameter("p", p.owner().key().principal()).setParameter("op", p.owner().key().operationId())
                            .setParameter("g", p.owner().generation()).setParameter("loc", digest("locator"))
                            .setParameter("fh", raw.hash).setParameter("fs", raw.size)
                            .setParameter("codec", selected == 0 ? "other-codec" : "document-root-schema-evidence")
                            .setParameter("v", selected == 1 ? 2 : 1).setParameter("b", payload)
                            .setParameter("eh", selected == 2 ? sha("wrong".getBytes(StandardCharsets.UTF_8)) : sha(payload));
                    q.executeUpdate();
                }, em -> {})).hasStackTraceContaining(selected == 0 ? "evidence_codec" : selected == 1 ? "evidence_version"
                        : selected == 2 ? "evidence_sha256" : "evidence_bytes");
                assertThat(count(c, "document_revision_schema_evidence")).isZero();
            }
        }
    }

    @Test void missingTerminalRollsBackEvidenceAndPublicationCanRetry() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            assertThatThrownBy(() -> publish(c, p, Fault.OMIT_OUTCOME, false,
                    (em, revision) -> insert(em, revision, p.owner(), 0, bytes(8)), em -> {})).isInstanceOf(RuntimeException.class);
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
            publish(c, p, Fault.NONE, false, (em, revision) -> insert(em, revision, p.owner(), 0, bytes(8)), em -> {});
            assertThat(count(c, "document_revision_schema_evidence")).isEqualTo(1);
        }
    }

    @Test void duplicateLocatorAndRevisionBudgetAreRefused() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                insertWithLocator(em, revision, p.owner(), 0, bytes(8), digest("same"));
                insertWithLocator(em, revision, p.owner(), 0, bytes(8), digest("same"));
            }, em -> {})).hasStackTraceContaining("document_revision_schema_evidence_pkey");
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
        }
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var reached = new java.util.concurrent.atomic.AtomicBoolean();
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                for (int i = 0; i < 4; i++) insertWithLocator(em, revision, p.owner(), 0, bytes(FOUR_MIB), digest("revision-" + i));
                assertThat(((Number)em.createNativeQuery("SELECT count(*) FROM document_revision_schema_evidence WHERE revision_id=:r")
                        .setParameter("r", revision).getSingleResult()).longValue()).isEqualTo(4);
                assertThat(((Number)em.createNativeQuery("SELECT sum(evidence_size) FROM document_revision_schema_evidence WHERE revision_id=:r")
                        .setParameter("r", revision).getSingleResult()).longValue()).isEqualTo(16L * 1024 * 1024);
                reached.set(true);
                insertWithLocator(em, revision, p.owner(), 0, bytes(1), digest("revision-overflow"));
            }, em -> {})).hasStackTraceContaining("exceeds revision budget");
            assertThat(reached).isTrue();
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
        }
    }

    @Test void operationBudgetSpansMembers() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 5);
            var reached = new java.util.concurrent.atomic.AtomicBoolean();
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                int member = ((Number)em.createNativeQuery("SELECT member_ordinal FROM document_revision_commits WHERE revision_id=:r")
                        .setParameter("r", revision).getSingleResult()).intValue();
                if (member < 4) {
                    for (int i = 0; i < 4; i++) insertWithLocator(em, revision, p.owner(), 0, bytes(FOUR_MIB), digest("op-" + member + "-" + i));
                } else {
                    assertThat(((Number)em.createNativeQuery("SELECT count(*) FROM document_revision_schema_evidence")
                            .getSingleResult()).longValue()).isEqualTo(16);
                    assertThat(((Number)em.createNativeQuery("SELECT sum(evidence_size) FROM document_revision_schema_evidence")
                            .getSingleResult()).longValue()).isEqualTo(64L * 1024 * 1024);
                    reached.set(true);
                    insertWithLocator(em, revision, p.owner(), 0, bytes(1), digest("operation-overflow"));
                }
            }, em -> {})).hasStackTraceContaining("exceeds operation budget");
            assertThat(reached).isTrue();
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
        }
    }

    @Test void rowCapsAllowTheLimitAndRefuseTheNextRow() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var reached = new java.util.concurrent.atomic.AtomicBoolean();
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                for (int i = 0; i < 1024; i++) insertWithLocator(em, revision, p.owner(), 0, bytes(1), digest("rows-" + i));
                assertThat(((Number)em.createNativeQuery("SELECT count(*) FROM document_revision_schema_evidence")
                        .getSingleResult()).longValue()).isEqualTo(1024);
                reached.set(true);
                insertWithLocator(em, revision, p.owner(), 0, bytes(1), digest("rows-overflow"));
            }, em -> {})).hasStackTraceContaining("exceeds revision budget");
            assertThat(reached).isTrue();
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
        }
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 5);
            var reached = new java.util.concurrent.atomic.AtomicBoolean();
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                int member = ((Number)em.createNativeQuery("SELECT member_ordinal FROM document_revision_commits WHERE revision_id=:r")
                        .setParameter("r", revision).getSingleResult()).intValue();
                if (member < 4) {
                    for (int i = 0; i < 1024; i++) insertWithLocator(em, revision, p.owner(), 0, bytes(1), digest("rows-" + member + "-" + i));
                } else {
                    assertThat(((Number)em.createNativeQuery("SELECT count(*) FROM document_revision_schema_evidence")
                            .getSingleResult()).longValue()).isEqualTo(4096);
                    reached.set(true);
                    insertWithLocator(em, revision, p.owner(), 0, bytes(1), digest("rows-operation-overflow"));
                }
            }, em -> {})).hasStackTraceContaining("exceeds operation budget");
            assertThat(reached).isTrue();
            assertThat(count(c, "document_revision_schema_evidence")).isZero();
        }
    }

    private static void insert(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, int ordinal, byte[] payload) {
        insertWithLocator(em, revision, owner, ordinal, payload, digest("locator-" + revision));
    }
    private static void insertVariant(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, int variant, byte[] payload) {
        var raw = fragment(em, revision, 0);
        var q = em.createNativeQuery("""
                INSERT INTO document_revision_schema_evidence(revision_id,revision_ordinal,account_id,principal,operation_id,
                 owner_generation,root_locator_sha256,fragment_sha256,fragment_size,evidence_codec,evidence_version,evidence_bytes,evidence_sha256)
                VALUES(:r,:o,:a,:p,:op,:g,:loc,:fh,:fs,'document-root-schema-evidence',1,:b,:eh)
                """);
        q.setParameter("r", revision).setParameter("o", variant == 2 ? 1 : 0)
                .setParameter("a", variant == 4 ? "foreign-account" : owner.key().account())
                .setParameter("p", variant == 5 ? "foreign-principal" : owner.key().principal())
                .setParameter("op", owner.key().operationId()).setParameter("g", variant == 6 ? owner.generation() + 1 : owner.generation())
                .setParameter("loc", digest("variant" + variant)).setParameter("fh", variant == 0 ? digest("wrong-raw") : raw.hash)
                .setParameter("fs", variant == 3 ? raw.size + 1 : raw.size).setParameter("b", payload).setParameter("eh", sha(payload));
        q.executeUpdate();
    }
    private static void insertWithLocator(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner,
            int ordinal, byte[] payload, byte[] locator) {
        var raw = fragment(em, revision, ordinal);
        em.createNativeQuery("""
                INSERT INTO document_revision_schema_evidence(revision_id,revision_ordinal,account_id,principal,operation_id,
                 owner_generation,root_locator_sha256,fragment_sha256,fragment_size,evidence_codec,evidence_version,evidence_bytes,evidence_sha256)
                VALUES(:r,:o,:a,:p,:op,:g,:loc,:fh,:fs,'document-root-schema-evidence',1,:b,:eh)
                """).setParameter("r", revision).setParameter("o", ordinal).setParameter("a", owner.key().account())
                .setParameter("p", owner.key().principal()).setParameter("op", owner.key().operationId()).setParameter("g", owner.generation())
                .setParameter("loc", locator).setParameter("fh", raw.hash).setParameter("fs", raw.size)
                .setParameter("b", payload).setParameter("eh", sha(payload)).executeUpdate();
    }
    private static Raw fragment(EntityManager em, UUID revision, int ordinal) {
        Object[] row = (Object[])em.createNativeQuery("""
                SELECT decode(o.expected_sha256,'hex'),o.expected_size FROM document_revision_parts p
                JOIN repository_physical_locations l ON l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART'
                JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                WHERE p.revision_id=:r AND p.revision_ordinal=:o ORDER BY p.part LIMIT 1
                """).setParameter("r", revision).setParameter("o", ordinal).getSingleResult();
        return new Raw((byte[])row[0], ((Number)row[1]).longValue());
    }
    private record Raw(byte[] hash, long size) {}
    private static byte[] bytes(int length) { return new byte[length]; }
    private static byte[] digest(String input) { return sha(input.getBytes(StandardCharsets.UTF_8)); }
    private static byte[] sha(byte[] input) {
        try { return MessageDigest.getInstance("SHA-256").digest(input); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
