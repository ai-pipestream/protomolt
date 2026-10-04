package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL constraint tests with synthetic native publications, not typed/provider qualification. */
@Testcontainers
class DocumentRevisionSchemaArtifactsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void migrationPreservesPublishedRevisionsWithoutInventingSchemaEvidence() {
        try (var c = context(POSTGRES, "54")) {
            var p = prepare(c, 2);
            stage(c, p.owner());
            var result = publish(c, p, Fault.NONE, em -> {});
            var before = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id
                    """).getResultList());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            var after = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id
                    """).getResultList());
            // Preparation publishes one legacy source per member; native publication adds one more.
            assertThat(after).isEqualTo(before).hasSize(2 * result.getMembersCount());
            assertThat(count(c, "document_revision_commits")).isEqualTo(2);
            assertThat(count(c, "repository_operation_success")).isEqualTo(1);
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
        }
    }

    @Test void revisionsShareNormalizedBytesAndReferencesAreImmutable() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            String hash = stage(c, p.owner());
            var result = publish(c, p, Fault.NONE, false,
                    (em, revision) -> insert(em, revision, p.owner(), hash), em -> {});
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(2);
            assertThat(result.getMembersCount()).isEqualTo(2);
            for (String statement : List.of("DELETE FROM document_revision_schema_artifacts",
                    "UPDATE document_revision_schema_artifacts SET principal=principal")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(statement).executeUpdate(); }))
                        .hasStackTraceContaining("Revision schema references are immutable");
            }
            var foreignTables = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT confrelid::regclass::text FROM pg_constraint
                    WHERE conrelid='document_revision_schema_artifacts'::regclass AND contype='f'
                    """).getResultList());
            assertThat(foreignTables).contains("repository_schema_artifacts", "document_revision_publications", "document_revision_commits")
                    .doesNotContain("repository_schema_artifact_claims");
        }
    }

    @Test void aDifferentOperationsClaimCannotSupplyTheCurrentCommit() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var other = otherOwner(c);
            String hash = stage(c, other);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false,
                    (em, revision) -> insert(em, revision, p.owner(), hash), em -> {}))
                    .hasStackTraceContaining("requires a current owner claim");
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
            assertThat(count(c, "repository_operation_success")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
        }
    }

    @Test void evenAFencedOtherOwnerCannotAttachToThisRevision() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var other = otherOwner(c);
            String hash = stage(c, other);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                RepositoryOperationLedger.fenceLiveOwner(em, other);
                insert(em, revision, other, hash);
            }, em -> {})).hasStackTraceContaining("differs from native commit owner or transaction");
        }
    }

    @Test void wrongAccountPrincipalOrGenerationCannotBorrowTheWriteFence() {
        for (int variant = 0; variant < 3; variant++) {
            try (var c = context(POSTGRES)) {
                var p = prepare(c, 1);
                String hash = stage(c, p.owner());
                int choice = variant;
                assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                    insert(em, revision, choice == 0 ? "foreign" : p.owner().key().account(),
                            choice == 1 ? "foreign" : p.owner().key().principal(), p.owner().key().operationId(),
                            choice == 2 ? p.owner().generation() + 1 : p.owner().generation(), hash);
                }, em -> {})).hasStackTraceContaining("live owner write fence");
            }
        }
    }

    @Test void sealingAndTerminalSuccessBothCloseReferenceInsertion() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            String hash = stage(c, p.owner());
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                em.createNativeQuery("UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=:id")
                        .setParameter("id", revision).executeUpdate();
                insert(em, revision, p.owner(), hash);
            }, em -> {})).hasStackTraceContaining("current unsealed native projection");
            var id = new java.util.concurrent.atomic.AtomicReference<UUID>();
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> id.set(revision),
                    em -> insert(em, id.get(), p.owner(), hash))).hasStackTraceContaining("terminal");
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
            var result = publish(c, p, Fault.NONE, em -> {});
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                insert(em, UUID.fromString(result.getMembers(0).getRevisionId()), p.owner(), hash);
            })).hasStackTraceContaining("terminal");
        }
    }

    @Test void missingArtifactAndAbortedPublicationLeaveNoReferences() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false,
                    (em, revision) -> insert(em, revision, p.owner(), "0".repeat(64)), em -> {}))
                    .hasStackTraceContaining("artifact is missing");
            String hash = stage(c, p.owner());
            assertThatThrownBy(() -> publish(c, p, Fault.OMIT_OUTCOME, false,
                    (em, revision) -> insert(em, revision, p.owner(), hash), em -> {})).isInstanceOf(RuntimeException.class);
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
            publish(c, p, Fault.NONE, false, (em, revision) -> insert(em, revision, p.owner(), hash), em -> {});
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(1);
        }
    }

    @Test void expirationRefusesInsertionButDoesNotRemoveCommittedReferences() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1, false, Duration.ofSeconds(10));
            String hash = stage(c, p.owner());
            publish(c, p, Fault.NONE, false, (em, revision) -> insert(em, revision, p.owner(), hash), em -> {});
            c.tx().readOnly(em -> { expire(em, p.owner()); return null; });
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(1);
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
        }
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1, false, Duration.ofSeconds(10));
            String hash = stage(c, p.owner());
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                expire(em, p.owner());
                insert(em, revision, p.owner(), hash);
            }, em -> {})).hasStackTraceContaining("live owner write fence");
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
        }
    }

    private static RepositoryOperationLedger.Owner otherOwner(Context c) {
        return new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", UUID.randomUUID()),
                new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("fixture")),
                UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
    }
    private static String stage(Context c, RepositoryOperationLedger.Owner owner) {
        var descriptor = DescriptorProtos.FileDescriptorSet.newBuilder().addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("retained.proto").setSyntax("proto3").setPackage("retained")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Record"))).build().toByteString();
        return new RepositorySchemaArtifacts(c.tx()).stage(owner, List.of(descriptor), () -> {}).getFirst();
    }
    private static void expire(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                FROM repository_operation_owners WHERE operation_id=:id
                """).setParameter("id", owner.key().operationId()).getSingleResult();
    }
    private static void insert(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, String hash) {
        insert(em, revision, owner.key().account(), owner.key().principal(), owner.key().operationId(), owner.generation(), hash);
    }
    private static void insert(EntityManager em, UUID revision, String account, String principal, UUID operation, long generation, String hash) {
        em.createNativeQuery("""
                INSERT INTO document_revision_schema_artifacts(revision_id,account_id,principal,operation_id,owner_generation,artifact_sha256)
                VALUES(:revision,:account,:principal,:operation,:generation,:sha)
                """).setParameter("revision", revision).setParameter("account", account).setParameter("principal", principal)
                .setParameter("operation", operation).setParameter("generation", generation)
                .setParameter("sha", HexFormat.of().parseHex(hash)).executeUpdate();
    }
}
