package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL lock waits; initial provider observations are synthetic. */
@Testcontainers
class DocumentHistoricalPublicationAuthorizationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void rechecksHistoricalReadAfterPublicationPolicyLockWait(boolean revokeRead) throws Exception {
        try (var c = context(POSTGRES)) {
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            var security = DocumentSecurity.newBuilder();
            for (var access : List.of(Access.ACCESS_READ, Access.ACCESS_WRITE)) security.addPermissions(AccessRule.newBuilder()
                    .setIdentityType("public").setIdentity("public").setAccess(access));
            var document = Document.newBuilder().setDocId("historical-race").setOwnership(OwnershipContext.newBuilder()
                    .setAccountId("account").setDatasourceId("source").setSecurity(security))
                    .setStructuredData(Any.pack(StringValue.of("retained"), "type.test")).build();
            var original = DocumentSchemaRetentionFixture.prepare(c, true, false, document, "node", "storage");
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(caller, fixture.address(), revision);
            var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, original.batch().policy(),
                    Map.of("member", DocumentPublicationCandidate.Mode.TYPED), Map.of("member", fixture.fragments()), Optional.empty(),
                    (m, occurrence) -> { throw new AssertionError("Unexpected current registry access"); }, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 100, 100, 1_000_000), Instant.now(), caller,
                    List.of(history), RepositoryReadControl.NONE)) {
                var placement = original.prepared().members().getFirst().placement();
                var prepared = assessment.preparePhysical(Map.of(placement.drive().id(), placement), Map.of(),
                        Duration.ofMinutes(5), Map.of(), RepositoryReadControl.NONE);
                var owner = new RepositoryOperationLedger(c.tx()).admitHistorical(caller,
                        new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                        prepared, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(caller, owner, prepared);
                var node = original.prepared().members().getFirst().nodeId();
                try (var blocker = c.emf().createEntityManager(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    blocker.getTransaction().begin();
                    try {
                        blocker.createNativeQuery("SELECT account_id FROM document_schema_policy_current WHERE account_id='account' FOR UPDATE")
                                .getSingleResult();
                        int pid = ((Number) blocker.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                        var publishing = executor.submit(() -> assessment.publish(caller, owner, prepared, Map.of(),
                                new RepositorySchemaArtifacts(c.tx()), new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), true, false),
                                RepositoryReadControl.NONE));
                        boolean waiting = false;
                        long end = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                        while (!publishing.isDone() && System.nanoTime() < end) {
                            waiting = c.tx().readOnly(em -> !em.createNativeQuery("""
                                    SELECT pid FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))
                                    AND query LIKE '%FROM document_schema_policy_current%'
                                    """).setParameter("blocker", pid).getResultList().isEmpty());
                            if (waiting) break;
                            Thread.sleep(10);
                        }
                        assertThat(waiting).as("publication reached the real policy-pointer lock after promotion and staging").isTrue();
                        if (revokeRead) c.tx().inTransaction(em -> {
                            em.createNativeQuery("SET LOCAL lock_timeout='2s'").executeUpdate();
                            em.createNativeQuery("""
                                    UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:node
                                    """).setParameter("node", node).setParameter("security",
                                    "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_WRITE\"}]}")
                                    .executeUpdate();
                        });
                        long beforeResume = new DocumentLedger(c.tx()).findByNodeId(node).orElseThrow().mutationRevision;
                        blocker.getTransaction().rollback();
                        if (revokeRead) assertThatThrownBy(() -> publishing.get(10, TimeUnit.SECONDS))
                                .isInstanceOf(ExecutionException.class).cause().isInstanceOfSatisfying(RepositoryException.class,
                                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
                        else assertThat(publishing.get(10, TimeUnit.SECONDS).getMembers(0).getMutationRevision()).isEqualTo(2);
                        var row = new DocumentLedger(c.tx()).findByNodeId(node).orElseThrow();
                        assertThat(row.mutationRevision).isEqualTo(revokeRead ? beforeResume : beforeResume + 1);
                        c.tx().readOnly(em -> {
                            var currentRevision = em.createNativeQuery("SELECT revision_id FROM document_revision_current WHERE node_id=:node")
                                    .setParameter("node", node).getSingleResult();
                            if (revokeRead) assertThat(currentRevision).isEqualTo(revision);
                            else assertThat(currentRevision).isNotEqualTo(revision);
                            assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM document_revision_commits WHERE operation_id=:operation")
                                    .setParameter("operation", command.operationId()).getSingleResult()).longValue()).isEqualTo(revokeRead ? 0 : 1);
                            assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM repository_operation_success WHERE operation_id=:operation")
                                    .setParameter("operation", command.operationId()).getSingleResult()).longValue()).isEqualTo(revokeRead ? 0 : 1);
                            assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM document_revision_schema_admissions WHERE operation_id=:operation")
                                    .setParameter("operation", command.operationId()).getSingleResult()).longValue()).isEqualTo(revokeRead ? 0 : 1);
                            return null;
                        });
                    } finally { if (blocker.getTransaction().isActive()) blocker.getTransaction().rollback(); }
                }
            } finally { assertThat(budget.reservedBytes()).isZero(); release(reads, history); }
        }
    }
}
