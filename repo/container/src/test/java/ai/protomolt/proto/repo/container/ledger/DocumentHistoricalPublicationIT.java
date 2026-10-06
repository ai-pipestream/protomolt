package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL and retained descriptors; initial provider observations are explicitly synthetic. */
@Testcontainers
class DocumentHistoricalPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final DocumentRevisionAssembly.Limits LIMITS =
            new DocumentRevisionAssembly.Limits(4_000_000, 32, 100, 100, 1_000_000);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void policyChangeOrLateSqlFailureLeavesNoPartialPublication(boolean changePolicy) throws Exception {
        try (var c = context(POSTGRES)) {
            var original = DocumentSchemaRetentionFixture.prepare(c);
            var policies = new DocumentSchemaPolicies(c.tx());
            policies.activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), revision);
            var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, original.batch().policy(),
                    Map.of("member", DocumentPublicationCandidate.Mode.TYPED), Map.of("member", fixture.fragments()), Optional.empty(),
                    (m, occurrence) -> { throw new AssertionError("Unexpected registry access"); }, budget, LIMITS, Instant.now(),
                    CALLER, List.of(history), RepositoryReadControl.NONE)) {
                var placement = original.prepared().members().getFirst().placement();
                var prepared = assessment.preparePhysical(Map.of(placement.drive().id(), placement), Map.of(),
                        Duration.ofMinutes(5), Map.of(), RepositoryReadControl.NONE);
                var owner = new RepositoryOperationLedger(c.tx()).admitHistorical(CALLER,
                        new RepositoryOperationLedger.Key("account", CALLER.principalName(), command.operationId()),
                        prepared, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, prepared);
                if (changePolicy) policies.activate(original.batch().policy().policy(), 1, () -> {});
                else c.tx().inTransaction(em -> {
                    em.createNativeQuery("""
                            CREATE FUNCTION refuse_historical_success() RETURNS trigger LANGUAGE plpgsql AS $$
                            BEGIN RAISE EXCEPTION 'injected historical terminal failure'; END $$
                            """).executeUpdate();
                    em.createNativeQuery("""
                            CREATE TRIGGER refuse_historical_success BEFORE INSERT ON repository_operation_success
                            FOR EACH ROW EXECUTE FUNCTION refuse_historical_success()
                            """).executeUpdate();
                });
                var error = catchThrowable(() -> assessment.publish(CALLER, owner, prepared, Map.of(),
                        new RepositorySchemaArtifacts(c.tx()), new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), false, false),
                        RepositoryReadControl.NONE));
                if (changePolicy) assertThat(error).isInstanceOf(DocumentSchemaPolicies.StalePolicy.class);
                else assertThat(error).hasStackTraceContaining("injected historical terminal failure");
                assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, command).state())
                        .isEqualTo(DocumentPublicationReplay.State.PENDING);
                var row = new DocumentLedger(c.tx()).findByNodeId(original.prepared().members().getFirst().nodeId()).orElseThrow();
                assertThat(row.mutationRevision).isEqualTo(1);
                c.tx().inTransaction(em -> {
                    assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM document_revision_schema_admissions WHERE operation_id=:operation")
                            .setParameter("operation", command.operationId()).getSingleResult()).longValue()).isZero();
                    assertThat(em.createNativeQuery("SELECT revision_id FROM document_revision_current WHERE node_id=:node")
                            .setParameter("node", row.nodeId).getSingleResult()).isEqualTo(revision);
                });
            } finally { assertThat(budget.reservedBytes()).isZero(); release(reads, history); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void retainedRevisionPublishesNewCurrentVersionsWithoutCopyingObjects(boolean typed) throws Exception {
        try (var c = context(POSTGRES)) {
            var original = DocumentSchemaRetentionFixture.prepare(c, typed);
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var firstRevision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, revision, manifest) -> { if (typed) original.retention().write(em, original.owner(), revision, () -> {}); });
            var fixture = new Fixture(original, firstRevision);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), firstRevision);
            var initial = new DocumentLedger(c.tx()).findByNodeId(original.prepared().members().getFirst().nodeId()).orElseThrow();
            var originalEntry = initial.readManifest().getParts(0);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try {
                for (int current = 1; current <= 2; current++) {
                    var historical = member(fixture, history).toBuilder().setDestination(
                            member(fixture, history).getDestination().toBuilder().setExpectedMutationRevision(current)).build();
                    var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                            .setOperationId(UUID.randomUUID().toString()).setMembers(0, historical).build());
                    try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, original.batch().policy(),
                            Map.of("member", typed ? DocumentPublicationCandidate.Mode.TYPED : DocumentPublicationCandidate.Mode.OPAQUE),
                            Map.of("member", fixture.fragments()), Optional.empty(),
                            (m, occurrence) -> { throw new AssertionError("Historical publication cannot resolve current registry"); },
                            budget, LIMITS, Instant.now(), CALLER, List.of(history), RepositoryReadControl.NONE)) {
                        var placement = original.prepared().members().getFirst().placement();
                        var prepared = assessment.preparePhysical(Map.of(placement.drive().id(), placement), Map.of(),
                                Duration.ofMinutes(5), Map.of(), RepositoryReadControl.NONE);
                        var operations = new RepositoryOperationLedger(c.tx());
                        var owner = operations.admitHistorical(CALLER,
                                new RepositoryOperationLedger.Key("account", CALLER.principalName(), command.operationId()),
                                prepared, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                        new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, prepared);
                        var commit = new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), false, false);
                        assertThatThrownBy(() -> commit.commit(CALLER, owner, prepared, Map.of(), Map.of(), () -> {}))
                                .isInstanceOf(UnsupportedOperationException.class);
                        var result = assessment.publish(CALLER, owner, prepared, Map.of(), new RepositorySchemaArtifacts(c.tx()),
                                commit, RepositoryReadControl.NONE);
                        assertThat(result.getMembersCount()).isEqualTo(1);
                        var row = new DocumentLedger(c.tx()).findByNodeId(initial.nodeId).orElseThrow();
                        assertThat(row.readManifest().getDocVersion()).isEqualTo(current + 1);
                        assertThat(row.readManifest().getParts(0)).isEqualTo(originalEntry);
                        var retained = new DocumentRevisionRetentionInventory(c.tx()).inspect(CALLER, fixture.address(),
                                firstRevision, RepositoryReadControl.NONE);
                        assertThat(retained.current()).isFalse();
                        long revisionCount = current + 1;
                        assertThat(retained.objects()).allSatisfy(object -> {
                            assertThat(object.historicalRevisions()).isEqualTo(revisionCount);
                            assertThat(object.currentRevisions()).isEqualTo(1);
                            assertThat(object.documentReaders()).isEqualTo(1);
                            assertThat(object.mirrors()).isEqualTo(revisionCount + 2);
                        });
                        assertThat(retained.artifacts()).allSatisfy(artifact -> assertThat(artifact.revisions()).isEqualTo(revisionCount));
                        c.tx().inTransaction(em -> {
                            var revisions = em.createNativeQuery("""
                                    SELECT p.revision_id,p.object_id,a.decision
                                    FROM document_revision_current c JOIN document_revision_parts p USING(revision_id)
                                    JOIN document_revision_schema_admissions a USING(revision_id) WHERE c.node_id=:node
                                    """).setParameter("node", initial.nodeId).getResultList();
                            assertThat(revisions).hasSize(1);
                            var revision = (Object[]) revisions.getFirst();
                            assertThat(revision[0]).isNotEqualTo(firstRevision);
                            assertThat(revision[1].toString()).isEqualTo(historical.getParts(0).getHistoricalReuse().getObject().getObjectId());
                            assertThat(revision[2]).isEqualTo(typed ? "TYPED" : "OPAQUE");
                            return null;
                        });
                    }
                    assertThat(budget.reservedBytes()).isZero();
                }
            } finally { release(reads, history); }
        }
    }
}
