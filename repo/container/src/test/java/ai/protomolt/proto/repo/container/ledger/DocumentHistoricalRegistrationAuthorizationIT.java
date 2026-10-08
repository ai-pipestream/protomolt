package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL authorization and retained sources; provider observations are fixture supplied. */
@Testcontainers
class DocumentHistoricalRegistrationAuthorizationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final DocumentSecurity POLICY = DocumentSecurity.newBuilder()
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_WRITE)).build();

    @ParameterizedTest @CsvSource({"false,false,false", "true,false,false", "false,true,false",
            "false,false,true", "false,true,true"})
    void capturedHistoryDoesNotPreserveRevokedAuthorityForRegistrationOrRetry(boolean beforeRegistration, boolean revokeKey,
            boolean loseStartReply) throws Exception {
        var binding = new RepositoryCredentialBinding("historical-test", UUID.randomUUID(), 1);
        var caller = new RepositoryCaller("scoped", false, Set.of("account"), Set.of(),
                revokeKey ? Optional.of(binding) : Optional.empty());
        var denial = revokeKey ? RepositoryException.Code.UNAUTHENTICATED : RepositoryException.Code.NOT_FOUND;
        try (var c = context(POSTGRES)) {
            var credentials = new RepositoryCredentialAuthorities(c.tx());
            var administrator = new RepositoryCaller("operator", true);
            if (revokeKey) credentials.register(administrator, binding, caller.principalName());
            var original = DocumentSchemaRetentionFixture.prepare(c);
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            setPolicy(c, fixture.address(), POLICY);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(caller, fixture.address(), revision);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try {
                var member = member(fixture, history);
                long currentRevision = new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(fixture.address())).orElseThrow().mutationRevision;
                member = member.toBuilder().setOwnership(member.getOwnership().toBuilder().setSecurity(POLICY))
                        .setDestination(member.getDestination().toBuilder().setExpectedMutationRevision(currentRevision)).build();
                var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setMembers(0, member).build());
                var key = new RepositoryOperationLedger.Key("account", "scoped", command.operationId());
                var placement = original.prepared().members().getFirst().placement();
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
                var loseReply = new java.util.concurrent.atomic.AtomicBoolean();
                var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                    if (!loseReply.get()) return;
                    long starts = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM repository_publication_assessment_starts WHERE operation_id=:id")
                            .setParameter("id", key.operationId()).getSingleResult()).longValue());
                    if (starts == 1 && loseReply.compareAndSet(true, false))
                        throw new java.sql.SQLException("injected START acknowledgement loss");
                });
                try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                            Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                     var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE)) {
                    var registration = DocumentPublicationRegistration.historical(new Tx(emf), budget, record, sources,
                            UUID.randomUUID(), new DocumentPublicationScopeCalls(), new DriveLedger(c.tx()), RepositoryReadControl.NONE);
                    var modes = Map.of("member", DocumentPublicationCandidate.Mode.TYPED);
                    List<?> before = List.of();
                    RepositoryOperationLedger.Owner registeredOwner = null;
                    if (!beforeRegistration) {
                        var owner = registration.admitInitial(caller, modes, RepositoryReadControl.NONE).orElseThrow();
                        registeredOwner = owner;
                        assertThat(registration.admitInitial(caller, modes, RepositoryReadControl.NONE)).contains(owner);
                        try (var execution = registration.historicalExecution(caller, owner, modes, RepositoryReadControl.NONE)) {
                            assertThat(budget.reservedBytes()).isPositive();
                        }
                        before = leases(c, key);
                    }
                    var writeOnly = POLICY.toBuilder().clearPermissions().addPermissions(POLICY.getPermissions(1)).build();
                    if (beforeRegistration) setPolicy(c, fixture.address(), writeOnly);
                    else {
                        try (var execution = registration.historicalExecution(caller, registeredOwner, modes, RepositoryReadControl.NONE)) {
                            if (loseStartReply) {
                                loseReply.set(true);
                                assertThatThrownBy(() -> execution.start(caller, Duration.ofMinutes(5), RepositoryReadControl.NONE))
                                        .hasStackTraceContaining("injected START acknowledgement loss");
                                assertThat(execution.progress().acknowledgedStart()).isEmpty();
                            }
                            if (revokeKey) credentials.revoke(administrator, binding, caller.principalName());
                            else setPolicy(c, fixture.address(), writeOnly);
                            denied(denial, () -> execution.admitUploads(caller, RepositoryReadControl.NONE));
                            denied(denial, () -> execution.start(caller, Duration.ofMinutes(5), RepositoryReadControl.NONE));
                            long starts = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM repository_publication_assessment_starts WHERE operation_id=:id")
                                    .setParameter("id", key.operationId()).getSingleResult()).longValue());
                            assertThat(starts).isEqualTo(loseStartReply ? 1 : 0);
                            assertThat(execution.progress().acknowledgedStart()).isEmpty();
                            for (String table : List.of("repository_schema_artifact_claims", "document_assessment_owners",
                                    "document_revision_commits")) {
                                long downstream = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                                        "SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                                        .setParameter("id", key.operationId()).getSingleResult()).longValue());
                                assertThat(downstream).as(table).isZero();
                            }
                        }
                    }
                    denied(denial, () -> registration.admitInitial(caller, modes, RepositoryReadControl.NONE));
                    if (beforeRegistration) for (String table : List.of("repository_execution_claims", "repository_coordinator_bindings",
                            "repository_publication_preparations", "repository_preparation_history_sets",
                            "repository_preparation_history_roots", "repository_publication_modes",
                            "repository_operations", "repository_operation_owners")) {
                        long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                                .setParameter("id", key.operationId()).getSingleResult()).longValue());
                        assertThat(count).as(table).isZero();
                    }
                    if (!beforeRegistration) {
                        var owner = registeredOwner;
                        denied(denial, () -> registration.historicalExecution(caller, owner, modes, RepositoryReadControl.NONE));
                        assertThat(leases(c, key)).isEqualTo(before);
                        assertThat(new RepositoryOperationLedger(c.tx()).find(key)).isPresent();
                    }
                }
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                release(reads, history);
            }
        }
    }

    private static void denied(RepositoryException.Code code, org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(code));
    }

    private static void setPolicy(Context c, NodeAddress address, DocumentSecurity policy) throws Exception {
        String json = com.google.protobuf.util.JsonFormat.printer().print(policy);
        c.tx().inTransaction(em -> {
            em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", json).setParameter("node", DocumentIds.nodeId(address)).executeUpdate();
        });
    }

    private static List<?> leases(Context c, RepositoryOperationLedger.Key key) {
        return c.tx().readOnly(em -> Arrays.asList((Object[]) em.createNativeQuery("""
                SELECT c.lease_until,o.lease_until FROM repository_execution_claims c
                JOIN repository_operation_owners o USING(account_id,principal,operation_id) WHERE c.operation_id=:id
                """).setParameter("id", key.operationId()).getSingleResult()));
    }
}
