package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.Context;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Current scoped authority is independent of the coordinating process's privileges. */
@Testcontainers
class ScopedHistoricalSuccessorActivationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller COORDINATOR = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(strings = {"allowed", "creation-revoked", "credential-revoked", "source-revoked", "revoked-after"})
    void activationRechecksLiveAuthorityAfterHistoricalCapture(String scenario) throws Exception {
        try (var c = context(POSTGRES)) {
            var binding = new RepositoryCredentialBinding("historical-successor-test", UUID.randomUUID(), 1);
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of(), Optional.of(binding));
            var authorities = new RepositoryCredentialAuthorities(c.tx());
            authorities.register(COORDINATOR, binding, caller.principalName());
            var grants = new RepositoryCreationGrants(c.tx(), new DriveLedger(c.tx()));
            try (var rig = historicalCreationInitial(c, record -> grants.install(COORDINATOR, RepositoryCreationGrants.prepare(
                    caller, record.command(), record.placements(), (System.currentTimeMillis()+300_000)*1000)))) {
                var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                        .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ)).build();
                setSourcePolicy(c, rig, com.google.protobuf.util.JsonFormat.printer().print(security));
                var plan = installedHistoricalSuccessor(c, rig);
                try (var later = capture(c, rig, caller)) {
                    if (scenario.equals("creation-revoked")) grants.revoke(COORDINATOR, rig.record().key());
                    if (scenario.equals("credential-revoked")) authorities.revoke(COORDINATOR, binding, caller.principalName());
                    if (scenario.equals("source-revoked")) setSourcePolicy(c, rig, "{}");
                    var activation = activation(c.tx(), c, rig, plan, later);
                    if (Set.of("creation-revoked", "credential-revoked", "source-revoked").contains(scenario)) {
                        var expected = scenario.equals("credential-revoked") ? RepositoryException.Code.UNAUTHENTICATED
                                : RepositoryException.Code.NOT_FOUND;
                        assertThatThrownBy(() -> activation.activate(COORDINATOR, caller, NONE))
                                .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(expected));
                        assertThat(count(c, "repository_successor_executions")).isZero();
                        assertThat(count(c, "repository_coordinator_bindings")).isEqualTo(1);
                        assertThat(count(c, "repository_historical_activations")).isZero();
                        assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(1);
                    } else {
                        var capture = activation.activate(COORDINATOR, caller, NONE);
                        if (scenario.equals("revoked-after")) {
                            grants.revoke(COORDINATOR, rig.record().key());
                            authorities.revoke(COORDINATOR, binding, caller.principalName());
                            setSourcePolicy(c, rig, "{}");
                            // Immutable confirmation reports a past commit; it grants no source access.
                            assertThat(activation.activate(COORDINATOR, caller, NONE)).isSameAs(capture);
                            assertThat(RepositoryHistoricalActivationEvidence.confirm(c.tx(), rig.budget(), COORDINATOR,
                                    plan, rig.record(), NONE)).isPresent();
                            assertThatThrownBy(() -> new DocumentReadLedger(c.tx(), UUID.randomUUID())
                                    .captureHistorical(caller, rig.fixture().address(), rig.fixture().revision()))
                                    .isInstanceOf(RepositoryException.class);
                        }
                        assertThat(capture.complete(COORDINATOR, Duration.ZERO, NONE)).isPresent();
                        assertThat(count(c, "repository_historical_activations")).isEqualTo(1);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"creation,before", "credential,before", "source,before",
            "creation,after", "credential,after", "source,after"})
    void successorAttachmentAndHeldExecutionRecheckRevocation(String revoked, String phase) throws Exception {
        try (var c = context(POSTGRES)) {
            var credential = new RepositoryCredentialBinding("successor-execution-test", UUID.randomUUID(), 1);
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of(), Optional.of(credential));
            var authorities = new RepositoryCredentialAuthorities(c.tx());
            authorities.register(COORDINATOR, credential, caller.principalName());
            var grants = new RepositoryCreationGrants(c.tx(), new DriveLedger(c.tx()));
            try (var rig = historicalCreationInitial(c, record -> grants.install(COORDINATOR, RepositoryCreationGrants.prepare(
                    caller, record.command(), record.placements(), (System.currentTimeMillis()+300_000)*1000)))) {
                var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                        .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ)).build();
                setSourcePolicy(c, rig, com.google.protobuf.util.JsonFormat.printer().print(security));
                var plan = installedHistoricalSuccessor(c, rig);
                var scopes = new DocumentPublicationScopeCalls();
                long baseline = rig.budget().reservedBytes();
                try (var later = capture(c, rig, caller); var work = later.sources().work()) {
                    var activation = activation(c.tx(), c, rig, plan, later);
                    var captured = activation.activateAccepted(COORDINATOR, caller, NONE, work);
                    var held = phase.equals("after") ? activation.openExecution(COORDINATOR, caller, work, scopes, NONE) : null;
                    try {
                        switch (revoked) {
                            case "creation" -> grants.revoke(COORDINATOR, rig.record().key());
                            case "credential" -> authorities.revoke(COORDINATOR, credential, caller.principalName());
                            case "source" -> setSourcePolicy(c, rig, "{}");
                            default -> throw new AssertionError(revoked);
                        }
                        var expected = revoked.equals("credential") ? RepositoryException.Code.UNAUTHENTICATED
                                : RepositoryException.Code.NOT_FOUND;
                        assertThatThrownBy(() -> {
                            if (held != null) held.start(caller, Duration.ofMinutes(1), NONE);
                            else try (var unexpected = activation.openExecution(COORDINATOR, caller, work, scopes, NONE)) {
                                throw new AssertionError("Revoked caller attached execution");
                            }
                        }).isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(expected));
                        for (var table : List.of("repository_publication_assessment_starts", "repository_schema_artifact_claims",
                                "document_assessment_owners", "document_revision_commits")) {
                            long rows = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM " + table + " WHERE operation_id=:o")
                                    .setParameter("o", rig.record().key().operationId()).getSingleResult()).longValue());
                            assertThat(rows).as(table).isZero();
                        }
                    } finally { if (held != null) held.close(); }
                    assertThat(scopes.isIdle()).isTrue();
                    assertThat(rig.budget().reservedBytes()).isEqualTo(baseline);
                    work.close();
                    assertThat(captured.complete(COORDINATOR, Duration.ZERO, NONE)).isPresent();
                } finally { scopes.close(); }
            }
        }
    }

    private static void setSourcePolicy(Context c, Rig rig, String policy) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                .setParameter("policy", policy).setParameter("node", ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(rig.fixture().address()))
                .executeUpdate(); });
    }
}
