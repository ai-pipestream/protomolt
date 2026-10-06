package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL authorization/journal checks; fixture object observations are synthetic, not provider evidence. */
@Testcontainers
class DocumentScopedRegistrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final DocumentSecurity POLICY = DocumentSecurity.newBuilder()
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_WRITE)).build();

    @Test void scopedSessionRegistersAndStartsWithoutGrantingPrivateBootstrapAccess() {
        try (var c = context(POSTGRES)) {
            var input = input(c); var budget = new PayloadBudget(32L * 1024 * 1024);
            var session = session(c.tx(), input, budget);
            try (var execution = session.begin(CALLER, NONE)) {
                execution.bindModes(Map.of("member", DocumentPublicationCandidate.Mode.OPAQUE));
                var owner = session.admit(CALLER, NONE).orElseThrow();
                assertThat(CALLER.processAuthority()).isFalse();
                var claim = owner.executionClaim().orElseThrow();
                denied(() -> new DocumentPublicationPreparationJournal(c.tx(), budget).readCommand(CALLER, owner.key(), 0, NONE));
                denied(() -> new DocumentPublicationModesJournal(c.tx(), budget).load(CALLER, claim, 0, NONE));
                denied(() -> new DocumentPublicationModesJournal(c.tx(), budget)
                        .bind(CALLER, claim, 0, Map.of("member", DocumentPublicationCandidate.Mode.OPAQUE), NONE));
                denied(() -> new DocumentAssessmentStartJournal(c.tx(), budget)
                        .start(CALLER, owner, input.command(), UUID.randomUUID(), Duration.ofHours(1), NONE));
                assertThat(rows(c, "repository_publication_assessment_starts", input.command())).isZero();
                var started = execution.beginAssessmentStage(CALLER, owner, Duration.ofHours(1), NONE);
                assertThat(new DocumentAssessmentStartJournal(c.tx(), budget).load(ADMIN, owner, input.command(), NONE))
                        .contains(started);
                denied(() -> new DocumentAssessmentStartJournal(c.tx(), budget).load(CALLER, owner, input.command(), NONE));
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(strings = {"read", "write", "account", "principal", "absent"})
    void deniedRegistrationCreatesNoClaimOrJournal(String denial) {
        try (var c = context(POSTGRES)) {
            var input = input(c); var budget = new PayloadBudget(32L * 1024 * 1024);
            if (denial.equals("absent")) {
                var member = input.command().intent().getMembers(0).toBuilder();
                member.setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true)
                        .setAddress(member.getDestination().getAddress().toBuilder().setDocId("new-document")));
                input = new Input(new DocumentPublicationCommand(input.command().intent().toBuilder().setMembers(0, member).build()),
                        input.placements(), input.destination(), input.source());
            }
            var session = session(c.tx(), input, budget);
            try (var execution = session.begin(CALLER, NONE)) {
                execution.bindModes(Map.of("member", DocumentPublicationCandidate.Mode.OPAQUE));
            }
            if (denial.equals("read")) setPolicy(c, input.source(), DocumentSecurity.getDefaultInstance());
            if (denial.equals("write")) setPolicy(c, input.destination(), DocumentSecurity.newBuilder()
                    .addPermissions(POLICY.getPermissions(0)).build());
            var caller = switch (denial) {
                case "account" -> new RepositoryCaller("principal", false, Set.of("other"), Set.of());
                case "principal" -> new RepositoryCaller("other", false, Set.of("account"), Set.of());
                default -> CALLER;
            };
            assertThatThrownBy(() -> session.admit(caller, NONE)).isInstanceOf(RepositoryException.class);
            for (String table : new String[]{"repository_execution_claims", "repository_publication_preparations",
                    "repository_publication_modes", "repository_operation_owners"}) {
                assertThat(rows(c, table, input.command())).as(table).isZero();
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void revokedAccessBeforeStagingRefusesMarkerAndActualUploadAdmission() {
        try (var c = context(POSTGRES)) {
            var input = input(c); var budget = new PayloadBudget(32L * 1024 * 1024);
            var session = session(c.tx(), input, budget);
            try (var execution = session.begin(CALLER, NONE)) {
                execution.bindModes(Map.of("member", DocumentPublicationCandidate.Mode.OPAQUE));
                var owner = session.admit(CALLER, NONE).orElseThrow();
                setPolicy(c, input.source(), DocumentSecurity.getDefaultInstance());
                assertThatThrownBy(() -> execution.beginAssessmentStage(CALLER, owner, Duration.ofHours(1), NONE))
                        .isInstanceOf(RepositoryException.class);
                assertThat(execution.assessmentStageStarted()).isFalse();
                assertThat(rows(c, "repository_publication_assessment_starts", input.command())).isZero();
                assertThatThrownBy(() -> new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx()))
                        .admit(CALLER, owner, session.prepared())).isInstanceOf(RepositoryException.class);
                assertThatThrownBy(() -> session.admit(CALLER, NONE)).isInstanceOf(RepositoryException.class);
                assertThat(rows(c, "repository_publication_preparations", input.command())).isEqualTo(1);
                UUID storedOwner = c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                        "SELECT owner_token FROM repository_operation_owners WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult());
                assertThat(storedOwner).isEqualTo(owner.token());
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static DocumentPublicationSession session(Tx tx, Input input, PayloadBudget budget) {
        return DocumentPublicationSession.journaled(tx, CALLER, input.command(), input.placements(), LEASE, budget);
    }

    private static void denied(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
    }

    private static int rows(Context c, String table, DocumentPublicationCommand command) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                .setParameter("id", command.operationId()).getSingleResult()).intValue());
    }

    private static void setPolicy(Context c, DocumentRecord row, DocumentSecurity policy) {
        row.writeSecurity(policy);
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:id")
                .setParameter("policy", row.security).setParameter("id", row.nodeId).executeUpdate(); });
    }

    private record Input(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements,
            DocumentRecord destination, DocumentRecord source) {}

    private static Input input(Context c) {
        var seed = prepare(c, 2);
        for (var source : seed.sources()) setPolicy(c, source.row(), POLICY);
        var destination = new DocumentLedger(c.tx()).findByNodeId(seed.sources().get(0).row().nodeId).orElseThrow();
        var source = new DocumentLedger(c.tx()).findByNodeId(seed.sources().get(1).row().nodeId).orElseThrow();
        var sourceCondition = seed.command().intent().getMembers(1).getDestination().toBuilder()
                .setExpectedMutationRevision(source.mutationRevision).build();
        var member = seed.command().intent().getMembers(0).toBuilder().setMemberId("member")
                .setDestination(seed.command().intent().getMembers(0).getDestination().toBuilder()
                        .setExpectedMutationRevision(destination.mutationRevision))
                .setOwnership(seed.command().intent().getMembers(0).getOwnership().toBuilder().setSecurity(POLICY))
                .clearParts();
        var retained = seed.sources().get(1);
        for (int i = 0; i < retained.slots().size(); i++) member.addParts(DocumentPublicationPart.newBuilder()
                .setSlot(retained.slots().get(i)).setReuse(PublicationReuse.newBuilder().setSource(sourceCondition)
                        .setSourceSlot(retained.slots().get(i)).setObject(retained.identities().get(i))));
        var command = new DocumentPublicationCommand(seed.command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).clearMembers().addMembers(member).build());
        var drive = new DriveLedger(c.tx()).findById(UUID.fromString(member.getDriveId())).orElseThrow();
        return new Input(command, Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow())), destination, source);
    }
}
