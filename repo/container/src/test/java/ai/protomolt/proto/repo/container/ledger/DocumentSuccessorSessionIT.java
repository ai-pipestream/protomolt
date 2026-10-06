package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real journal/owner lifecycle; manager/provider execution is qualified separately. */
@Testcontainers
class DocumentSuccessorSessionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void attachesInstalledIdentityAndRestoresStickyAssessmentStart() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            var session = session(c, plan, budget, new DocumentPublicationScopeCalls());
            assertThat(session.seeds()).isEqualTo(plan.next().seeds());
            assertThat(session.discardableBeforeRegistration()).isFalse();
            var owner = session.admit(CALLER, NONE).orElseThrow();
            assertThat(owner.generation()).isEqualTo(2);
            assertThat(owner.token()).isEqualTo(plan.next().seeds().ownerNonce());
            assertThat(owner.executionClaim().orElseThrow().epoch()).isEqualTo(2);
            assertThat(session.admit(CALLER, NONE)).contains(owner);
            final DocumentAssessmentStartJournal.Started started;
            try (var execution = session.begin(CALLER, NONE)) {
                assertThat(execution.assessmentStageStarted()).isFalse();
                started = execution.beginAssessmentStage(CALLER, owner, LEASE, NONE);
            }
            var restored = session(c, plan, budget, new DocumentPublicationScopeCalls());
            assertThat(restored.admit(CALLER, NONE)).contains(owner);
            try (var execution = restored.begin(CALLER, NONE)) {
                assertThat(execution.assessmentStageStarted()).isTrue();
                assertThatThrownBy(() -> execution.beginAssessmentStage(CALLER, owner, LEASE, NONE))
                        .hasMessageContaining("already started");
            }
            var retained = new DocumentAssessmentStartJournal(c.tx(), budget).load(CALLER, owner, plan.next().command(), NONE).orElseThrow();
            assertThat(retained).isEqualTo(started);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void failedUnactivatedAttachmentRetainsDrainIdentityForReconciliation() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            var session = session(c, plan, budget, new DocumentPublicationScopeCalls());
            assertThatThrownBy(() -> session.admit(CALLER, NONE)).hasStackTraceContaining("locally drained");
            assertThat(session.discardableBeforeRegistration()).isFalse();
            var identity = session.drainIdentity().orElseThrow();
            assertThat(identity.epoch()).isEqualTo(2);
            assertThat(identity.token()).isEqualTo(plan.reservation().successorToken());
            assertThat(identity.incarnation()).isEqualTo(plan.reservation().successorIncarnation());
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            assertThat(session.admit(CALLER, NONE).orElseThrow().generation()).isEqualTo(2);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void currentScopedRightsCannotBeReplacedWithAdministrativeActivation() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            var session = session(c, plan, budget, new DocumentPublicationScopeCalls());
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> session.admit(scoped, NONE)).isInstanceOfSatisfying(RepositoryException.class,
                    e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(session.drainIdentity()).isPresent();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void coordinatorAndModesCannotChange() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            assertThatThrownBy(() -> DocumentPublicationSession.successor(c.tx(), CALLER, plan, budget,
                    UUID.randomUUID(), new DocumentPublicationScopeCalls())).hasMessageContaining("incarnation");
            var session = session(c, plan, budget, new DocumentPublicationScopeCalls());
            try (var execution = session.begin(CALLER, NONE)) {
                assertThatThrownBy(() -> execution.bindModes(Map.of("member-0", DocumentPublicationCandidate.Mode.OPAQUE)))
                        .hasMessageContaining("modes changed");
            }
        }
    }

    @Test void closedRegistrationBarrierAndLocalDrainPreventAttachment() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000); var calls = new DocumentPublicationScopeCalls();
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            var session = session(c, plan, budget, calls);
            session.admit(CALLER, NONE);
            calls.close();
            assertThatThrownBy(() -> session.admit(CALLER, NONE)).isInstanceOfSatisfying(RepositoryException.class,
                    e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
            var identity = session.drainIdentity().orElseThrow();
            assertThat(RepositoryCoordinatorDrain.beginRetained(c.tx(), CALLER, identity, NONE)).isTrue();
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, identity, NONE);
            var another = session(c, plan, budget, new DocumentPublicationScopeCalls());
            assertThatThrownBy(() -> another.admit(CALLER, NONE)).hasStackTraceContaining("locally drained");
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void terminalOwnerCannotBeAttachedAgain() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            var session = session(c, plan, budget, new DocumentPublicationScopeCalls());
            var owner = session.admit(CALLER, NONE).orElseThrow();
            new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, plan.next().command(), NONE);
            assertThatThrownBy(() -> session.admit(CALLER, NONE)).isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void ownerTupleMustMatchClaimScopeAndPositiveGeneration() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            var owner = session(c, plan, budget, new DocumentPublicationScopeCalls()).admit(CALLER, NONE).orElseThrow();
            var other = new RepositoryOperationLedger.Key(owner.key().account(), owner.key().principal(), UUID.randomUUID());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryOperationLedger.lockLiveOwner(em, other, owner.generation(), owner.token(), owner.executionClaim());
            })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("claim scope");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryOperationLedger.lockLiveOwner(em, owner.key(), 0, owner.token(), owner.executionClaim());
            })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("claim scope");
        }
    }

    private static RepositorySuccessorInstall.Plan installed(Context c) {
        var plan = RepositorySuccessorInstallIT.plan(c);
        RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
        return plan;
    }
    private static DocumentPublicationSession session(Context c, RepositorySuccessorInstall.Plan plan,
            PayloadBudget budget, DocumentPublicationScopeCalls calls) {
        return DocumentPublicationSession.successor(c.tx(), CALLER, plan, budget, plan.reservation().successorIncarnation(), calls);
    }
}
