package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Admission identity over actual SQL; payload/provider observations in the seed are fixtures. */
@Testcontainers
class DocumentPublicationSessionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);

    @Test void committedAdmissionWithLostAcknowledgmentReusesNonceAndEveryAttemptIdentity() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) throw new java.sql.SQLException("Admission acknowledgment lost after commit", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var session = new DocumentPublicationSession(new Tx(emf), CALLER, input.command(), input.placements(), LEASE);
                var plan = session.prepared();
                var attempt = plan.members().getFirst().attempt().orElseThrow();
                assertThat(plan.members().get(1).attempt()).isEmpty();
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isEmpty();
                armed.set(true);
                assertThatThrownBy(() -> session.admit(CALLER, RepositoryReadControl.NONE))
                        .hasStackTraceContaining("Admission acknowledgment lost after commit");
                assertThat(armed).isFalse();
                var committedToken = c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                        "SELECT owner_token FROM repository_operation_owners WHERE operation_id=:id")
                        .setParameter("id", input.command().operationId()).getSingleResult());
                var owner = session.admit(CALLER, RepositoryReadControl.NONE).orElseThrow();
                assertThat(owner.token()).isEqualTo(committedToken);
                assertThat(owner.generation()).isEqualTo(1);
                assertThat(session.prepared()).isSameAs(plan);
                assertThat(session.prepared().members().getFirst().attempt().orElseThrow()).isSameAs(attempt);
                assertThat(session.admit(CALLER, RepositoryReadControl.NONE)).contains(owner); // no implicit lease extension
                var contender = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
                assertThat(contender.admit(CALLER, RepositoryReadControl.NONE)).isEmpty(); // never silently takes over
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command())).orElseThrow().generation()).isEqualTo(1);
            }
        }
    }

    @Test void cancellationAfterAdmissionRetainsIdentityForExactRetry() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
            var plan = session.prepared();
            var checks = new AtomicInteger();
            assertThatThrownBy(() -> session.admit(CALLER, new RepositoryReadControl() {
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public boolean isCancelled() { return checks.incrementAndGet() >= 2; }
            })).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isPresent();
            assertThat(session.admit(CALLER, RepositoryReadControl.NONE)).isPresent();
            assertThat(session.prepared()).isSameAs(plan);
        }
    }

    @Test void callerBindingsAreRecheckedBeforeEveryAdmissionWithoutWritingOtherScope() {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command(), input.placements(), LEASE);
            var wrongPrincipal = new RepositoryCaller("another", true);
            var wrongAccount = new RepositoryCaller("principal", false, java.util.Set.of("another"), java.util.Set.of());
            for (var caller : java.util.List.of(wrongPrincipal, wrongAccount)) {
                assertThatThrownBy(() -> session.admit(caller, RepositoryReadControl.NONE)).isInstanceOf(RepositoryException.class);
                assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command()))).isEmpty();
            }
            assertThatThrownBy(() -> new DocumentPublicationSession(c.tx(), wrongAccount, input.command(), input.placements(), LEASE))
                    .isInstanceOf(RepositoryException.class);
            assertThat(session.admit(CALLER, RepositoryReadControl.NONE)).isPresent();
        }
    }

    private record Input(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements) {}
    private static Input input(Context c) {
        var seed = prepare(c, 2, true);
        var command = new DocumentPublicationCommand(seed.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var id = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var drive = new DriveLedger(c.tx()).findById(id).orElseThrow();
        var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
        return new Input(command, Map.of(id, DocumentUploadPlan.Placement.sample(drive, "native-test", profile)));
    }
    private static RepositoryOperationLedger.Key key(DocumentPublicationCommand command) {
        return new RepositoryOperationLedger.Key(command.intent().getAccountId(), CALLER.principalName(), command.operationId());
    }
}
