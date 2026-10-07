package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import jakarta.persistence.EntityManagerFactory;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Instruments real PostgreSQL sessions; synthetic part observations do not qualify provider behavior. */
@Testcontainers
class DocumentAssessmentPreflightIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(strings = {"cancelled", "interrupted", "principal", "unauthenticated"})
    void rejectedPreflightDoesNotOpenSqlSession(String variant) {
        try (var c = context(POSTGRES)) {
            var fixture = prepare(c, 1);
            var opened = new AtomicInteger();
            var observedFactory = (EntityManagerFactory) java.lang.reflect.Proxy.newProxyInstance(
                    EntityManagerFactory.class.getClassLoader(), new Class<?>[]{EntityManagerFactory.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("createEntityManager")) opened.incrementAndGet();
                        try { return method.invoke(c.emf(), args); }
                        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var reconciliation = new DocumentAssessmentReconciliation(new Tx(observedFactory));
            var valid = new RepositoryCaller(fixture.owner().key().principal(), true);
            var caller = variant.equals("unauthenticated") ? null : variant.equals("principal")
                    ? new RepositoryCaller("another-principal", true) : valid;
            var budget = new PayloadBudget(32_000_000);
            var assessment = UUID.randomUUID();
            var deadline = Instant.now().plusSeconds(60).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
            Runnable control = () -> { if (variant.equals("cancelled")) throw new CancellationException("test cancelled"); };
            try {
                if (variant.equals("interrupted")) Thread.currentThread().interrupt();
                var failure = catchThrowable(() -> reconciliation.observeRetained(caller, fixture.owner(),
                        fixture.command(), Map.of(), assessment, "00".repeat(32), deadline, budget, control));
                if (variant.equals("cancelled") || variant.equals("interrupted"))
                    assertThat(failure).isInstanceOf(CancellationException.class);
                else assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,
                        e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            } finally { if (variant.equals("interrupted")) Thread.interrupted(); }
            assertThat(opened).hasValue(0);
            assertThat(budget.reservedBytes()).isZero();
            assertThat(reconciliation.observeRetained(valid, fixture.owner(), fixture.command(), Map.of(),
                    assessment, "00".repeat(32), deadline, budget, () -> {})).isEmpty();
            assertThat(opened).hasValue(1);
        }
    }
}
