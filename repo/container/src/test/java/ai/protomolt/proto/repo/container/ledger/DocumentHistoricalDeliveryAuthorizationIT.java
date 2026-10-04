package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Cancellation/expiry during a real SQL wait must not admit delivery afterwards. */
@Testcontainers
class DocumentHistoricalDeliveryAuthorizationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void controlIsRecheckedAfterDeliveryAuthorizationWait(boolean deadline) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1);
            var published = publish(c, f, Fault.NONE, em -> {});
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(new RepositoryCaller("reader", true),
                    f.command().intent().getMembers(0).getDestination().getAddress(),
                    UUID.fromString(published.getMembers(0).getRevisionId()));
            var stopped = new AtomicBoolean();
            var control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return !deadline && stopped.get(); }
                @Override public long remainingNanos() { return deadline && stopped.get() ? 0 : Long.MAX_VALUE; }
            };
            try (var blocker = c.emf().createEntityManager();
                    var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                blocker.getTransaction().begin();
                try {
                    blocker.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:node FOR UPDATE")
                            .setParameter("node", f.sources().getFirst().row().nodeId).getSingleResult();
                    int pid = ((Number) blocker.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                    var delivery = executor.submit(() -> history.authorizeDelivery(control));
                    long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    boolean waiting = false;
                    while (System.nanoTime() < end && !delivery.isDone()) {
                        waiting = c.tx().readOnly(em -> !em.createNativeQuery(
                                "SELECT pid FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))")
                                .setParameter("blocker", pid).getResultList().isEmpty());
                        if (waiting) break;
                        Thread.sleep(10);
                    }
                    assertThat(waiting).as("delivery authorization reached the real document row lock").isTrue();
                    stopped.set(true);
                    blocker.getTransaction().rollback();
                    assertThatThrownBy(() -> delivery.get(5, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class)
                            .hasStackTraceContaining(deadline ? "Document read deadline exceeded" : "Document read cancelled");
                } finally {
                    if (blocker.getTransaction().isActive()) blocker.getTransaction().rollback();
                }
            } finally {
                history.close();
                assertThat(history.awaitDrained(Duration.ofSeconds(5))).isTrue();
                history.release(); ledger.fence(); ledger.attestLocalQuiescence();
            }
        }
    }
}
