package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.Objects;

/** Explicit per-lock and per-statement limits, not a whole-operation or network deadline. */
record SqlTimeouts(Duration lockWait, Duration statement) {
    SqlTimeouts {
        milliseconds(lockWait);
        milliseconds(statement);
        if (lockWait.compareTo(statement) > 0)
            throw new IllegalArgumentException("SQL lock wait must not exceed statement timeout");
    }

    void apply(EntityManager em) {
        // Transaction-local settings revert on both commit and rollback, including pooled connections.
        em.createNativeQuery("""
                SELECT pg_catalog.set_config('lock_timeout',:lock,true),pg_catalog.set_config('statement_timeout',:statement,true)
                """).setParameter("lock", milliseconds(lockWait) + "ms")
                .setParameter("statement", milliseconds(statement) + "ms").getSingleResult();
    }

    private static long milliseconds(Duration value) {
        Objects.requireNonNull(value);
        if (value.compareTo(Duration.ofMillis(1)) < 0 || value.compareTo(Duration.ofDays(1)) > 0
                || value.getNano() % 1_000_000 != 0)
            throw new IllegalArgumentException("SQL timeout requires whole milliseconds from one millisecond to one day");
        return value.toMillis();
    }
}
