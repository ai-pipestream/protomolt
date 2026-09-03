package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link Tx} unit-of-work semantics over scriptable JDK-proxy JPA fakes (no
 * database): begin/commit on success, rollback-and-rethrow on failure with
 * RuntimeExceptions propagated intact and checked failures wrapped in
 * {@link LedgerException}, the EntityManager always closed, and the read-only
 * path never opening a transaction. The database-backed behavior is covered
 * by the ledger ITs.
 */
class TxTest {

    /** Records the JPA lifecycle calls Tx is contracted to make. */
    private static final class FakeJpa {
        int begins;
        int commits;
        int rollbacks;
        int emCloses;
        int emfCloses;
        int plainCreates;
        int hintedCreates;
        boolean txActive = true;
        RuntimeException rollbackFailure;
        Map<?, ?> lastHints;

        final EntityTransaction transaction = proxy(EntityTransaction.class, (p, method, args) -> {
            switch (method.getName()) {
                case "begin" -> begins++;
                case "commit" -> commits++;
                case "rollback" -> {
                    if (rollbackFailure != null) {
                        throw rollbackFailure;
                    }
                    rollbacks++;
                }
                case "isActive" -> {
                    return txActive;
                }
                default -> {
                    return defaultValue(method);
                }
            }
            return null;
        });

        final EntityManager entityManager = proxy(EntityManager.class, (p, method, args) -> {
            switch (method.getName()) {
                case "getTransaction" -> {
                    return transaction;
                }
                case "close" -> emCloses++;
                default -> {
                    return defaultValue(method);
                }
            }
            return null;
        });

        final EntityManagerFactory emf = proxy(EntityManagerFactory.class, (p, method, args) -> {
            switch (method.getName()) {
                case "createEntityManager" -> {
                    if (args == null || args.length == 0) {
                        plainCreates++;
                    } else {
                        hintedCreates++;
                        lastHints = (Map<?, ?>) args[0];
                    }
                    return entityManager;
                }
                case "close" -> emfCloses++;
                default -> {
                    return defaultValue(method);
                }
            }
            return null;
        });

        @SuppressWarnings("unchecked")
        private static <T> T proxy(Class<T> type, InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
        }

        /** Type-appropriate default for the calls Tx never makes. */
        private static Object defaultValue(Method method) {
            Class<?> type = method.getReturnType();
            if (type == boolean.class) {
                return false;
            }
            if (type.isPrimitive() && type != void.class) {
                return 0;
            }
            return null;
        }
    }

    @Test
    void inTransactionBeginsCommitsReturnsTheResultAndClosesTheManager() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);

        String result = tx.inTransaction(em -> {
            assertThat(em).isSameAs(fake.entityManager);
            assertThat(fake.begins).isEqualTo(1); // begun before the work runs
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(fake.begins).isEqualTo(1);
        assertThat(fake.commits).isEqualTo(1);
        assertThat(fake.rollbacks).isZero();
        assertThat(fake.emCloses).isEqualTo(1);
    }

    @Test
    void theConsumerOverloadRunsInsideTheSameCommitUnit() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);

        StringBuilder seen = new StringBuilder();
        tx.inTransaction(em -> {
            seen.append("worked");
        });

        assertThat(seen.toString()).isEqualTo("worked");
        assertThat(fake.commits).isEqualTo(1);
        assertThat(fake.emCloses).isEqualTo(1);
    }

    @Test
    void aRuntimeExceptionRollsBackAndPropagatesIntact() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);
        RuntimeException failure = new RuntimeException("constraint violation");

        // The cast disambiguates Tx's Function/Consumer overloads (a lambda
        // that cannot complete normally matches both) — the same cast the
        // ledger code itself needs.
        assertThatThrownBy(() -> tx.inTransaction((Function<EntityManager, Void>) em -> {
            throw failure;
        })).isSameAs(failure); // intact, not wrapped

        assertThat(fake.commits).isZero();
        assertThat(fake.rollbacks).isEqualTo(1);
        assertThat(fake.emCloses).isEqualTo(1);
    }

    @Test
    void aCheckedFailureIsWrappedInLedgerExceptionWithTheCause() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);
        Exception checked = new Exception("checked failure");

        // Function.apply declares no checked exceptions; the wrap branch is
        // only reachable through a sneaky throw, which is how a bridge or
        // proxy method can still surface one to callers.
        assertThatThrownBy(() -> tx.inTransaction((Function<EntityManager, Void>)
                em -> sneakyThrow(checked)))
                .isInstanceOf(LedgerException.class)
                .hasMessage("transactional work failed")
                .hasCause(checked);

        assertThat(fake.rollbacks).isEqualTo(1);
        assertThat(fake.emCloses).isEqualTo(1);
    }

    @Test
    void aRollbackFailureDoesNotMaskTheOriginalException() {
        FakeJpa fake = new FakeJpa();
        fake.rollbackFailure = new RuntimeException("rollback broke");
        Tx tx = new Tx(fake.emf);
        RuntimeException original = new RuntimeException("the work failed");

        assertThatThrownBy(() -> tx.inTransaction((Function<EntityManager, Void>) em -> {
            throw original;
        })).isSameAs(original);
    }

    @Test
    void noRollbackIsAttemptedWhenTheTransactionIsNoLongerActive() {
        FakeJpa fake = new FakeJpa();
        fake.txActive = false; // e.g. the work already completed the tx itself
        Tx tx = new Tx(fake.emf);
        RuntimeException failure = new RuntimeException("boom");

        assertThatThrownBy(() -> tx.inTransaction((Function<EntityManager, Void>) em -> {
            throw failure;
        })).isSameAs(failure);
        assertThat(fake.rollbacks).isZero();
    }

    @Test
    void readOnlyOpensNoTransactionAndStillClosesTheManager() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);

        String result = tx.readOnly(em -> "read");

        assertThat(result).isEqualTo("read");
        assertThat(fake.begins).isZero();
        assertThat(fake.commits).isZero();
        assertThat(fake.rollbacks).isZero();
        assertThat(fake.plainCreates).isEqualTo(1);
        assertThat(fake.hintedCreates).isZero();
        assertThat(fake.emCloses).isEqualTo(1);
    }

    @Test
    void readOnlyWithHintsUsesTheHintedFactoryCall() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);
        Map<String, Object> hints = Map.of("org.hibernate.readOnly", true);

        tx.readOnly(em -> "read", hints);

        assertThat(fake.hintedCreates).isEqualTo(1);
        assertThat(fake.lastHints).isEqualTo(hints);
        assertThat(fake.plainCreates).isZero();

        // Empty and null hints take the plain creation path.
        tx.readOnly(em -> "read", Map.of());
        tx.readOnly(em -> "read", null);
        assertThat(fake.hintedCreates).isEqualTo(1);
        assertThat(fake.plainCreates).isEqualTo(2);
    }

    @Test
    void readOnlyRethrowsRuntimeExceptionsIntact() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);
        RuntimeException failure = new RuntimeException("bad read");

        assertThatThrownBy(() -> tx.readOnly(em -> {
            throw failure;
        })).isSameAs(failure);
        assertThat(fake.emCloses).isEqualTo(1);
        assertThat(fake.begins).isZero();
    }

    @Test
    void readOnlyWrapsCheckedFailuresInLedgerException() {
        FakeJpa fake = new FakeJpa();
        Tx tx = new Tx(fake.emf);
        Exception checked = new Exception("checked read failure");

        assertThatThrownBy(() -> tx.readOnly(em -> sneakyThrow(checked)))
                .isInstanceOf(LedgerException.class)
                .hasMessage("read-only work failed")
                .hasCause(checked);
        assertThat(fake.emCloses).isEqualTo(1);
    }

    @Test
    void closeClosesTheFactory() {
        FakeJpa fake = new FakeJpa();
        try (Tx tx = new Tx(fake.emf)) {
            tx.readOnly(em -> "x");
        }
        assertThat(fake.emfCloses).isEqualTo(1);
    }

    /** Throws {@code t} regardless of its (checked) type. */
    @SuppressWarnings("unchecked")
    private static <E extends Throwable> Void sneakyThrow(Throwable t) throws E {
        throw (E) t;
    }
}
