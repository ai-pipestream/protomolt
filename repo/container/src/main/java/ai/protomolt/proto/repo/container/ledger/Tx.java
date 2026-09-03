package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The transactional {@link EntityManager} wrapper — the ONLY way ledger code
 * talks to the database.
 * <p>
 * Usage contract (this is the whole framework):
 * <ul>
 *   <li>Callers run on <b>virtual threads</b>. Every unit of work is short,
 *   blocking JDBC — exactly what virtual threads are for. Never hold a
 *   transaction open across a slow external call.</li>
 *   <li><b>One EntityManager per unit of work.</b> Each method opens a fresh
 *   EM and ALWAYS closes it, so there is no session leakage and nothing to
 *   share. Entities returned from these methods are detached snapshots —
     * mutate and re-save them deliberately; lazy loading does not survive
 *   the call.</li>
 *   <li>EntityManagers are <b>never shared across threads</b>; the wrapper
 *   itself is thread-safe (it holds only the thread-safe
 *   {@link EntityManagerFactory}), so a single instance serves the whole
 *   service.</li>
 * </ul>
 * Error semantics: on ANY exception the transaction is rolled back (if still
 * active) and the exception propagates — {@link RuntimeException}s (including
 * JPA {@code PersistenceException}s such as constraint violations) are
 * rethrown intact so callers can react to their exact type; anything checked
 * is wrapped in {@link LedgerException} with the original as cause. Nothing
 * is swallowed.
 */
public final class Tx implements AutoCloseable {

    private final EntityManagerFactory emf;

    /**
     * @param emf the entity manager factory to open sessions from
     */
    public Tx(EntityManagerFactory emf) {
        this.emf = emf;
    }

    /**
     * Run {@code work} inside a RESOURCE_LOCAL transaction: open EM → begin →
     * invoke → commit. On any failure, roll back (if active) and rethrow.
     *
     * @param work the unit of work
     * @param <T>  result type
     * @return the work's result (any returned entities are detached)
     */
    public <T> T inTransaction(Function<EntityManager, T> work) {
        EntityManager em = emf.createEntityManager();
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                T result = work.apply(em);
                tx.commit();
                return result;
            } catch (RuntimeException e) {
                rollbackIfActive(tx);
                throw e;
            } catch (Exception e) {
                rollbackIfActive(tx);
                throw new LedgerException("transactional work failed", e);
            }
        } finally {
            em.close();
        }
    }

    /**
     * {@link #inTransaction(Function)} for work with no result.
     *
     * @param work the unit of work
     */
    public void inTransaction(Consumer<EntityManager> work) {
        inTransaction(em -> {
            work.accept(em);
            return null;
        });
    }

    /**
     * Run {@code work} with an EntityManager but NO transaction — for pure
     * reads where the atomicity and lock footprint of a tx buy nothing. The
     * EM is still always closed.
     *
     * @param work the read
     * @param <T>  result type
     * @return the read's result (any returned entities are detached)
     */
    public <T> T readOnly(Function<EntityManager, T> work) {
        return readOnly(work, Map.of());
    }

    /**
     * {@link #readOnly(Function)} with EntityManager creation hints (e.g.
     * Hibernate's {@code org.hibernate.readOnly}).
     *
     * @param work  the read
     * @param hints hints passed to {@link EntityManagerFactory#createEntityManager(Map)}
     * @param <T>   result type
     * @return the read's result (any returned entities are detached)
     */
    public <T> T readOnly(Function<EntityManager, T> work, Map<String, Object> hints) {
        EntityManager em = hints == null || hints.isEmpty()
                ? emf.createEntityManager()
                : emf.createEntityManager(hints);
        try {
            return work.apply(em);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new LedgerException("read-only work failed", e);
        } finally {
            em.close();
        }
    }

    private static void rollbackIfActive(EntityTransaction tx) {
        try {
            if (tx.isActive()) {
                tx.rollback();
            }
        } catch (RuntimeException rollbackFailure) {
            // The original failure is the one that matters; a rollback
            // failure on top of it must not mask it.
        }
    }

    /**
     * Close the underlying {@link EntityManagerFactory}. Provided so a Tx can
     * own the EMF lifecycle in try-with-resources; harmless when the factory
     * is owned elsewhere (e.g. {@link LedgerDatabase}) since
     * {@code EntityManagerFactory.close()} is idempotent.
     */
    @Override
    public void close() {
        emf.close();
    }
}
