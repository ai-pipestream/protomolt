package ai.pipestream.proto.jobs.service.store;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * The workflow-runs store's persistence bootstrap and transaction wrapper:
 * HikariCP pool → Flyway migration, in that order, then plain JDBC. Same
 * shape as the account store's database wrapper: the pool is sized to what
 * the database can serve (virtual threads make the pool, not the thread
 * count, the concurrency limit), Flyway owns the schema, and every unit of
 * work is one short blocking transaction on a virtual thread.
 * <p>
 * Transaction semantics: {@link #inTransaction} opens a pooled connection,
 * runs the work, commits; on ANY exception it rolls back and rethrows —
 * {@link RuntimeException}s (including {@link WorkflowRunStoreException}s)
 * propagate intact, checked {@link SQLException}s are wrapped with the
 * original as cause. Nothing is swallowed, and the connection always
 * returns to the pool.
 */
public final class WorkflowRunDatabase implements AutoCloseable {

    private final HikariDataSource dataSource;

    /**
     * Boot the store: start the pool, then migrate the schema. Construction
     * fails fast — nothing starts lazily.
     *
     * @param config connection and migration settings
     */
    public WorkflowRunDatabase(WorkflowRunStoreConfig config) {
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("workflow-runs-store");
        hikari.setJdbcUrl(config.jdbcUrl());
        hikari.setUsername(config.username());
        hikari.setPassword(config.password());
        hikari.setMaximumPoolSize(config.maxPoolSize());
        // Fail fast when the pool is exhausted: a caller on a virtual thread
        // costs nothing to queue, but a wedged database should surface as an
        // error quickly, not as a silent hang.
        hikari.setConnectionTimeout(10_000);
        // Retire connections well under the database's own idle/connection
        // lifetime so the server never kills a connection the pool still
        // believes is live.
        hikari.setMaxLifetime(600_000);
        hikari.setIdleTimeout(300_000);
        // Prepared-statement caching lives in the pgjdbc driver (HikariCP
        // deliberately has none of its own).
        hikari.addDataSourceProperty("preparedStatementCacheQueries", "256");
        hikari.addDataSourceProperty("preparedStatementCacheSizeMiB", "5");
        this.dataSource = new HikariDataSource(hikari);

        try {
            Flyway.configure()
                    .dataSource(dataSource)
                    .locations(config.migrationLocation())
                    .load()
                    .migrate();
        } catch (RuntimeException e) {
            // Don't leak the pool if migration fails.
            this.dataSource.close();
            throw e;
        }
    }

    /**
     * Run {@code work} inside one transaction: borrow → autoCommit off →
     * invoke → commit. On any failure, roll back and rethrow.
     *
     * @param work the unit of work
     * @param <T> result type
     * @return the work's result
     */
    public <T> T inTransaction(Function<Connection, T> work) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                T result = work.apply(c);
                c.commit();
                return result;
            } catch (RuntimeException e) {
                rollbackQuietly(c);
                throw e;
            } catch (Exception e) {
                rollbackQuietly(c);
                throw WorkflowRunStoreException.wrap("transactional work failed", e);
            }
        } catch (SQLException e) {
            throw WorkflowRunStoreException.wrap("could not borrow a connection", e);
        }
    }

    /**
     * Run {@code work} on an auto-commit connection — for pure reads where
     * the atomicity and lock footprint of an explicit transaction buy
     * nothing.
     *
     * @param work the read
     * @param <T> result type
     * @return the read's result
     */
    public <T> T readOnly(Function<Connection, T> work) {
        try (Connection c = dataSource.getConnection()) {
            c.setReadOnly(true);
            return work.apply(c);
        } catch (SQLException e) {
            throw WorkflowRunStoreException.wrap("read-only work failed", e);
        }
    }

    /**
     * The underlying pooled datasource, for components that need raw JDBC.
     *
     * @return the HikariCP datasource
     */
    public DataSource dataSource() {
        return dataSource;
    }

    /** Drain the pool. */
    @Override
    public void close() {
        dataSource.close();
    }

    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException rollbackFailure) {
            // The original failure is the one that matters; a rollback
            // failure on top of it must not mask it.
        }
    }
}
