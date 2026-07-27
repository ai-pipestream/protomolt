package ai.pipestream.proto.repo.container.ledger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * The ledger's persistence bootstrap: connection pool → schema migration →
 * JPA entity manager factory, in that order.
 * <p>
 * Framework-free wiring: the HikariCP pool is built directly, Flyway owns
 * the schema ({@code db/migration}), and the {@code document-ledger}
 * persistence unit (see {@code META-INF/persistence.xml}) is handed the pool
 * as a live {@link DataSource} instance — the XML deliberately carries no
 * datasource configuration so the same unit works against any environment.
 * Hibernate runs with {@code hbm2ddl.auto=validate}, so entity↔schema drift
 * fails fast at boot instead of at first query.
 * <p>
 * Pool sizing note: callers run on virtual threads, so thread count is NOT a
 * bound on database concurrency — the pool is. Size {@code maxPoolSize} to
 * what the database can actually serve.
 */
public final class LedgerDatabase implements AutoCloseable {

    private final HikariDataSource dataSource;
    private final EntityManagerFactory entityManagerFactory;

    /**
     * Boot the ledger: start the pool, migrate the schema, then build and
     * validate the entity manager factory.
     *
     * @param config connection and migration settings
     * @throws LedgerException if migration or EMF bootstrap fails
     */
    public LedgerDatabase(LedgerConfig config) {
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("document-ledger");
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

            Map<String, Object> properties = new HashMap<>();
            // The persistence unit XML carries no datasource on purpose:
            // the live pool is injected here.
            properties.put("hibernate.connection.datasource", dataSource);
            // Flyway owns the schema; Hibernate only proves the mappings
            // agree with it.
            properties.put("hibernate.hbm2ddl.auto", "validate");
            properties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
            this.entityManagerFactory =
                    Persistence.createEntityManagerFactory("document-ledger", properties);
        } catch (RuntimeException e) {
            // Don't leak the pool if migration/EMF bootstrap fails.
            this.dataSource.close();
            throw e;
        }
    }

    /**
     * The JPA entity manager factory over the ledger pool. Share it (e.g. via
     * a single {@link Tx}) — it is thread-safe and expensive to build.
     *
     * @return the entity manager factory
     */
    public EntityManagerFactory entityManagerFactory() {
        return entityManagerFactory;
    }

    /**
     * The underlying pooled datasource, for components that need raw JDBC.
     *
     * @return the HikariCP datasource
     */
    public DataSource dataSource() {
        return dataSource;
    }

    /** Shut down the entity manager factory, then drain the pool. */
    @Override
    public void close() {
        entityManagerFactory.close();
        dataSource.close();
    }
}
