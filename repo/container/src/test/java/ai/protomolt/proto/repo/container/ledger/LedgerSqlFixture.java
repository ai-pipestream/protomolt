package ai.protomolt.proto.repo.container.ledger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Migrated ledger schema in a per-test PostgreSQL schema plus the pool and transaction wrapper over it. */
final class LedgerSqlFixture {

    record Context(HikariDataSource pool, EntityManagerFactory emf, Tx tx) implements AutoCloseable {
        public void close() { try { emf.close(); } finally { pool.close(); } }
    }

    static Context context(PostgreSQLContainer postgres) {
        return context(postgres, "latest");
    }

    static Context context(PostgreSQLContainer postgres, String target) {
        String schema = "ledger_" + UUID.randomUUID().toString().replace("-", "");
        Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target(target).load().migrate();
        var config = new HikariConfig(); config.setJdbcUrl(postgres.getJdbcUrl()); config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword()); config.setSchema(schema); config.setMaximumPoolSize(3);
        var pool = new HikariDataSource(config);
        try {
            var emf = Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", pool, "hibernate.hbm2ddl.auto", "validate"));
            return new Context(pool, emf, new Tx(emf));
        } catch (RuntimeException | Error failure) { pool.close(); throw failure; }
    }
}
