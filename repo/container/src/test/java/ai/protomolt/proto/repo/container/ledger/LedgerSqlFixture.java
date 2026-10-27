package ai.protomolt.proto.repo.container.ledger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.sql.Connection;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Migrated ledger schema in a per-test PostgreSQL schema, the pool and transaction wrapper over it, and raw SQL row fixtures. */
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

    static UUID abandonedDocument(Connection connection) throws Exception {
        UUID attempt = UUID.randomUUID(), node = UUID.randomUUID();
        connection.setAutoCommit(false);
        try {
            execute(connection, """
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                        storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                    VALUES ('%s','%s','account',0,'retention-original','retention-realm','namespace',1,0,
                        gen_random_uuid(),clock_timestamp()+interval '1 second','PLANNING')
                    """.formatted(attempt, node));
            execute(connection, """
                    INSERT INTO document_part_attempt_objects(attempt_id,ordinal,part,sub_key,storage_realm,
                        storage_namespace,object_key,expected_size,expected_sha256,content_type)
                    VALUES ('%s',0,1,'','retention-realm','namespace','documents/account/%s/attempts/%s/core',0,'%s','text/plain')
                    """.formatted(attempt, node, attempt, "a".repeat(64)));
            execute(connection, "UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id='" + attempt + "'");
            connection.commit();
        } finally { connection.rollback(); connection.setAutoCommit(true); }
        try (var statement = connection.createStatement(); var result = statement.executeQuery(
                "SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.05) FROM document_part_attempts WHERE attempt_id='" + attempt + "'")) {
            assertThat(result.next()).isTrue();
        }
        execute(connection, "INSERT INTO document_part_attempt_cleanup(attempt_id,cleanup_token,claim_until,state) VALUES ('" + attempt + "',gen_random_uuid(),clock_timestamp()+interval '1 minute','DELETING')");
        return attempt;
    }

    static void execute(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement()) { statement.executeUpdate(sql); }
    }

    static long number(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue(); return result.getLong(1);
        }
    }
}
