package ai.pipestream.proto.repo.container.ledger;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boot smoke test against a real PostgreSQL: Flyway migrates the schema and
 * Hibernate's {@code hbm2ddl.auto=validate} proves the entity mappings agree
 * with it. If a column, type or table drifts between the entities and
 * V1__initial_schema.sql, this test fails at EMF bootstrap.
 */
@Testcontainers
class LedgerDatabaseIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test
    void bootsMigratesAndValidatesMappings() {
        LedgerConfig config = new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        try (LedgerDatabase database = new LedgerDatabase(config)) {
            assertThat(database.dataSource()).isInstanceOf(com.zaxxer.hikari.HikariDataSource.class);
            assertThat(database.entityManagerFactory().isOpen()).isTrue();
            // Prove the EMF is actually usable, not just constructed.
            try (Tx tx = new Tx(database.entityManagerFactory())) {
                Long driveCount = tx.readOnly(em ->
                        em.createQuery("SELECT COUNT(d) FROM DriveRecord d", Long.class)
                                .getSingleResult());
                assertThat(driveCount).isZero();
            }
        }
    }
}
