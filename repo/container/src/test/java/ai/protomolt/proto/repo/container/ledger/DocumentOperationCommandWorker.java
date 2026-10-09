package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Fresh JVM receives only database connection, authenticated scope and operation ID. */
public final class DocumentOperationCommandWorker {
    public static void main(String[] args) {
        var env = System.getenv();
        var config = new HikariConfig();
        config.setJdbcUrl(env.get("TEST_DB_URL")); config.setUsername(env.get("TEST_DB_USER"));
        config.setPassword(env.get("TEST_DB_PASSWORD")); config.setSchema(env.get("TEST_DB_SCHEMA"));
        config.setMaximumPoolSize(1);
        try (var pool = new HikariDataSource(config);
                var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                        Map.of("hibernate.connection.datasource", pool, "hibernate.hbm2ddl.auto", "validate"))) {
            var caller = new RepositoryCaller(args[1], false, Set.of(args[0]), Set.of());
            var command = new DocumentOperationCommands(new Tx(emf)).load(caller, args[0], UUID.fromString(args[2]), RepositoryReadControl.NONE)
                    .orElseThrow();
            System.out.println("COMMAND_OK|" + command.operationId() + "|" + command.sha256() + "|" + command.intent().getMembersCount());
        }
    }
}
