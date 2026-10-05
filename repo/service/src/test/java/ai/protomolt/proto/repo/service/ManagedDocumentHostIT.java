package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import java.sql.DriverManager;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real host ownership and restart of the native publication reader incarnation. */
@Testcontainers
class ManagedDocumentHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Test void hostOwnsFreshPublicationResourcesAndQuiescesThemBeforeDatabaseClose() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(),
                "document-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("document-host-original", "document-host-realm", true));
        try (var database = new ai.protomolt.proto.repo.container.ledger.LedgerDatabase(config.ledger())) {
            // Migrate before installing a fault around actual host registration SQL.
        }
        sql("""
                CREATE FUNCTION test_fail_second_host_reader() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE state='ACTIVE') THEN
                    RAISE EXCEPTION 'deliberate second host reader registration failure';
                  END IF;
                  RETURN NEW;
                END $$
                """);
        sql("CREATE TRIGGER test_fail_second_host_reader BEFORE INSERT ON repository_reader_incarnations FOR EACH ROW EXECUTE FUNCTION test_fail_second_host_reader()");
        try {
            assertThatThrownBy(() -> RepoServices.build(config))
                    .hasStackTraceContaining("deliberate second host reader registration failure");
            assertThat(count("ACTIVE")).isZero();
            assertThat(count("QUIESCED")).isEqualTo(1);
        } finally {
            sql("DROP TRIGGER test_fail_second_host_reader ON repository_reader_incarnations");
            sql("DROP FUNCTION test_fail_second_host_reader()");
        }
        for (int restart = 0; restart < 2; restart++) {
            try (var host = RepoServices.build(config)) {
                host.startInProcess("document-host-" + UUID.randomUUID());
                assertThat(host.documentPublication()).isNotNull();
                // Managed archive and document readers share the incarnation ledger.
                assertThat(count("ACTIVE")).isEqualTo(2);
                assertThat(count("QUIESCED")).isEqualTo(1 + 2 * restart);
            }
            assertThat(count("ACTIVE")).isZero();
            assertThat(count("QUIESCED")).isEqualTo(1 + 2 * (restart + 1));
        }
    }

    private static long count(String state) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.prepareStatement("SELECT count(*) FROM repository_reader_incarnations WHERE state=?")) {
            query.setString(1, state);
            try (var result = query.executeQuery()) { result.next(); return result.getLong(1); }
        }
    }

    private static void sql(String statement) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.createStatement()) {
            query.execute(statement);
        }
    }
}
