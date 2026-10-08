package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
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

/** Real composition, PostgreSQL registration and provider lifecycle; no termination proof is inferred. */
@Testcontainers
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class ReaderHostCompositionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer STORAGE = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @Container static final org.testcontainers.containers.GenericContainer<?> REDIS =
            new org.testcontainers.containers.GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    private static RepoServiceConfig config() {
        return new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                STORAGE.getEndpoint().toString(), STORAGE.getRegion(), STORAGE.getAccessKey(), STORAGE.getSecretKey(),
                "host-binding", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("host-" + UUID.randomUUID(), "host-realm", true));
    }

    private static ReaderHostOptions identity() {
        return new ReaderHostOptions(UUID.randomUUID(), "composition-test", UUID.randomUUID().toString());
    }

    private static RepoServices open(RepoServiceConfig config, ReaderHostOptions identity) {
        return RepoServices.buildHosted(config, BridgeEngine.standard(), null, null, null, identity);
    }

    @Test void bothReadersShareIdentityAndDuplicateStartupCannotFenceTheLiveHost() throws Exception {
        var config = config();
        var identity = identity();
        try (var host = open(config, identity)) {
            assertThat(host.documentHistory()).isNotNull();
            assertThat(readers(identity, "ACTIVE")).isEqualTo(2);
            assertThat(hostState(identity)).isEqualTo("ACTIVE");
            assertThatThrownBy(() -> open(config, identity)).isInstanceOf(RuntimeException.class);
            assertThat(hostState(identity)).isEqualTo("ACTIVE");
            assertThat(readers(identity, "ACTIVE")).isEqualTo(2);
        }
        assertThat(hostState(identity)).isEqualTo("FENCED");
        assertThat(readers(identity, "ACTIVE")).isZero();
        assertThat(readers(identity, "QUIESCED")).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM repository_reader_host_terminations WHERE execution=?", identity)).isZero();
        assertThatThrownBy(() -> open(config, identity)).isInstanceOf(RuntimeException.class);
        assertThat(readers(identity, "QUIESCED")).isEqualTo(2);
        var replacement = identity();
        try (var host = open(config, replacement)) {
            assertThat(readers(replacement, "ACTIVE")).isEqualTo(2);
        }
        assertThat(hostState(replacement)).isEqualTo("FENCED");
    }

    @Test void partialStartupDrainsRegisteredReaderAndFencesOnlyItsOwnHost() throws Exception {
        var config = config();
        var identity = identity();
        try (var database = new ai.protomolt.proto.repo.container.ledger.LedgerDatabase(config.ledger())) {
            execute("""
                    CREATE FUNCTION reject_composition_document_reader() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF NEW.host_execution = '%s'::uuid AND NEW.state='ACTIVE'
                         AND EXISTS(SELECT 1 FROM repository_reader_incarnations
                                    WHERE host_execution=NEW.host_execution AND state='ACTIVE') THEN
                        RAISE EXCEPTION 'rejected composition document reader';
                      END IF;
                      RETURN NEW;
                    END $$
                    """.formatted(identity.execution()));
            execute("CREATE TRIGGER reject_composition_document_reader BEFORE INSERT ON repository_reader_incarnations FOR EACH ROW EXECUTE FUNCTION reject_composition_document_reader()");
            try {
                assertThatThrownBy(() -> open(config, identity))
                        .hasStackTraceContaining("rejected composition document reader");
                assertThat(hostState(identity)).isEqualTo("FENCED");
                assertThat(readers(identity, "ACTIVE")).isZero();
                // Archive drained locally, failed document registration reserved a terminal tombstone.
                assertThat(readers(identity, "QUIESCED")).isEqualTo(2);
                assertThat(count("SELECT count(*) FROM repository_reader_host_terminations WHERE execution=?", identity)).isZero();
            } finally {
                execute("DROP TRIGGER reject_composition_document_reader ON repository_reader_incarnations");
                execute("DROP FUNCTION reject_composition_document_reader()");
            }
        }
    }

    @Test void identityRejectsMissingAndUnboundedSupervisorValues() {
        assertThatThrownBy(() -> new ReaderHostOptions(null, "host", "boot")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ReaderHostOptions(UUID.randomUUID(), " ", "boot"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReaderHostOptions(UUID.randomUUID(), "host", "b".repeat(513)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void boundedArchiveBindsOnlyItsOwnReader() throws Exception {
        var env = RepoBoundedArchiveMainTest.environment();
        env.put(LedgerConfig.ENV_JDBC_URL, POSTGRES.getJdbcUrl());
        env.put(LedgerConfig.ENV_USERNAME, POSTGRES.getUsername());
        env.put(LedgerConfig.ENV_PASSWORD, POSTGRES.getPassword());
        env.put(RepoServiceConfig.ENV_REDIS_URI, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        env.put(ManagedStoragePolicy.ENV_GENERATION, "bounded-host-" + UUID.randomUUID());
        var identity = identity();
        var limits = new BoundedArchiveOptions(1024, 4096, 4, 65536, 2);
        try (var host = RepoServices.buildBoundedArchiveHosted(RepoServiceConfig.fromEnvironment(env), limits, identity)) {
            assertThat(host.archiveRepository()).isNotNull();
            assertThat(readers(identity, "ACTIVE")).isEqualTo(1);
            assertThatThrownBy(host::documentHistory).isInstanceOf(IllegalStateException.class)
                    .hasMessage("Managed document storage is not configured");
        }
        assertThat(readers(identity, "QUIESCED")).isEqualTo(1);
        assertThat(hostState(identity)).isEqualTo("FENCED");
    }

    @Test void blockedHostFenceRetainsDatabaseForCloseRetry() throws Exception {
        var identity = identity();
        try (var host = open(config(), identity)) {
            try (var holder = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
                holder.setAutoCommit(false);
                try (var lock = holder.prepareStatement("SELECT execution FROM repository_reader_host_executions WHERE execution=? FOR UPDATE")) {
                    lock.setObject(1, identity.execution());
                    try (var rows = lock.executeQuery()) { assertThat(rows.next()).isTrue(); }
                }
                long started = System.nanoTime();
                assertThatThrownBy(() -> host.close(java.time.Duration.ofMillis(500)))
                        .isInstanceOf(RuntimeException.class).hasStackTraceContaining("timeout");
                assertThat(java.time.Duration.ofNanos(System.nanoTime() - started)).isLessThan(java.time.Duration.ofSeconds(5));
                assertThat(readers(identity, "QUIESCED")).isEqualTo(2);
                assertThat(hostState(identity)).isEqualTo("ACTIVE");
                holder.rollback();
            }
            // Success requires the same borrowed database to have survived the failed close.
        }
        assertThat(hostState(identity)).isEqualTo("FENCED");
    }

    private static long readers(ReaderHostOptions identity, String state) throws Exception {
        return count("SELECT count(*) FROM repository_reader_incarnations WHERE host_execution=? AND state='" + state + "'", identity);
    }

    private static long count(String sql, ReaderHostOptions identity) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var query = connection.prepareStatement(sql)) {
            query.setObject(1, identity.execution());
            try (var rows = query.executeQuery()) { assertThat(rows.next()).isTrue(); return rows.getLong(1); }
        }
    }

    private static String hostState(ReaderHostOptions identity) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var query = connection.prepareStatement("SELECT state,host_identity,boot_identity FROM repository_reader_host_executions WHERE execution=?")) {
            query.setObject(1, identity.execution());
            try (var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(2)).isEqualTo(identity.hostIdentity());
                assertThat(rows.getString(3)).isEqualTo(identity.bootIdentity());
                return rows.getString(1);
            }
        }
    }

    private static void execute(String sql) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.createStatement()) { statement.execute(sql); }
    }
}
