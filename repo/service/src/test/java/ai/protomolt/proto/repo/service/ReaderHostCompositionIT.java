package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real composition, PostgreSQL registration and provider lifecycle; no termination proof is inferred. */
@Testcontainers
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class ReaderHostCompositionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final org.testcontainers.containers.GenericContainer<?> REDIS =
            new org.testcontainers.containers.GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    private static Map<String, String> boundedEnv() {
        var env = RepoBoundedArchiveMainTest.environment();
        env.put(LedgerConfig.ENV_JDBC_URL, POSTGRES.getJdbcUrl());
        env.put(LedgerConfig.ENV_USERNAME, POSTGRES.getUsername());
        env.put(LedgerConfig.ENV_PASSWORD, POSTGRES.getPassword());
        env.put(RepoServiceConfig.ENV_REDIS_URI, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        env.put(ManagedStoragePolicy.ENV_GENERATION, "bounded-host-" + UUID.randomUUID());
        return env;
    }

    private static ReaderHostOptions identity() {
        return new ReaderHostOptions(UUID.randomUUID(), "composition-test", UUID.randomUUID().toString());
    }

    private static RepoServices open(Map<String, String> env, ReaderHostOptions identity) {
        return RepoServices.buildBoundedArchiveHosted(RepoServiceConfig.fromEnvironment(env),
                new BoundedArchiveOptions(1024, 4096, 4, 65536, 2), identity);
    }

    @Test void duplicateStartupCannotFenceTheLiveHost() throws Exception {
        var env = boundedEnv();
        var identity = identity();
        try (var host = open(env, identity)) {
            assertThat(host.archiveRepository()).isNotNull();
            assertThat(readers(identity, "ACTIVE")).isEqualTo(1);
            assertThat(hostState(identity)).isEqualTo("ACTIVE");
            assertThatThrownBy(() -> open(env, identity)).isInstanceOf(RuntimeException.class);
            assertThat(hostState(identity)).isEqualTo("ACTIVE");
            assertThat(readers(identity, "ACTIVE")).isEqualTo(1);
        }
        assertThat(hostState(identity)).isEqualTo("FENCED");
        assertThat(readers(identity, "ACTIVE")).isZero();
        assertThat(readers(identity, "QUIESCED")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM repository_reader_host_terminations WHERE execution=?", identity)).isZero();
        assertThatThrownBy(() -> open(env, identity)).isInstanceOf(RuntimeException.class);
        assertThat(readers(identity, "QUIESCED")).isEqualTo(1);
        var replacement = identity();
        try (var host = open(env, replacement)) {
            assertThat(readers(replacement, "ACTIVE")).isEqualTo(1);
        }
        assertThat(hostState(replacement)).isEqualTo("FENCED");
    }

    @Test void partialStartupFencesOnlyItsOwnHost() throws Exception {
        var env = boundedEnv();
        var identity = identity();
        try (var database = new ai.protomolt.proto.repo.container.ledger.LedgerDatabase(
                RepoServiceConfig.fromEnvironment(env).ledger())) {
            execute("""
                    CREATE FUNCTION reject_composition_reader() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF NEW.host_execution = '%s'::uuid AND NEW.state='ACTIVE' THEN
                        RAISE EXCEPTION 'rejected composition reader';
                      END IF;
                      RETURN NEW;
                    END $$
                    """.formatted(identity.execution()));
            execute("CREATE TRIGGER reject_composition_reader BEFORE INSERT ON repository_reader_incarnations FOR EACH ROW EXECUTE FUNCTION reject_composition_reader()");
            try {
                assertThatThrownBy(() -> open(env, identity))
                        .hasStackTraceContaining("rejected composition reader");
                assertThat(hostState(identity)).isEqualTo("FENCED");
                assertThat(readers(identity, "ACTIVE")).isZero();
                assertThat(count("SELECT count(*) FROM repository_reader_host_terminations WHERE execution=?", identity)).isZero();
            } finally {
                execute("DROP TRIGGER reject_composition_reader ON repository_reader_incarnations");
                execute("DROP FUNCTION reject_composition_reader()");
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
        var identity = identity();
        try (var host = open(boundedEnv(), identity)) {
            assertThat(host.archiveRepository()).isNotNull();
            assertThat(readers(identity, "ACTIVE")).isEqualTo(1);
        }
        assertThat(readers(identity, "QUIESCED")).isEqualTo(1);
        assertThat(hostState(identity)).isEqualTo("FENCED");
    }

    @Test void blockedHostFenceRetainsDatabaseForCloseRetry() throws Exception {
        var identity = identity();
        try (var host = open(boundedEnv(), identity)) {
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
                assertThat(readers(identity, "QUIESCED")).isEqualTo(1);
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
