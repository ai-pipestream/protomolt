package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Persisted-command recovery over PostgreSQL; seed object observations are synthetic fixtures. */
@Testcontainers
class DocumentOperationCommandsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", false, Set.of("account"), Set.of());

    @Test void reloadsExactCommandInFreshJvmWithoutOriginalRequest(@TempDir Path temp) throws Exception {
        try (var c = context(POSTGRES)) {
            var expected = prepare(c, 2, true).command();
            var reader = new DocumentOperationCommands(new Tx(c.emf()));
            var actual = reader.load(CALLER, "account", expected.operationId(), RepositoryReadControl.NONE).orElseThrow();
            assertThat(actual.intent()).isEqualTo(expected.intent());
            assertThat(actual.canonical()).isEqualTo(expected.canonical());
            var output = temp.resolve("command-worker.log");
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", java.util.Objects.requireNonNull(System.getProperty("protomolt.test.runtimeClasspath")),
                    DocumentOperationCommandWorker.class.getName(), "account", "principal", expected.operationId().toString())
                    .redirectErrorStream(true).redirectOutput(output.toFile());
            builder.environment().putAll(Map.of("TEST_DB_URL", POSTGRES.getJdbcUrl(), "TEST_DB_USER", POSTGRES.getUsername(),
                    "TEST_DB_PASSWORD", POSTGRES.getPassword(), "TEST_DB_SCHEMA", c.pool().getSchema()));
            var worker = builder.start();
            try {
                assertThat(worker.waitFor(45, TimeUnit.SECONDS)).as("fresh command reader; log: %s", output).isTrue();
                assertThat(worker.exitValue()).as("fresh command reader status; log: %s", output).isZero();
            } finally {
                if (worker.isAlive()) {
                    worker.destroyForcibly();
                    assertThat(worker.waitFor(5, TimeUnit.SECONDS)).isTrue();
                }
            }
            assertThat(Files.readString(output)).contains("COMMAND_OK|" + expected.operationId() + "|" + expected.sha256() + "|2");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"malformed", "member-order", "unknown-field", "operation-id", "wrong-account", "invalid-intent", "codec", "version"})
    void refusesUnsupportedOrCorruptStoredCommands(String fault) {
        try (var c = context(POSTGRES)) {
            var expected = prepare(c, 2, true).command();
            var semantic = expected.intent().toBuilder().clearOperationId();
            String codec = DocumentPublicationCommand.CODEC;
            int version = DocumentPublicationCommand.ENCODING_VERSION;
            String account = "account";
            ByteString bytes = expected.canonical();
            switch (fault) {
                case "malformed" -> bytes = ByteString.copyFrom(new byte[]{(byte) 0x80});
                case "member-order" -> bytes = semantic.clearMembers().addMembers(expected.intent().getMembers(1))
                        .addMembers(expected.intent().getMembers(0)).build().toByteString();
                case "unknown-field" -> bytes = semantic.setUnknownFields(UnknownFieldSet.newBuilder().addField(50000,
                        UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build().toByteString();
                case "operation-id" -> bytes = expected.intent().toByteString();
                case "wrong-account" -> account = "another-account";
                case "invalid-intent" -> bytes = semantic.clearMembers().build().toByteString();
                case "codec" -> codec = "another-command";
                case "version" -> version = 2;
                default -> throw new AssertionError(fault);
            }
            var key = new RepositoryOperationLedger.Key(account, "principal", UUID.randomUUID());
            new RepositoryOperationLedger(c.tx()).admit(key, new RepositoryOperationLedger.EncodedCommand(codec, version, bytes),
                    UUID.randomUUID(), Duration.ofMinutes(5));
            var code = fault.equals("codec") || fault.equals("version") ? RepositoryException.Code.UNSUPPORTED : RepositoryException.Code.DATA_LOSS;
            assertThatThrownBy(() -> new DocumentOperationCommands(c.tx()).load(new RepositoryCaller("principal", true),
                    key.account(), key.operationId(), RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(code));
        }
    }

    @Test void scopesReadsToAuthenticatedPrincipalAndChecksCancellation() {
        try (var c = context(POSTGRES)) {
            var expected = prepare(c, 2, true).command();
            var reader = new DocumentOperationCommands(c.tx());
            assertThat(reader.load(new RepositoryCaller("another-principal", false, Set.of("account"), Set.of()),
                    "account", expected.operationId(), RepositoryReadControl.NONE)).isEmpty();
            assertThatThrownBy(() -> reader.load(new RepositoryCaller("principal", false, Set.of("other"), Set.of()),
                    "account", expected.operationId(), RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            for (int cancelAt : new int[]{2, 3}) {
                var checks = new java.util.concurrent.atomic.AtomicInteger();
                assertThatThrownBy(() -> reader.load(CALLER, "account", expected.operationId(), new RepositoryReadControl() {
                    @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public boolean isCancelled() { return checks.incrementAndGet() >= cancelAt; }
                })).isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            }
        }
    }

    @Test void detectsDigestCorruptionBeyondNormalDatabaseConstraints() {
        try (var c = context(POSTGRES)) {
            var expected = prepare(c, 2, true).command();
            // Isolated test-schema corruption. Ordinary writes cannot bypass these guards.
            c.tx().inTransaction(em -> {
                String constraint = (String) em.createNativeQuery("""
                        SELECT conname FROM pg_constraint WHERE conrelid='repository_operations'::regclass
                        AND contype='c' AND pg_get_constraintdef(oid) LIKE '%command_sha256%'
                        """).getSingleResult();
                assertThat(constraint).matches("[a-z_][a-z0-9_]*");
                em.createNativeQuery("ALTER TABLE repository_operations DROP CONSTRAINT \"" + constraint + "\"").executeUpdate();
                em.createNativeQuery("SET LOCAL session_replication_role=replica").executeUpdate();
                em.createNativeQuery("UPDATE repository_operations SET command_sha256=decode(repeat('00',32),'hex') WHERE operation_id=:id")
                        .setParameter("id", expected.operationId()).executeUpdate();
                return null;
            });
            assertThatThrownBy(() -> new DocumentOperationCommands(c.tx()).load(CALLER, "account", expected.operationId(), RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
        }
    }
}
