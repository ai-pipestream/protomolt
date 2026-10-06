package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL and child-process death at initial registration; no provider or automatic takeover claim. */
@Testcontainers
class DocumentInitialRegistrationCrashIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @ParameterizedTest @ValueSource(strings = {"before", "after"})
    void processDeathPreservesAtomicInitialRegistration(String phase, @TempDir Path temp) throws Exception {
        try (var c = context(POSTGRES)) {
            var source = prepare(c, 2, true);
            var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
            var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
            var drive = UUID.fromString(command.intent().getMembers(0).getDriveId());
            var placement = DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(drive).orElseThrow(),
                    "native-test", new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
            var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                    Map.of(drive, placement), Duration.ofMinutes(5), 0);
            var bytes = DocumentPublicationPreparationCodec.encode(record).toByteArray();
            var input = temp.resolve("writer-only-input.bin"); Files.write(input, bytes);
            var token = UUID.randomUUID();
            run(c, temp.resolve("writer.log"), phase.equals("before") ? 81 : 82,
                    "write-" + phase, command.operationId().toString(), input.toString(), command.sha256(), token.toString());
            // Reap the writer and remove its request before launching the independent reader.
            Files.delete(input);
            String log = run(c, temp.resolve("reader.log"), 0, "read-" + phase, command.operationId().toString());
            if (phase.equals("before")) {
                assertThat(log).contains("REGISTRATION_ABSENT_OK");
                var budget = new PayloadBudget(64_000_000);
                var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
                var caller = new RepositoryCaller("principal", true);
                var claim = journal.acquireInitial(caller, record, token, RepositoryReadControl.NONE);
                assertThat(journal.acquireInitial(caller, record, token, RepositoryReadControl.NONE)).isEqualTo(claim);
                assertThat(budget.reservedBytes()).isZero();
            } else {
                assertThat(log).contains("REGISTRATION_RETAINED_OK|" + command.sha256() + "|"
                        + HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)));
                UUID retainedToken = c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                        "SELECT claim_token FROM repository_execution_claims WHERE operation_id=:id")
                        .setParameter("id", command.operationId()).getSingleResult());
                assertThat(retainedToken).isEqualTo(token);
            }
        }
    }

    private static String run(Context c, Path output, int exit, String... args) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", Objects.requireNonNull(System.getProperty("protomolt.test.runtimeClasspath")), DocumentInitialRegistrationWorker.class.getName()));
        command.addAll(List.of(args));
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile());
        builder.environment().putAll(Map.of("TEST_DB_URL", POSTGRES.getJdbcUrl(), "TEST_DB_USER", POSTGRES.getUsername(),
                "TEST_DB_PASSWORD", POSTGRES.getPassword(), "TEST_DB_SCHEMA", c.pool().getSchema()));
        var child = builder.start();
        try {
            assertThat(child.waitFor(45, TimeUnit.SECONDS)).as("child completes: %s", output).isTrue();
            assertThat(Files.size(output)).isLessThan(1_048_576);
            String log = Files.readString(output);
            assertThat(child.exitValue()).as(log).isEqualTo(exit);
            return log;
        } finally {
            if (child.isAlive()) { child.destroyForcibly(); assertThat(child.waitFor(5, TimeUnit.SECONDS)).isTrue(); }
        }
    }
}
