package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Optional focused entrypoints; the full storage gate still requires these same hosts. */
class HistoricalRuntimeQualificationTest {
    @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path directory;

    @Test void reconciliation() throws Exception {
        run(null, "HISTORICAL_RECONCILIATION_HOST_OK", "SCOPED_INSTALLED_HISTORICAL_CREATE_RECONCILED_PUBLICATION_OK",
                "HISTORICAL_RECONCILIATION_REVOKED_OK", "HISTORICAL_RECONCILIATION_EXPIRED_OK", "HISTORICAL_RECONCILIATION_RELEASED_OK");
    }
    @Test void initialOwner() throws Exception {
        run("initial-owner", "HISTORICAL_INITIAL_OWNER_HOST_OK", "SCOPED_INITIAL_HISTORICAL_PUBLICATION_OK",
                "SCOPED_INITIAL_HISTORICAL_CREATE_RECONCILED_OK", "HISTORICAL_UPLOAD_REPLAY_OK",
                "HISTORICAL_UPLOAD_REVOKE_OK", "HISTORICAL_UPLOAD_CANCEL_OK", "HISTORICAL_UPLOAD_LOST_PROVIDER_REPLY_OK",
                "HISTORICAL_UPLOAD_LOST_VERIFICATION_REPLY_OK", "HISTORICAL_UPLOAD_SHUTDOWN_OK", "HISTORICAL_UPLOAD_EXPIRE_OK",
                "HISTORICAL_UPLOAD_TAKEOVER_OK", "HISTORICAL_RUNTIME_SHUTDOWN_OK", "HISTORICAL_INITIAL_REJECTION_OK",
                "HISTORICAL_INITIAL_REJECTION_REPLY_LOST_OK", "HISTORICAL_SUCCESSOR_REJECTION_OK");
    }
    @Test void selfSupersession() throws Exception {
        run("self-supersession", "HISTORICAL_SELF_SUPERSESSION_HOST_OK", "SCOPED_HISTORICAL_SELF_SUPERSESSION_INSTALLED_OK",
                "SCOPED_HISTORICAL_SELF_SUPERSESSION_PUBLICATION_OK", "SCOPED_INSTALLED_HISTORICAL_MULTICALL_PUBLICATION_OK");
    }
    @Test void overlappingGenerations() throws Exception {
        run("overlap", "HISTORICAL_GENERATION_OVERLAP_HOST_OK", "SCOPED_HISTORICAL_GENERATION_OVERLAP_PUBLICATION_OK");
    }
    @Test void commitWinner() throws Exception {
        run("commit-wins", "HISTORICAL_PUBLICATION_COMMIT_WINNER_HOST_OK", "HISTORICAL_POST_FINALIZATION_PUBLICATION_WINS_OK", "HISTORICAL_LOSING_LOCAL_SUCCESSOR_RETIRED_OK", "HISTORICAL_LOSER_NEW_FIRST_OK");
    }

    @Test void commitWinnerOldFirst() throws Exception {
        run("commit-wins-old-first", "HISTORICAL_PUBLICATION_COMMIT_WINNER_HOST_OK",
                "HISTORICAL_POST_FINALIZATION_PUBLICATION_WINS_OK", "HISTORICAL_LOSING_LOCAL_SUCCESSOR_RETIRED_OK",
                "HISTORICAL_LOSER_OLD_FIRST_OK");
    }

    @Test void claimExpiresBeforeFinalization() throws Exception {
        run("claim-expires", "HISTORICAL_CLAIM_EXPIRY_HOST_OK", "HISTORICAL_PRE_FINALIZATION_CLAIM_EXPIRY_OK",
                "SCOPED_HISTORICAL_EXPIRED_PUBLISHER_RETIRED_OK", "HISTORICAL_POST_ROLLBACK_SUCCESSOR_PUBLICATION_OK");
    }

    @Test void takeoverBeforeClaim() throws Exception {
        run("takeover-first", "HISTORICAL_TAKEOVER_FIRST_HOST_OK", "HISTORICAL_TAKEOVER_BEFORE_CLAIM_OK",
                "SCOPED_HISTORICAL_EXPIRED_PUBLISHER_RETIRED_OK", "HISTORICAL_POST_ROLLBACK_SUCCESSOR_PUBLICATION_OK");
    }

    private void run(String mode, String... markers) throws Exception {
        var compiled = StorageRuntimeProbeCompiler.compile(directory);
        try (var postgres = new PostgreSQLContainer("postgres:18-alpine");
             var storage = new AssessmentStorageBackend("localstack")) {
            postgres.start(); storage.start();
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    compiled.classpath() + java.io.File.pathSeparator + compiled.probe(),
                    "ai.protomolt.proto.repo.container.ledger.HistoricalOwnerReconciliationHost", compiled.bundle().toString());
            if (mode != null) builder.command().add(mode);
            builder.environment().put("PROTOMOLT_TEST_RUNTIME_BUNDLE", compiled.bundle().toString());
            builder.environment().put("PROTOMOLT_TEST_JDBC", postgres.getJdbcUrl());
            builder.environment().put("PROTOMOLT_TEST_USER", postgres.getUsername());
            builder.environment().put("PROTOMOLT_TEST_PASSWORD", postgres.getPassword());
            builder.environment().put("PROTOMOLT_TEST_S3_ENDPOINT", storage.getEndpoint().toString());
            builder.environment().put("PROTOMOLT_TEST_S3_REGION", storage.getRegion());
            builder.environment().put("PROTOMOLT_TEST_S3_ACCESS", storage.getAccessKey());
            builder.environment().put("PROTOMOLT_TEST_S3_SECRET", storage.getSecretKey());
            var log = directory.resolve("historical-qualification.log");
            var process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                assertThat(process.waitFor(90, TimeUnit.SECONDS)).as("Historical host completed; log: %s", log).isTrue();
                assertThat(Files.size(log)).isLessThan(1_048_576);
                String output = Files.readString(log);
                assertThat(process.exitValue()).as(output).isZero();
                assertThat(output).contains(markers);
                if (!"initial-owner".equals(mode)) {
                    assertThat(output).contains("SCOPED_HISTORICAL_PROPOSED_OWNER_INSTALLED_OK",
                            "claim-expires".equals(mode) ? "SCOPED_HISTORICAL_EXPIRED_PUBLISHER_RETIRED_OK" : "SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK");
                }
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
        }
    }
}
