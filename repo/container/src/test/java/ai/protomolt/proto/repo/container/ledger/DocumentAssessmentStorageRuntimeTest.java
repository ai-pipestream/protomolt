package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real observation and database host in a fresh standard JVM, without ambient test classes. */
class DocumentAssessmentStorageRuntimeTest {
    // The historical publication case deliberately holds an origin across a 20s
    // retention deadline. Add its wait plus bounded fixture setup to the existing
    // host budget; individual operation/provider deadlines remain unchanged.
    private static final long SQL_HOST_TIMEOUT_SECONDS = 180 + 30;
    @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path directory;

    @Test void observedAssessmentAndSqlRunTogetherOnProductionJars() throws Exception {
        var compiled = StorageRuntimeProbeCompiler.compile(directory);
        var bundle = compiled.bundle();
        var classpath = compiled.classpath();
        var probe = compiled.probe();
        final int redisPort;
        try (var reservation=new java.net.ServerSocket(0)) { redisPort=reservation.getLocalPort(); }
        // Docker can reassign an ephemeral published port on restart. Pin this fixture endpoint.
        try (var postgres = new PostgreSQLContainer("postgres:18-alpine");
                var redis = new org.testcontainers.containers.GenericContainer<>("redis:7-alpine")
                        .withCommand("redis-server","--appendonly","yes","--appendfsync","always","--maxmemory-policy","noeviction").withExposedPorts(6379)
                        .withCreateContainerCmdModifier(command -> command.getHostConfig().withPortBindings(
                                new com.github.dockerjava.api.model.PortBinding(
                                        com.github.dockerjava.api.model.Ports.Binding.bindPort(redisPort),
                                        com.github.dockerjava.api.model.ExposedPort.tcp(6379))));
                var storage = new AssessmentStorageBackend(System.getProperty("protomolt.test.nativeStorage", "localstack"))) {
            postgres.start();
            storage.start();
            redis.start();
            assertThat(redis.getMappedPort(6379)).isEqualTo(redisPort);
            var control=Files.createDirectory(directory.resolve("delayed-control"));
            try (var gate=new RedisDelayedRequestGate(redis.getHost(),redis.getMappedPort(6379),control.resolve("arm"))) {
            var boundedRestart=directory.resolve("bounded-restart");
            var log = directory.resolve("host.log");
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.AssessmentStorageProbe", bundle.toString());
            builder.environment().put("PROTOMOLT_TEST_BOUNDED_RESTART_DIR",boundedRestart.toString());
            builder.environment().put("PROTOMOLT_TEST_JDBC", postgres.getJdbcUrl());
            builder.environment().put("PROTOMOLT_TEST_REDIS_URI",gate.uri());
            builder.environment().put("PROTOMOLT_TEST_DELAYED_CONTROL",control.toString());
            builder.environment().put("PROTOMOLT_TEST_RUNTIME_BUNDLE", bundle.toString());
            builder.environment().put("PROTOMOLT_TEST_USER", postgres.getUsername());
            builder.environment().put("PROTOMOLT_TEST_PASSWORD", postgres.getPassword());
            builder.environment().put("PROTOMOLT_TEST_S3_ENDPOINT", storage.getEndpoint().toString());
            builder.environment().put("PROTOMOLT_TEST_S3_REGION", storage.getRegion());
            builder.environment().put("PROTOMOLT_TEST_S3_ACCESS", storage.getAccessKey());
            builder.environment().put("PROTOMOLT_TEST_S3_SECRET", storage.getSecretKey());
            var request = directory.resolve("restart-request.properties");
            builder.environment().put("PROTOMOLT_TEST_RESTART_REQUEST", request.toString());
            // Replay runs before the aggregate writer creates lease-sensitive fixtures.
            // Its rejected receipt remains in this database for the later fresh reader.
            try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                 var statement = connection.createStatement()) {
                statement.executeUpdate("CREATE DATABASE assessment_replay");
            }
            var replayBuilder = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.AssessmentOperationReplayHost", bundle.toString());
            replayBuilder.environment().putAll(builder.environment());
            String replayJdbc = postgres.getJdbcUrl().replaceFirst(
                    "/" + java.util.regex.Pattern.quote(postgres.getDatabaseName()) + "(?=\\?|$)", "/assessment_replay");
            assertThat(replayJdbc).isNotEqualTo(postgres.getJdbcUrl());
            replayBuilder.environment().put("PROTOMOLT_TEST_JDBC", replayJdbc);
            var replayLog = directory.resolve("operation-replay.log");
            var replay = replayBuilder.redirectErrorStream(true).redirectOutput(replayLog.toFile()).start();
            final String replayResult;
            try {
                assertThat(replay.waitFor(90, TimeUnit.SECONDS)).as("Replay host completed; log: %s", replayLog).isTrue();
                assertThat(Files.size(replayLog)).isLessThan(1_048_576);
                replayResult = Files.readString(replayLog);
                assertThat(replay.exitValue()).as(replayResult).isZero();
                assertThat(replayResult).contains("ASSESSMENT_OPERATION_REPLAY_HOST_OK", "ASSESSMENT_OPERATION_REPLAY_OK",
                        "JOURNALED_RESTORATION_CLAIM_LOSS_OK", "ASSESSMENT_POLICY_ADVANCEMENT_REPLAY_OK");
            } finally {
                if (replay.isAlive()) {
                    replay.destroyForcibly();
                    assertThat(replay.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
            var process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                // This host runs the aggregate provider, publication and crash-recovery probes.
                // Their operation-specific deadlines remain separate from this harness cap.
                assertThat(process.waitFor(SQL_HOST_TIMEOUT_SECONDS, TimeUnit.SECONDS)).as("Observed SQL host completed; log: %s", log).isTrue();
                assertThat(Files.size(log)).isLessThan(1_048_576);
                // Preserve every existing marker assertion across both mandatory hosts.
                String result = Files.readString(log) + "\n" + replayResult;
                assertThat(process.exitValue()).as(result).isZero();
                assertThat(result).contains("OBSERVED_SQL_HOST_OK","BOUNDED_DOCUMENT_HOST_STARTUP_OK","BOUNDED_DOCUMENT_PUBLICATION_HISTORY_OK","BOUNDED_DOCUMENT_TRANSPORT_OK","BOUNDED_DOCUMENT_HISTORY_TRANSPORT_OK","BOUNDED_DOCUMENT_READ_SHUTDOWN_OK","BOUNDED_DOCUMENT_DELAYED_REQUEST_OK","BOUNDED_DOCUMENT_TYPED_REJECTION_OK","BOUNDED_PUBLICATION_PUT_SHUTDOWN_OK","BOUNDED_PUBLICATION_RPC_CANCELLATION_OK","BOUNDED_PUBLICATION_SCHEMA_SHUTDOWN_OK","BOUNDED_PUBLIC_CONSUMER_LIBRARY_OK","BOUNDED_PUBLIC_CONSUMER_RPC_OK");
                assertThat(result).contains("JOURNALED_SUCCESSOR_PUBLICATION_OK", "FENCED_SCHEMA_WORKER_DRAIN_OK");
                assertThat(result).contains("RECOVERY_OWNER_TERMINAL_DISPOSAL_OK");
                assertThat(result).contains("RECOVERY_OPEN_TERMINAL_DISPOSAL_OK");
                assertThat(result).contains("OBSERVED_ASSESSMENT_CREATION_OK", "CLOSED_SCOPE_ASSESSMENT_ACK_OK");
                assertThat(result).contains("ASSESSMENT_CAPTURE_FAULTS_OK");
                assertThat(result).contains("ASSESSMENT_PROVIDER_READS_OK");
                assertThat(result).contains("JOURNALED_RESTORATION_DRAIN_OK");
                assertThat(result).contains("MANAGED_JOURNALED_SCHEMA_DRAIN_OK");
                assertThat(result).contains("MANAGED_PUBLICATION_TRANSPORT_PARITY_OK");
                assertThat(result).contains("MANAGED_PUBLICATION_HOST_DRAIN_OK");
                assertThat(result).contains("MANAGED_RECOVERY_ACCEPTED_PUBLICATION_DRAIN_OK");
                assertThat(result).contains("MANAGED_EXPIRED_PUBLICATION_RECOVERY_OK");
                assertThat(result).contains("CLAIMED_HISTORICAL_PUBLICATION_LOST_ACK_OK");
                assertThat(result).contains("CLAIMED_HISTORICAL_PUBLICATION_EXPIRED_OK");
                assertThat(result).contains("CLAIMED_HISTORICAL_PUBLICATION_REVOKED_OK");
                assertThat(result).contains("CLAIMED_HISTORICAL_MIXED_PUBLICATION_OK", "SCOPED_CLAIMED_HISTORICAL_MIXED_PUBLICATION_OK");
                assertThat(result).contains("SCOPED_HISTORICAL_MIXED_SUCCESSOR_PUBLICATION_OK");
                assertThat(result).contains("SCOPED_INSTALLED_HISTORICAL_MULTICALL_PUBLICATION_OK");
                assertThat(result).contains("SCOPED_HISTORICAL_PROPOSED_OWNER_INSTALLED_OK");
                assertThat(result).contains("SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK");
                assertThat(result).contains("HISTORICAL_CREATE_RECONCILED_OK", "HISTORICAL_CREATE_ROLLBACK_RECONCILED_OK",
                        "HISTORICAL_RECONCILIATION_MANIFEST_REFUSED_OK");
                assertThat(result).contains("ASSESSMENT_MIXED_REUSE_OK", "HISTORICAL_ASSESSMENT_CREATE_OK", "HISTORICAL_ASSESSMENT_LOST_ACK_OK", "CLAIMED_HISTORICAL_ASSESSMENT_CREATE_OK", "CLAIMED_HISTORICAL_ASSESSMENT_MIXED_OK", "CLAIMED_HISTORICAL_MIXED_ORIGIN_CONTENTION_OK", "CLAIMED_HISTORICAL_STAGE_REVOCATION_OK", "CLAIMED_HISTORICAL_STAGE_WINS_OK", "CLAIMED_HISTORICAL_CREATE_WINS_OK", "CLAIMED_HISTORICAL_ASSESSMENT_LOST_ACK_OK", "CLAIMED_HISTORICAL_ASSESSMENT_ROLLBACK_OK", "CLAIMED_HISTORICAL_START_ROLLBACK_CREATE_OK", "CLAIMED_HISTORICAL_START_LOST_ACK_REFUSED_OK", "CLAIMED_HISTORICAL_START_CONCURRENT_CREATE_OK", "CLAIMED_HISTORICAL_SUCCESSOR_CREATE_OK", "CLAIMED_HISTORICAL_SUCCESSOR_PUBLICATION_OK", "CLAIMED_HISTORICAL_OPAQUE_PUBLICATION_OK", "HISTORICAL_PUBLICATION_OK", "HISTORICAL_PUBLICATION_LOST_ACK_OK", "HISTORICAL_MIXED_UPLOAD_OK", "HISTORICAL_UNVERIFIED_UPLOAD_REFUSED_OK");
                assertThat(result).contains("HISTORICAL_MIXED_MEMBER_PROVIDER_OK", "HISTORICAL_MIXED_MEMBER_UNVERIFIED_REFUSED_OK");
                assertThat(result).contains("SCOPED_NATIVE_ASSESSMENT_EXECUTION_OK");
                assertThat(result).contains("ASSESSMENT_SOURCE_ADVANCED_OK");
                assertThat(result).contains("ASSESSMENT_REPLAY_INPUTS_OK","ASSESSMENT_REPLAY_INPUTS_BOUNDED_TX_OK");
                assertThat(result).contains("ASSESSMENT_OPERATION_REPLAY_OK");
                assertThat(result).contains("JOURNALED_ASSESSMENT_COMMIT_RECOVERY_OK", "JOURNALED_ASSESSMENT_DECISION_OK",
                        "JOURNALED_ASSESSMENT_HANDLE_RESUME_OK", "JOURNALED_RESTORATION_CLAIM_LOSS_OK");
                assertThat(result).contains("JOURNALED_OBSERVED_MODE_MISMATCH_OK");
                assertThat(result).contains("JOURNALED_DIRECT_COMMIT_MISMATCH_OK", "JOURNALED_DIRECT_COMMIT_MATCH_OK");
                assertThat(result).contains("ASSESSMENT_POLICY_ADVANCEMENT_REPLAY_OK");
                assertThat(result).contains("ASSESSMENT_MIXED_REPLAY_OK");
                assertThat(result).contains("ASSESSMENT_REPLAY_CANCELLED_DELIVERY_OK");
                assertThat(result).contains("ASSESSMENT_REJECTION_ACCEPTED_REFUSED", "ASSESSMENT_REJECTION_LOST_ACK_OK",
                        "ASSESSMENT_REJECTION_CANCEL_AFTER_COMMIT_OK", "ASSESSMENT_REJECTION_STALE_POLICY_OK",
                        "ASSESSMENT_REJECTION_TERMINAL_READ_OK", "REJECTED_ASSESSMENT_CAPTURE_FAULTS_OK",
                        "REJECTED_ASSESSMENT_AUTHORITY_GUARDS_OK", "REJECTED_ASSESSMENT_EXPIRY_WAITS_OK",
                        "REJECTED_ASSESSMENT_SOURCE_AUTHORIZATION_OK", "NATIVE_ASSESSMENT_PREPARATION_OK", "PROMOTED_ASSESSMENT_COMMIT_OK", "ASSESSMENT_STAGE_LOST_ACK_DISCOVERY_OK", "NATIVE_ASSESSMENT_EXECUTION_OK", "NATIVE_PRETERMINAL_RESTART_OK", "NATIVE_ASSESSMENT_RUNTIME_OK", "NATIVE_SCHEMA_METADATA_REVISIONS_OK", "NATIVE_HISTORICAL_MATERIALIZATION_OK", "NATIVE_HISTORICAL_MATERIALIZATION_TRANSPORT_OK", "NATIVE_HISTORICAL_MATERIALIZATION_LIFECYCLE_OK");
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
            assertThat(Files.isRegularFile(request)).as("Writer persisted restart identities").isTrue();
            var restartLog = directory.resolve("restart.log");
            builder.command(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.AssessmentRestartProbe", request.toString());
            var restarted = builder.redirectOutput(restartLog.toFile()).start();
            try {
                assertThat(restarted.waitFor(75, TimeUnit.SECONDS)).as("Restart acknowledgement completed").isTrue();
                assertThat(Files.size(restartLog)).isLessThan(1_048_576);
                String result = Files.readString(restartLog);
                assertThat(restarted.exitValue()).as(result).isZero();
                assertThat(result).contains("RESTARTED_ASSESSMENT_ACK_OK", "RESTARTED_ASSESSMENT_REVOCATION_OK",
                        "RESTARTED_ASSESSMENT_CAPTURE_REVOCATION_OK",
                        "RESTARTED_ASSESSMENT_OWNER_FENCE_OK");
            } finally {
                if (restarted.isAlive()) {
                    restarted.destroyForcibly();
                    assertThat(restarted.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
            var rejectedLog = directory.resolve("rejected-restart.log");
            builder.command(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.RejectedAssessmentRestartProbe",
                    request + ".rejected", bundle.toString());
            var rejectedBuilder = new ProcessBuilder(builder.command());
            rejectedBuilder.environment().putAll(replayBuilder.environment());
            var rejected = rejectedBuilder.redirectErrorStream(true).redirectOutput(rejectedLog.toFile()).start();
            try {
                assertThat(rejected.waitFor(30, TimeUnit.SECONDS)).as("Rejected evidence restart completed").isTrue();
                assertThat(Files.size(rejectedLog)).isLessThan(1_048_576);
                String result = Files.readString(rejectedLog);
                assertThat(rejected.exitValue()).as(result).isZero();
                assertThat(result).contains("RESTARTED_REJECTION_EVIDENCE_OK");
            } finally {
                if (rejected.isAlive()) {
                    rejected.destroyForcibly();
                    assertThat(rejected.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            }
            // Only the public operation ID crosses processes. No command, nonce or token handoff file.
            builder.environment().remove("PROTOMOLT_TEST_RESTART_REQUEST");
            builder.environment().put("PROTOMOLT_TEST_CRASH_OPERATION", java.util.UUID.randomUUID().toString());
            for (String role : List.of("writer", "reader")) {
                builder.command(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                        classpath + java.io.File.pathSeparator + probe,
                        "ai.protomolt.proto.repo.container.ledger.JournaledAssessmentCrashProbe", role, bundle.toString());
                var crashLog = directory.resolve("journaled-crash-" + role + ".log");
                var child = builder.redirectOutput(crashLog.toFile()).start();
                try {
                    assertThat(child.waitFor(40, TimeUnit.SECONDS)).as("Journaled crash %s completed", role).isTrue();
                    assertThat(child.isAlive()).isFalse();
                    assertThat(Files.size(crashLog)).isLessThan(1_048_576);
                    String output = Files.readString(crashLog);
                    assertThat(child.exitValue()).as(output).isEqualTo(role.equals("writer") ? 86 : 0);
                    if (role.equals("reader")) assertThat(output).contains("JOURNALED_FORCED_CRASH_RECOVERY_OK");
                } finally {
                    if (child.isAlive()) {
                        child.destroyForcibly();
                        assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
                    }
                }
                // The loop cannot start the reader until the writer's expected halt is confirmed and reaped.
            }
            String previousStart=redis.getDockerClient().inspectContainerCmd(redis.getContainerId()).exec().getState().getStartedAt();
            redis.getDockerClient().restartContainerCmd(redis.getContainerId()).exec();
            assertThat(redis.getDockerClient().inspectContainerCmd(redis.getContainerId()).exec().getState().getStartedAt())
                    .isNotEqualTo(previousStart);
            assertThat(redis.getDockerClient().inspectContainerCmd(redis.getContainerId()).exec().getNetworkSettings()
                    .getPorts().getBindings().get(com.github.dockerjava.api.model.ExposedPort.tcp(6379))[0].getHostPortSpec())
                    .isEqualTo(Integer.toString(redisPort));
            assertThat(redis.execInContainer("redis-cli","PING").getStdout().trim()).isEqualTo("PONG");
            var boundedLog=directory.resolve("bounded-restart.log");
            builder.command(Path.of(System.getProperty("java.home"),"bin","java").toString(),
                    "-XX:+DisableAttachMechanism","-XX:-EnableDynamicAgentLoading","-cp",
                    classpath+java.io.File.pathSeparator+probe,
                    "ai.protomolt.proto.repo.service.BoundedDocumentRestartProbe",bundle.toString(),boundedRestart.toString());
            var boundedProcess=builder.redirectOutput(boundedLog.toFile()).start();
            try {
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(480);
                boolean delivered=false;
                while (!boundedProcess.waitFor(100,TimeUnit.MILLISECONDS)) {
                    if (!delivered && Files.exists(control.resolve("deliver"))) {
                        gate.deliver();
                        Files.createFile(control.resolve("delivered"));
                        delivered=true;
                    }
                    assertThat(System.nanoTime()).as("Redis restart qualification deadline").isLessThan(deadline);
                }
                assertThat(delivered).as("Original queued request delivered after durable cleanup").isTrue();
                assertThat(Files.size(boundedLog)).isLessThan(1_048_576);
                assertThat(boundedProcess.exitValue()).as(Files.readString(boundedLog)).isZero();
                assertThat(Files.readString(boundedLog)).contains("BOUNDED_DOCUMENT_RESTART_OK","BOUNDED_DOCUMENT_HISTORY_TRANSPORT_OK","BOUNDED_DOCUMENT_ORPHAN_CLEANUP_OK","DOCUMENT_CLEANUP_RETENTION_FILTER_OK","DOCUMENT_CLEANUP_PROVIDER_RETRY_OK","DOCUMENT_DELAYED_WRITE_RECOVERY_OK");
                assertThat(Files.readString(boundedLog)).doesNotContain("Document attempt cleanup failed");
            } finally {
                if (boundedProcess.isAlive()) {
                    boundedProcess.destroyForcibly();
                    assertThat(boundedProcess.waitFor(10,TimeUnit.SECONDS)).isTrue();
                }
            }
            // Keep independent qualification after every lease-sensitive restart.
            // New owner-recovery qualification has a separate bounded JVM and database;
            // it does not consume or enlarge the established aggregate host deadline.
            for (String databaseName : List.of("historical_reconciliation", "historical_self_supersession", "historical_generation_overlap", "historical_commit_winner", "historical_commit_winner_old_first", "historical_claim_expiry")) {
                boolean claimExpiry = databaseName.equals("historical_claim_expiry");
                boolean oldFirst = databaseName.equals("historical_commit_winner_old_first");
                boolean commitWins = databaseName.equals("historical_commit_winner") || oldFirst;
                boolean overlap = databaseName.equals("historical_generation_overlap");
                boolean selfSupersession = databaseName.equals("historical_self_supersession");
                try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                     var statement = connection.createStatement()) {
                    statement.executeUpdate("CREATE DATABASE " + databaseName);
                }
                var reconciliationBuilder = new ProcessBuilder(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                        classpath + java.io.File.pathSeparator + probe,
                        "ai.protomolt.proto.repo.container.ledger.HistoricalOwnerReconciliationHost", bundle.toString());
                if (claimExpiry) reconciliationBuilder.command().add("claim-expires");
                if (commitWins) reconciliationBuilder.command().add(oldFirst ? "commit-wins-old-first" : "commit-wins");
                if (overlap) reconciliationBuilder.command().add("overlap");
                if (selfSupersession) reconciliationBuilder.command().add("self-supersession");
                reconciliationBuilder.environment().putAll(builder.environment());
                String reconciliationJdbc = postgres.getJdbcUrl().replaceFirst(
                        "/" + java.util.regex.Pattern.quote(postgres.getDatabaseName()) + "(?=\\?|$)", "/" + databaseName);
                assertThat(reconciliationJdbc).isNotEqualTo(postgres.getJdbcUrl());
                reconciliationBuilder.environment().put("PROTOMOLT_TEST_JDBC", reconciliationJdbc);
                var reconciliationLog = directory.resolve(databaseName.replace('_', '-') + ".log");
                var reconciliation = reconciliationBuilder.redirectErrorStream(true).redirectOutput(reconciliationLog.toFile()).start();
                try {
                    assertThat(reconciliation.waitFor(90, TimeUnit.SECONDS)).as("Historical reconciliation host completed; log: %s", reconciliationLog).isTrue();
                    assertThat(Files.size(reconciliationLog)).isLessThan(1_048_576);
                    String result = Files.readString(reconciliationLog);
                    assertThat(reconciliation.exitValue()).as(result).isZero();
                    if (claimExpiry) {
                        assertThat(result).contains("HISTORICAL_CLAIM_EXPIRY_HOST_OK", "HISTORICAL_PRE_FINALIZATION_CLAIM_EXPIRY_OK");
                    } else if (commitWins) {
                        assertThat(result).contains("HISTORICAL_PUBLICATION_COMMIT_WINNER_HOST_OK",
                                "HISTORICAL_POST_FINALIZATION_PUBLICATION_WINS_OK", "HISTORICAL_LOSING_LOCAL_SUCCESSOR_RETIRED_OK");
                        assertThat(result).contains(oldFirst ? "HISTORICAL_LOSER_OLD_FIRST_OK" : "HISTORICAL_LOSER_NEW_FIRST_OK");
                    } else if (overlap) {
                        assertThat(result).contains("HISTORICAL_GENERATION_OVERLAP_HOST_OK",
                                "SCOPED_HISTORICAL_GENERATION_OVERLAP_PUBLICATION_OK");
                    } else if (selfSupersession) {
                        assertThat(result).contains("HISTORICAL_SELF_SUPERSESSION_HOST_OK",
                                "SCOPED_HISTORICAL_SELF_SUPERSESSION_INSTALLED_OK", "SCOPED_HISTORICAL_SELF_SUPERSESSION_PUBLICATION_OK",
                                "SCOPED_INSTALLED_HISTORICAL_MULTICALL_PUBLICATION_OK");
                    } else {
                        assertThat(result).contains("SCOPED_INSTALLED_HISTORICAL_CREATE_RECONCILED_PUBLICATION_OK", "HISTORICAL_RECONCILIATION_HOST_OK");
                        assertThat(result).contains("HISTORICAL_RECONCILIATION_REVOKED_OK", "HISTORICAL_RECONCILIATION_EXPIRED_OK",
                                "HISTORICAL_RECONCILIATION_RELEASED_OK");
                    }
                    assertThat(result).contains("SCOPED_HISTORICAL_PROPOSED_OWNER_INSTALLED_OK");
                    assertThat(result).contains(claimExpiry ? "SCOPED_HISTORICAL_EXPIRED_PUBLISHER_RETIRED_OK" : "SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK");
                } finally {
                    if (reconciliation.isAlive()) {
                        reconciliation.destroyForcibly();
                        assertThat(reconciliation.waitFor(10, TimeUnit.SECONDS)).isTrue();
                    }
                }
            }
            }
        }
    }
}
