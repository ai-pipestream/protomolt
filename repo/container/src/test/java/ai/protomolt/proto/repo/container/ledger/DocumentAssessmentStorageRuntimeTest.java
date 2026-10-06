package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real observation and database host in a fresh standard JVM, without ambient test classes. */
class DocumentAssessmentStorageRuntimeTest {
    @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path directory;

    @Test void observedAssessmentAndSqlRunTogetherOnProductionJars() throws Exception {
        String bundleProperty = System.getProperty("protomolt.test.admissionRuntimeBundle");
        String hostProperty = System.getProperty("protomolt.test.storageHostClasspath");
        assertThat(bundleProperty).as("Run :protomolt-repo-container:admissionStorageTest").isNotBlank();
        assertThat(hostProperty).isNotBlank();
        var bundle = Path.of(bundleProperty);
        var inventory = DocumentRuntimeInventory.read(bundle, () -> {});
        var jars = new LinkedHashMap<String, Path>();
        inventory.identities().forEach(artifact -> jars.put(artifact.getArtifactSha256(),
                bundle.resolve("artifacts/" + artifact.getArtifactSha256() + ".jar")));
        // The production host shares admission dependencies. Include each exact
        // artifact once, by bytes; conflicting class providers still fail observation.
        for (String entry : hostProperty.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            var path = Path.of(entry);
            var identity = DocumentRuntimeArtifact.observe("storage-host", path,
                    new DocumentRuntimeArtifact.Limits(Files.size(path), 65536, 64), () -> {});
            jars.putIfAbsent(identity.getArtifactSha256(), path);
        }
        String classpath = String.join(java.io.File.pathSeparator, jars.values().stream().map(Path::toString).toList());
        var classes = Files.createDirectory(directory.resolve("classes"));
        var sources = new ArrayList<String>();
        for (String name : List.of("FencedSchemaWorkerProbe", "JournaledSuccessorPublicationProbe", "ManagedJournaledDrainProbe", "ObservedAssessmentProbe", "AssessmentCreationProbe", "AssessmentCaptureFaultProbe", "AssessmentProviderProbe", "AssessmentMixedReuseProbe", "AssessmentReplayInputsProbe", "AssessmentOperationReplayProbe", "JournaledAssessmentProbe", "AssessmentRejectionProbe", "AssessmentStorageProbe", "AssessmentRestartProbe", "RejectedAssessmentRestartProbe", "RejectedAssessmentExpiryProbe", "RejectedAssessmentSourceProbe", "NativeAssessmentPreparationProbe", "PromotedAssessmentCommitProbe", "AssessmentStageFaultProbe", "NativeAssessmentExecutionProbe", "NativeAssessmentRestartProbe", "NativeAssessmentRuntimeProbe", "NativeSchemaRevisionProbe", "HistoricalAssessmentCreationProbe", "HistoricalPublicationProbe", "HistoricalMixedPublicationProbe", "NativeHistoricalMaterializationProbe", "NativeHistoricalMaterializationTransportProbe", "NativeHistoricalMaterializationLifecycleProbe")) {
            var source = directory.resolve(name + ".java");
            try (var input = getClass().getResourceAsStream("/runtime-inventory/" + name + ".java")) {
                assertThat(input).isNotNull(); Files.copy(input, source);
            }
            sources.add(source.toString());
        }
        var crashSource = directory.resolve("JournaledAssessmentCrashProbe.java");
        try (var input = getClass().getResourceAsStream("/runtime-inventory/JournaledAssessmentCrashProbe.java")) {
            assertThat(input).isNotNull(); Files.copy(input, crashSource);
        }
        sources.add(crashSource.toString());
        var arguments = new ArrayList<>(List.of("-proc:none", "-classpath", classpath, "-d", classes.toString()));
        arguments.addAll(sources);
        assertThat(javax.tools.ToolProvider.getSystemJavaCompiler().run(null, null, null, arguments.toArray(String[]::new))).isZero();
        var probe = directory.resolve("storage-probe.jar");
        try (var output = new java.util.jar.JarOutputStream(Files.newOutputStream(probe)); var paths = Files.walk(classes)) {
            for (var file : paths.filter(Files::isRegularFile).sorted().toList()) {
                output.putNextEntry(new java.util.jar.JarEntry(classes.relativize(file).toString().replace(java.io.File.separatorChar, '/')));
                Files.copy(file, output); output.closeEntry();
            }
        }
        try (var postgres = new PostgreSQLContainer("postgres:18-alpine");
                var storage = new AssessmentStorageBackend(System.getProperty("protomolt.test.nativeStorage", "localstack"))) {
            postgres.start();
            storage.start();
            var log = directory.resolve("host.log");
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp",
                    classpath + java.io.File.pathSeparator + probe,
                    "ai.protomolt.proto.repo.container.ledger.AssessmentStorageProbe", bundle.toString());
            builder.environment().put("PROTOMOLT_TEST_JDBC", postgres.getJdbcUrl());
            builder.environment().put("PROTOMOLT_TEST_RUNTIME_BUNDLE", bundle.toString());
            builder.environment().put("PROTOMOLT_TEST_USER", postgres.getUsername());
            builder.environment().put("PROTOMOLT_TEST_PASSWORD", postgres.getPassword());
            builder.environment().put("PROTOMOLT_TEST_S3_ENDPOINT", storage.getEndpoint().toString());
            builder.environment().put("PROTOMOLT_TEST_S3_REGION", storage.getRegion());
            builder.environment().put("PROTOMOLT_TEST_S3_ACCESS", storage.getAccessKey());
            builder.environment().put("PROTOMOLT_TEST_S3_SECRET", storage.getSecretKey());
            var request = directory.resolve("restart-request.properties");
            builder.environment().put("PROTOMOLT_TEST_RESTART_REQUEST", request.toString());
            var process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                // This host runs the aggregate provider, publication and crash-recovery probes.
                // Their operation-specific deadlines remain separate from this harness cap.
                assertThat(process.waitFor(180, TimeUnit.SECONDS)).as("Observed SQL host completed; log: %s", log).isTrue();
                assertThat(Files.size(log)).isLessThan(1_048_576);
                String result = Files.readString(log);
                assertThat(process.exitValue()).as(result).isZero();
                assertThat(result).contains("OBSERVED_SQL_HOST_OK");
                assertThat(result).contains("JOURNALED_SUCCESSOR_PUBLICATION_OK", "FENCED_SCHEMA_WORKER_DRAIN_OK");
                assertThat(result).contains("OBSERVED_ASSESSMENT_CREATION_OK", "CLOSED_SCOPE_ASSESSMENT_ACK_OK");
                assertThat(result).contains("ASSESSMENT_CAPTURE_FAULTS_OK");
                assertThat(result).contains("ASSESSMENT_PROVIDER_READS_OK");
                assertThat(result).contains("JOURNALED_RESTORATION_DRAIN_OK");
                assertThat(result).contains("MANAGED_JOURNALED_SCHEMA_DRAIN_OK");
                assertThat(result).contains("ASSESSMENT_MIXED_REUSE_OK", "HISTORICAL_ASSESSMENT_CREATE_OK", "HISTORICAL_ASSESSMENT_LOST_ACK_OK", "HISTORICAL_PUBLICATION_OK", "HISTORICAL_PUBLICATION_LOST_ACK_OK", "HISTORICAL_MIXED_UPLOAD_OK", "HISTORICAL_UNVERIFIED_UPLOAD_REFUSED_OK");
                assertThat(result).contains("HISTORICAL_MIXED_MEMBER_PROVIDER_OK", "HISTORICAL_MIXED_MEMBER_UNVERIFIED_REFUSED_OK");
                assertThat(result).contains("SCOPED_NATIVE_ASSESSMENT_EXECUTION_OK");
                assertThat(result).contains("ASSESSMENT_SOURCE_ADVANCED_OK");
                assertThat(result).contains("ASSESSMENT_REPLAY_INPUTS_OK");
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
            var rejected = builder.redirectOutput(rejectedLog.toFile()).start();
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
        }
    }
}
