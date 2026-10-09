package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Trusted handoff fixture; private owner capability is never a public request or command-line argument. */
public final class NativeAssessmentRestartProbe {
    static void run(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner) throws Exception {
        var handoff = Files.createTempFile("assessment-handoff-", ".properties",
                java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
                        java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
        Path log = null;
        Process process = null;
        try {
            log = Files.createTempFile("assessment-handoff-", ".log");
            var state = new Properties();
            state.setProperty("command", Base64.getEncoder().encodeToString(command.intent().toByteArray()));
            state.setProperty("principal", owner.key().principal());
            state.setProperty("generation", Long.toString(owner.generation()));
            state.setProperty("nonce", owner.token().toString());
            state.setProperty("lease", owner.leaseUntil().toString());
            try (var out = Files.newOutputStream(handoff)) { state.store(out, "Private test owner handoff"); }
            process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:+DisableAttachMechanism", "-XX:-EnableDynamicAgentLoading", "-cp", System.getProperty("java.class.path"),
                    NativeAssessmentRestartProbe.class.getName(), handoff.toString(),
                    Objects.requireNonNull(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")))
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            require(process.waitFor(25, TimeUnit.SECONDS), "fresh process completed");
            require(Files.size(log) < 1_048_576, "bounded restart output");
            String result = Files.readString(log);
            require(process.exitValue() == 0, "fresh process failed: " + result);
            require(result.contains("NATIVE_PRETERMINAL_RESTART_OK"), "fresh process completed original assessment");
        } finally {
            try {
                if (process != null && process.isAlive()) {
                    process.destroyForcibly();
                    require(process.waitFor(10, TimeUnit.SECONDS), "fresh process stopped");
                }
            } finally {
                try { Files.deleteIfExists(handoff); }
                finally { if (log != null) Files.deleteIfExists(log); }
            }
        }
        System.out.println("NATIVE_PRETERMINAL_RESTART_OK");
    }

    public static void main(String[] args) throws Exception {
        var file = Path.of(args[0]);
        require(Files.size(file) < 2_000_000, "bounded handoff input");
        var state = new Properties();
        try (var input = Files.newInputStream(file)) { state.load(input); }
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(Base64.getDecoder().decode(state.getProperty("command"))));
        var owner = new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key(command.intent().getAccountId(),
                state.getProperty("principal"), command.operationId()), Long.parseLong(state.getProperty("generation")),
                UUID.fromString(state.getProperty("nonce")), Instant.parse(state.getProperty("lease")));
        var caller = new RepositoryCaller(owner.key().principal(), true);
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[1]), () -> {});
        try (var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")));
                var provider = new AssessmentProviderProbe()) {
            var tx = new Tx(database.entityManagerFactory());
            var replay = new DocumentPublicationReplay(tx);
            var before = replay.observe(caller, command);
            require(before.result().isEmpty() && before.rejection().isEmpty(), "stage is pre-terminal");
            var original = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
            var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
            var reader = new DocumentPartReader((generation, profile) -> {
                require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact original backend");
                return provider.store();
            }, 2, 16_000_000, payload);
            try (reader) {
                var execution = new DocumentPublicationAssessmentExecution(tx, new DriveLedger(tx), reads, reader, budget,
                        new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), observation,
                        Duration.ofMinutes(2), Duration.ofSeconds(1));
                try {
                    execution.resume(caller, owner, command, RepositoryReadControl.NONE);
                    throw new AssertionError("Invalid assessment returned successful publication");
                } catch (DocumentPublicationReplay.Terminated terminal) {
                    var receipt = terminal.receipt();
                    require(receipt.hasAssessment()
                            && receipt.getAssessment().getAssessmentId().equals(original.stage().assessment().toString())
                            && receipt.getAssessment().getManifestSha256().equals(original.stage().manifestSha256()),
                            "decision binds the original stage");
                    require(replay.observe(caller, command).rejection().orElseThrow().equals(receipt), "exact durable receipt replay");
                }
            } finally {
                require(reader.awaitIdle(Duration.ofSeconds(5)), "fresh reader drains");
                reads.releaseDrained(1);
                require(reads.outstandingReads() == 0 && budget.reservedBytes() == 0 && payload.reservedBytes() == 0,
                        "fresh process releases SQL and memory reservations");
            }
        }
        System.out.println("NATIVE_PRETERMINAL_RESTART_OK");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
