package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureServiceGrpc;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;

/** Installed fixture process proof, separate from workflow-worker crash qualification. */
class AuthoringFixtureProcessTest {
    private static final String ID = "ab123456-1234-4234-8234-123456789abc";
    @TempDir Path directory;

    @Test
    void competingProcessesCommitOneRecordAndKilledServersCanRecoverIt() throws Exception {
        Path records = directory.resolve("records");
        Process left = null;
        Process right = null;
        Process restored = null;
        ManagedChannel leftChannel = null;
        ManagedChannel rightChannel = null;
        ManagedChannel restoredChannel = null;
        try {
            Path leftLog = directory.resolve("left.log");
            Path rightLog = directory.resolve("right.log");
            left = start(records, leftLog);
            right = start(records, rightLog);
            leftChannel = channel(awaitPort(left, leftLog));
            rightChannel = channel(awaitPort(right, rightLog));
            var leftStub = AuthoringFixtureServiceGrpc.newBlockingStub(leftChannel);
            var rightStub = AuthoringFixtureServiceGrpc.newBlockingStub(rightChannel);
            ready(leftStub);
            ready(rightStub);
            Outcome first;
            Outcome second;
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var a = executor.submit(() -> write(leftStub, "from-left"));
                var b = executor.submit(() -> write(rightStub, "from-right"));
                first = a.get(45, TimeUnit.SECONDS);
                second = b.get(45, TimeUnit.SECONDS);
            }
            if (first.status() != Status.Code.OK && second.status() != Status.Code.OK
                    || first.status() != Status.Code.ALREADY_EXISTS
                    && second.status() != Status.Code.ALREADY_EXISTS) {
                System.err.println("Fixture race outcomes: " + first + "; " + second);
                System.err.println("Left fixture: " + Files.readString(leftLog));
                System.err.println("Right fixture: " + Files.readString(rightLog));
            }
            assertThat(java.util.List.of(first.status(), second.status()))
                    .containsExactlyInAnyOrder(Status.Code.OK, Status.Code.ALREADY_EXISTS);
            Outcome winner = first.status() == Status.Code.OK ? first : second;
            try (var files = Files.list(records)) {
                assertThat(files.filter(path -> path.getFileName().toString().endsWith(".pb")).count())
                        .isEqualTo(1);
            }
            byte[] committed = Files.readAllBytes(records.resolve(ID + ".pb"));
            // These kills occur after the successful write, not at a worker checkpoint window.
            kill(left);
            kill(right);
            Path restoredLog = directory.resolve("restored.log");
            restored = start(records, restoredLog);
            restoredChannel = channel(awaitPort(restored, restoredLog));
            var restoredStub = AuthoringFixtureServiceGrpc.newBlockingStub(restoredChannel);
            ready(restoredStub);
            Outcome retry = write(restoredStub, winner.content());
            assertThat(retry.status()).isEqualTo(Status.Code.OK);
            assertThat(retry.response()).isEqualTo(winner.response());
            assertThat(Files.readAllBytes(records.resolve(ID + ".pb"))).isEqualTo(committed);
            assertThat(write(restoredStub, "different-content").status())
                    .isEqualTo(Status.Code.ALREADY_EXISTS);
            assertThat(Files.readAllBytes(records.resolve(ID + ".pb"))).isEqualTo(committed);
        } finally {
            close(leftChannel);
            close(rightChannel);
            close(restoredChannel);
            kill(left);
            kill(right);
            kill(restored);
        }
    }

    private Process start(Path records, Path log) throws Exception {
        Path launcher = Path.of("build/install/authoring-fixture/bin/authoring-fixture").toAbsolutePath();
        assertThat(launcher).exists();
        return new ProcessBuilder(launcher.toString(), "0", records.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static int awaitPort(Process process, Path log) throws Exception {
        var pattern = Pattern.compile("AuthoringFixtureService listening on port (\\d+)");
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            String output = Files.exists(log) ? Files.readString(log) : "";
            var match = pattern.matcher(output);
            if (match.find()) return Integer.parseInt(match.group(1));
            assertThat(process.isAlive()).as(output).isTrue();
            Thread.sleep(50);
        }
        throw new AssertionError("fixture did not bind: " + Files.readString(log));
    }

    private static ManagedChannel channel(int port) {
        return ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
    }

    private static Outcome write(AuthoringFixtureServiceGrpc.AuthoringFixtureServiceBlockingStub stub,
            String content) {
        try {
            var response = stub.withDeadlineAfter(30, TimeUnit.SECONDS).writeRecord(
                    WriteRecordRequest.newBuilder().setOperationId(ID).setContent(content).build());
            return new Outcome(content, response, Status.Code.OK);
        } catch (StatusRuntimeException error) {
            return new Outcome(content, null, error.getStatus().getCode());
        }
    }

    // A listening socket does not establish that a fresh JVM has initialized its
    // gRPC connection and validator. Probe a read-only operation before the race;
    // the test measures record identity and restart recovery, not cold-start latency.
    private static void ready(AuthoringFixtureServiceGrpc.AuthoringFixtureServiceBlockingStub stub) {
        assertThat(stub.withWaitForReady().withDeadlineAfter(30, TimeUnit.SECONDS)
                .normalizeText(NormalizeTextRequest.newBuilder().setText(" ready ").build())
                .getText()).isEqualTo("ready");
    }

    private static void close(ManagedChannel channel) throws InterruptedException {
        if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static void kill(Process process) throws InterruptedException {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
            assertThat(process.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private record Outcome(String content, WriteRecordResponse response, Status.Code status) {}
}
