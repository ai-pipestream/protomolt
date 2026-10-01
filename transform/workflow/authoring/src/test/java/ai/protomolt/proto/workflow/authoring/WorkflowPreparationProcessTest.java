package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Real JVM ownership and pending-intent recovery, not a host power-loss test. */
class WorkflowPreparationProcessTest {
    @TempDir Path directory;

    @Test
    void killedOwnerReleasesProcessLockAndPendingIntentSurvives() throws Exception {
        var intent = FileSystemWorkflowPreparationRepositoryTest.intent(
                "dcff5265-1466-46c0-bbe0-a9dc39da0e82", 1, 1,
                "dcff5265-1466-46c0-bbe0-a9dc39da0e83", "{}");
        Path input = directory.resolve("intent.pb");
        Files.write(input, intent.toByteArray());
        Path ledger = directory.resolve("ledger");
        Path firstLog = directory.resolve("first.log");
        Path secondLog = directory.resolve("second.log");
        Process first = null;
        Process second = null;
        try {
            first = start(ledger, input, firstLog, "hold");
            await(first, firstLog, "reserved");
            var before = new FileSystemWorkflowPreparationRepository(ledger)
                    .find(intent.getRequest().getTaskId(), 1, 1).orElseThrow();
            assertThat(before.hasPending()).isTrue();
            second = start(ledger, input, secondLog, "return");
            await(second, secondLog, "waiting");
            assertThat(second.waitFor(500, TimeUnit.MILLISECONDS)).as(Files.readString(secondLog)).isFalse();
            assertThat(Files.readString(secondLog)).doesNotContain("reserved");

            kill(first);
            assertThat(second.waitFor(45, TimeUnit.SECONDS)).as(Files.readString(secondLog)).isTrue();
            assertThat(second.exitValue()).as(Files.readString(secondLog)).isZero();
            assertThat(Files.readString(secondLog)).contains("reserved");
            var after = new FileSystemWorkflowPreparationRepository(ledger)
                    .find(intent.getRequest().getTaskId(), 1, 1).orElseThrow();
            assertThat(after).isEqualTo(before);
        } finally {
            kill(first);
            kill(second);
        }
    }

    private static Process start(Path ledger, Path input, Path log, String mode) throws Exception {
        String classpath = System.getProperty("protomolt.authoring.test.classpath");
        assertThat(classpath).isNotBlank();
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, Child.class.getName(), ledger.toString(), input.toString(), mode)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static void await(Process process, Path log, String marker) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (System.nanoTime() < deadline) {
            String output = Files.exists(log) ? Files.readString(log) : "";
            if (output.contains(marker)) return;
            assertThat(process.isAlive()).as(output).isTrue();
            Thread.sleep(25);
        }
        throw new AssertionError("Child did not reach " + marker + ": " + Files.readString(log));
    }

    private static void kill(Process process) throws Exception {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** Separate JVM entry point; stdin holds the first process inside its callback. */
    public static final class Child {
        public static void main(String[] args) throws Exception {
            var repository = new FileSystemWorkflowPreparationRepository(Path.of(args[0]));
            var intent = WorkflowPreparationIntent.parseFrom(Files.readAllBytes(Path.of(args[1])));
            var request = intent.getRequest();
            System.out.println("waiting");
            repository.withExclusiveIntent(request.getTaskId(), request.getAttempt(), request.getRevision(),
                    session -> {
                        session.reserveOrMatch(intent);
                        System.out.println("reserved");
                        if (args[2].equals("hold")) System.in.read();
                        return null;
                    });
        }
    }
}
