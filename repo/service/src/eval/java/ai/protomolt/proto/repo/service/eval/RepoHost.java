package ai.protomolt.proto.repo.service.eval;

import ai.protomolt.proto.repo.service.RepoServiceMain;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The production entry point running in its own JVM: {@link RepoServiceMain} booted
 * from a complete environment, exactly as a container does, with its log captured so
 * readiness is read off the same line an operator would look for and shutdown is
 * judged by the exit code and the absence of exceptions.
 */
final class RepoHost implements AutoCloseable {
    private static final String READY_LINE = "repo-service listening on port ";
    private static final String HTTP_LINE = "repository HTTP port ";
    private static final String LIFECYCLE_LINE = "repo lifecycle loops started";

    final Process process;
    final Path log;
    final int grpcPort;
    final int httpPort;
    final ManagedChannel channel;

    private RepoHost(Process process, Path log, int grpcPort, int httpPort) {
        this.process = process;
        this.log = log;
        this.grpcPort = grpcPort;
        this.httpPort = httpPort;
        this.channel = NettyChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
    }

    /** Starts the host and waits for its readiness line, failing with the whole log otherwise. */
    static RepoHost launch(Map<String, String> environment, Path log) throws Exception {
        var builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx512m",
                "-cp", System.getProperty("protomolt.eval.runtimeClasspath"),
                RepoServiceMain.class.getName())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile());
        // Nothing from the Gradle worker's own environment reaches the host.
        builder.environment().keySet().removeIf(key -> key.startsWith("DOCUMENT_PLATFORM_")
                || key.startsWith("PROTOMOLT_"));
        builder.environment().putAll(environment);
        Process process = builder.start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (process.isAlive() && System.nanoTime() < deadline) {
                List<String> lines = Files.readAllLines(log);
                // The gRPC port line is written when Netty binds, before the HTTP routes
                // and the lifecycle loops start; the host is ready once all three are up.
                int grpcPort = portAfter(lines, READY_LINE);
                int httpPort = portAfter(lines, HTTP_LINE);
                boolean lifecycle = lines.stream().anyMatch(line -> line.contains(LIFECYCLE_LINE));
                if (grpcPort > 0 && httpPort > 0 && lifecycle) {
                    return new RepoHost(process, log, grpcPort, httpPort);
                }
                Thread.sleep(100);
            }
            throw new AssertionError("repo-service did not become ready (alive=" + process.isAlive()
                    + "):\n" + Files.readString(log));
        } catch (Throwable failure) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            throw failure;
        }
    }

    /** The integer that follows {@code marker} on the first log line carrying it, or -1. */
    private static int portAfter(List<String> lines, String marker) {
        for (String line : lines) {
            int at = line.indexOf(marker);
            if (at < 0) continue;
            int start = at + marker.length();
            int end = start;
            while (end < line.length() && Character.isDigit(line.charAt(end))) end++;
            if (end > start) return Integer.parseInt(line.substring(start, end));
        }
        return -1;
    }

    String logText() throws IOException {
        return Files.readString(log);
    }

    /**
     * SIGTERM, then the same judgement an operator applies: the process drains and exits
     * with 143 and its log carries neither an uncaught exception nor a failed shutdown.
     */
    @Override
    public void close() throws Exception {
        try {
            if (process.isAlive()) process.destroy();
            assertThat(process.waitFor(60, TimeUnit.SECONDS))
                    .as("repo-service must drain on SIGTERM:\n%s", logText()).isTrue();
            assertThat(process.exitValue()).as("SIGTERM exit on Linux").isEqualTo(143);
            assertThat(logText()).doesNotContain("Exception in thread", "Shutdown failed");
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
            channel.shutdownNow();
            channel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
