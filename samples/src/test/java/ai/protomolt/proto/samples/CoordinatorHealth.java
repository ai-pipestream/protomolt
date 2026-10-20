package ai.protomolt.proto.samples;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/** Waits for a launched coordinator process to answer its HTTP health route. */
final class CoordinatorHealth {
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(150);

    private CoordinatorHealth() {
    }

    /**
     * Polls {@code /health} until it returns 200, with the default failure messages.
     *
     * @param process the coordinator process; its exit fails the wait at once
     * @param port the coordinator's HTTP port
     * @param log the coordinator's log, appended to either failure message
     * @throws Exception when the process exits, the wait times out or is interrupted
     */
    static void await(Process process, int port, Path log) throws Exception {
        await(process, port, log, "coordinator exited", "coordinator health timeout");
    }

    /**
     * Polls {@code /health} until it returns 200.
     *
     * @param process the coordinator process; its exit fails the wait at once
     * @param port the coordinator's HTTP port
     * @param log the coordinator's log, appended to either failure message
     * @param exitedMessage failure message when the process exits first
     * @param timeoutMessage failure message when the deadline passes; the last
     *     failed probe, if any, is attached as the cause
     * @throws Exception when the process exits, the wait times out or is interrupted
     */
    static void await(Process process, int port, Path log, String exitedMessage, String timeoutMessage)
            throws Exception {
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new AssertionError(exitedMessage + ":\n" + Files.readString(log),
                    lastFailure);
            try {
                var response = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health"))
                                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) return;
                lastFailure = new IllegalStateException("health returned HTTP " + response.statusCode()
                        + ": " + response.body());
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        throw new AssertionError(timeoutMessage + ":\n" + Files.readString(log), lastFailure);
    }
}
