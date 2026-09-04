package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.serve.ProtoMoltServe;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces the production defect where a worker facing the same malformed model reply
 * looped {@code pollOnce()} forever on a seconds-scale backoff: 437 prompts and about 129M
 * input tokens over 3.5 hours before the task lease expired underneath it. {@link
 * AgentHost#run()} must instead back off in minutes for rejected model replies and give up
 * on a batch that keeps failing, rather than retry it without limit.
 */
class AgentHostBatchFailureTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WORKER = "kimi-worker";
    private static final String TASK_ONE = "11111111-1111-4111-8111-111111111111";
    private static final String TASK_TWO = "22222222-2222-4222-8222-222222222222";
    private static final AgentHost.Sleeper NO_SLEEP = duration -> { };

    @TempDir
    Path temporary;

    @Test
    void aBatchThatOnlyEverGetsGarbageGivesUpAtTheConfiguredMaxWithoutSleepingForReal()
            throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("give-up-workspace"));
        Path workerState = temporary.resolve("give-up-state/kimi.json");
        int maxBatchFailures = 3;
        TrackingGarbageProvider provider = new TrackingGarbageProvider();
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AgentHost worker = hostWithBatchLimit(endpoint, workerState, workspace, provider,
                    maxBatchFailures);
            try (worker; McpHttpClient coordinatorSide = new McpHttpClient(endpoint,
                    () -> null)) {
                worker.connect();
                offer(coordinatorSide, TASK_ONE);

                long startNanos = System.nanoTime();
                worker.run();
                long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

                // The whole point of the injected Sleeper is that giving up after a handful
                // of rejections never touches the real minutes-scale clock; a slow test here
                // would mean the seam is not actually wired into the backoff.
                assertThat(elapsedMillis).isLessThan(5_000);
                assertThat(worker.gaveUp()).isTrue();
                assertThat(worker.state().cursor()).isZero();
                assertThat(worker.state().pending()).isNull();
                assertThat(provider.closed).isTrue();
            }
        }
    }

    @Test
    void aValidReplyAfterNearMaxRejectionsResetsTheBatchFailureCounter() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("reset-workspace"));
        Path workerState = temporary.resolve("reset-state/kimi.json");
        int maxBatchFailures = 3;
        TwoTaskProvider provider = new TwoTaskProvider(TASK_ONE);
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AgentHost worker = hostWithBatchLimit(endpoint, workerState, workspace, provider,
                    maxBatchFailures);
            try (worker; McpHttpClient coordinatorSide = new McpHttpClient(endpoint,
                    () -> null)) {
                worker.connect();
                offer(coordinatorSide, TASK_ONE);

                // Drive the first task's batch by hand: two rejections (one short of the
                // configured max), then a valid reply that completes it. If the counter is
                // not reset by that success, the very next rejection on task two would push
                // the total to the max and give up immediately.
                assertThatThrownBy(worker::pollOnce).isInstanceOf(ModelReplyException.class);
                assertThatThrownBy(worker::pollOnce).isInstanceOf(ModelReplyException.class);
                assertThat(worker.pollOnce()).isTrue();
                long cursorAfterTaskOne = worker.state().cursor();
                assertThat(cursorAfterTaskOne).isPositive();

                offer(coordinatorSide, TASK_TWO);

                PrintStream originalErr = System.err;
                ByteArrayOutputStream captured = new ByteArrayOutputStream();
                System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
                try {
                    worker.run();
                } finally {
                    System.setErr(originalErr);
                }

                // Task two only ever gets garbage. If task one's success had not reset the
                // counter, task two's first rejection alone would already hit the max (2
                // carried over + 1) and the log below would report 1 rejection, not 3.
                assertThat(worker.gaveUp()).isTrue();
                assertThat(worker.state().cursor()).isEqualTo(cursorAfterTaskOne);
                String log = captured.toString(StandardCharsets.UTF_8);
                assertThat(log).contains("giving up on the batch after cursor "
                        + cursorAfterTaskOne + " after 3 rejected replies");
            }
        }
    }

    @Test
    void aPlainAgentHostExceptionNeverCountsTowardTheModelReplyLimit() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("transport-workspace"));
        Path workerState = temporary.resolve("transport-state/kimi.json");
        int maxBatchFailures = 2;
        int stopAfterCalls = 9;
        AtomicInteger calls = new AtomicInteger();
        AgentProvider provider = new AgentProvider() {
            @Override
            public String name() {
                return "kimi";
            }

            @Override
            public String sessionId() {
                return "transport-failure-session";
            }

            @Override
            public String prompt(String value) {
                if (calls.incrementAndGet() > stopAfterCalls) {
                    // Not an AgentHostException: this is the test's own way to stop what
                    // would otherwise be an unbounded loop, since a plain transport failure
                    // is never capped by design.
                    throw new RuntimeException("test harness stop");
                }
                throw new AgentHostException("simulated transport failure");
            }

            @Override
            public void close() {
            }
        };
        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0))) {
            URI endpoint = URI.create("http://127.0.0.1:" + serve.httpPort() + "/mcp");
            AgentHost worker = hostWithBatchLimit(endpoint, workerState, workspace, provider,
                    maxBatchFailures);
            try (worker; McpHttpClient coordinatorSide = new McpHttpClient(endpoint,
                    () -> null)) {
                worker.connect();
                offer(coordinatorSide, TASK_ONE);

                assertThatThrownBy(worker::run)
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("test harness stop");

                assertThat(worker.gaveUp()).isFalse();
                assertThat(worker.state().cursor()).isZero();
                assertThat(calls.get()).isGreaterThan(maxBatchFailures);
            }
        }
    }

    private AgentHost hostWithBatchLimit(URI endpoint, Path statePath, Path workspace,
                                         AgentProvider provider, int maxBatchFailures) {
        AgentHostStateStore store = new AgentHostStateStore(statePath);
        AgentHostState state = store.loadOrCreate(WORKER, AgentRole.WORKER, "kimi", workspace);
        return new AgentHost(new AgentHost.Config(AgentRole.WORKER, WORKER, null,
                workspace.toAbsolutePath(), Duration.ofMillis(100), 64, false,
                maxBatchFailures),
                new McpHttpClient(endpoint, () -> null), provider, store, state, NO_SLEEP);
    }

    private static void offer(McpHttpClient coordinatorSide, String taskId) {
        ObjectNode spec = MAPPER.createObjectNode().put("objective", "do the work");
        spec.putArray("requiredChecks").addObject().put("name", "unit-tests");
        coordinatorSide.callTool("delegation-offer", MAPPER.createObjectNode()
                .put("workerId", WORKER).put("taskId", taskId)
                .put("leaseSeconds", 300).set("spec", spec));
    }

    /** Always answers with text that is not JSON at all, and tracks whether it was closed. */
    private static final class TrackingGarbageProvider implements AgentProvider {
        private volatile boolean closed;

        @Override
        public String name() {
            return "kimi";
        }

        @Override
        public String sessionId() {
            return "garbage-session";
        }

        @Override
        public String prompt(String value) {
            return "not a JSON reply, no braces here at all";
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * Answers a first task's offer with garbage twice, then a valid accept; answers a second
     * task's offer with garbage forever.
     */
    private static final class TwoTaskProvider implements AgentProvider {
        private final String firstTask;
        private final AtomicInteger firstTaskCalls = new AtomicInteger();

        TwoTaskProvider(String firstTask) {
            this.firstTask = firstTask;
        }

        @Override
        public String name() {
            return "kimi";
        }

        @Override
        public String sessionId() {
            return "two-task-session";
        }

        @Override
        public String prompt(String value) {
            ObjectNode packet = (ObjectNode) readPacket(value);
            JsonNode firstEvent = packet.path("events").get(0);
            String taskId = firstEvent.path("taskId").asText();
            long cursor = firstEvent.path("cursor").asLong();
            if (firstTask.equals(taskId) && firstTaskCalls.incrementAndGet() > 4) {
                return accept(cursor, firstTask);
            }
            return "not a JSON reply, no braces here at all";
        }

        private static JsonNode readPacket(String value) {
            try {
                return MAPPER.readTree(value.substring(value.lastIndexOf("Packet:\n") + 8));
            } catch (Exception e) {
                throw new AgentHostException("two-task provider could not read packet", e);
            }
        }

        private static String accept(long cursor, String taskId) {
            return "{\"handledEventCursors\":[" + cursor + "],\"commands\":[{"
                    + "\"tool\":\"delegation-accept\",\"arguments\":{"
                    + "\"taskId\":\"" + taskId + "\",\"attempt\":1}}]}";
        }

        @Override
        public void close() {
        }
    }
}
