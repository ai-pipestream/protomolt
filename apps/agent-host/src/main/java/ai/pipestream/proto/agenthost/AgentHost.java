package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Long-polls one delegation feed and drives one model session. The cursor advances only after
 * the model acknowledges every relevant event and all validated commands are accepted by MCP.
 */
final class AgentHost implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> WORKER_PAYLOADS = Set.of(
            "offer", "expired", "cancellation", "revisionRequested", "accepted",
            "taskMessage");
    private static final Set<String> COORDINATOR_PAYLOADS = Set.of(
            "accept", "reject", "progress", "checkpoint", "blocked", "failed",
            "cancelled", "completion", "taskMessage");

    record Config(AgentRole role, String identity, String model, Path workspace,
                  Duration pollTimeout, int maxEvents) {
        Config {
            if (role == null || identity == null
                    || !identity.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                    || workspace == null || !workspace.isAbsolute()
                    || pollTimeout == null || pollTimeout.isNegative()
                    || pollTimeout.compareTo(Duration.ofSeconds(30)) > 0
                    || maxEvents < 1 || maxEvents > 256) {
                throw new IllegalArgumentException("invalid agent host configuration");
            }
        }
    }

    private final Config config;
    private final McpHttpClient mcp;
    private final AgentProvider provider;
    private final AgentHostStateStore states;
    private final AtomicBoolean closed = new AtomicBoolean();

    private AgentHostState state;

    AgentHost(Config config, McpHttpClient mcp, AgentProvider provider,
              AgentHostStateStore states, AgentHostState state) {
        this.config = config;
        this.mcp = mcp;
        this.provider = provider;
        this.states = states;
        this.state = state;
        syncProviderSession();
    }

    /** Registers a worker when necessary, or verifies that its durable stream is still live. */
    void connect() {
        if (config.role() != AgentRole.WORKER) {
            return;
        }
        ObjectNode listed = readTool("delegation-worker-list", MAPPER.createObjectNode());
        JsonNode existing = null;
        for (JsonNode worker : listed.path("workers")) {
            if (config.identity().equals(worker.path("workerId").asText())) {
                existing = worker;
                break;
            }
        }
        if (existing != null && existing.path("admitted").asBoolean()
                && existing.path("connected").asBoolean()) {
            return;
        }
        if (existing == null && state.cursor() > 0) {
            throw new AgentHostException("coordinator no longer knows worker '"
                    + config.identity() + "'; refusing to guess across transcript loss");
        }
        ObjectNode arguments = MAPPER.createObjectNode();
        arguments.put("workerId", config.identity());
        arguments.put("provider", provider.name());
        if (config.model() != null) {
            arguments.put("model", config.model());
        }
        ArrayNode capabilities = arguments.putArray("capabilities");
        capabilities.addObject().put("name", "structured-delegation")
                .put("description", "Consumes event batches and emits validated MCP commands");
        ObjectNode registered = mcp.callTool("delegation-worker-register", arguments);
        if (!registered.path("admitted").asBoolean()) {
            throw new AgentHostException("worker admission was rejected: "
                    + registered.path("reason").asText("no reason supplied"));
        }
    }

    /**
     * Gives the coordinator agent one durable startup turn. The bootstrap is executed once even
     * after a process restart and must produce at least one task offer.
     */
    void bootstrap(String objective) {
        if (config.role() != AgentRole.COORDINATOR) {
            throw new AgentHostException("only a coordinator host accepts a bootstrap objective");
        }
        if (state.pending() != null) {
            executePending();
        }
        if (state.bootstrapped()) {
            return;
        }
        if (objective == null || objective.isBlank()) {
            throw new AgentHostException("bootstrap objective is empty");
        }
        ObjectNode workers = readTool("delegation-worker-list", MAPPER.createObjectNode());
        ObjectNode packet = MAPPER.createObjectNode();
        packet.put("kind", "bootstrap");
        packet.put("objective", objective);
        packet.set("workers", workers.path("workers"));
        AgentTurn turn = prompt(packet, List.of());
        if (turn.commands().stream().noneMatch(
                command -> "delegation-offer".equals(command.tool()))) {
            throw new AgentHostException("coordinator bootstrap must offer at least one task");
        }
        state = state.withPending(new AgentHostState.PendingTurn(
                state.cursor(), List.of(), turn.commands(), 0, true));
        states.save(state);
        executePending();
    }

    /** Performs one long poll and one model turn when the batch has relevant events. */
    boolean pollOnce() {
        if (state.pending() != null) {
            // A command remains pending across process and coordinator restarts. Re-establish
            // the worker stream before retrying it so durable-before-visible execution does
            // not depend on the stream that existed when the command was produced.
            connect();
            executePending();
            return true;
        }
        connect();
        ObjectNode arguments = MAPPER.createObjectNode();
        arguments.put("afterCursor", state.cursor());
        arguments.put("timeoutMs", config.pollTimeout().toMillis());
        arguments.put("maxEvents", config.maxEvents());
        ObjectNode watched = readTool("delegation-watch", arguments);
        ArrayNode events = watched.withArray("events");
        if (events.isEmpty()) {
            return false;
        }
        long targetCursor = watched.path("cursor").asLong(state.cursor());
        if (targetCursor <= state.cursor()) {
            throw new AgentHostException("delegation watch cursor did not advance");
        }
        ArrayNode relevant = MAPPER.createArrayNode();
        List<Long> relevantCursors = new ArrayList<>();
        for (JsonNode event : events) {
            if (isRelevant(event)) {
                relevant.add(event);
                relevantCursors.add(event.path("cursor").asLong());
            }
        }
        if (relevant.isEmpty()) {
            state = state.withCursor(targetCursor);
            states.save(state);
            return true;
        }
        ObjectNode packet = MAPPER.createObjectNode();
        packet.put("kind", "delegation-events");
        packet.put("role", config.role().name().toLowerCase(java.util.Locale.ROOT));
        packet.put("identity", config.identity());
        packet.set("events", relevant);
        AgentTurn turn = prompt(packet, relevantCursors);
        state = state.withPending(new AgentHostState.PendingTurn(
                targetCursor, relevantCursors, turn.commands(), 0, false));
        states.save(state);
        executePending();
        return true;
    }

    /** Runs until closed, keeping long waits and model turns on one virtual thread. */
    void run() {
        int failures = 0;
        while (!closed.get()) {
            try {
                pollOnce();
                failures = 0;
            } catch (AgentHostException e) {
                if (closed.get()) {
                    return;
                }
                failures = Math.min(failures + 1, 5);
                System.err.println("agent-host: " + e.getMessage());
                try {
                    Thread.sleep(Duration.ofSeconds(1L << (failures - 1)));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    AgentHostState state() {
        return state;
    }

    private AgentTurn prompt(ObjectNode packet, List<Long> expectedCursors) {
        String prompt = promptText(packet, expectedCursors, null);
        AgentHostException firstFailure;
        try {
            String response = provider.prompt(prompt);
            syncProviderSession();
            return parseTurn(response, packet, expectedCursors);
        } catch (AgentHostException e) {
            firstFailure = e;
            syncProviderSession();
        }
        String repair = promptText(packet, expectedCursors, firstFailure.getMessage());
        String response = provider.prompt(repair);
        syncProviderSession();
        return parseTurn(response, packet, expectedCursors);
    }

    private AgentTurn parseTurn(String response, ObjectNode packet,
                                List<Long> expectedCursors) {
        AgentTurn turn = AgentTurn.parse(response, config.role(), expectedCursors,
                config.identity());
        validateRequiredActions(packet, turn);
        return turn;
    }

    private void validateRequiredActions(ObjectNode packet, AgentTurn turn) {
        Set<String> completionTasks = new HashSet<>();
        if (config.role() == AgentRole.COORDINATOR) {
            for (JsonNode event : packet.path("events")) {
                if (event.path("entry").path("workerFrame").has("completion")) {
                    completionTasks.add(event.path("taskId").asText());
                }
            }
        }
        for (JsonNode event : packet.path("events")) {
            String taskId = event.path("taskId").asText();
            JsonNode entry = event.path("entry");
            JsonNode frame = config.role() == AgentRole.WORKER
                    ? entry.path("coordinatorFrame") : entry.path("workerFrame");
            if (config.role() == AgentRole.WORKER) {
                if (frame.has("offer")) {
                    requireTaskCommand(turn, taskId, Set.of("delegation-accept"),
                            "a task offer requires delegation-accept");
                }
                if (frame.has("revisionRequested")) {
                    requireTaskCommand(turn, taskId, Set.of(
                                    "delegation-message", "delegation-progress",
                                    "delegation-checkpoint", "delegation-candidate"),
                            "a revision request requires a task action");
                }
                JsonNode message = frame.path("taskMessage");
                if (message.isObject()) {
                    String kind = message.path("kind").asText();
                    if ("TASK_MESSAGE_KIND_QUESTION".equals(kind)) {
                        requireTaskCommand(turn, taskId, Set.of("delegation-message"),
                                "a task question requires delegation-message");
                    } else if ("TASK_MESSAGE_KIND_GUIDANCE".equals(kind)) {
                        requireTaskCommand(turn, taskId, Set.of(
                                        "delegation-message", "delegation-progress",
                                        "delegation-checkpoint", "delegation-candidate"),
                                "task guidance cannot be acknowledged without a task action");
                    }
                }
            } else {
                if (frame.has("completion")) {
                    requireTaskCommand(turn, taskId, Set.of("delegation-review"),
                            "a completion candidate requires delegation-review");
                }
                if (frame.has("accept") || frame.has("progress")
                        || frame.has("checkpoint")) {
                    requireStepAction(turn, taskId, completionTasks);
                }
                JsonNode message = frame.path("taskMessage");
                if (message.isObject()
                        && "TASK_MESSAGE_KIND_QUESTION".equals(
                        message.path("kind").asText())) {
                    requireTaskCommand(turn, taskId, Set.of("delegation-message"),
                            "a task question requires delegation-message");
                }
            }
        }
    }

    private static void requireTaskCommand(AgentTurn turn, String taskId,
                                           Set<String> tools, String message) {
        boolean present = turn.commands().stream().anyMatch(command ->
                tools.contains(command.tool())
                        && taskId.equals(command.arguments().path("taskId").asText()));
        if (!present) {
            throw new AgentHostException(message + " for task " + taskId);
        }
    }

    /**
     * A worker accept, progress, or checkpoint is a short step the coordinator must steer:
     * answer it with task guidance or a cancellation. A delegation-review also satisfies the
     * requirement when the same batch carries the task's completion candidate, which keeps
     * the combined end-of-task batch answerable in one turn.
     */
    private static void requireStepAction(AgentTurn turn, String taskId,
                                          Set<String> completionTasks) {
        boolean present = turn.commands().stream().anyMatch(command -> {
            if (!taskId.equals(command.arguments().path("taskId").asText())) {
                return false;
            }
            if ("delegation-cancel".equals(command.tool())) {
                return true;
            }
            if ("delegation-message".equals(command.tool())) {
                return "TASK_MESSAGE_KIND_GUIDANCE".equals(
                        command.arguments().path("kind").asText());
            }
            return "delegation-review".equals(command.tool())
                    && completionTasks.contains(taskId);
        });
        if (!present) {
            throw new AgentHostException("a worker accept, progress, or checkpoint "
                    + "requires coordinator guidance or cancellation for task " + taskId);
        }
    }

    private String promptText(ObjectNode packet, List<Long> expectedCursors, String error) {
        String allowed = config.role() == AgentRole.WORKER
                ? "host-ack, delegation-accept, delegation-message, delegation-progress, "
                + "delegation-checkpoint, delegation-candidate"
                : "host-ack, delegation-offer, delegation-message, delegation-review, "
                + "delegation-cancel";
        StringBuilder prompt = new StringBuilder();
        prompt.append("You are the ")
                .append(config.role().name().toLowerCase(java.util.Locale.ROOT))
                .append(" agent attached to ProtoMolt as '").append(config.identity())
                .append("'. Process every supplied event. Return only one JSON object with "
                        + "handledEventCursors and commands. handledEventCursors must equal ")
                .append(expectedCursors).append(" in that order. Allowed tools: ")
                .append(allowed).append(". Each command has tool and arguments. Use host-ack "
                        + "with a short reason when an event needs no protocol action. Do not "
                        + "claim completion without the required passing evidence and a commit "
                        + "or artifact. Do not add Markdown fences or prose outside the JSON. ")
                .append(commandContract());
        if (error != null) {
            prompt.append(" Your previous response was rejected: ").append(error)
                    .append(". Correct the response for the same packet.");
        }
        prompt.append("\nPacket:\n").append(packet);
        return prompt.toString();
    }

    private String commandContract() {
        return config.role() == AgentRole.WORKER
                ? "Worker argument contract: host-ack={reason}; "
                + "delegation-accept={taskId,attempt}; "
                + "delegation-message={taskId,kind,text}; "
                + "delegation-progress={taskId,attempt,message}; "
                + "delegation-checkpoint={taskId,attempt,resumeToken,note}; "
                + "delegation-candidate={taskId,candidate}, where candidate has "
                + "attempt,revision,summary,evidence:[{checkName,verdict,ranAt,detail}],"
                + "commits:[{repository,commit,subject}]. verdict must be "
                + "CHECK_VERDICT_PASSED and commit must be a full 40-character SHA. "
                + "An offer requires delegation-accept. A question requires "
                + "delegation-message. Guidance and revision requests require a task "
                + "action and cannot use host-ack alone. Use exactly these field names "
                + "and no others."
                : "Coordinator argument contract: host-ack={reason}; "
                + "delegation-offer={workerId,taskId,leaseSeconds,spec}; "
                + "delegation-message={taskId,recipient,kind,text}; "
                + "delegation-review accept={taskId,decision,verdict}; "
                + "delegation-review revise={taskId,decision,feedback,failedChecks}; "
                + "delegation-cancel={taskId,reason}. A completion candidate requires "
                + "delegation-review. A question requires delegation-message. A worker "
                + "accept, progress, or checkpoint requires guidance (a delegation-message "
                + "of kind TASK_MESSAGE_KIND_GUIDANCE) or delegation-cancel; "
                + "delegation-review also answers them when the same batch carries the "
                + "task's completion candidate. Use "
                + "exactly these field names and no others.";
    }

    private boolean isRelevant(JsonNode event) {
        if (config.role() == AgentRole.WORKER
                && !config.identity().equals(event.path("workerId").asText())) {
            return false;
        }
        JsonNode entry = event.path("entry");
        JsonNode frame = config.role() == AgentRole.WORKER
                ? entry.path("coordinatorFrame") : entry.path("workerFrame");
        if (!frame.isObject()) {
            return false;
        }
        Set<String> payloads = config.role() == AgentRole.WORKER
                ? WORKER_PAYLOADS : COORDINATOR_PAYLOADS;
        for (String payload : payloads) {
            if (frame.has(payload)) {
                return true;
            }
        }
        return false;
    }

    private ObjectNode readTool(String tool, ObjectNode arguments) {
        try {
            return mcp.callTool(tool, arguments);
        } catch (AgentHostException first) {
            mcp.reconnect();
            return mcp.callTool(tool, arguments);
        }
    }

    private void executePending() {
        AgentHostState.PendingTurn pending = state.pending();
        while (pending != null && pending.nextCommand() < pending.commands().size()) {
            AgentTurn.Command command = pending.commands().get(pending.nextCommand());
            if (!"host-ack".equals(command.tool())) {
                mcp.callTool(command.tool(), command.arguments());
            }
            state = state.commandAdvanced();
            states.save(state);
            pending = state.pending();
        }
        if (state.pending() != null) {
            state = state.completePending();
            states.save(state);
        }
    }

    private void syncProviderSession() {
        String sessionId = provider.sessionId();
        if (sessionId != null && !sessionId.isBlank()
                && !sessionId.equals(state.providerSessionId())) {
            state = state.withProviderSession(sessionId);
            states.save(state);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            provider.close();
            mcp.close();
        }
    }
}
