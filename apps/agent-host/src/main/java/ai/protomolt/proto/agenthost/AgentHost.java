package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

    /**
     * @param resetOnTranscriptLoss whether a coordinator that no longer knows this worker
     *     may be rejoined by discarding the local transcript position. Off by default:
     *     resuming against a transcript that is gone would invent continuity. On, it is the
     *     operator stating that the loss is real, which is what a rebuilt coordinator means.
     */
    record Config(AgentRole role, String identity, String model, Path workspace,
                  Duration pollTimeout, int maxEvents, boolean resetOnTranscriptLoss) {
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

        /** A configuration that refuses to rejoin across transcript loss. */
        Config(AgentRole role, String identity, String model, Path workspace,
               Duration pollTimeout, int maxEvents) {
            this(role, identity, model, workspace, pollTimeout, maxEvents, false);
        }
    }

    private final Config config;
    private final McpHttpClient mcp;
    private final AgentProvider provider;
    private final AgentHostStateStore states;
    /** The deliverable contracts of the tasks this host was offered, by task id. */
    private final Map<String, DeliverableContract> contracts = new LinkedHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private AgentHostState state;

    AgentHost(Config config, McpHttpClient mcp, AgentProvider provider,
              AgentHostStateStore states, AgentHostState state) {
        this.config = config;
        this.mcp = mcp;
        this.provider = provider;
        this.states = states;
        this.state = state;
        state.contracts().forEach((taskId, json) -> {
            DeliverableContract.Builder contract = DeliverableContract.newBuilder();
            try {
                JsonFormat.parser().merge(json, contract);
                contracts.put(taskId, contract.build());
            } catch (InvalidProtocolBufferException unreadable) {
                // a contract the state file cannot describe is not one this host can use
            }
        });
        provider.outputSchema(AgentTurn.outputSchema(config.role(), contracts));
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
            if (!config.resetOnTranscriptLoss()) {
                throw new AgentHostException("coordinator no longer knows worker '"
                        + config.identity() + "'; refusing to guess across transcript loss."
                        + " Rerun with --reset-on-transcript-loss to rejoin from the start,"
                        + " which discards this worker's recorded position");
            }
            // Say exactly what is being dropped. A rejoin that reports nothing looks the
            // same as one that never lost anything, and the difference is a whole
            // transcript.
            System.err.println("agent-host: coordinator no longer knows worker '"
                    + config.identity() + "'; rejoining from the start and discarding the"
                    + " recorded position at cursor " + state.cursor()
                    + (state.pending() == null ? "" : " and one partly executed batch"));
            state = state.withoutTranscriptPosition();
            states.save(state);
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
        noteContracts(relevant);
        ObjectNode packet = MAPPER.createObjectNode();
        packet.put("kind", "delegation-events");
        packet.put("role", config.role().name().toLowerCase(java.util.Locale.ROOT));
        packet.put("identity", config.identity());
        packet.set("events", presented(relevant));
        AgentTurn turn = prompt(packet, relevantCursors);
        state = state.withPending(new AgentHostState.PendingTurn(
                targetCursor, relevantCursors, turn.commands(), 0, false));
        states.save(state);
        executePending();
        reportUnfinishedWork(turn);
        return true;
    }

    /**
     * Reports work this turn took on without finishing. A worker turn runs only when the
     * coordinator sends a frame, and its own frames are filtered out, so a turn that accepts
     * a task and submits no candidate for it leaves the task where nothing can move it: the
     * coordinator waits for the worker and the worker waits for an event that will not
     * arrive unless the coordinator sends one for another reason. Left unreported that is a
     * task which simply stops, indistinguishable from one still being worked on, until the
     * lease expires minutes later with no explanation.
     *
     * <p>This is a report, not a refusal. Accepting now and finishing on a later frame is
     * legitimate when the coordinator has more to say, so the host cannot know the task is
     * doomed and does not pretend to. It states what the model returned and what follows
     * from it, which is the part nobody could see before.
     */
    private void reportUnfinishedWork(AgentTurn turn) {
        if (config.role() != AgentRole.WORKER) {
            return;
        }
        Set<String> accepted = new LinkedHashSet<>();
        Set<String> submitted = new HashSet<>();
        List<String> tools = new ArrayList<>();
        for (AgentTurn.Command command : turn.commands()) {
            tools.add(command.tool());
            String taskId = command.arguments().path("taskId").asText("");
            if (taskId.isEmpty()) {
                continue;
            }
            if (ACCEPT_TOOL.equals(command.tool())) {
                accepted.add(taskId);
            } else if (CANDIDATE_TOOL.equals(command.tool())) {
                submitted.add(taskId);
            }
        }
        accepted.removeAll(submitted);
        for (String taskId : accepted) {
            System.err.println("agent-host: accepted task " + taskId
                    + " and submitted no candidate for it. This turn returned ["
                    + String.join(", ", tools) + "]. A worker turn runs only on a"
                    + " coordinator frame, so unless the coordinator sends another one this"
                    + " task will not progress and its lease will expire unworked.");
        }
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

    /** The deliverable contracts this host knows, by task id. */
    Map<String, DeliverableContract> contracts() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(contracts));
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
                config.identity(), contracts);
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

    /**
     * Records the deliverable contract of each offer in the batch and forgets the contract
     * of each task the batch closes, persisting the change and handing providers with
     * enforced structured output the schema that now applies.
     */
    private void noteContracts(ArrayNode events) {
        if (config.role() != AgentRole.WORKER) {
            return;
        }
        boolean changed = false;
        for (JsonNode event : events) {
            JsonNode frame = event.path("entry").path("coordinatorFrame");
            String taskId = event.path("taskId").asText();
            JsonNode contract = frame.path("offer").path("spec").path("contract");
            if (contract.isObject()) {
                DeliverableContract.Builder builder = DeliverableContract.newBuilder();
                try {
                    JsonFormat.parser().merge(contract.toString(), builder);
                    contracts.put(taskId, builder.build());
                    changed = true;
                } catch (InvalidProtocolBufferException unreadable) {
                    System.err.println("agent-host: the offer for task " + taskId
                            + " names a deliverable contract this host cannot read: "
                            + unreadable.getMessage());
                }
            } else if (frame.has("offer") && contracts.remove(taskId) != null) {
                changed = true;
            }
            if ((frame.has("accepted") || frame.has("cancellation") || frame.has("expired"))
                    && contracts.remove(taskId) != null) {
                changed = true;
            }
        }
        if (!changed) {
            return;
        }
        Map<String, String> serialized = new LinkedHashMap<>();
        contracts.forEach((taskId, contract) -> {
            try {
                serialized.put(taskId, JsonFormat.printer().omittingInsignificantWhitespace()
                        .print(contract));
            } catch (InvalidProtocolBufferException e) {
                throw new AgentHostException("could not record the contract of task "
                        + taskId, e);
            }
        });
        state = state.withContracts(serialized);
        states.save(state);
        provider.outputSchema(AgentTurn.outputSchema(config.role(), contracts));
    }

    /**
     * The batch as the model reads it: an offer's contract shows its type name and its
     * rendered schema as an object, and the descriptor bytes the host resolves itself are
     * replaced by a note, since base64 is noise to a model and the bytes are large.
     */
    private static ArrayNode presented(ArrayNode events) {
        ArrayNode shown = events.deepCopy();
        for (JsonNode event : shown) {
            JsonNode contract = event.path("entry").path("coordinatorFrame").path("offer")
                    .path("spec").path("contract");
            if (!(contract instanceof ObjectNode visible)) {
                continue;
            }
            JsonNode bytes = visible.remove("descriptorSet");
            if (bytes != null) {
                visible.put("descriptorSetNote", "the host holds the descriptor set ("
                        + bytes.asText().length() + " base64 characters) and reads the"
                        + " result with it");
            }
            JsonNode schema = visible.remove("jsonSchema");
            if (schema != null && schema.isTextual()) {
                try {
                    visible.set("schema", MAPPER.readTree(schema.asText()));
                } catch (java.io.IOException notJson) {
                    visible.put("schema", schema.asText());
                }
            }
        }
        return shown;
    }

    /** One sentence per known contract, so the model knows which type each task returns. */
    private String contractLines() {
        if (contracts.isEmpty()) {
            return "";
        }
        StringBuilder lines = new StringBuilder();
        contracts.forEach((taskId, contract) -> lines.append(" Task ").append(taskId)
                .append(" requires delegation-candidate with candidate.result of type ")
                .append(contract.getTypeName())
                .append(" (\"@type\": \"type.googleapis.com/").append(contract.getTypeName())
                .append("\" plus the fields of the schema in its offer)."));
        return lines.toString();
    }

    private String promptText(ObjectNode packet, List<Long> expectedCursors, String error) {
        String allowed = String.join(", ", AgentTurn.tools(config.role()));
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
                .append(commandContract()).append(contractLines());
        if (error != null) {
            prompt.append(" Your previous response was rejected: ").append(error)
                    .append(". Correct the response for the same packet.");
        }
        prompt.append("\nPacket:\n").append(packet);
        return prompt.toString();
    }

    /**
     * What a command may contain, in the words of the delegation request messages.
     *
     * <p>The shape and the message rules are rendered from the same descriptors the turn
     * is validated against, so the prompt cannot describe a command the host would refuse.
     * What follows them is the host's own policy: which event obliges which verb, which is
     * a property of the loop rather than of any request message.
     */
    private String commandContract() {
        String contract = AgentTurn.commandContract(config.role());
        return config.role() == AgentRole.WORKER
                ? contract + " An offer requires delegation-accept. A question requires "
                + "delegation-message. Guidance and revision requests require a task "
                + "action and cannot use host-ack alone."
                : contract + " A completion candidate requires delegation-review. A "
                + "question requires delegation-message. A worker accept, progress, or "
                + "checkpoint requires guidance (a delegation-message of kind "
                + "TASK_MESSAGE_KIND_GUIDANCE) or delegation-cancel; delegation-review "
                + "also answers them when the same batch carries the task's completion "
                + "candidate.";
    }

    /** The worker command that takes on a task. */
    private static final String ACCEPT_TOOL = "delegation-accept";
    /** The worker command that submits finished work for review. */
    private static final String CANDIDATE_TOOL = "delegation-candidate";

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
