package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.ConsoleSessions;
import ai.pipestream.proto.delegation.DelegationBridge;
import ai.pipestream.proto.delegation.DelegationReducer;
import ai.pipestream.proto.delegation.InProcessDelegationCoordinator;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.TaskMessageKind;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/** Purpose-built, cookie-authenticated HTTP projection of the delegation transcript. */
final class TaskConsoleApiHandler implements HttpHandler {

    private static final String PREFIX = "/api/tasks";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonFormat.Printer PROTO_JSON =
            JsonFormat.printer().omittingInsignificantWhitespace();
    private static final int MAX_BODY_BYTES = 20 * 1024;
    private static final int MAX_EVENTS = 256;

    private final DelegationBridge bridge;
    private final ConsoleSessions sessions;

    TaskConsoleApiHandler(DelegationBridge bridge, ConsoleSessions sessions) {
        this.bridge = java.util.Objects.requireNonNull(bridge, "delegation bridge");
        this.sessions = java.util.Objects.requireNonNull(sessions, "task console sessions");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            Caller caller = sessions.caller(exchange).orElse(null);
            if (caller == null) {
                respondError(exchange, 401, "authentication required");
                return;
            }
            if (!caller.holds(Scopes.WORKER_COORDINATE)) {
                respondError(exchange, 403, "caller '" + caller.name() + "' does not hold '"
                        + Scopes.WORKER_COORDINATE + "', which the task console requires");
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if (!path.equals(PREFIX) && !path.startsWith(PREFIX + "/")) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            String rest = path.substring(PREFIX.length());
            String method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
            try {
                if ((rest.isEmpty() || "/".equals(rest)) && "GET".equals(method)) {
                    listTasks(exchange);
                } else if ("/workers".equals(rest) && "GET".equals(method)) {
                    listWorkers(exchange);
                } else if ("/events".equals(rest) && "GET".equals(method)) {
                    watchEvents(exchange);
                } else {
                    taskRoute(exchange, method, rest);
                }
            } catch (IllegalArgumentException e) {
                respondError(exchange, 400, e.getMessage());
            } catch (IllegalStateException e) {
                respondError(exchange, 409, e.getMessage());
            }
        }
    }

    private void listWorkers(HttpExchange exchange) throws IOException {
        ArrayNode workers = JSON.createArrayNode();
        for (InProcessDelegationCoordinator.WorkerView worker
                : bridge.coordinator().workers()) {
            ObjectNode node = workers.addObject();
            node.put("workerId", worker.workerId());
            node.put("admitted", worker.admitted());
            node.put("connected", worker.connected());
            node.put("provider", worker.hello().getProvider());
            node.put("model", worker.hello().getModel());
            ArrayNode capabilities = node.putArray("capabilities");
            worker.hello().getCapabilitiesList().forEach(capability ->
                    capabilities.add(capability.getName()));
        }
        ObjectNode response = JSON.createObjectNode();
        response.set("workers", workers);
        respondJson(exchange, 200, response);
    }

    private void listTasks(HttpExchange exchange) throws IOException {
        Projection projection = projection();
        ObjectNode response = JSON.createObjectNode();
        ArrayNode tasks = response.putArray("tasks");
        projection.states().values().forEach(state ->
                tasks.add(taskJson(state, projection.offers().get(state.taskId()),
                        projection.lastCursors().getOrDefault(state.taskId(), 0L))));
        response.put("cursor", projection.cursor());
        response.set("findings", findingsJson(projection.reduced().findings()));
        respondJson(exchange, 200, response);
    }

    private void taskRoute(HttpExchange exchange, String method, String rest)
            throws IOException {
        String[] parts = rest.split("/", -1);
        if (parts.length < 2 || parts[1].isBlank()) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        String taskId = decodedUuid(parts[1]);
        if (parts.length == 2 && "GET".equals(method)) {
            taskDetail(exchange, taskId);
            return;
        }
        if (parts.length == 3 && "messages".equals(parts[2]) && "POST".equals(method)) {
            sendMessage(exchange, taskId);
            return;
        }
        exchange.sendResponseHeaders(404, -1);
    }

    private void taskDetail(HttpExchange exchange, String taskId) throws IOException {
        Projection projection = projection();
        DelegationReducer.TaskState state = projection.states().get(taskId);
        if (state == null) {
            respondError(exchange, 404, "unknown task");
            return;
        }
        ObjectNode response = JSON.createObjectNode();
        response.set("task", taskJson(state, projection.offers().get(taskId),
                projection.lastCursors().getOrDefault(taskId, 0L)));
        ArrayNode events = response.putArray("events");
        bridge.coordinator().eventsAfter(taskId, 0).stream().limit(MAX_EVENTS)
                .map(TaskConsoleApiHandler::eventJson).forEach(events::add);
        response.put("cursor", projection.cursor());
        response.set("findings", findingsJson(projection.reduced().findings().stream()
                .filter(finding -> taskId.equals(finding.taskId())).toList()));
        respondJson(exchange, 200, response);
    }

    private void watchEvents(HttpExchange exchange) throws IOException {
        Map<String, String> query = query(exchange);
        long after = boundedLong(query.get("after"), 0, Long.MAX_VALUE, 0, "after");
        int timeout = (int) boundedLong(query.get("timeoutMs"), 0, 30_000, 25_000,
                "timeoutMs");
        int max = (int) boundedLong(query.get("maxEvents"), 1, MAX_EVENTS, 64,
                "maxEvents");
        String taskId = query.getOrDefault("taskId", "");
        if (!taskId.isBlank()) {
            taskId = decodedUuid(taskId);
        }
        List<InProcessDelegationCoordinator.Event> events =
                bridge.coordinator().eventsAfter(taskId, after);
        if (events.isEmpty() && timeout > 0) {
            try {
                bridge.coordinator().waitForEvent(taskId, after, Duration.ofMillis(timeout));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                respondError(exchange, 503, "event wait interrupted");
                return;
            }
            events = bridge.coordinator().eventsAfter(taskId, after);
        }
        ObjectNode response = JSON.createObjectNode();
        ArrayNode result = response.putArray("events");
        events.stream().limit(max).map(TaskConsoleApiHandler::eventJson)
                .forEach(result::add);
        long cursor = events.stream().limit(max).mapToLong(
                InProcessDelegationCoordinator.Event::cursor).max().orElse(after);
        response.put("cursor", cursor);
        response.put("truncated", events.size() > max);
        respondJson(exchange, 200, response);
    }

    private void sendMessage(HttpExchange exchange, String taskId) throws IOException {
        byte[] body = BoundedBodies.read(exchange.getRequestBody(), MAX_BODY_BYTES);
        if (body == null) {
            exchange.sendResponseHeaders(413, -1);
            return;
        }
        JsonNode parsed;
        try {
            parsed = JSON.readTree(body);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid JSON");
        }
        if (parsed == null || !parsed.isObject()) {
            throw new IllegalArgumentException("message body must be a JSON object");
        }
        String recipient = requiredText(parsed, "recipient", 128);
        String text = requiredText(parsed, "text", 16_384);
        String replyTo = optionalText(parsed, "replyTo", 128);
        TaskMessageKind kind = messageKind(requiredText(parsed, "kind", 32));
        var message = bridge.sendCoordinatorMessage(recipient, taskId, kind, text,
                replyTo, List.of());
        ObjectNode response = JSON.createObjectNode();
        response.set("message", protoJson(message));
        respondJson(exchange, 201, response);
    }

    private Projection projection() {
        var coordinator = bridge.coordinator();
        DelegationReducer.Result reduced = coordinator.state();
        Map<String, OfferView> offers = new LinkedHashMap<>();
        Map<String, Long> lastCursors = new HashMap<>();
        long cursor = 0;
        for (InProcessDelegationCoordinator.Event event : coordinator.eventsAfter("", 0)) {
            cursor = Math.max(cursor, event.cursor());
            if (!event.taskId().isBlank()) {
                lastCursors.put(event.taskId(), event.cursor());
            }
            TranscriptEntry entry = event.entry();
            if (entry.hasCoordinatorFrame()) {
                DelegateResponse frame = entry.getCoordinatorFrame();
                if (frame.hasOffer()) {
                    offers.put(frame.getTaskId(), new OfferView(entry.getWorkerId(),
                            frame.getOffer()));
                }
            }
        }
        return new Projection(reduced, reduced.tasks(), offers, lastCursors, cursor);
    }

    private static ObjectNode taskJson(DelegationReducer.TaskState state, OfferView offer,
                                       long lastCursor) {
        ObjectNode node = JSON.createObjectNode();
        node.put("taskId", state.taskId());
        node.put("phase", state.phase().name().toLowerCase(Locale.ROOT));
        node.put("attempt", state.attempt());
        node.put("workerId", offer == null ? state.holder() : offer.workerId());
        node.put("objective", offer == null ? "" : offer.offer().getSpec().getObjective());
        node.put("candidateRevision", state.candidateRevision());
        node.put("lastProgressSeq", state.lastProgressSeq());
        node.put("lastCheckpointSeq", state.lastCheckpointSeq());
        node.put("lastCursor", lastCursor);
        return node;
    }

    private static ArrayNode findingsJson(List<DelegationReducer.Finding> findings) {
        ArrayNode result = JSON.createArrayNode();
        findings.forEach(finding -> {
            ObjectNode node = result.addObject();
            node.put("taskId", finding.taskId());
            node.put("frameId", finding.frameId());
            node.put("kind", finding.kind());
            node.put("error", finding.error());
        });
        return result;
    }

    private static ObjectNode eventJson(InProcessDelegationCoordinator.Event event) {
        ObjectNode node = JSON.createObjectNode();
        node.put("cursor", event.cursor());
        node.put("workerId", event.workerId());
        node.put("taskId", event.taskId());
        node.put("lane", event.entry().getLane().name());
        node.set("entry", protoJson(event.entry()));
        return node;
    }

    private static JsonNode protoJson(com.google.protobuf.Message message) {
        try {
            return JSON.readTree(PROTO_JSON.print(message));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("could not render delegation event", e);
        } catch (IOException e) {
            throw new IllegalStateException("could not parse rendered delegation event", e);
        }
    }

    private static TaskMessageKind messageKind(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "question" -> TaskMessageKind.TASK_MESSAGE_KIND_QUESTION;
            case "answer" -> TaskMessageKind.TASK_MESSAGE_KIND_ANSWER;
            case "guidance" -> TaskMessageKind.TASK_MESSAGE_KIND_GUIDANCE;
            case "note" -> TaskMessageKind.TASK_MESSAGE_KIND_NOTE;
            default -> throw new IllegalArgumentException(
                    "kind must be question, answer, guidance, or note");
        };
    }

    private static String requiredText(JsonNode object, String field, int maxLength) {
        String value = optionalText(object, field, maxLength);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private static String optionalText(JsonNode object, String field, int maxLength) {
        JsonNode node = object.get(field);
        if (node == null || node.isNull()) {
            return "";
        }
        if (!node.isTextual() || node.asText().length() > maxLength) {
            throw new IllegalArgumentException(field + " must be a bounded string");
        }
        return node.asText();
    }

    private static String decodedUuid(String value) {
        String decoded = URLDecoder.decode(value, StandardCharsets.UTF_8);
        try {
            return UUID.fromString(decoded).toString();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("task id must be a UUID");
        }
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> result = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isBlank()) {
            return result;
        }
        for (String pair : raw.split("&")) {
            int equals = pair.indexOf('=');
            String name = equals < 0 ? pair : pair.substring(0, equals);
            String value = equals < 0 ? "" : pair.substring(equals + 1);
            result.put(URLDecoder.decode(name, StandardCharsets.UTF_8),
                    URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return result;
    }

    private static long boundedLong(String value, long minimum, long maximum,
                                    long fallback, String name) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed < minimum || parsed > maximum) {
                throw new IllegalArgumentException(name + " is outside its allowed range");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
    }

    private static void respondJson(HttpExchange exchange, int status, JsonNode json)
            throws IOException {
        byte[] body = JSON.writeValueAsBytes(json);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    private static void respondError(HttpExchange exchange, int status, String message)
            throws IOException {
        ObjectNode body = JSON.createObjectNode();
        body.put("error", message == null || message.isBlank() ? "request failed" : message);
        respondJson(exchange, status, body);
    }

    private record OfferView(String workerId, TaskOffer offer) {
    }

    private record Projection(DelegationReducer.Result reduced,
                              Map<String, DelegationReducer.TaskState> states,
                              Map<String, OfferView> offers,
                              Map<String, Long> lastCursors,
                              long cursor) {
    }
}
