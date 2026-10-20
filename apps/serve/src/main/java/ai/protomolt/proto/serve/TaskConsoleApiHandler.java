package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.receipt.DelegationRecordProjector;
import ai.protomolt.proto.delegation.v1.CheckpointReference;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.workflow.RecordSigning;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Purpose-built, cookie-authenticated HTTP projection of the delegation transcript.
 *
 * <p>The handler owns authorization, routing, and the status mapping, in that order: an
 * unauthenticated request is a 401, an authenticated caller without
 * {@link Scopes#WORKER_COORDINATE} is a 403, and only then does the route decide; a refused
 * input is a 400 and a refused state transition a 409. Inbound decoding lives in
 * {@link TaskConsoleRequests} and the transcript-to-JSON rendering in
 * {@link TaskConsoleProjection}.
 */
final class TaskConsoleApiHandler implements HttpHandler {

    private static final String PREFIX = "/api/tasks";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonFormat.Printer PROTO_JSON =
            JsonFormat.printer().omittingInsignificantWhitespace();
    private static final int MAX_BODY_BYTES = 20 * 1024;
    private static final int MAX_OFFER_BYTES = 1024 * 1024;
    private static final int MAX_DESCRIPTOR_BYTES = 512 * 1024;
    private static final int MAX_EVENTS = 256;

    private final DelegationBridge bridge;
    private final ConsoleSessions sessions;
    private final RecordSigning signing;

    TaskConsoleApiHandler(DelegationBridge bridge, ConsoleSessions sessions) {
        this(bridge, sessions, null);
    }

    TaskConsoleApiHandler(DelegationBridge bridge, ConsoleSessions sessions,
                          RecordSigning signing) {
        this.bridge = java.util.Objects.requireNonNull(bridge, "delegation bridge");
        this.sessions = java.util.Objects.requireNonNull(sessions, "task console sessions");
        this.signing = signing;
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
                } else if ("/offer".equals(rest) && "POST".equals(method)) {
                    offerTask(exchange);
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
        respondJson(exchange, 200, TaskConsoleProjection.workersJson(bridge.coordinator()));
    }

    private void listTasks(HttpExchange exchange) throws IOException {
        respondJson(exchange, 200,
                TaskConsoleProjection.tasksJson(TaskConsoleProjection.project(
                        bridge.coordinator())));
    }

    private void taskRoute(HttpExchange exchange, String method, String rest)
            throws IOException {
        // rest is "/{taskId}" or "/{taskId}/{messages|review|review-retry|record|cancel}".
        if (rest.length() < 2 || rest.charAt(0) != '/') {
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        int slash = rest.indexOf('/', 1);
        String idSegment = slash < 0 ? rest.substring(1) : rest.substring(1, slash);
        if (idSegment.isBlank()) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        String taskId = TaskConsoleRequests.decodedUuid(idSegment);
        if (slash < 0) {
            if ("GET".equals(method)) {
                taskDetail(exchange, taskId);
                return;
            }
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        String sub = rest.substring(slash + 1);
        if ("messages".equals(sub) && "POST".equals(method)) {
            sendMessage(exchange, taskId);
            return;
        }
        if ("review".equals(sub) && "POST".equals(method)) {
            review(exchange, taskId);
            return;
        }
        if ("review-retry".equals(sub) && "POST".equals(method)) {
            retryReview(exchange, taskId);
            return;
        }
        if ("record".equals(sub) && "POST".equals(method)) {
            exportRecord(exchange, taskId);
            return;
        }
        if ("cancel".equals(sub) && "POST".equals(method)) {
            cancelTask(exchange, taskId);
            return;
        }
        exchange.sendResponseHeaders(404, -1);
    }

    private void taskDetail(HttpExchange exchange, String taskId) throws IOException {
        TaskConsoleProjection.Projection projection =
                TaskConsoleProjection.project(bridge.coordinator());
        DelegationReducer.TaskState state = projection.states().get(taskId);
        if (state == null) {
            respondError(exchange, 404, "unknown task");
            return;
        }
        respondJson(exchange, 200, TaskConsoleProjection.taskDetailJson(bridge.coordinator(),
                projection, taskId, state, MAX_EVENTS));
    }

    private void watchEvents(HttpExchange exchange) throws IOException {
        Map<String, String> query = TaskConsoleRequests.query(exchange);
        long after = TaskConsoleRequests.boundedLong(query.get("after"), 0, Long.MAX_VALUE, 0,
                "after");
        int timeout = (int) TaskConsoleRequests.boundedLong(query.get("timeoutMs"), 0, 30_000,
                25_000, "timeoutMs");
        int max = (int) TaskConsoleRequests.boundedLong(query.get("maxEvents"), 1, MAX_EVENTS,
                64, "maxEvents");
        String taskId = query.getOrDefault("taskId", "");
        if (!taskId.isBlank()) {
            taskId = TaskConsoleRequests.decodedUuid(taskId);
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
        events.stream().limit(max)
                .map(event -> TaskConsoleProjection.eventJson(bridge.coordinator(), event))
                .forEach(result::add);
        long cursor = events.stream().limit(max).mapToLong(
                InProcessDelegationCoordinator.Event::cursor).max().orElse(after);
        response.put("cursor", cursor);
        response.put("truncated", events.size() > max);
        respondJson(exchange, 200, response);
    }

    private void sendMessage(HttpExchange exchange, String taskId) throws IOException {
        JsonNode parsed = TaskConsoleRequests.readObjectBody(exchange, MAX_BODY_BYTES, "message");
        if (parsed == null) {
            exchange.sendResponseHeaders(413, -1);
            return;
        }
        String recipient = TaskConsoleRequests.requiredText(parsed, "recipient", 128);
        String text = TaskConsoleRequests.requiredText(parsed, "text", 16_384);
        String replyTo = TaskConsoleRequests.optionalText(parsed, "replyTo", 128);
        TaskMessageKind kind = TaskConsoleRequests.messageKind(
                TaskConsoleRequests.requiredText(parsed, "kind", 32));
        var message = bridge.sendCoordinatorMessage(recipient, taskId, kind, text,
                replyTo, List.of());
        ObjectNode response = JSON.createObjectNode();
        response.set("message", TaskConsoleProjection.protoJson(message));
        respondJson(exchange, 201, response);
    }

    /**
     * Offers a durable task to a worker: the one coordinator move the browser
     * could not make. The task identity is generated here, the spec is bounded
     * the way every console body is, and the offer lands on the transcript
     * like any other protocol fact.
     */
    private void offerTask(HttpExchange exchange) throws IOException {
        JsonNode parsed = TaskConsoleRequests.readObjectBody(exchange, MAX_OFFER_BYTES, "offer");
        if (parsed == null) {
            exchange.sendResponseHeaders(413, -1);
            return;
        }
        String workerId = TaskConsoleRequests.requiredText(parsed, "workerId", 128);
        String requestedId = TaskConsoleRequests.optionalText(parsed, "taskId", 36);
        String taskId = requestedId.isBlank() ? UUID.randomUUID().toString()
                : TaskConsoleRequests.decodedUuid(requestedId);
        TaskSpec previous = TaskSpec.getDefaultInstance();
        if (!requestedId.isBlank()) {
            if (!bridge.coordinator().state().tasks().containsKey(taskId)) {
                throw new IllegalArgumentException("taskId must name an existing task to re-offer");
            }
            for (var event : bridge.coordinator().eventsAfter(taskId, 0)) {
                if (event.entry().getCoordinatorFrame().hasOffer()) {
                    previous = event.entry().getCoordinatorFrame().getOffer().getSpec();
                }
            }
        }
        // Retain protocol fields the bounded form cannot edit (constraints/context).
        TaskSpec.Builder spec = TaskSpec.newBuilder()
                .mergeFrom(previous).clearAllowedScope().clearRequiredChecks().clearContract()
                .setObjective(TaskConsoleRequests.requiredText(parsed, "objective", 4096));
        JsonNode contract = parsed.get("contract");
        if (contract != null && !contract.isNull()) {
            if (!contract.isObject() || contract.has("jsonSchema")) {
                throw new IllegalArgumentException(
                        "contract must contain descriptorSet and typeName; jsonSchema is derived by the server");
            }
            byte[] descriptor;
            try {
                descriptor = java.util.Base64.getDecoder().decode(
                        TaskConsoleRequests.requiredText(contract, "descriptorSet",
                                MAX_OFFER_BYTES));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("contract.descriptorSet must be base64 descriptor bytes");
            }
            if (descriptor.length == 0 || descriptor.length > MAX_DESCRIPTOR_BYTES) {
                throw new IllegalArgumentException("contract.descriptorSet must contain 1 to 524288 bytes");
            }
            spec.setContract(DeliverableContract.newBuilder()
                    .setDescriptorSet(ByteString.copyFrom(descriptor))
                    .setTypeName(TaskConsoleRequests.requiredText(contract, "typeName", 512)));
        }
        JsonNode scopes = parsed.get("allowedScopes");
        if (scopes != null && !scopes.isNull()) {
            if (!scopes.isArray() || scopes.size() > 64) {
                throw new IllegalArgumentException(
                        "allowedScopes must be an array of at most 64 entries");
            }
            for (JsonNode scope : scopes) {
                if (!scope.isTextual() || scope.asText().isBlank()
                        || scope.asText().length() > 512) {
                    throw new IllegalArgumentException(
                            "each allowed scope is a non-empty path of at most 512 characters");
                }
                spec.addAllowedScope(scope.asText());
            }
        }
        JsonNode checks = parsed.get("requiredChecks");
        if (checks != null && !checks.isNull()) {
            if (!checks.isArray() || checks.size() > 64) {
                throw new IllegalArgumentException(
                        "requiredChecks must be an array of at most 64 checks");
            }
            for (JsonNode check : checks) {
                if (!check.isObject()) {
                    throw new IllegalArgumentException(
                            "each required check is an object with 'name' and 'description'");
                }
                spec.addRequiredChecks(AcceptanceCheck.newBuilder()
                        .setName(TaskConsoleRequests.requiredText(check, "name", 128))
                        .setDescription(TaskConsoleRequests.optionalText(check, "description",
                                2048)));
            }
        }
        long leaseMinutes = TaskConsoleRequests.boundedLong(
                parsed.path("leaseMinutes").isMissingNode()
                        || parsed.path("leaseMinutes").isNull() ? null
                        : parsed.path("leaseMinutes").asText(),
                1, 24 * 60, 30, "leaseMinutes");
        CheckpointReference resumeFrom = null;
        JsonNode resume = parsed.get("resumeFrom");
        if (resume != null && !resume.isNull()) {
            if (requestedId.isBlank() || !resume.isObject()) {
                throw new IllegalArgumentException("resumeFrom requires an existing taskId and checkpoint object");
            }
            resumeFrom = CheckpointReference.newBuilder()
                    .setAttempt(TaskConsoleRequests.requiredPositiveInt(resume, "attempt"))
                    .setCheckpointSeq(TaskConsoleRequests.requiredPositiveInt(resume,
                            "checkpointSeq"))
                    .setResumeToken(TaskConsoleRequests.requiredText(resume, "resumeToken",
                            512)).build();
        }
        TaskOffer offer = bridge.offer(workerId, taskId, spec.build(),
                Duration.ofMinutes(leaseMinutes), resumeFrom);
        ObjectNode response = JSON.createObjectNode();
        response.put("taskId", taskId);
        response.put("workerId", workerId);
        response.put("attempt", offer.getAttempt());
        respondJson(exchange, 201, response);
    }

    private void cancelTask(HttpExchange exchange, String taskId) throws IOException {
        JsonNode parsed = TaskConsoleRequests.readObjectBody(exchange, MAX_BODY_BYTES, "cancel");
        if (parsed == null) {
            exchange.sendResponseHeaders(413, -1);
            return;
        }
        bridge.cancel(taskId, TaskConsoleRequests.requiredText(parsed, "reason", 2048));
        respondJson(exchange, 200, JSON.createObjectNode().put("taskId", taskId));
    }

    /**
     * Projects the task's transcript into a signed work record: the receipt a
     * delegation hands over. The projector refuses a task still in flight, and
     * an unsigned server refuses by naming what signing needs.
     */
    private void exportRecord(HttpExchange exchange, String taskId) throws IOException {
        if (signing == null) {
            respondError(exchange, 503, "work-record signing is not configured; set "
                    + RecordSigning.ENV_KEY_FILE + ", " + RecordSigning.ENV_KEY_ID
                    + ", and " + RecordSigning.ENV_ISSUER);
            return;
        }
        List<TranscriptEntry> entries = bridge.coordinator().eventsAfter(taskId, 0).stream()
                .map(InProcessDelegationCoordinator.Event::entry)
                .toList();
        WorkRecord manifest;
        try {
            manifest = DelegationRecordProjector.project(taskId, entries,
                    new DelegationRecordProjector.Issuance(
                            "record-" + taskId, signing.issuer(), signing.signer().keyId(),
                            Timestamps.fromMillis(System.currentTimeMillis()), ""));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e.getMessage());
        }
        byte[] record = signing.signer().sign(manifest).toByteArray();
        ObjectNode response = JSON.createObjectNode();
        response.put("recordBase64", java.util.Base64.getEncoder().encodeToString(record));
        byte[] transcript = WorkRecords.deterministicBytes(
                Transcript.newBuilder().addAllEntries(entries).build());
        response.put("transcriptBase64", java.util.Base64.getEncoder().encodeToString(transcript));
        response.put("manifestDigest",
                WorkRecords.sha256Hex(WorkRecords.canonicalBytes(manifest)));
        response.put("recordId", "record-" + taskId);
        respondJson(exchange, 200, response);
    }

    /**
     * Applies a human review decision to the open candidate. The console session is
     * the external reviewer the manual review policy leaves candidates pending for:
     * acceptance and revision both demand the reviewer's words, because a judgement
     * without a reason is not one the transcript can defend later.
     */
    private void review(HttpExchange exchange, String taskId) throws IOException {
        JsonNode parsed = TaskConsoleRequests.readObjectBody(exchange, MAX_BODY_BYTES, "review");
        if (parsed == null) {
            exchange.sendResponseHeaders(413, -1);
            return;
        }
        String decision = TaskConsoleRequests.requiredText(parsed, "decision", 16);
        CandidateReviewer.ReviewDecision applied = switch (decision) {
            case "accept" -> CandidateReviewer.ReviewDecision.accept(
                    TaskConsoleRequests.requiredText(parsed, "verdict", 4096));
            case "revise" -> CandidateReviewer.ReviewDecision.revise(
                    TaskConsoleRequests.requiredText(parsed, "feedback", 4096),
                    TaskConsoleRequests.failedChecks(parsed));
            default -> throw new IllegalArgumentException(
                    "decision must be 'accept' or 'revise'");
        };
        int attempt = TaskConsoleRequests.requiredPositiveInt(parsed, "attempt");
        int revision = TaskConsoleRequests.requiredPositiveInt(parsed, "revision");
        bridge.review(taskId, attempt, revision, applied);
        DelegationReducer.TaskState state = bridge.coordinator().state().tasks().get(taskId);
        ObjectNode response = JSON.createObjectNode();
        response.put("decision", decision);
        response.put("phase", state == null ? ""
                : state.phase().name().toLowerCase(Locale.ROOT));
        respondJson(exchange, 200, response);
    }

    private void retryReview(HttpExchange exchange, String taskId) throws IOException {
        byte[] body = TaskConsoleRequests.readBody(exchange, MAX_BODY_BYTES);
        if (body == null) {
            exchange.sendResponseHeaders(413, -1);
            return;
        }
        var request = ai.protomolt.proto.delegation.v1.RetryCandidateReviewRequest.newBuilder();
        try {
            JsonFormat.parser().merge(new String(body, java.nio.charset.StandardCharsets.UTF_8), request);
        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("invalid review retry request");
        }
        if (!request.getTaskId().isEmpty() && !request.getTaskId().equals(taskId)) {
            throw new IllegalArgumentException("retry task identity differs from route");
        }
        request.setTaskId(taskId);
        var response = bridge.retryCandidateReview(request.build());
        respondJson(exchange, 200, JSON.readTree(PROTO_JSON.print(response)));
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
}
