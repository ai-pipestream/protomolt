package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.contract.DeliverableContracts;
import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The outbound projection of the task console API: how coordinator state renders to the
 * console's JSON. Owns the transcript fold behind the task list (the reduction, the latest
 * offer per task, and each task's last cursor), the per-task and per-event JSON shapes, and
 * the protobuf-to-JSON rendering, including the per-task {@code Any} resolution against the
 * offer that preceded the event.
 */
final class TaskConsoleProjection {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonFormat.Printer PROTO_JSON =
            JsonFormat.printer().omittingInsignificantWhitespace();

    private TaskConsoleProjection() {
    }

    /** The latest offer for one task, with the worker it was offered to. */
    record OfferView(String workerId, TaskOffer offer) {
    }

    /** The folded transcript: the reduction, the latest offers, per-task cursors, the high-water mark. */
    record Projection(DelegationReducer.Result reduced,
                      Map<String, OfferView> offers,
                      Map<String, Long> lastCursors,
                      long cursor) {

        /** Derived, not stored: the task states are the reduction's own, always. */
        Map<String, DelegationReducer.TaskState> states() {
            return reduced.tasks();
        }
    }

    /** Folds the coordinator's complete event history into the console's projection. */
    static Projection project(InProcessDelegationCoordinator coordinator) {
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
        return new Projection(reduced, offers, lastCursors, cursor);
    }

    /** The {@code GET /api/tasks/workers} body: every known worker and its capabilities. */
    static ObjectNode workersJson(InProcessDelegationCoordinator coordinator) {
        ArrayNode workers = JSON.createArrayNode();
        for (InProcessDelegationCoordinator.WorkerView worker : coordinator.workers()) {
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
        return response;
    }

    /** The {@code GET /api/tasks} body: every task summary, the cursor, and the findings. */
    static ObjectNode tasksJson(Projection projection) {
        ObjectNode response = JSON.createObjectNode();
        ArrayNode tasks = response.putArray("tasks");
        projection.states().values().forEach(state ->
                tasks.add(taskSummaryJson(state, projection.offers().get(state.taskId()),
                        projection.lastCursors().getOrDefault(state.taskId(), 0L))));
        response.put("cursor", projection.cursor());
        response.set("findings", findingsJson(projection.reduced().findings()));
        return response;
    }

    /** The {@code GET /api/tasks/{id}} body: the task summary, its events, and its findings. */
    static ObjectNode taskDetailJson(InProcessDelegationCoordinator coordinator,
                                     Projection projection, String taskId,
                                     DelegationReducer.TaskState state, int maxEvents) {
        ObjectNode response = JSON.createObjectNode();
        response.set("task", taskSummaryJson(state, projection.offers().get(taskId),
                projection.lastCursors().getOrDefault(taskId, 0L)));
        ArrayNode events = response.putArray("events");
        coordinator.eventsAfter(taskId, 0).stream().limit(maxEvents)
                .map(event -> eventJson(coordinator, event)).forEach(events::add);
        response.put("cursor", projection.cursor());
        response.set("findings", findingsJson(projection.reduced().findings().stream()
                .filter(finding -> taskId.equals(finding.taskId())).toList()));
        return response;
    }

    /** One task row: phase, attempt, holder, objective, progress markers, and review state. */
    private static ObjectNode taskSummaryJson(DelegationReducer.TaskState state,
                                              OfferView offer, long lastCursor) {
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
        var review = state.review();
        ObjectNode reviewJson = node.putObject("review");
        reviewJson.put("status", review.status().name().toLowerCase(Locale.ROOT));
        if (review.started().hasIdentity()) {
            var identity = review.started().getIdentity();
            reviewJson.put("invocationId", identity.getInvocationId());
            reviewJson.put("attempt", identity.getAttempt());
            reviewJson.put("revision", identity.getRevision());
            reviewJson.put("startedAt", Timestamps.toString(review.started().getStartedAt()));
            reviewJson.put("deadline", Timestamps.toString(review.started().getDeadline()));
        }
        if (review.failureCode().getNumber() != 0) {
            reviewJson.put("failureCode", review.failureCode().name()
                    .replace("REVIEW_FAILURE_CODE_", "").toLowerCase(Locale.ROOT));
        }
        return node;
    }

    /** The findings array: one row per finding with its task, frame, kind, and error. */
    static ArrayNode findingsJson(List<DelegationReducer.Finding> findings) {
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

    /** One event row, with the transcript entry rendered against its task's offer contract. */
    static ObjectNode eventJson(InProcessDelegationCoordinator coordinator,
                                InProcessDelegationCoordinator.Event event) {
        ObjectNode node = JSON.createObjectNode();
        node.put("cursor", event.cursor());
        node.put("workerId", event.workerId());
        node.put("taskId", event.taskId());
        node.put("lane", event.entry().getLane().name());
        // Resolve Any against the offer that preceded this event, not a global registry:
        // callers may reuse a type name in different tasks or later attempts.
        JsonFormat.Printer printer = PROTO_JSON;
        DeliverableContract contract = null;
        for (var prior : coordinator.eventsAfter(event.taskId(), 0)) {
            if (prior.cursor() > event.cursor()) break;
            if (prior.entry().hasCoordinatorFrame()
                    && prior.entry().getCoordinatorFrame().hasOffer()) {
                var spec = prior.entry().getCoordinatorFrame().getOffer().getSpec();
                contract = spec.hasContract() ? spec.getContract() : null;
            }
        }
        if (contract != null) {
            printer = printer.usingTypeRegistry(DeliverableContracts.typeRegistry(contract));
        }
        node.set("entry", protoJson(event.entry(), printer));
        return node;
    }

    /** Renders a protobuf message as JSON with the console's compact printer. */
    static JsonNode protoJson(com.google.protobuf.Message message) {
        return protoJson(message, PROTO_JSON);
    }

    /** Renders a protobuf message as JSON with the given printer. */
    static JsonNode protoJson(com.google.protobuf.Message message, JsonFormat.Printer printer) {
        try {
            return JSON.readTree(printer.print(message));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("could not render delegation event", e);
        } catch (IOException e) {
            throw new IllegalStateException("could not parse rendered delegation event", e);
        }
    }
}
