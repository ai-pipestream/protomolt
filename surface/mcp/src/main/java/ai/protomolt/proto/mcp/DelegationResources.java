package ai.protomolt.proto.mcp;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Exposes a live delegation bridge as bounded MCP resources, so an agent can browse
 * workers, tasks, and transcripts without spending tool calls.
 *
 * <p>URIs: {@code protomolt://delegation/workers} lists registered workers;
 * {@code protomolt://delegation/tasks} lists task states as the reducer sees them;
 * {@code protomolt://delegation/tasks/{taskId}/transcript} is one task's recorded frames
 * in cursor order. Task transcripts are addressable through the advertised template and
 * intentionally not enumerated. Every document is JSON and explicitly bounded.</p>
 */
public final class DelegationResources implements McpResources {

    /** The resource root every delegation URI hangs under. */
    public static final String ROOT = "protomolt://delegation";

    /** The registered-worker index URI. */
    public static final String WORKERS_URI = ROOT + "/workers";

    /** The task-state index URI. */
    public static final String TASKS_URI = ROOT + "/tasks";

    /** Upper bound on workers or tasks one document returns. */
    public static final int MAX_ROWS = 256;

    /** Upper bound on transcript entries one document returns. */
    public static final int MAX_TRANSCRIPT_ENTRIES = 256;

    private final DelegationBridge bridge;

    public DelegationResources(DelegationBridge bridge) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
    }

    /** The resource index: the workers and tasks roots only. */
    @Override
    public ArrayNode list(ObjectMapper mapper) {
        ArrayNode resources = mapper.createArrayNode();
        resource(resources, WORKERS_URI, "delegation-workers",
                "Registered delegation workers: identity, admission, capabilities");
        resource(resources, TASKS_URI, "delegation-tasks",
                "Delegation task states as the lifecycle reducer sees them");
        return resources;
    }

    @Override
    public ArrayNode templates(ObjectMapper mapper) {
        ArrayNode templates = mapper.createArrayNode();
        ObjectNode entry = templates.addObject();
        entry.put("uriTemplate", TASKS_URI + "/{taskId}/transcript");
        entry.put("name", "delegation-transcript");
        entry.put("description",
                "One delegation task's recorded frames in cursor order, bounded");
        entry.put("mimeType", "application/json");
        return templates;
    }

    /** The contents of one resource, or empty when the URI is not served. */
    @Override
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
        if (WORKERS_URI.equals(uri)) {
            return Optional.of(contents(mapper, uri, workersDocument(mapper)));
        }
        if (TASKS_URI.equals(uri)) {
            return Optional.of(contents(mapper, uri, tasksDocument(mapper)));
        }
        String prefix = TASKS_URI + "/";
        if (uri.startsWith(prefix) && uri.endsWith("/transcript")) {
            String taskId = URLDecoder.decode(
                    uri.substring(prefix.length(), uri.length() - "/transcript".length()),
                    StandardCharsets.UTF_8);
            if (taskId.isBlank() || taskId.contains("/")) {
                return Optional.empty();
            }
            return Optional.of(contents(mapper, uri, transcriptDocument(mapper, taskId)));
        }
        return Optional.empty();
    }

    private ObjectNode workersDocument(ObjectMapper mapper) {
        ObjectNode document = mapper.createObjectNode();
        ArrayNode workers = document.putArray("workers");
        List<InProcessDelegationCoordinator.WorkerView> all = bridge.coordinator().workers();
        all.stream().limit(MAX_ROWS).forEach(worker -> workers.add(workerJson(mapper, worker)));
        document.put("truncated", all.size() > MAX_ROWS);
        return document;
    }

    private ObjectNode tasksDocument(ObjectMapper mapper) {
        ObjectNode document = mapper.createObjectNode();
        ArrayNode tasks = document.putArray("tasks");
        Map<String, DelegationReducer.TaskState> all = bridge.coordinator().state().tasks();
        all.values().stream().limit(MAX_ROWS).forEach(task -> {
            ObjectNode node = tasks.addObject();
            node.put("taskId", task.taskId());
            node.put("phase", task.phase().name());
            node.put("attempt", task.attempt());
            node.put("holder", task.holder());
            node.put("candidateRevision", task.candidateRevision());
            node.put("lastProgressSeq", task.lastProgressSeq());
            node.put("lastCheckpointSeq", task.lastCheckpointSeq());
        });
        document.put("truncated", all.size() > MAX_ROWS);
        return document;
    }

    private ObjectNode transcriptDocument(ObjectMapper mapper, String taskId) {
        ObjectNode document = mapper.createObjectNode();
        document.put("taskId", taskId);
        ArrayNode entries = document.putArray("entries");
        List<InProcessDelegationCoordinator.Event> all =
                bridge.coordinator().eventsAfter(taskId, 0);
        all.stream().limit(MAX_TRANSCRIPT_ENTRIES).forEach(event -> {
            ObjectNode node = entries.addObject();
            node.put("cursor", event.cursor());
            node.put("lane", event.entry().getLane().name());
            node.put("workerId", event.workerId());
            node.set("entry", entryJson(mapper, event));
        });
        document.put("truncated", all.size() > MAX_TRANSCRIPT_ENTRIES);
        return document;
    }

    private static ObjectNode workerJson(ObjectMapper mapper,
                                         InProcessDelegationCoordinator.WorkerView worker) {
        ObjectNode node = mapper.createObjectNode();
        WorkerHello hello = worker.hello();
        node.put("workerId", worker.workerId());
        node.put("admitted", worker.admitted());
        node.put("connected", worker.connected());
        node.put("provider", hello.getProvider());
        node.put("model", hello.getModel());
        ArrayNode capabilities = node.putArray("capabilities");
        for (WorkerCapability capability : hello.getCapabilitiesList()) {
            capabilities.add(capability.getName());
        }
        return node;
    }

    private static ObjectNode entryJson(ObjectMapper mapper,
                                        InProcessDelegationCoordinator.Event event) {
        try {
            return (ObjectNode) mapper.readTree(JsonFormat.printer()
                    .omittingInsignificantWhitespace().print(event.entry()));
        } catch (Exception e) {
            throw new IllegalStateException("failed to render a transcript entry", e);
        }
    }

    private static void resource(ArrayNode resources, String uri, String name,
                                 String description) {
        ObjectNode entry = resources.addObject();
        entry.put("uri", uri);
        entry.put("name", name);
        entry.put("description", description);
        entry.put("mimeType", "application/json");
    }

    private static ObjectNode contents(ObjectMapper mapper, String uri, ObjectNode document) {
        ObjectNode contents = mapper.createObjectNode();
        contents.put("uri", uri);
        contents.put("mimeType", "application/json");
        contents.put("text", document.toString());
        return contents;
    }
}
