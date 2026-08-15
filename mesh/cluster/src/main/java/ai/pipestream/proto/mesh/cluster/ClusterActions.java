package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.util.List;
import java.util.Objects;

/**
 * Catalog actions for one persistent mesh cluster directory. Mutating actions accept the
 * canonical proto3 JSON form of the corresponding cluster contract and return the directory
 * apply outcome. The directory validates and durably records each mutation before it becomes
 * visible. Snapshot and sweep expose the current projection and explicit TTL expiry.
 */
public final class ClusterActions {

    private ClusterActions() {
    }

    /** Registers the node, processor, capacity, liveness, snapshot, and sweep actions. */
    public static ActionCatalog register(ActionCatalog catalog,
                                         PersistentClusterDirectory directory) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(directory, "directory");
        return catalog
                .register(new RegisterNode(directory))
                .register(new Heartbeat(directory))
                .register(new RegisterProcessor(directory))
                .register(new UpdateCapacity(directory))
                .register(new Snapshot(directory))
                .register(new Sweep(directory));
    }

    private abstract static class ClusterAction implements ProtoAction {
        final PersistentClusterDirectory directory;

        ClusterAction(PersistentClusterDirectory directory) {
            this.directory = directory;
        }

        static ObjectNode schema(String field, String description) {
            ObjectNode schema = JsonNodeFactory.instance.objectNode();
            schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
            schema.put("type", "object");
            schema.putObject("properties").putObject(field)
                    .put("type", "object").put("description", description);
            schema.putArray("required").add(field);
            schema.put("additionalProperties", false);
            return schema;
        }

        static ObjectNode emptySchema() {
            ObjectNode schema = JsonNodeFactory.instance.objectNode();
            schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
            schema.put("type", "object");
            schema.putObject("properties");
            schema.put("additionalProperties", false);
            return schema;
        }

        static ObjectNode object(ObjectNode input, String field) throws ActionException {
            JsonNode node = input.get(field);
            if (node instanceof ObjectNode object) {
                return object;
            }
            throw invalid("'" + field + "' must be an object", "/" + field);
        }

        static <B extends Message.Builder> Message parse(ObjectNode input, String field,
                                                          B builder) throws ActionException {
            try {
                JsonFormat.parser().merge(object(input, field).toString(), builder);
                return builder.build();
            } catch (ActionException e) {
                throw e;
            } catch (Exception e) {
                throw invalid("Invalid protobuf JSON: " + e.getMessage(), "/" + field);
            }
        }

        static ObjectNode render(Message message, ActionContext context) throws ActionException {
            try {
                JsonNode json = context.objectMapper().readTree(
                        JsonFormat.printer().omittingInsignificantWhitespace().print(message));
                if (json instanceof ObjectNode object) {
                    return object;
                }
                throw new IllegalStateException("protobuf JSON was not an object");
            } catch (JsonProcessingException e) {
                throw new ActionException("render-failed", "Failed to render protobuf JSON", null);
            } catch (Exception e) {
                throw new ActionException("render-failed", e.getMessage(), null);
            }
        }

        static ObjectNode outcome(ClusterDirectory.ApplyOutcome outcome,
                                  PersistentClusterDirectory directory,
                                  ActionContext context) throws ActionException {
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", true);
            result.put("outcome", outcome.name());
            ClusterSnapshot snapshot = directory.snapshot();
            result.put("snapshotSeq", snapshot.getSnapshotSeq());
            result.put("snapshotFingerprint", snapshot.getFingerprint());
            return result;
        }

        static ActionException invalid(String message, String pointer) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("pointer", pointer);
            return new ActionException("invalid-input", message, details);
        }

        static ActionException rejected(RuntimeException e) {
            String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            return new ActionException("cluster-rejected", message);
        }
    }

    private static final class RegisterNode extends ClusterAction {
        RegisterNode(PersistentClusterDirectory directory) {
            super(directory);
        }

        @Override public String name() { return "mesh-node-register"; }

        @Override public String description() {
            return "Registers or refreshes one fenced mesh node advertisement after durable validation.";
        }

        @Override public ObjectNode inputSchema() {
            return schema("advertisement", "NodeAdvertisement in canonical proto3 JSON.");
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            NodeAdvertisement advertisement = (NodeAdvertisement) parse(input, "advertisement",
                    NodeAdvertisement.newBuilder());
            try {
                return outcome(directory.register(advertisement), directory, context);
            } catch (RuntimeException e) {
                throw rejected(e);
            }
        }
    }

    private static final class Heartbeat extends ClusterAction {
        Heartbeat(PersistentClusterDirectory directory) {
            super(directory);
        }

        @Override public String name() { return "mesh-node-heartbeat"; }

        @Override public String description() {
            return "Extends one registered node's liveness window with a fenced heartbeat.";
        }

        @Override public ObjectNode inputSchema() {
            return schema("presence", "NodePresence in canonical proto3 JSON.");
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            NodePresence presence = (NodePresence) parse(input, "presence",
                    NodePresence.newBuilder());
            try {
                return outcome(directory.heartbeat(presence), directory, context);
            } catch (RuntimeException e) {
                throw rejected(e);
            }
        }
    }

    private static final class RegisterProcessor extends ClusterAction {
        RegisterProcessor(PersistentClusterDirectory directory) {
            super(directory);
        }

        @Override public String name() { return "mesh-processor-register"; }

        @Override public String description() {
            return "Registers or renews one health-gated processor lease on a registered node.";
        }

        @Override public ObjectNode inputSchema() {
            return schema("advertisement", "ProcessorAdvertisement in canonical proto3 JSON.");
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ProcessorAdvertisement advertisement = (ProcessorAdvertisement) parse(
                    input, "advertisement", ProcessorAdvertisement.newBuilder());
            try {
                return outcome(directory.registerProcessor(advertisement), directory, context);
            } catch (RuntimeException e) {
                throw rejected(e);
            }
        }
    }

    private static final class UpdateCapacity extends ClusterAction {
        UpdateCapacity(PersistentClusterDirectory directory) {
            super(directory);
        }

        @Override public String name() { return "mesh-capacity-update"; }

        @Override public String description() {
            return "Publishes a fenced point-in-time node or processor capacity snapshot.";
        }

        @Override public ObjectNode inputSchema() {
            return schema("capacity", "CapacityAdvertisement in canonical proto3 JSON.");
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            CapacityAdvertisement capacity = (CapacityAdvertisement) parse(input, "capacity",
                    CapacityAdvertisement.newBuilder());
            try {
                return outcome(directory.updateCapacity(capacity), directory, context);
            } catch (RuntimeException e) {
                throw rejected(e);
            }
        }
    }

    private static final class Snapshot extends ClusterAction {
        Snapshot(PersistentClusterDirectory directory) {
            super(directory);
        }

        @Override public String name() { return "mesh-snapshot"; }

        @Override public String description() {
            return "Returns the deterministic cluster directory snapshot and eligibility state.";
        }

        @Override public ObjectNode inputSchema() { return emptySchema(); }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", true);
            result.set("snapshot", render(directory.snapshot(), context));
            return result;
        }
    }

    private static final class Sweep extends ClusterAction {
        Sweep(PersistentClusterDirectory directory) {
            super(directory);
        }

        @Override public String name() { return "mesh-sweep"; }

        @Override public String description() {
            return "Expires elapsed processor leases and node presence windows, cascading node loss.";
        }

        @Override public ObjectNode inputSchema() { return emptySchema(); }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            List<ClusterEvent> expired;
            try {
                expired = directory.sweep();
            } catch (RuntimeException e) {
                throw rejected(e);
            }
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", true);
            ArrayNode events = result.putArray("events");
            for (ClusterEvent event : expired) {
                events.add(render(event, context));
            }
            result.put("expiredCount", expired.size());
            result.put("snapshotSeq", directory.snapshot().getSnapshotSeq());
            return result;
        }
    }
}
