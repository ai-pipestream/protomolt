package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.mesh.cluster.v1.ApplyOutcome;
import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.DirectoryCommit;
import ai.pipestream.proto.mesh.cluster.v1.GetSnapshotRequest;
import ai.pipestream.proto.mesh.cluster.v1.HeartbeatRequest;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.RegisterNodeRequest;
import ai.pipestream.proto.mesh.cluster.v1.RegisterProcessorRequest;
import ai.pipestream.proto.mesh.cluster.v1.SweepRequest;
import ai.pipestream.proto.mesh.cluster.v1.UpdateCapacityRequest;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
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

    /**
     * Enforces request contracts on the catalog path, which does not sit behind the
     * validating interceptor the gRPC surface uses.
     */
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private abstract static class ClusterAction implements ProtoAction {
        final PersistentClusterDirectory directory;

        ClusterAction(PersistentClusterDirectory directory) {
            this.directory = directory;
        }

        /**
         * The input schema for a verb, derived from the request message it accepts.
         *
         * <p>The generator folds the message's declared rules into the schema, so a caller
         * reading the manifest sees the same contract the directory enforces.
         */
        static ObjectNode schemaOf(Descriptor request) {
            return new ObjectMapper().valueToTree(
                    ProtoJsonSchemaGenerator.create().generate(request));
        }

        /**
         * Reads the envelope into a request and holds it to the message's declared rules.
         *
         * <p>The envelope is the request message's canonical proto3 JSON form. Calls arriving
         * over gRPC pass a validating interceptor; calls arriving through the catalog do not,
         * so the rules are applied here rather than left to the directory to rediscover.
         */
        static <B extends Message.Builder> B parse(ObjectNode input, B builder)
                throws ActionException {
            try {
                JsonFormat.parser().merge(input.toString(), builder);
            } catch (Exception e) {
                throw invalid("Invalid protobuf JSON: " + e.getMessage(), "/");
            }
            ValidationResult result = VALIDATOR.validate(builder.build());
            if (!result.valid()) {
                ObjectNode details = JsonNodeFactory.instance.objectNode();
                ArrayNode violations = details.putArray("violations");
                StringBuilder prose = new StringBuilder();
                for (ValidationResult.Violation violation : result.violations()) {
                    ObjectNode node = violations.addObject();
                    node.put("field", violation.path());
                    node.put("ruleId", violation.ruleId());
                    node.put("message", violation.message());
                    if (prose.length() > 0) {
                        prose.append("; ");
                    }
                    prose.append(violation.path()).append(' ').append(violation.message());
                }
                throw new ActionException("invalid-input",
                        "The request does not satisfy its contract: " + prose, details);
            }
            return builder;
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
            ClusterSnapshot snapshot = directory.snapshot();
            DirectoryCommit commit = DirectoryCommit.newBuilder()
                    .setOutcome(ApplyOutcome.valueOf("APPLY_OUTCOME_" + outcome.name()))
                    .setSnapshotSeq(snapshot.getSnapshotSeq())
                    .setSnapshotFingerprint(snapshot.getFingerprint())
                    .build();
            ObjectNode result = context.objectMapper().createObjectNode();
            result.set("commit", render(commit, context));
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

        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }

        @Override public String description() {
            return "Registers or refreshes one fenced mesh node advertisement after durable validation.";
        }

        @Override public ObjectNode inputSchema() {
            return schemaOf(RegisterNodeRequest.getDescriptor());
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            NodeAdvertisement advertisement =
                    parse(input, RegisterNodeRequest.newBuilder()).build().getAdvertisement();
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

        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }

        @Override public String description() {
            return "Extends one registered node's liveness window with a fenced heartbeat.";
        }

        @Override public ObjectNode inputSchema() {
            return schemaOf(HeartbeatRequest.getDescriptor());
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            NodePresence presence =
                    parse(input, HeartbeatRequest.newBuilder()).build().getPresence();
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

        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }

        @Override public String description() {
            return "Registers or renews one health-gated processor lease on a registered node.";
        }

        @Override public ObjectNode inputSchema() {
            return schemaOf(RegisterProcessorRequest.getDescriptor());
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ProcessorAdvertisement advertisement = parse(
                    input, RegisterProcessorRequest.newBuilder()).build().getAdvertisement();
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

        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }

        @Override public String description() {
            return "Publishes a fenced point-in-time node or processor capacity snapshot.";
        }

        @Override public ObjectNode inputSchema() {
            return schemaOf(UpdateCapacityRequest.getDescriptor());
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            CapacityAdvertisement capacity =
                    parse(input, UpdateCapacityRequest.newBuilder()).build().getCapacity();
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

        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }

        @Override public String description() {
            return "Returns the deterministic cluster directory snapshot and eligibility state.";
        }

        @Override public ObjectNode inputSchema() {
            return schemaOf(SweepRequest.getDescriptor());
        }

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

        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }

        @Override public String description() {
            return "Expires elapsed processor leases and node presence windows, cascading node loss.";
        }

        @Override public ObjectNode inputSchema() {
            return schemaOf(SweepRequest.getDescriptor());
        }

        @Override public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            List<ClusterEvent> expired;
            try {
                expired = directory.sweep();
            } catch (RuntimeException e) {
                throw rejected(e);
            }
            ObjectNode result = context.objectMapper().createObjectNode();
            ArrayNode events = result.putArray("events");
            for (ClusterEvent event : expired) {
                events.add(render(event, context));
            }
            result.put("snapshotSeq", directory.snapshot().getSnapshotSeq());
            return result;
        }
    }
}
