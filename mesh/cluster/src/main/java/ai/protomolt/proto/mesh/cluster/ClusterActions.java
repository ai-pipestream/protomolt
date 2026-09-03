package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.mesh.cluster.v1.ApplyOutcome;
import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCommit;
import ai.protomolt.proto.mesh.cluster.v1.GetSnapshotRequest;
import ai.protomolt.proto.mesh.cluster.v1.GetSnapshotResponse;
import ai.protomolt.proto.mesh.cluster.v1.HeartbeatRequest;
import ai.protomolt.proto.mesh.cluster.v1.HeartbeatResponse;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.RegisterNodeRequest;
import ai.protomolt.proto.mesh.cluster.v1.RegisterNodeResponse;
import ai.protomolt.proto.mesh.cluster.v1.RegisterProcessorRequest;
import ai.protomolt.proto.mesh.cluster.v1.RegisterProcessorResponse;
import ai.protomolt.proto.mesh.cluster.v1.SweepRequest;
import ai.protomolt.proto.mesh.cluster.v1.SweepResponse;
import ai.protomolt.proto.mesh.cluster.v1.SweepResponse;
import ai.protomolt.proto.mesh.cluster.v1.UpdateCapacityRequest;
import ai.protomolt.proto.mesh.cluster.v1.UpdateCapacityResponse;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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

        /**
         * Where an applied change landed. Every reply on this service carries one, so the
         * verbs differ only in the response message they set it on.
         */
        static DirectoryCommit commit(ClusterDirectory.ApplyOutcome outcome,
                                      PersistentClusterDirectory directory) {
            ClusterSnapshot snapshot = directory.snapshot();
            return DirectoryCommit.newBuilder()
                    .setOutcome(ApplyOutcome.valueOf("APPLY_OUTCOME_" + outcome.name()))
                    .setSnapshotSeq(snapshot.getSnapshotSeq())
                    .setSnapshotFingerprint(snapshot.getFingerprint())
                    .build();
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

        @Override public Descriptor requestType() {
            return RegisterNodeRequest.getDescriptor();
        }

        @Override public Descriptor responseType() {
            return RegisterNodeResponse.getDescriptor();
        }

        @Override public Message execute(Message input, ActionContext context)
                throws ActionException {
            NodeAdvertisement advertisement =
                    CatalogContract.as(input, RegisterNodeRequest.getDefaultInstance(), name())
                            .getAdvertisement();
            try {
                return RegisterNodeResponse.newBuilder()
                        .setCommit(commit(directory.register(advertisement), directory)).build();
            } catch (RuntimeException e) {
                throw rejected(e);
            }
        }
    }

    /**
     * Extends a node's liveness. Presence is soft state, so this verb writes nothing durable
     * and its reply carries the {@code snapshot_seq} the directory already stood at: an
     * accepted heartbeat does not advance the sequence, because no event was recorded.
     * {@code outcome} is what reports whether the heartbeat landed. A caller that watches
     * {@code snapshot_seq} is watching for durable membership change, which is what it now
     * means and is what a heartbeat by definition is not.
     */
    private static final class Heartbeat extends ClusterAction {
        Heartbeat(PersistentClusterDirectory directory) {
            super(directory);
        }

        @Override public String name() { return "mesh-node-heartbeat"; }

        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }

        @Override public String description() {
            return "Extends one registered node's liveness window with a fenced heartbeat.";
        }

        @Override public Descriptor requestType() {
            return HeartbeatRequest.getDescriptor();
        }

        @Override public Descriptor responseType() {
            return HeartbeatResponse.getDescriptor();
        }

        @Override public Message execute(Message input, ActionContext context)
                throws ActionException {
            NodePresence presence =
                    CatalogContract.as(input, HeartbeatRequest.getDefaultInstance(), name())
                            .getPresence();
            try {
                return HeartbeatResponse.newBuilder()
                        .setCommit(commit(directory.heartbeat(presence), directory)).build();
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

        @Override public Descriptor requestType() {
            return RegisterProcessorRequest.getDescriptor();
        }

        @Override public Descriptor responseType() {
            return RegisterProcessorResponse.getDescriptor();
        }

        @Override public Message execute(Message input, ActionContext context)
                throws ActionException {
            ProcessorAdvertisement advertisement = CatalogContract.as(input, RegisterProcessorRequest.getDefaultInstance(), name())
                            .getAdvertisement();
            try {
                return RegisterProcessorResponse.newBuilder()
                        .setCommit(commit(directory.registerProcessor(advertisement), directory)).build();
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

        @Override public Descriptor requestType() {
            return UpdateCapacityRequest.getDescriptor();
        }

        @Override public Descriptor responseType() {
            return UpdateCapacityResponse.getDescriptor();
        }

        @Override public Message execute(Message input, ActionContext context)
                throws ActionException {
            CapacityAdvertisement capacity =
                    CatalogContract.as(input, UpdateCapacityRequest.getDefaultInstance(), name())
                            .getCapacity();
            try {
                return UpdateCapacityResponse.newBuilder()
                        .setCommit(commit(directory.updateCapacity(capacity), directory)).build();
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

        @Override public Descriptor requestType() {
            return GetSnapshotRequest.getDescriptor();
        }

        @Override public Descriptor responseType() {
            return GetSnapshotResponse.getDescriptor();
        }

        @Override public Message execute(Message input, ActionContext context)
                throws ActionException {
            return GetSnapshotResponse.newBuilder()
                    .setSnapshot(directory.snapshot()).build();
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

        @Override public Descriptor requestType() {
            return SweepRequest.getDescriptor();
        }

        @Override public Descriptor responseType() {
            return SweepResponse.getDescriptor();
        }

        @Override public Message execute(Message input, ActionContext context)
                throws ActionException {
            List<ClusterEvent> expired;
            try {
                expired = directory.sweep();
            } catch (RuntimeException e) {
                throw rejected(e);
            }
            return SweepResponse.newBuilder()
                    .addAllEvents(expired)
                    .setSnapshotSeq(directory.snapshot().getSnapshotSeq())
                    .build();
        }
    }
}
