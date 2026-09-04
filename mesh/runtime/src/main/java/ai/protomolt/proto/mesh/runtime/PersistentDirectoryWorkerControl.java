package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.cluster.PersistentClusterDirectory;
import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.runtime.v1.WorkerCapacity;
import ai.protomolt.proto.mesh.runtime.v1.WorkerDrainProgress;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHeartbeat;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import com.google.protobuf.util.Timestamps;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Directory worker-control adapter that preserves node and processor epoch fences. */
public final class PersistentDirectoryWorkerControl implements WorkerDirectoryControl {

    private final PersistentClusterDirectory directory;
    private final Clock clock;
    private final Map<String, Long> heartbeatSequences = new LinkedHashMap<>();
    private final Map<String, Long> capacitySequences = new LinkedHashMap<>();

    public PersistentDirectoryWorkerControl(PersistentClusterDirectory directory) {
        this(directory, Clock.systemUTC());
    }

    public PersistentDirectoryWorkerControl(
            PersistentClusterDirectory directory, Clock clock) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public synchronized void heartbeat(WorkerHello hello, WorkerHeartbeat heartbeat) {
        var node = requireNode(hello);
        var existing = directory.snapshot().getNodesList().stream()
                .filter(record -> record.getAdvertisement().getNodeId()
                        .equals(hello.getNodeId()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "worker node disappeared from directory"));
        String key = hello.getNodeId() + "\u0000" + hello.getNodeIncarnationEpoch();
        long sequence = Math.max(existing.getPresence().getHeartbeatSeq(),
                heartbeatSequences.getOrDefault(key, 0L)) + 1;
        heartbeatSequences.put(key, sequence);
        var observed = heartbeat.getObservedAt();
        PresenceState state = existing.getPresence().getState()
                == PresenceState.PRESENCE_STATE_DRAINING
                ? PresenceState.PRESENCE_STATE_DRAINING
                : PresenceState.PRESENCE_STATE_ACTIVE;
        directory.heartbeat(NodePresence.newBuilder()
                .setNodeId(hello.getNodeId())
                .setClusterId(node.getClusterId())
                .setState(state)
                .setLastHeartbeatAt(observed)
                .setHeartbeatSeq(sequence)
                .setTtl(node.getTtl())
                .setNodeEpoch(hello.getNodeIncarnationEpoch())
                .setExpiresAt(Timestamps.add(observed, node.getTtl()))
                .build());
    }

    @Override
    public synchronized void capacity(WorkerHello hello, WorkerCapacity capacity) {
        requireNode(hello);
        var observed = RemoteValidation.timestamp(clock.instant());
        updateCapacity(hello.getNodeId(), "", hello.getNodeIncarnationEpoch(),
                capacity, observed);
        Map<String, Long> leases = new LinkedHashMap<>();
        hello.getProcessorLeasesList().forEach(lease ->
                leases.put(lease.getProcessorId(), lease.getLeaseEpoch()));
        for (var contract : hello.getContractsList()) {
            Long leaseEpoch = leases.get(contract.getProcessorId());
            if (leaseEpoch == null) {
                throw new IllegalArgumentException(
                        "worker capacity has no processor lease for "
                                + contract.getProcessorId());
            }
            updateCapacity(hello.getNodeId(), contract.getProcessorId(), leaseEpoch,
                    capacity, observed);
        }
    }

    @Override
    public synchronized void beginDrain(WorkerHello hello, String reason) {
        Objects.requireNonNull(reason, "reason");
        var node = requireNode(hello);
        var existing = directory.snapshot().getNodesList().stream()
                .filter(record -> record.getAdvertisement().getNodeId()
                        .equals(hello.getNodeId()))
                .findFirst()
                .orElseThrow();
        String key = hello.getNodeId() + "\u0000" + hello.getNodeIncarnationEpoch();
        long sequence = Math.max(existing.getPresence().getHeartbeatSeq(),
                heartbeatSequences.getOrDefault(key, 0L)) + 1;
        heartbeatSequences.put(key, sequence);
        var observed = RemoteValidation.timestamp(clock.instant());
        directory.heartbeat(NodePresence.newBuilder()
                .setNodeId(hello.getNodeId())
                .setClusterId(node.getClusterId())
                .setState(PresenceState.PRESENCE_STATE_DRAINING)
                .setLastHeartbeatAt(observed)
                .setHeartbeatSeq(sequence)
                .setTtl(node.getTtl())
                .setNodeEpoch(hello.getNodeIncarnationEpoch())
                .setExpiresAt(Timestamps.add(observed, node.getTtl()))
                .build());
    }

    @Override
    public void drainProgress(WorkerHello hello, WorkerDrainProgress progress) {
        if (progress.getDrained() && progress.getActiveClaims() != 0) {
            throw new IllegalArgumentException(
                    "a drained worker cannot report active claims");
        }
    }

    private ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement requireNode(
            WorkerHello hello) {
        var node = directory.node(hello.getNodeId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "worker names an absent directory node"));
        if (node.getEpoch() != hello.getNodeIncarnationEpoch()) {
            throw new IllegalArgumentException("worker node incarnation is fenced");
        }
        return node;
    }

    private void updateCapacity(
            String nodeId,
            String processorId,
            long sourceEpoch,
            WorkerCapacity capacity,
            com.google.protobuf.Timestamp observed) {
        String key = nodeId + "\u0000" + processorId + "\u0000" + sourceEpoch;
        long existing = processorId.isBlank()
                ? directory.snapshot().getNodesList().stream()
                .filter(record -> record.getAdvertisement().getNodeId().equals(nodeId))
                .filter(ai.protomolt.proto.mesh.cluster.v1.NodeRecord::hasCapacity)
                .mapToLong(record -> record.getCapacity().getSeq()).max().orElse(0)
                : directory.snapshot().getCapacitiesList().stream()
                .filter(record -> record.getNodeId().equals(nodeId)
                        && record.getProcessorId().equals(processorId))
                .mapToLong(CapacityAdvertisement::getSeq).max().orElse(0);
        long sequence = Math.max(existing, capacitySequences.getOrDefault(key, 0L)) + 1;
        capacitySequences.put(key, sequence);
        directory.updateCapacity(CapacityAdvertisement.newBuilder()
                .setNodeId(nodeId)
                .setProcessorId(processorId)
                .setMaxInFlight(capacity.getMaxInFlight())
                .setInFlight(capacity.getInFlight())
                .setObservedAt(observed)
                .setSeq(sequence)
                .setSourceEpoch(sourceEpoch)
                .build());
    }
}
