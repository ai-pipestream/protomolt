package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.Endpoint;
import ai.protomolt.proto.mesh.cluster.v1.NodeRecord;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** Resolves exact processor contracts only to active, ready, capacity-bearing instances. */
public final class DirectoryProcessorResolver {

    private final Supplier<ProcessorDirectoryClient.View> directory;
    private final Clock clock;

    public DirectoryProcessorResolver(ProcessorDirectoryClient directory, Clock clock) {
        this(Objects.requireNonNull(directory, "directory")::view, clock);
    }

    public DirectoryProcessorResolver(
            Supplier<ProcessorDirectoryClient.View> directory, Clock clock) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public List<Instance> resolve(
            ProcessorContract contract, long payloadBytes, String preferredNodeId) {
        if (payloadBytes < 0) {
            throw new IllegalArgumentException("payloadBytes must not be negative");
        }
        ProcessorContract exact = ProcessorContracts.canonical(contract);
        var view = directory.get();
        if (!view.initialized()) {
            return List.of();
        }
        Instant now = clock.instant();
        return view.processors().values().stream()
                .filter(processor -> ProcessorContracts.exactMatch(
                        processor.getContract(), exact))
                .filter(processor -> preferredNodeId == null || preferredNodeId.isBlank()
                        || processor.getNodeId().equals(preferredNodeId))
                .map(processor -> instance(view, processor, payloadBytes, now))
                .flatMap(Optional::stream)
                .sorted(java.util.Comparator.comparing(Instance::processorId))
                .toList();
    }

    private static Optional<Instance> instance(
            ProcessorDirectoryClient.View view,
            ProcessorAdvertisement processor,
            long payloadBytes,
            Instant now) {
        NodeRecord node = view.nodes().get(processor.getNodeId());
        if (node == null
                || node.getPresence().getState() != PresenceState.PRESENCE_STATE_ACTIVE
                || !RemoteValidation.instant(node.getPresence().getExpiresAt()).isAfter(now)
                || !RemoteValidation.instant(processor.getLeaseExpiresAt()).isAfter(now)
                || processor.getNodeEpoch() != node.getAdvertisement().getEpoch()) {
            return Optional.empty();
        }
        var readiness = view.readiness().get(processor.getProcessorId());
        if (readiness != null && !readiness.getReady()) {
            return Optional.empty();
        }
        CapacityAdvertisement nodeCapacity = node.hasCapacity()
                ? node.getCapacity() : null;
        CapacityAdvertisement processorCapacity =
                view.processorCapacity().get(processor.getProcessorId());
        int available = Math.min(available(nodeCapacity, payloadBytes),
                available(processorCapacity, payloadBytes));
        if (available <= 0) {
            return Optional.empty();
        }
        Optional<Endpoint> endpoint = node.getAdvertisement().getEndpointsList().stream()
                .filter(Endpoint::getDirect)
                .findFirst();
        return endpoint.map(value -> new Instance(
                processor.getProcessorId(), processor.getNodeId(),
                node.getAdvertisement().getEpoch(), processor.getLeaseEpoch(),
                processor.getContract(), value, available));
    }

    private static int available(CapacityAdvertisement capacity, long payloadBytes) {
        if (capacity == null || capacity.getMaxInFlight() == 0
                || capacity.getInFlight() >= capacity.getMaxInFlight()) {
            return 0;
        }
        if (payloadBytes > 0 && capacity.getMaxPayloadBytes() > 0
                && payloadBytes > capacity.getMaxPayloadBytes()) {
            return 0;
        }
        return Math.toIntExact((long) capacity.getMaxInFlight() - capacity.getInFlight());
    }

    public record Instance(
            String processorId,
            String nodeId,
            long nodeIncarnationEpoch,
            long processorLeaseEpoch,
            ProcessorContract contract,
            Endpoint endpoint,
            int availableCapacity) {
    }
}
