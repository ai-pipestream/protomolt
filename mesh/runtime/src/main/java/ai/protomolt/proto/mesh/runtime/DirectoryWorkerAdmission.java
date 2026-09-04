package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorLeaseBinding;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Admits a worker only against the exact live node incarnation and processor leases. */
public final class DirectoryWorkerAdmission implements RemoteWorkerAdmission {

    private final Supplier<ProcessorDirectoryClient.View> directory;
    private final Clock clock;
    private final Consumer<ProcessorContract> contractRegistrar;

    public DirectoryWorkerAdmission(ProcessorDirectoryClient directory, Clock clock) {
        this(Objects.requireNonNull(directory, "directory")::view, clock, ignored -> { });
    }

    public DirectoryWorkerAdmission(
            Supplier<ProcessorDirectoryClient.View> directory,
            Clock clock,
            Consumer<ProcessorContract> contractRegistrar) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.contractRegistrar = Objects.requireNonNull(contractRegistrar,
                "contractRegistrar");
    }

    @Override
    public Decision admit(WorkerHello hello) {
        var view = directory.get();
        if (!view.initialized()) {
            return refused("processor directory has no initial snapshot", view);
        }
        var node = view.nodes().get(hello.getNodeId());
        if (node == null) {
            return refused("directory has no node '" + hello.getNodeId() + "'", view);
        }
        if (node.getAdvertisement().getEpoch() != hello.getNodeIncarnationEpoch()) {
            return refused("worker node incarnation is fenced", view);
        }
        Instant now = clock.instant();
        if (node.getPresence().getState() != PresenceState.PRESENCE_STATE_ACTIVE
                || !RemoteValidation.instant(node.getPresence().getExpiresAt()).isAfter(now)) {
            return refused("worker node is not active", view);
        }
        boolean endpoint = node.getAdvertisement().getEndpointsList().stream()
                .anyMatch(candidate -> candidate.getEndpointId().equals(hello.getEndpointId()));
        if (!endpoint) {
            return refused("worker endpoint is not advertised by its node", view);
        }
        Map<String, ProcessorLeaseBinding> leases = new LinkedHashMap<>();
        for (ProcessorLeaseBinding lease : hello.getProcessorLeasesList()) {
            if (leases.putIfAbsent(lease.getProcessorId(), lease) != null) {
                return refused("worker repeats processor lease '"
                        + lease.getProcessorId() + "'", view);
            }
        }
        if (leases.size() != hello.getContractsCount()) {
            return refused("worker must bind exactly one lease to every contract", view);
        }
        Duration reconnectGrace = null;
        for (ProcessorContract contract : hello.getContractsList()) {
            ProcessorLeaseBinding lease = leases.get(contract.getProcessorId());
            if (lease == null
                    || !lease.getContractFingerprint().equals(
                    contract.getContractFingerprint())) {
                return refused("worker lease does not bind exact contract '"
                        + contract.getProcessorId() + "'", view);
            }
            var advertised = view.processors().get(contract.getProcessorId());
            if (advertised == null
                    || !advertised.getNodeId().equals(hello.getNodeId())
                    || advertised.getNodeEpoch() != hello.getNodeIncarnationEpoch()
                    || advertised.getLeaseEpoch() != lease.getLeaseEpoch()
                    || !RemoteValidation.instant(advertised.getLeaseExpiresAt())
                    .isAfter(now)
                    || !ProcessorContracts.exactMatch(advertised.getContract(), contract)) {
                return refused("processor contract or lease is not active in the directory: "
                        + contract.getProcessorId(), view);
            }
            var readiness = view.readiness().get(contract.getProcessorId());
            if (readiness != null && !readiness.getReady()) {
                return refused("processor is administratively not ready: "
                        + contract.getProcessorId(), view);
            }
            Duration processorGrace = advertised.getSupportsSessionResume()
                    ? RemoteValidation.duration(advertised.getMaxDisconnectGrace())
                    : Duration.ZERO;
            reconnectGrace = reconnectGrace == null
                    || processorGrace.compareTo(reconnectGrace) < 0
                    ? processorGrace : reconnectGrace;
        }
        try {
            hello.getContractsList().forEach(contractRegistrar);
        } catch (IllegalArgumentException | IllegalStateException failure) {
            return refused("worker processor contract registration failed: "
                    + failure.getMessage(), view);
        }
        return new Decision(true, "admitted", view.generation(), view.eventSequence(),
                reconnectGrace == null ? Duration.ZERO : reconnectGrace);
    }

    private static Decision refused(String reason, ProcessorDirectoryClient.View view) {
        return new Decision(false, reason, view.generation(), view.eventSequence());
    }
}
