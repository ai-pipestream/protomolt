package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.Endpoint;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.NodeRecord;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorReadinessOverlay;
import ai.protomolt.proto.mesh.cluster.v1.TlsMode;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorLeaseBinding;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DirectoryWorkerAdmissionTest {

    private static final Instant NOW = Instant.parse("2026-09-04T00:00:00Z");

    @Test
    void admitsOnlyTheExactLiveNodeProcessorAndLease() {
        ProcessorContract contract = contract("processor", "a".repeat(64));
        var registered = new ArrayList<ProcessorContract>();
        DirectoryWorkerAdmission admission = new DirectoryWorkerAdmission(
                () -> view(contract, NOW.plusSeconds(30), true), clock(), registered::add);

        var decision = admission.admit(hello(contract, 7, 11));

        assertThat(decision.admitted()).isTrue();
        assertThat(decision.directoryGeneration()).isEqualTo(4);
        assertThat(decision.directoryEventSequence()).isEqualTo(19);
        assertThat(decision.reconnectGrace()).isEqualTo(java.time.Duration.ofSeconds(12));
        assertThat(registered).containsExactly(contract);
    }

    @Test
    void refusesExpiredProcessorLeaseStaleNodeAndReadinessOverlay() {
        ProcessorContract contract = contract("processor", "a".repeat(64));

        assertThat(new DirectoryWorkerAdmission(
                () -> view(contract, NOW, true), clock(), ignored -> { })
                .admit(hello(contract, 7, 11)).reason())
                .contains("not active in the directory");

        assertThat(new DirectoryWorkerAdmission(
                () -> view(contract, NOW.plusSeconds(30), true), clock(), ignored -> { })
                .admit(hello(contract, 8, 11)).reason())
                .contains("incarnation is fenced");

        assertThat(new DirectoryWorkerAdmission(
                () -> view(contract, NOW.plusSeconds(30), false), clock(), ignored -> { })
                .admit(hello(contract, 7, 11)).reason())
                .contains("administratively not ready");
    }

    @Test
    void refusesContractOrLeaseDriftWithoutRegisteringIt() {
        ProcessorContract contract = contract("processor", "a".repeat(64));
        var registered = new ArrayList<ProcessorContract>();
        DirectoryWorkerAdmission admission = new DirectoryWorkerAdmission(
                () -> view(contract, NOW.plusSeconds(30), true), clock(), registered::add);

        assertThat(admission.admit(hello(contract, 7, 12)).reason())
                .contains("not active in the directory");
        assertThat(admission.admit(hello(
                contract("processor", "b".repeat(64)), 7, 11)).reason())
                .contains("not active in the directory");
        assertThat(registered).isEmpty();
    }

    private static ProcessorDirectoryClient.View view(
            ProcessorContract contract, Instant leaseExpiry, boolean ready) {
        var advertisedAt = Timestamps.fromMillis(NOW.minusSeconds(1).toEpochMilli());
        NodeAdvertisement node = NodeAdvertisement.newBuilder()
                .setNodeId("node")
                .setClusterId("cluster")
                .addEndpoints(Endpoint.newBuilder()
                        .setEndpointId("grpc")
                        .setAddress("127.0.0.1:9090")
                        .setTlsMode(TlsMode.TLS_MODE_SYSTEM)
                        .setDirect(true))
                .setAdvertisedAt(advertisedAt)
                .setTtl(Durations.fromSeconds(60))
                .setEpoch(7)
                .setSeq(1)
                .build();
        NodePresence presence = NodePresence.newBuilder()
                .setNodeId("node")
                .setClusterId("cluster")
                .setState(PresenceState.PRESENCE_STATE_ACTIVE)
                .setLastHeartbeatAt(advertisedAt)
                .setHeartbeatSeq(1)
                .setTtl(Durations.fromSeconds(60))
                .setNodeEpoch(7)
                .setExpiresAt(Timestamps.fromMillis(NOW.plusSeconds(30).toEpochMilli()))
                .build();
        CapacityAdvertisement nodeCapacity = capacity("", 7, 8, 2);
        NodeRecord record = NodeRecord.newBuilder()
                .setAdvertisement(node)
                .setPresence(presence)
                .setCapacity(nodeCapacity)
                .build();
        ProcessorAdvertisement processor = ProcessorAdvertisement.newBuilder()
                .setProcessorId("processor")
                .setNodeId("node")
                .setNodeEpoch(7)
                .setLeaseEpoch(11)
                .setAdvertisedAt(advertisedAt)
                .setLeaseExpiresAt(Timestamps.fromMillis(leaseExpiry.toEpochMilli()))
                .setSeq(1)
                .setSupportsSessionResume(true)
                .setMaxDisconnectGrace(Durations.fromSeconds(12))
                .setContract(contract)
                .build();
        ProcessorReadinessOverlay readiness = ProcessorReadinessOverlay.newBuilder()
                .setProcessorId("processor")
                .setNodeId("node")
                .setNodeEpoch(7)
                .setProcessorLeaseEpoch(11)
                .setReady(ready)
                .setRevision(1)
                .setUpdatedAt(advertisedAt)
                .build();
        return new ProcessorDirectoryClient.View(true, 4, 19, "fingerprint",
                Map.of("node", record), Map.of("processor", processor),
                Map.of("processor", capacity("processor", 11, 6, 1)),
                Map.of("processor", readiness));
    }

    private static CapacityAdvertisement capacity(
            String processorId, long epoch, int max, int inFlight) {
        return CapacityAdvertisement.newBuilder()
                .setNodeId("node")
                .setProcessorId(processorId)
                .setMaxInFlight(max)
                .setInFlight(inFlight)
                .setObservedAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                .setSourceEpoch(epoch)
                .setSeq(1)
                .build();
    }

    private static WorkerHello hello(ProcessorContract contract, long nodeEpoch, long leaseEpoch) {
        return WorkerHello.newBuilder()
                .setWorkerId("worker")
                .setNodeId("node")
                .setNodeIncarnationEpoch(nodeEpoch)
                .setEndpointId("grpc")
                .addContracts(contract)
                .addProcessorLeases(ProcessorLeaseBinding.newBuilder()
                        .setProcessorId(contract.getProcessorId())
                        .setLeaseEpoch(leaseEpoch)
                        .setContractFingerprint(contract.getContractFingerprint()))
                .build();
    }

    private static ProcessorContract contract(String processorId, String fingerprint) {
        return ProcessorContracts.canonical(ProcessorContract.newBuilder()
                .setProcessorId(processorId)
                .setMaxOutputs(fingerprint.startsWith("b") ? 2 : 1)
                .build());
    }

    private static Clock clock() {
        return Clock.fixed(NOW, ZoneOffset.UTC);
    }
}
