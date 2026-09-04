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
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DirectoryProcessorResolverTest {

    private static final Instant NOW = Instant.parse("2026-09-04T00:00:00Z");

    @Test
    void resolvesTheMinimumLiveCapacityForAnExactContract() {
        ProcessorContract contract = contract("a".repeat(64));
        DirectoryProcessorResolver resolver = new DirectoryProcessorResolver(
                () -> view(contract, NOW.plusSeconds(30), true, 8, 2, 5, 1), clock());

        assertThat(resolver.resolve(contract, 1024, "node"))
                .singleElement()
                .satisfies(instance -> {
                    assertThat(instance.nodeIncarnationEpoch()).isEqualTo(7);
                    assertThat(instance.processorLeaseEpoch()).isEqualTo(11);
                    assertThat(instance.availableCapacity()).isEqualTo(4);
                    assertThat(instance.endpoint().getEndpointId()).isEqualTo("grpc");
                });
    }

    @Test
    void excludesExpiredUnreadyOverweightAndContractDriftedInstances() {
        ProcessorContract contract = contract("a".repeat(64));

        assertThat(new DirectoryProcessorResolver(
                () -> view(contract, NOW, true, 8, 2, 5, 1), clock())
                .resolve(contract, 1, "")).isEmpty();
        assertThat(new DirectoryProcessorResolver(
                () -> view(contract, NOW.plusSeconds(30), false, 8, 2, 5, 1), clock())
                .resolve(contract, 1, "")).isEmpty();
        assertThat(new DirectoryProcessorResolver(
                () -> view(contract, NOW.plusSeconds(30), true, 8, 2, 5, 1), clock())
                .resolve(contract, 2_000_000, "")).isEmpty();
        assertThat(new DirectoryProcessorResolver(
                () -> view(contract, NOW.plusSeconds(30), true, 8, 2, 5, 1), clock())
                .resolve(contract("b".repeat(64)), 1, "")).isEmpty();
    }

    private static ProcessorDirectoryClient.View view(
            ProcessorContract contract,
            Instant leaseExpiry,
            boolean ready,
            int nodeMax,
            int nodeInFlight,
            int processorMax,
            int processorInFlight) {
        var observed = Timestamps.fromMillis(NOW.minusSeconds(1).toEpochMilli());
        var endpoint = Endpoint.newBuilder()
                .setEndpointId("grpc")
                .setAddress("127.0.0.1:9090")
                .setTlsMode(TlsMode.TLS_MODE_SYSTEM)
                .setDirect(true);
        NodeAdvertisement node = NodeAdvertisement.newBuilder()
                .setNodeId("node")
                .setClusterId("cluster")
                .addEndpoints(endpoint)
                .setAdvertisedAt(observed)
                .setTtl(Durations.fromSeconds(60))
                .setEpoch(7)
                .setSeq(1)
                .build();
        NodePresence presence = NodePresence.newBuilder()
                .setNodeId("node")
                .setClusterId("cluster")
                .setState(PresenceState.PRESENCE_STATE_ACTIVE)
                .setLastHeartbeatAt(observed)
                .setHeartbeatSeq(1)
                .setTtl(Durations.fromSeconds(60))
                .setNodeEpoch(7)
                .setExpiresAt(Timestamps.fromMillis(NOW.plusSeconds(30).toEpochMilli()))
                .build();
        NodeRecord record = NodeRecord.newBuilder()
                .setAdvertisement(node)
                .setPresence(presence)
                .setCapacity(capacity("", 7, nodeMax, nodeInFlight))
                .build();
        ProcessorAdvertisement processor = ProcessorAdvertisement.newBuilder()
                .setProcessorId("processor")
                .setNodeId("node")
                .setNodeEpoch(7)
                .setLeaseEpoch(11)
                .setAdvertisedAt(observed)
                .setLeaseExpiresAt(Timestamps.fromMillis(leaseExpiry.toEpochMilli()))
                .setSeq(1)
                .setContract(contract)
                .build();
        ProcessorReadinessOverlay readiness = ProcessorReadinessOverlay.newBuilder()
                .setProcessorId("processor")
                .setNodeId("node")
                .setNodeEpoch(7)
                .setProcessorLeaseEpoch(11)
                .setReady(ready)
                .setRevision(1)
                .setUpdatedAt(observed)
                .build();
        return new ProcessorDirectoryClient.View(true, 4, 19, "fingerprint",
                Map.of("node", record), Map.of("processor", processor),
                Map.of("processor", capacity(
                        "processor", 11, processorMax, processorInFlight)),
                Map.of("processor", readiness));
    }

    private static CapacityAdvertisement capacity(
            String processorId, long epoch, int max, int inFlight) {
        return CapacityAdvertisement.newBuilder()
                .setNodeId("node")
                .setProcessorId(processorId)
                .setMaxInFlight(max)
                .setInFlight(inFlight)
                .setMaxPayloadBytes(1_000_000)
                .setObservedAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                .setSourceEpoch(epoch)
                .setSeq(1)
                .build();
    }

    private static ProcessorContract contract(String fingerprint) {
        return ProcessorContracts.canonical(ProcessorContract.newBuilder()
                .setProcessorId("processor")
                .setMaxOutputs(fingerprint.startsWith("b") ? 2 : 1)
                .build());
    }

    private static Clock clock() {
        return Clock.fixed(NOW, ZoneOffset.UTC);
    }
}
