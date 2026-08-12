package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.Endpoint;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.PresenceState;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.TlsMode;
import ai.pipestream.proto.mesh.v1.ProcessorKind;
import ai.pipestream.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Shared builders and the fake clock for the cluster directory tests. Every builder produces
 * a record that passes {@link ClusterValidation}; tests corrupt copies to prove rejection.
 */
final class ClusterFixtures {

    static final Instant T0 = Instant.ofEpochSecond(1_700_000_000L);
    static final Duration TTL = Duration.newBuilder().setSeconds(30).build();
    static final String CLUSTER_ID = "cluster-a";
    static final String SCHEMA_TYPE = "acme.docs.v1.Document";
    static final String SCHEMA_FINGERPRINT = "ab".repeat(32);
    static final String OTHER_FINGERPRINT = "cd".repeat(32);

    private ClusterFixtures() {
    }

    static Timestamp ts(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    static ClusterDescriptor.Builder clusterBuilder() {
        return ClusterDescriptor.newBuilder()
                .setClusterId(CLUSTER_ID)
                .setDisplayName("Cluster A")
                .setTrustDomain("pipestream")
                .addProtocolRevisions(1)
                .setCreatedAt(ts(T0));
    }

    static ClusterDescriptor cluster() {
        ClusterDescriptor.Builder builder = clusterBuilder();
        return builder.setFingerprint(ClusterValidation.descriptorFingerprint(builder.build()))
                .build();
    }

    static SchemaReference schema() {
        return schema(SCHEMA_TYPE, SCHEMA_FINGERPRINT);
    }

    static SchemaReference schema(String typeName, String fingerprint) {
        return SchemaReference.newBuilder()
                .setTypeName(typeName)
                .setDescriptorFingerprint(fingerprint)
                .build();
    }

    static NodeAdvertisement.Builder nodeBuilder(String nodeId, long epoch, long seq) {
        return NodeAdvertisement.newBuilder()
                .setNodeId(nodeId)
                .setClusterId(CLUSTER_ID)
                .addCapabilities("relay")
                .addSchemasServed(schema())
                .addEndpoints(Endpoint.newBuilder()
                        .setEndpointId("grpc-main")
                        .setAddress("node1.example:9090")
                        .setTlsMode(TlsMode.TLS_MODE_SYSTEM)
                        .setDirect(true))
                .setAdvertisedAt(ts(T0))
                .setTtl(TTL)
                .setEpoch(epoch)
                .setSeq(seq);
    }

    static NodeAdvertisement node(String nodeId) {
        return nodeBuilder(nodeId, 1, 1).build();
    }

    static ProcessorAdvertisement.Builder processorBuilder(String processorId, String nodeId) {
        return ProcessorAdvertisement.newBuilder()
                .setProcessorId(processorId)
                .setNodeId(nodeId)
                .setKind(ProcessorKind.PROCESSOR_KIND_LLM)
                .addCapabilities("llm-generate")
                .addAcceptedSchemas(schema())
                .setNodeEpoch(1)
                .setLeaseEpoch(1)
                .setAdvertisedAt(ts(T0))
                .setLeaseExpiresAt(ts(T0.plusSeconds(60)))
                .setSeq(1);
    }

    static NodePresence.Builder presenceBuilder(String nodeId, long heartbeatSeq) {
        return NodePresence.newBuilder()
                .setNodeId(nodeId)
                .setClusterId(CLUSTER_ID)
                .setState(PresenceState.PRESENCE_STATE_ACTIVE)
                .setLastHeartbeatAt(ts(T0))
                .setHeartbeatSeq(heartbeatSeq)
                .setNodeEpoch(1)
                .setTtl(TTL)
                .setExpiresAt(ts(T0.plusSeconds(30)));
    }

    static CapacityAdvertisement.Builder capacityBuilder(String nodeId, long seq) {
        return CapacityAdvertisement.newBuilder()
                .setNodeId(nodeId)
                .setMaxInFlight(16)
                .setInFlight(3)
                .setMaxPayloadBytes(4_194_304L)
                .setObservedAt(ts(T0))
                .setSourceEpoch(1)
                .setSeq(seq);
    }

    /** A settable wall clock, so TTL expiry tests advance time without sleeping. */
    static final class MutableClock extends Clock {
        private Instant instant;

        MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public Instant instant() {
            return instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        void advance(java.time.Duration duration) {
            instant = instant.plus(duration);
        }
    }
}
