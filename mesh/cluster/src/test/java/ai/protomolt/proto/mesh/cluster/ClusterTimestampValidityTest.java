package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.ClusterFixtures.MutableClock;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves every Timestamp field in the cluster contract is rejected when it violates the
 * well-known-type validity rules (seconds within 0001-01-01T00:00:00Z..9999-12-31T23:59:59Z,
 * nanos within [0, 999999999]).
 */
class ClusterTimestampValidityTest {

    /** One second before the WKT minimum (0001-01-01T00:00:00Z is -62135596800). */
    private static final long INVALID_SECONDS_LOW = -62135596801L;
    /** One second past the WKT maximum (9999-12-31T23:59:59Z is 253402300799). */
    private static final long INVALID_SECONDS_HIGH = 253402300800L;

    private static Timestamp invalidSeconds() {
        return Timestamp.newBuilder().setSeconds(INVALID_SECONDS_LOW).build();
    }

    private static Timestamp invalidSecondsHigh() {
        return Timestamp.newBuilder().setSeconds(INVALID_SECONDS_HIGH).build();
    }

    private static Timestamp invalidNanos() {
        return Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(1_000_000_000).build();
    }

    @Test
    void clusterCreatedAtWithInvalidSecondsIsRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(ClusterFixtures.clusterBuilder()
                .setCreatedAt(invalidSeconds())
                .setFingerprint("ab".repeat(32))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.created_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void nodeAdvertisedAtWithInvalidSecondsIsRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(ClusterFixtures.nodeBuilder("node-1", 1, 1)
                .setAdvertisedAt(invalidSecondsHigh())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisement.advertised_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void nodeAdvertisedAtWithInvalidNanosIsRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(ClusterFixtures.nodeBuilder("node-1", 1, 1)
                .setAdvertisedAt(invalidNanos())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisement.advertised_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void processorAdvertisedAtWithInvalidSecondsIsRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .setAdvertisedAt(invalidSeconds())
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisement.advertised_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void processorLeaseExpiresAtWithInvalidNanosIsRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.processorBuilder("proc-1", "node-1")
                        .setLeaseExpiresAt(invalidNanos())
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisement.lease_expires_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void capacityObservedAtWithInvalidSecondsIsRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.capacityBuilder("node-1", 1)
                        .setObservedAt(invalidSecondsHigh())
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot.observed_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void presenceTimestampsWithInvalidValuesAreRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.presenceBuilder("node-1", 1)
                        .setLastHeartbeatAt(invalidNanos())
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("presence.last_heartbeat_at")
                .hasMessageContaining("valid protobuf Timestamp");
        assertThatThrownBy(() -> ClusterValidation.validate(
                ClusterFixtures.presenceBuilder("node-1", 1)
                        .setExpiresAt(invalidSeconds())
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("presence.expires_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void eventOccurredAtWithInvalidSecondsIsRejected() {
        assertThatThrownBy(() -> ClusterValidation.validate(
                ai.protomolt.proto.mesh.cluster.v1.ClusterEvent.newBuilder()
                        .setSeq(1)
                        .setOccurredAt(invalidSeconds())
                        .setType(ai.protomolt.proto.mesh.cluster.v1.ClusterEventType
                                .CLUSTER_EVENT_TYPE_NODE_REGISTERED)
                        .setNodeId("node-1")
                        .setNode(ClusterFixtures.node("node-1"))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("event.occurred_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void snapshotCapturedAtWithInvalidNanosIsRejected() {
        ClusterDirectory directory = new ClusterDirectory(
                ClusterFixtures.cluster(), new MutableClock(ClusterFixtures.T0));
        ClusterSnapshot corrupted = directory.snapshot().toBuilder()
                .setCapturedAt(invalidNanos())
                .build();

        assertThatThrownBy(() -> ClusterValidation.validate(corrupted))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot.captured_at")
                .hasMessageContaining("valid protobuf Timestamp");
    }
}
