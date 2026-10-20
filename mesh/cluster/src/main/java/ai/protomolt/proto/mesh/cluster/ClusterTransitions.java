package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.protomolt.proto.mesh.cluster.v1.NodePresence;
import ai.protomolt.proto.mesh.cluster.v1.PresenceState;
import ai.protomolt.proto.mesh.cluster.v1.ProcessorAdvertisement;
import com.google.protobuf.Timestamp;
import java.time.Instant;

/**
 * The transition rules both directory drivers share: fencing gates, refresh classification,
 * and presence arithmetic. Live apply ({@link ClusterDirectory}) and replay
 * ({@link ClusterEventReplay}) are one state machine; these are the gates that make them agree.
 * Every rule here is pure: no clock reads, no event emission, no I/O.
 *
 * <p><b>Fencing.</b> A changed record is accepted only from a strictly newer position: a
 * higher epoch, or the same epoch with a higher sequence. Expired identities retain fencing
 * tombstones so a delayed frame cannot revive an old incarnation.
 *
 * <p><b>Refresh classification.</b> A refresh is a record identical to the registered one in
 * every field but those a lease renewal moves. The test copies those fields across and asks
 * for equality, so any field added later is treated as identity by default: a genuine change
 * stays durable unless someone deliberately classifies it as a refresh.
 */
final class ClusterTransitions {

    private ClusterTransitions() {
    }

    /** Throws when {@code condition} does not hold, with the given refusal message. */
    static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /** Refuses a record that names a cluster other than the one this directory serves. */
    static void requireCluster(ClusterDescriptor cluster, String clusterId, String what) {
        require(clusterId.equals(cluster.getClusterId()),
                what + " names cluster '" + clusterId + "' but this directory serves cluster '"
                        + cluster.getClusterId() + "'");
    }

    /**
     * The fencing gate for a changed record: the fresh position must advance the registered
     * one, or the update conflicts.
     */
    static void requireFresher(long epoch, long seq, long currentEpoch, long currentSeq,
            String identity) {
        require(epoch > currentEpoch || (epoch == currentEpoch && seq > currentSeq),
                "conflicting update for " + identity + ": epoch " + epoch + " seq " + seq
                        + " does not advance the registered epoch " + currentEpoch + " seq "
                        + currentSeq);
    }

    /** The fencing gate for a new identity: it must advance the retained tombstone, if any. */
    static void requireNewerThanTombstone(ClusterDirectoryState.Position position, long epoch,
            long seq, String identity) {
        if (position != null) {
            requireFresher(epoch, seq, position.epoch(), position.seq(), identity);
        }
    }

    /**
     * True when a fresh node advertisement differs from the registered one only in the fields
     * a lease refresh moves.
     */
    static boolean refreshOnly(NodeAdvertisement existing, NodeAdvertisement fresh) {
        return existing.toBuilder()
                .setAdvertisedAt(fresh.getAdvertisedAt())
                .setTtl(fresh.getTtl())
                .setSeq(fresh.getSeq())
                .build()
                .equals(fresh);
    }

    /** The refresh rule for a processor advertisement: lease window and sequence may move. */
    static boolean refreshOnly(ProcessorAdvertisement existing,
                               ProcessorAdvertisement fresh) {
        return existing.toBuilder()
                .setAdvertisedAt(fresh.getAdvertisedAt())
                .setLeaseExpiresAt(fresh.getLeaseExpiresAt())
                .setSeq(fresh.getSeq())
                .build()
                .equals(fresh);
    }

    /** The refresh rule for a capacity snapshot: observation time and sequence may move. */
    static boolean refreshOnly(CapacityAdvertisement existing,
                               CapacityAdvertisement fresh) {
        return existing.toBuilder()
                .setObservedAt(fresh.getObservedAt())
                .setSeq(fresh.getSeq())
                .build()
                .equals(fresh);
    }

    /** Arms a node's presence from its registration: ACTIVE within the advertised TTL window. */
    static NodePresence initialPresence(NodeAdvertisement advertisement) {
        return NodePresence.newBuilder()
                .setNodeId(advertisement.getNodeId())
                .setClusterId(advertisement.getClusterId())
                .setState(PresenceState.PRESENCE_STATE_ACTIVE)
                .setLastHeartbeatAt(advertisement.getAdvertisedAt())
                .setHeartbeatSeq(advertisement.getSeq())
                .setTtl(advertisement.getTtl())
                .setNodeEpoch(advertisement.getEpoch())
                .setExpiresAt(timestamp(instant(advertisement.getAdvertisedAt())
                        .plus(duration(advertisement.getTtl()))))
                .build();
    }

    /**
     * Merges a same-epoch re-advertisement into the armed presence: a newer heartbeat, or one
     * at the same instant with a longer TTL, extends the window; anything else leaves it.
     */
    static NodePresence mergePresence(NodePresence current, NodeAdvertisement advertisement) {
        Instant heartbeat = instant(advertisement.getAdvertisedAt());
        Instant advertisedExpiry = heartbeat.plus(duration(advertisement.getTtl()));
        Instant currentHeartbeat = instant(current.getLastHeartbeatAt());
        if (heartbeat.isAfter(currentHeartbeat)
                || (heartbeat.equals(currentHeartbeat)
                && advertisedExpiry.isAfter(instant(current.getExpiresAt())))) {
            return current.toBuilder()
                    .setLastHeartbeatAt(advertisement.getAdvertisedAt())
                    .setTtl(advertisement.getTtl())
                    .setExpiresAt(timestamp(advertisedExpiry))
                    .build();
        }
        return current;
    }

    /** Converts a wire timestamp to a Java instant without losing the nanos. */
    static Instant instant(Timestamp value) {
        return Instant.ofEpochSecond(value.getSeconds(), value.getNanos());
    }

    /** Converts a wire duration to a Java duration without losing the nanos. */
    static java.time.Duration duration(com.google.protobuf.Duration value) {
        return java.time.Duration.ofSeconds(value.getSeconds(), value.getNanos());
    }

    /** Converts a Java instant to a wire timestamp without losing the nanos. */
    static Timestamp timestamp(Instant value) {
        return Timestamp.newBuilder()
                .setSeconds(value.getEpochSecond())
                .setNanos(value.getNano())
                .build();
    }
}
