package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.MeshDigest;
import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.stream.Collectors;

/**
 * The fail-fast validation layer of the cluster directory contract, following the
 * {@code MeshValidation} conventions: well-known-type timestamp validity runs first (an
 * out-of-range Timestamp must be rejected before any CEL comparison sees it), then the
 * contract's own {@code validate.v1} annotations (field bounds plus the message-level CEL
 * rules: lease-outlives-advertisement, in-flight-within-limit, presence-expiry-after-heartbeat,
 * processor-events-name-processor) run through {@link ProtoValidator}, then the checks
 * annotations cannot express (canonical fingerprint agreement) run here.
 *
 * <p>Every failure throws {@link IllegalArgumentException} with a field-precise message.
 */
public final class ClusterValidation {

    private ClusterValidation() {
    }

    /**
     * Validates a cluster descriptor: timestamp validity, annotations, and agreement between
     * the declared fingerprint and the canonical fingerprint of the descriptor's identity
     * material.
     *
     * @param cluster the cluster descriptor to validate
     */
    public static void validate(ClusterDescriptor cluster) {
        require(cluster != null, "cluster must not be null");
        validateTimestamp(cluster.getCreatedAt(), "cluster.created_at");
        validateAnnotations(cluster);
        String actual = descriptorFingerprint(cluster);
        require(cluster.getFingerprint().equals(actual),
                "cluster.fingerprint does not match the canonical fingerprint of the descriptor");
    }

    /**
     * Validates a node advertisement: timestamp validity and annotations.
     *
     * @param advertisement the advertisement to validate
     */
    public static void validate(NodeAdvertisement advertisement) {
        require(advertisement != null, "advertisement must not be null");
        validateTimestamp(advertisement.getAdvertisedAt(), "advertisement.advertised_at");
        validateAnnotations(advertisement);
    }

    /**
     * Validates a processor advertisement: timestamp validity and annotations (including the
     * lease-outlives-advertisement message CEL).
     *
     * @param advertisement the advertisement to validate
     */
    public static void validate(ProcessorAdvertisement advertisement) {
        require(advertisement != null, "advertisement must not be null");
        validateTimestamp(advertisement.getAdvertisedAt(), "advertisement.advertised_at");
        validateTimestamp(advertisement.getLeaseExpiresAt(), "advertisement.lease_expires_at");
        validateAnnotations(advertisement);
    }

    /**
     * Validates a capacity snapshot: timestamp validity and annotations (including the
     * in-flight-within-limit message CEL).
     *
     * @param snapshot the snapshot to validate
     */
    public static void validate(CapacityAdvertisement snapshot) {
        require(snapshot != null, "snapshot must not be null");
        validateTimestamp(snapshot.getObservedAt(), "snapshot.observed_at");
        validateAnnotations(snapshot);
    }

    /**
     * Validates a presence record: timestamp validity and annotations (including the
     * presence-expiry-after-heartbeat message CEL).
     *
     * @param presence the presence record to validate
     */
    public static void validate(NodePresence presence) {
        require(presence != null, "presence must not be null");
        validateTimestamp(presence.getLastHeartbeatAt(), "presence.last_heartbeat_at");
        validateTimestamp(presence.getExpiresAt(), "presence.expires_at");
        validateAnnotations(presence);
    }

    /**
     * Validates a cluster event: timestamp validity and annotations (including the
     * processor-events-name-processor message CEL). The directory's own emissions always
     * validate; this is the check a replay consumer runs.
     *
     * @param event the event to validate
     */
    public static void validate(ai.pipestream.proto.mesh.cluster.v1.ClusterEvent event) {
        require(event != null, "event must not be null");
        validateTimestamp(event.getOccurredAt(), "event.occurred_at");
        validateAnnotations(event);
    }

    /**
     * Validates a cluster snapshot: timestamp validity, annotations, and agreement between the
     * declared fingerprint and the canonical fingerprint of the snapshot's content.
     *
     * @param snapshot the snapshot to validate
     */
    public static void validate(ClusterSnapshot snapshot) {
        require(snapshot != null, "snapshot must not be null");
        validateTimestamp(snapshot.getCapturedAt(), "snapshot.captured_at");
        validateAnnotations(snapshot);
        String actual = snapshotFingerprint(snapshot);
        require(snapshot.getFingerprint().equals(actual),
                "snapshot.fingerprint does not match the canonical fingerprint of the snapshot");
    }

    /**
     * Returns the canonical fingerprint of a cluster descriptor: the lowercase SHA-256 hex of
     * the descriptor serialized with the fingerprint field cleared. protobuf-java serializes
     * the message deterministically (fields in tag order; no maps occur in the cluster
     * contract), so the fingerprint depends on content alone. This is the {@link MeshDigest}
     * convention applied to a self-describing message.
     *
     * @param cluster the descriptor to fingerprint
     * @return the lowercase SHA-256 hex fingerprint
     */
    public static String descriptorFingerprint(ClusterDescriptor cluster) {
        return MeshDigest.sha256(cluster.toBuilder().clearFingerprint().build().toByteArray());
    }

    /**
     * Returns the canonical fingerprint of a cluster snapshot: the lowercase SHA-256 hex of the
     * snapshot serialized with the fingerprint field cleared.
     *
     * @param snapshot the snapshot to fingerprint
     * @return the lowercase SHA-256 hex fingerprint
     */
    public static String snapshotFingerprint(ClusterSnapshot snapshot) {
        return MeshDigest.sha256(snapshot.toBuilder().clearFingerprint().build().toByteArray());
    }

    /**
     * Rejects a {@link Timestamp} that violates the well-known-type validity rules (seconds
     * within 0001-01-01T00:00:00Z..9999-12-31T23:59:59Z, nanos within [0, 999999999]).
     */
    private static void validateTimestamp(Timestamp value, String field) {
        require(Timestamps.isValid(value), field + " must be a valid protobuf Timestamp");
    }

    private static void validateAnnotations(com.google.protobuf.Message message) {
        ValidationResult result = ProtoValidator.forMessageType(message.getDescriptorForType())
                .validate(message);
        if (!result.valid()) {
            throw new IllegalArgumentException("message fails the cluster contract annotations: "
                    + result.violations().stream()
                    .map(v -> "[" + v.path() + "] " + v.ruleId() + ": " + v.message())
                    .collect(Collectors.joining("; ")));
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
