package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.MeshDigest;
import ai.pipestream.proto.mesh.MeshValidation;
import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.CapabilityDescription;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.Endpoint;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.NodeRecord;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The fail-fast validation layer of the cluster directory contract, following the
 * {@code MeshValidation} conventions: well-known-type timestamp validity runs first (an
 * out-of-range Timestamp must be rejected before any CEL comparison sees it), then the
 * contract's own {@code validate.v1} annotations (field bounds plus the message-level CEL
 * rules for lease expiry, capacity, presence expiry, and event coherence) run through
 * {@link ProtoValidator}, then the checks annotations cannot express (canonical fingerprint
 * agreement, cross-record ownership, and stable ordering) run here.
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
        validateDuration(advertisement.getTtl(), "advertisement.ttl");
        advertisement.getSchemasServedList().forEach(MeshValidation::validate);
        requireUnique(advertisement.getEndpointsList(), Endpoint::getEndpointId,
                "advertisement.endpoints.endpoint_id");
        advertisement.getEndpointsList().forEach(ClusterValidation::validateEndpoint);
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
        if (advertisement.hasMaxDisconnectGrace()) {
            validateDuration(advertisement.getMaxDisconnectGrace(),
                    "advertisement.max_disconnect_grace");
        }
        advertisement.getAcceptedSchemasList().forEach(MeshValidation::validate);
        requireUnique(advertisement.getCapabilityDetailsList(), CapabilityDescription::getName,
                "advertisement.capability_details.name");
        for (CapabilityDescription detail : advertisement.getCapabilityDetailsList()) {
            require(advertisement.getCapabilitiesList().contains(detail.getName()),
                    "advertisement.capability_details names '" + detail.getName()
                            + "' but capabilities does not contain it");
        }
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
        validateDuration(presence.getTtl(), "presence.ttl");
        validateAnnotations(presence);
    }

    /**
     * Validates a cluster event: timestamp validity, nested detail validity, and annotations.
     * The directory's own emissions always validate; this is the check a replay consumer runs.
     *
     * @param event the event to validate
     */
    public static void validate(ClusterEvent event) {
        require(event != null, "event must not be null");
        validateTimestamp(event.getOccurredAt(), "event.occurred_at");
        switch (event.getDetailCase()) {
            case NODE -> validate(event.getNode());
            case PROCESSOR -> validate(event.getProcessor());
            case PRESENCE -> validate(event.getPresence());
            case CAPACITY -> validate(event.getCapacity());
            case DETAIL_NOT_SET -> {
                // The message CEL below returns the field-precise contract violation.
            }
        }
        validateAnnotations(event);
    }

    /** Validates one complete, gap-free event log in sequence and timestamp order. */
    public static void validateEventLog(List<ClusterEvent> events) {
        require(events != null, "events must not be null");
        long expectedSeq = 1;
        Instant previous = null;
        for (ClusterEvent event : events) {
            validate(event);
            require(event.getSeq() == expectedSeq,
                    "event.seq " + event.getSeq() + " does not match expected " + expectedSeq);
            Instant occurred = instant(event.getOccurredAt());
            require(previous == null || !occurred.isBefore(previous),
                    "event.occurred_at moves backward at seq " + event.getSeq());
            previous = occurred;
            expectedSeq++;
        }
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
        require(snapshot.hasCluster(), "snapshot.cluster is required");
        validate(snapshot.getCluster());

        Set<String> nodeIds = new HashSet<>();
        String previousNodeId = null;
        for (NodeRecord record : snapshot.getNodesList()) {
            require(record.hasAdvertisement(), "snapshot.nodes.advertisement is required");
            require(record.hasPresence(), "snapshot.nodes.presence is required");
            NodeAdvertisement node = record.getAdvertisement();
            NodePresence presence = record.getPresence();
            validate(node);
            validate(presence);
            require(node.getClusterId().equals(snapshot.getCluster().getClusterId()),
                    "snapshot node '" + node.getNodeId() + "' names another cluster");
            require(presence.getNodeId().equals(node.getNodeId())
                            && presence.getClusterId().equals(node.getClusterId())
                            && presence.getNodeEpoch() == node.getEpoch(),
                    "snapshot presence does not match node '" + node.getNodeId() + "'");
            require(previousNodeId == null || previousNodeId.compareTo(node.getNodeId()) < 0,
                    "snapshot.nodes must be strictly ordered by node_id");
            require(nodeIds.add(node.getNodeId()),
                    "snapshot contains duplicate node '" + node.getNodeId() + "'");
            previousNodeId = node.getNodeId();
            if (record.hasCapacity()) {
                CapacityAdvertisement capacity = record.getCapacity();
                validate(capacity);
                require(capacity.getNodeId().equals(node.getNodeId())
                                && capacity.getProcessorId().isEmpty()
                                && capacity.getSourceEpoch() == node.getEpoch(),
                        "snapshot node capacity does not match node '" + node.getNodeId() + "'");
            }
        }

        Set<String> processorIds = new HashSet<>();
        java.util.Map<String, ProcessorAdvertisement> processors = new java.util.HashMap<>();
        String previousProcessorId = null;
        for (ProcessorAdvertisement processor : snapshot.getProcessorsList()) {
            validate(processor);
            require(nodeIds.contains(processor.getNodeId()),
                    "snapshot processor '" + processor.getProcessorId()
                            + "' names an absent node");
            NodeRecord owner = snapshot.getNodesList().stream()
                    .filter(record -> record.getAdvertisement().getNodeId()
                            .equals(processor.getNodeId()))
                    .findFirst().orElseThrow();
            require(processor.getNodeEpoch() == owner.getAdvertisement().getEpoch(),
                    "snapshot processor '" + processor.getProcessorId()
                            + "' carries a stale node_epoch");
            require(previousProcessorId == null
                            || previousProcessorId.compareTo(processor.getProcessorId()) < 0,
                    "snapshot.processors must be strictly ordered by processor_id");
            require(processorIds.add(processor.getProcessorId()),
                    "snapshot contains duplicate processor '" + processor.getProcessorId() + "'");
            processors.put(processor.getProcessorId(), processor);
            previousProcessorId = processor.getProcessorId();
        }

        String previousCapacityKey = null;
        for (CapacityAdvertisement capacity : snapshot.getCapacitiesList()) {
            validate(capacity);
            require(!capacity.getProcessorId().isEmpty(),
                    "snapshot.capacities contains node-wide capacity outside NodeRecord");
            ProcessorAdvertisement processor = processors.get(capacity.getProcessorId());
            require(processor != null
                            && processor.getNodeId().equals(capacity.getNodeId())
                            && processor.getLeaseEpoch() == capacity.getSourceEpoch(),
                    "snapshot capacity does not match processor '"
                            + capacity.getProcessorId() + "'");
            String key = capacity.getNodeId() + "\n" + capacity.getProcessorId();
            require(previousCapacityKey == null || previousCapacityKey.compareTo(key) < 0,
                    "snapshot.capacities must be strictly ordered by node_id and processor_id");
            previousCapacityKey = key;
        }
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

    /** Rejects a protobuf Duration outside the WKT seconds and nanos ranges. */
    private static void validateDuration(com.google.protobuf.Duration value, String field) {
        require(Durations.isValid(value), field + " must be a valid protobuf Duration");
    }

    private static void validateEndpoint(Endpoint endpoint) {
        String address = endpoint.getAddress();
        if (address.matches("^[A-Za-z0-9][A-Za-z0-9.-]{0,253}:[0-9]{1,5}$")) {
            int port = Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
            require(port >= 1 && port <= 65_535,
                    "endpoint.address port must be between 1 and 65535: '" + address + "'");
        }
    }

    private static <T> void requireUnique(List<T> values, Function<T, String> identity,
            String field) {
        Set<String> seen = new HashSet<>();
        for (T value : values) {
            String id = identity.apply(value);
            require(seen.add(id), field + " contains duplicate '" + id + "'");
        }
    }

    private static Instant instant(Timestamp value) {
        return Instant.ofEpochSecond(value.getSeconds(), value.getNanos());
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
