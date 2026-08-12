package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import ai.pipestream.proto.mesh.cluster.v1.CapabilityDescription;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import ai.pipestream.proto.mesh.v1.ProcessorKind;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Timestamp;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The pure mapping from a delegation {@link WorkerHello} to a leased mesh
 * {@link ProcessorAdvertisement}. The bridge is additive: it reads the hello and derives an
 * advertisement, and it changes no delegation lifecycle semantics. Admission, offers,
 * heartbeats, renewals, and expiry remain the delegation coordinator's business; the derived
 * advertisement is simply how a delegated worker becomes visible to mesh discovery.
 *
 * <p>Mapping decisions:
 *
 * <ul>
 *   <li>{@code processor_id} is the hello's {@code worker_id}: both are the session's stable
 *       path-safe worker identity;</li>
 *   <li>{@code kind} is {@code PROCESSOR_KIND_LLM} when the hello names a provider and
 *       {@code PROCESSOR_KIND_DETERMINISTIC} when it does not, matching the hello's own
 *       contract that an empty provider marks a deterministic worker;</li>
 *   <li>{@code capabilities} are the hello's capability names in declaration order,
 *       deduplicated, because an advertisement's capability list is a set;</li>
 *   <li>{@code accepted_schemas} is empty: a {@code WorkerHello} declares no schema set, so
 *       the bridge invents none. A coordinator that knows the worker's schemas registers a
 *       follow-up advertisement under a newer sequence;</li>
 *   <li>{@code advertised_at} is the caller's instant and {@code lease_expires_at} is that
 *       instant plus the caller's lease duration; the caller owns the lease terms because the
 *       hello carries none.</li>
 * </ul>
 *
 * <p>The hello's annotations are validated before mapping and the derived advertisement is
 * validated before it is returned; every failure is an {@link IllegalArgumentException}.
 */
public final class DelegationBridge {

    private DelegationBridge() {
    }

    /**
     * Derives a leased processor advertisement from a worker hello.
     *
     * @param hello the worker hello to map
     * @param nodeId the mesh node the worker's stream terminates on
     * @param nodeEpoch the fencing epoch of that node incarnation
     * @param leaseEpoch the lease fencing epoch the coordinator assigned
     * @param seq the advertisement sequence inside the lease epoch
     * @param leaseDuration how long the derived lease runs; must be positive
     * @param now the instant the derivation happens at, on the caller's clock
     * @return the leased processor advertisement
     * @throws IllegalArgumentException when the hello fails its annotations, the lease
     *     duration is not positive, or the derived advertisement fails validation
     */
    public static ProcessorAdvertisement toProcessorAdvertisement(WorkerHello hello,
            String nodeId, long nodeEpoch, long leaseEpoch, long seq, Duration leaseDuration,
            Instant now) {
        require(hello != null, "hello must not be null");
        require(now != null, "now must not be null");
        require(leaseDuration != null && !leaseDuration.isNegative() && !leaseDuration.isZero(),
                "leaseDuration must be positive");
        validateHello(hello);
        ProcessorKind kind = hello.getProvider().isEmpty()
                ? ProcessorKind.PROCESSOR_KIND_DETERMINISTIC
                : ProcessorKind.PROCESSOR_KIND_LLM;
        Map<String, WorkerCapability> capabilities = new LinkedHashMap<>();
        hello.getCapabilitiesList().forEach(capability ->
                capabilities.putIfAbsent(capability.getName(), capability));
        ProcessorAdvertisement.Builder builder = ProcessorAdvertisement.newBuilder()
                .setProcessorId(hello.getWorkerId())
                .setNodeId(nodeId)
                .setNodeEpoch(nodeEpoch)
                .setKind(kind)
                .addAllCapabilities(capabilities.keySet())
                .setProvider(hello.getProvider())
                .setModel(hello.getModel())
                .setModelVersion(hello.getModelVersion())
                .setLeaseEpoch(leaseEpoch)
                .setSeq(seq)
                .setAdvertisedAt(timestamp(now))
                .setLeaseExpiresAt(timestamp(now.plus(leaseDuration)));
        capabilities.values().forEach(capability -> builder.addCapabilityDetails(
                CapabilityDescription.newBuilder()
                        .setName(capability.getName())
                        .setDescription(capability.getDescription())));
        ProcessorAdvertisement advertisement = builder.build();
        ClusterValidation.validate(advertisement);
        return advertisement;
    }

    private static void validateHello(WorkerHello hello) {
        ValidationResult result = ProtoValidator.forMessageType(hello.getDescriptorForType())
                .validate(hello);
        if (!result.valid()) {
            throw new IllegalArgumentException("hello fails the delegation contract annotations: "
                    + result.violations().stream()
                    .map(v -> "[" + v.path() + "] " + v.ruleId() + ": " + v.message())
                    .collect(Collectors.joining("; ")));
        }
    }

    private static Timestamp timestamp(Instant value) {
        return Timestamp.newBuilder()
                .setSeconds(value.getEpochSecond())
                .setNanos(value.getNano())
                .build();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
