package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.MeshValidation;
import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcomeKind;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcome;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.RetryStrategy;
import ai.protomolt.proto.mesh.runtime.v1.SettlementEffect;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorLeaseBinding;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.protobuf.util.Durations;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/** Fail-fast validation shared by the channel, coordinator, and worker. */
final class RemoteValidation {

    private static final Pattern PROCESSOR =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._/-]{0,127}");
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");
    private static final Pattern WORKER =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private RemoteValidation() {
    }

    static void contract(ProcessorContract contract, DescriptorRegistry descriptors) {
        Objects.requireNonNull(contract, "contract");
        if (!ProcessorContracts.canonical(contract).equals(contract)) {
            throw new IllegalArgumentException("processor contract must carry its canonical "
                    + "output order and contract_fingerprint: " + contract.getProcessorId());
        }
        annotations(contract);
        if (!PROCESSOR.matcher(contract.getProcessorId()).matches()) {
            throw new IllegalArgumentException(
                    "processor_id is not path-safe: " + contract.getProcessorId());
        }
        if (!contract.hasInputSchema() || contract.getOutputSchemasCount() == 0) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " requires input_schema and output_schemas");
        }
        RuntimeSchemas.resolve(descriptors, contract.getInputSchema());
        Set<DescriptorIdentity> outputs = new HashSet<>();
        for (SchemaReference output : contract.getOutputSchemasList()) {
            RuntimeSchemas.resolve(descriptors, output);
            if (!outputs.add(RuntimeSchemas.identity(output))) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " repeats output schema " + output.getTypeName());
            }
        }
        if (contract.getMaxOutputs() < 1 || contract.getMaxOutputs() > 100_000) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " max_outputs must be between 1 and 100000");
        }
    }

    static void work(ProcessorWork work, DescriptorRegistry descriptors) {
        Objects.requireNonNull(work, "work");
        annotations(work);
        uuid(work.getDeliveryId(), "delivery_id");
        uuid(work.getRunId(), "run_id");
        uuid(work.getInvocationId(), "invocation_id");
        if (!IDENTIFIER.matcher(work.getNodeId()).matches()) {
            throw new IllegalArgumentException("node_id is not an identifier: "
                    + work.getNodeId());
        }
        if (work.getInvocationOrdinal() < 1) {
            throw new IllegalArgumentException("invocation_ordinal must be positive");
        }
        if (!work.hasContract()) {
            throw new IllegalArgumentException("processor work requires a contract");
        }
        contract(work.getContract(), descriptors);
        if (!work.hasInput()) {
            throw new IllegalArgumentException("processor work requires input envelope");
        }
        MeshValidation.validateStructure(work.getInput());
        RuntimeSchemas.resolve(descriptors, work.getInput().getSchema());
        if (!RuntimeSchemas.same(
                work.getContract().getInputSchema(), work.getInput().getSchema())) {
            throw new IllegalArgumentException("processor work input schema does not match "
                    + work.getContract().getProcessorId() + " contract");
        }
        if (!work.hasDeadline()) {
            throw new IllegalArgumentException("processor work requires a deadline");
        }
        timestamp(work.getDeadline(), "deadline");
        if (work.getMaxAttempts() < 1 || work.getMaxAttempts() > 100) {
            throw new IllegalArgumentException("max_attempts must be between 1 and 100");
        }
    }

    static void hello(WorkerHello hello, DescriptorRegistry descriptors) {
        Objects.requireNonNull(hello, "hello");
        annotations(hello);
        workerId(hello.getWorkerId());
        if (hello.getNodeId().isBlank() || hello.getNodeIncarnationEpoch() < 1
                || hello.getEndpointId().isBlank()) {
            throw new IllegalArgumentException(
                    "worker hello requires node_id, node_incarnation_epoch, and endpoint_id");
        }
        if (hello.getContractsCount() == 0) {
            throw new IllegalArgumentException("worker must advertise at least one contract");
        }
        Set<String> ids = new HashSet<>();
        for (ProcessorContract contract : hello.getContractsList()) {
            contract(contract, descriptors);
            if (!ids.add(contract.getProcessorId())) {
                throw new IllegalArgumentException("worker repeats processor contract "
                        + contract.getProcessorId());
            }
        }
        Set<String> leaseIds = new HashSet<>();
        for (ProcessorLeaseBinding lease : hello.getProcessorLeasesList()) {
            if (!leaseIds.add(lease.getProcessorId())) {
                throw new IllegalArgumentException("worker repeats processor lease "
                        + lease.getProcessorId());
            }
            if (lease.getLeaseEpoch() < 1 || lease.getContractFingerprint().isBlank()) {
                throw new IllegalArgumentException("worker processor lease requires epoch and "
                        + "contract_fingerprint: " + lease.getProcessorId());
            }
        }
        if (!leaseIds.equals(ids)) {
            throw new IllegalArgumentException(
                    "worker hello must bind exactly one lease to every contract");
        }
    }

    static void claim(DeliveryClaim claim, DescriptorRegistry descriptors) {
        Objects.requireNonNull(claim, "claim");
        annotations(claim);
        if (!claim.hasWork()) {
            throw new IllegalArgumentException("delivery claim requires work");
        }
        work(claim.getWork(), descriptors);
        workerId(claim.getWorkerId());
        uuid(claim.getLeaseToken(), "lease_token");
        if (claim.getAttempt() < 1
                || claim.getAttempt() > claim.getWork().getMaxAttempts()) {
            throw new IllegalArgumentException("claim attempt must be between 1 and max_attempts");
        }
        if (!claim.hasLeaseExpiresAt()) {
            throw new IllegalArgumentException("delivery claim requires lease_expires_at");
        }
        java.time.Instant leaseExpiry = instant(claim.getLeaseExpiresAt());
        java.time.Instant workDeadline = instant(claim.getWork().getDeadline());
        if (leaseExpiry.isAfter(workDeadline)) {
            throw new IllegalArgumentException(
                    "claim lease_expires_at must not exceed the work deadline");
        }
    }

    static void failure(ProcessorFailure failure) {
        Objects.requireNonNull(failure, "failure");
        annotations(failure);
        uuid(failure.getDeliveryId(), "delivery_id");
        uuid(failure.getLeaseToken(), "lease_token");
        uuid(failure.getCompletionId(), "completion_id");
        if (!failure.hasOutcome()) {
            throw new IllegalArgumentException("processor failure requires a typed outcome");
        }
        outcome(failure.getOutcome());
    }

    static void outcome(ProcessorOutcome outcome) {
        Objects.requireNonNull(outcome, "outcome");
        annotations(outcome);
        uuid(outcome.getOutcomeId(), "outcome_id");
        if (outcome.getKind() == ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_UNSPECIFIED
                || outcome.getKind() == ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_SUCCESS
                || outcome.getKind() == ProcessorOutcomeKind.UNRECOGNIZED) {
            throw new IllegalArgumentException(
                    "processor failure requires a non-success outcome kind");
        }
        if (outcome.getSettlementEffect() == SettlementEffect.SETTLEMENT_EFFECT_UNSPECIFIED
                || outcome.getSettlementEffect() == SettlementEffect.UNRECOGNIZED) {
            throw new IllegalArgumentException(
                    "processor outcome requires a settlement effect");
        }
        if (!outcome.hasRetryAdvice()
                || outcome.getRetryAdvice().getStrategy()
                == RetryStrategy.RETRY_STRATEGY_UNSPECIFIED
                || outcome.getRetryAdvice().getStrategy() == RetryStrategy.UNRECOGNIZED) {
            throw new IllegalArgumentException(
                    "processor outcome requires explicit retry advice");
        }
        var advice = outcome.getRetryAdvice();
        if (advice.hasDelay() && (!Durations.isValid(advice.getDelay())
                || Durations.compare(advice.getDelay(),
                com.google.protobuf.Duration.getDefaultInstance()) < 0)) {
            throw new IllegalArgumentException("retry delay must not be negative");
        }
        if (advice.hasMaximumDelay() && (!Durations.isValid(advice.getMaximumDelay())
                || Durations.compare(advice.getMaximumDelay(),
                com.google.protobuf.Duration.getDefaultInstance()) < 0)) {
            throw new IllegalArgumentException("retry maximum_delay must not be negative");
        }
        switch (advice.getStrategy()) {
            case RETRY_STRATEGY_NONE -> {
                if (outcome.getKind()
                        == ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_RETRYABLE) {
                    throw new IllegalArgumentException(
                            "retryable outcome requires a retry strategy");
                }
            }
            case RETRY_STRATEGY_FIXED_DELAY, RETRY_STRATEGY_EXPONENTIAL_BACKOFF -> {
                if (!advice.hasDelay()) {
                    throw new IllegalArgumentException(
                            "delay-based retry strategy requires delay");
                }
            }
            case RETRY_STRATEGY_RETRY_AFTER -> {
                if (!advice.hasRetryAfter()) {
                    throw new IllegalArgumentException(
                            "retry-after strategy requires retry_after");
                }
                instant(advice.getRetryAfter());
            }
            case RETRY_STRATEGY_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException(
                    "processor outcome requires explicit retry advice");
        }
    }

    static void workerId(String workerId) {
        if (!WORKER.matcher(workerId).matches()) {
            throw new IllegalArgumentException("worker_id is not a slug: " + workerId);
        }
    }

    static boolean supports(
            Collection<ProcessorContract> contracts, ProcessorContract work) {
        return contracts.stream().anyMatch(candidate -> sameContract(candidate, work));
    }

    static boolean sameContract(ProcessorContract first, ProcessorContract second) {
        return ProcessorContracts.exactMatch(first, second);
    }

    static java.time.Instant instant(Timestamp timestamp) {
        timestamp(timestamp, "timestamp");
        return java.time.Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    static Timestamp timestamp(java.time.Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    static java.time.Duration duration(com.google.protobuf.Duration duration) {
        Objects.requireNonNull(duration, "duration");
        if (!com.google.protobuf.util.Durations.isValid(duration)) {
            throw new IllegalArgumentException("duration must be a valid protobuf Duration");
        }
        return java.time.Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
    }

    static void uuid(String value, String field) {
        try {
            UUID.fromString(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(field + " must be a UUID: " + value, e);
        }
    }

    static void annotations(Message message) {
        var result = ProtoValidator.forMessageType(message.getDescriptorForType())
                .validate(message);
        if (!result.valid()) {
            throw new IllegalArgumentException(
                    message.getDescriptorForType().getFullName()
                            + " violates its descriptor rules: " + result.violations());
        }
    }

    private static void timestamp(Timestamp timestamp, String field) {
        if (!Timestamps.isValid(timestamp)) {
            throw new IllegalArgumentException(field + " must be a valid protobuf Timestamp");
        }
    }

    private static Set<DescriptorIdentity> identities(List<SchemaReference> schemas) {
        Set<DescriptorIdentity> result = new HashSet<>();
        schemas.forEach(schema -> result.add(RuntimeSchemas.identity(schema)));
        return result;
    }
}
