package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.OutcomeCause;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcome;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcomeKind;
import ai.protomolt.proto.mesh.runtime.v1.RetryAdvice;
import ai.protomolt.proto.mesh.runtime.v1.RetryJitterPolicy;
import ai.protomolt.proto.mesh.runtime.v1.RetryStrategy;
import ai.protomolt.proto.mesh.runtime.v1.SettlementEffect;

import java.util.UUID;

/** Exact typed processor outcomes used by worker, channel, and recovery records. */
public final class ProcessorOutcomes {

    private ProcessorOutcomes() {
    }

    public static ProcessorOutcome retryable(
            String code, String message, String processorId, int attempt, int maximumAttempts) {
        return base(ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_RETRYABLE,
                SettlementEffect.SETTLEMENT_EFFECT_RELEASE,
                code, message, processorId, attempt)
                .setRetryAdvice(RetryAdvice.newBuilder()
                        .setStrategy(RetryStrategy.RETRY_STRATEGY_FIXED_DELAY)
                        .setDelay(com.google.protobuf.Duration.getDefaultInstance())
                        .setMaximumAttempts(maximumAttempts)
                        .setJitterPolicy(RetryJitterPolicy.RETRY_JITTER_POLICY_NONE))
                .build();
    }

    public static ProcessorOutcome permanent(
            String code, String message, String processorId, int attempt) {
        return base(ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_PERMANENT,
                SettlementEffect.SETTLEMENT_EFFECT_DEAD_LETTER,
                code, message, processorId, attempt)
                .setRetryAdvice(RetryAdvice.newBuilder()
                        .setStrategy(RetryStrategy.RETRY_STRATEGY_NONE)
                        .setJitterPolicy(RetryJitterPolicy.RETRY_JITTER_POLICY_NONE))
                .build();
    }

    public static ProcessorOutcome cancelled(
            String message, String processorId, int attempt) {
        return base(ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_CANCELLED,
                SettlementEffect.SETTLEMENT_EFFECT_RELEASE,
                "processor-cancelled", message, processorId, attempt)
                .setRetryAdvice(RetryAdvice.newBuilder()
                        .setStrategy(RetryStrategy.RETRY_STRATEGY_NONE)
                        .setJitterPolicy(RetryJitterPolicy.RETRY_JITTER_POLICY_NONE))
                .build();
    }

    private static ProcessorOutcome.Builder base(
            ProcessorOutcomeKind kind,
            SettlementEffect effect,
            String code,
            String message,
            String processorId,
            int attempt) {
        return ProcessorOutcome.newBuilder()
                .setOutcomeId(UUID.randomUUID().toString())
                .setKind(kind)
                .setSettlementEffect(effect)
                .addCauses(OutcomeCause.newBuilder()
                        .setCode(code)
                        .setMessage(message)
                        .setProcessorId(processorId)
                        .setAttempt(attempt));
    }
}
