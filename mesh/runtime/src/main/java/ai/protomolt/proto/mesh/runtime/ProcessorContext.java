package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;

import java.time.Instant;
import java.util.Objects;

/** Immutable identity and deadline for one processor invocation. */
public record ProcessorContext(
        String runId,
        String nodeId,
        String invocationId,
        long invocationOrdinal,
        Instant deadline,
        ProcessorCancellation cancellation,
        WorkRecoveryIdentity recoveryIdentity) {

    public ProcessorContext(
            String runId,
            String nodeId,
            String invocationId,
            long invocationOrdinal,
            Instant deadline) {
        this(runId, nodeId, invocationId, invocationOrdinal, deadline,
                ProcessorCancellation.none(), WorkRecoveryIdentity.empty());
    }

    public ProcessorContext(
            String runId,
            String nodeId,
            String invocationId,
            long invocationOrdinal,
            Instant deadline,
            ProcessorCancellation cancellation) {
        this(runId, nodeId, invocationId, invocationOrdinal, deadline,
                cancellation, WorkRecoveryIdentity.empty());
    }

    public ProcessorContext {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(invocationId, "invocationId");
        Objects.requireNonNull(deadline, "deadline");
        Objects.requireNonNull(cancellation, "cancellation");
        Objects.requireNonNull(recoveryIdentity, "recoveryIdentity");
        if (runId.isBlank() || nodeId.isBlank() || invocationId.isBlank()) {
            throw new IllegalArgumentException("processor context ids must not be blank");
        }
        if (invocationOrdinal < 1) {
            throw new IllegalArgumentException("invocationOrdinal must be positive");
        }
    }

    /** Exact published and frontier identities needed by a dead-letter replay. */
    public record WorkRecoveryIdentity(
            String workflowName,
            String workflowVersion,
            String planFingerprint,
            long deploymentRevision,
            String edgeId,
            String channelPolicyId,
            long sourceHistorySequence,
            String namespace,
            String retentionPolicyReference,
            String legalHoldPolicyReference,
            String payloadStoreProfile,
            ChannelPolicy channelPolicy,
            ChannelPolicy durableSpillPolicy) {
        public WorkRecoveryIdentity {
            workflowName = value(workflowName);
            workflowVersion = value(workflowVersion);
            planFingerprint = value(planFingerprint);
            edgeId = value(edgeId);
            channelPolicyId = value(channelPolicyId);
            namespace = value(namespace);
            retentionPolicyReference = value(retentionPolicyReference);
            legalHoldPolicyReference = value(legalHoldPolicyReference);
            payloadStoreProfile = value(payloadStoreProfile);
            channelPolicy = Objects.requireNonNull(channelPolicy, "channelPolicy");
            durableSpillPolicy = Objects.requireNonNull(
                    durableSpillPolicy, "durableSpillPolicy");
        }

        static WorkRecoveryIdentity empty() {
            return new WorkRecoveryIdentity("", "", "", 0,
                    "", "", 0, "", "", "", "",
                    ChannelPolicy.getDefaultInstance(), ChannelPolicy.getDefaultInstance());
        }

        private static String value(String value) {
            return value == null ? "" : value;
        }
    }
}
