package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLeaseFrontier;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/** Externalization and descendant-lease ownership used by the flow runtime. */
public interface PayloadLifecycle {

    PayloadExternalizer.Externalized stage(
            EntityEnvelope envelope,
            ChannelPolicy policy,
            String namespace,
            String ownerId,
            Instant leaseExpiresAt);

    void restore(PayloadLeaseFrontier lease);

    void settle(PayloadLeaseFrontier lease);

    static PayloadLifecycle inlineOnly() {
        return new PayloadLifecycle() {
            @Override
            public PayloadExternalizer.Externalized stage(EntityEnvelope envelope,
                    ChannelPolicy policy, String namespace, String ownerId,
                    Instant leaseExpiresAt) {
                if (envelope.hasClaimCheck()) {
                    throw new IllegalStateException(
                            "claim-check-store-unavailable: payload lease cannot be acquired");
                }
                if (envelope.hasPayload()
                        && envelope.getPayload().getValue().size()
                        > policy.getInlineByteLimit()
                        && !policy.getPayloadStoreProfile().isBlank()) {
                    throw new IllegalStateException(
                            "claim-check-store-unavailable: payload exceeds inline limit");
                }
                return new PayloadExternalizer.Externalized(envelope, null);
            }

            @Override
            public void restore(PayloadLeaseFrontier lease) {
                throw new IllegalStateException(
                        "claim-check-store-unavailable: checkpoint contains payload lease");
            }

            @Override
            public void settle(PayloadLeaseFrontier lease) {
                throw new IllegalStateException(
                        "claim-check-store-unavailable: checkpoint contains payload lease");
            }
        };
    }

    static PayloadLifecycle stored(PayloadStore store, Clock clock) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(clock, "clock");
        PayloadExternalizer externalizer = new PayloadExternalizer(store);
        return new PayloadLifecycle() {
            @Override
            public PayloadExternalizer.Externalized stage(EntityEnvelope envelope,
                    ChannelPolicy policy, String namespace, String ownerId,
                    Instant leaseExpiresAt) {
                return externalizer.externalize(envelope, policy, namespace, ownerId,
                        leaseExpiresAt);
            }

            @Override
            public void restore(PayloadLeaseFrontier lease) {
                store.acquire(lease.getIdentity(), lease.getOwnerId(), lease.getLeaseId(),
                        RemoteValidation.instant(lease.getExpiresAt()));
            }

            @Override
            public void settle(PayloadLeaseFrontier lease) {
                store.release(lease.getIdentity(), lease.getOwnerId(), lease.getLeaseId());
                store.markEligible(lease.getIdentity(), clock.instant(),
                        lease.getRetentionPolicyReference(),
                        lease.getLegalHoldPolicyReference());
            }
        };
    }
}
