package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLeaseFrontier;
import ai.protomolt.proto.mesh.v1.ClaimCheck;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Compiled-policy inline-to-claim-check transition with a pre-acceptance lease. */
public final class PayloadExternalizer {

    private final PayloadStore store;

    public PayloadExternalizer(PayloadStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    public Externalized externalize(
            EntityEnvelope envelope,
            ChannelPolicy policy,
            String namespace,
            String ownerId,
            Instant leaseExpiresAt) {
        Objects.requireNonNull(envelope, "envelope");
        if (envelope.hasClaimCheck()) {
            if (policy.getPayloadStoreProfile().isBlank()) {
                throw new IllegalStateException(
                        "claim-check-profile-missing: channel policy cannot lease payload");
            }
            ClaimCheck claim = envelope.getClaimCheck();
            String payloadNamespace = claim.getPayloadNamespace().isBlank()
                    ? namespace : claim.getPayloadNamespace();
            PayloadIdentity identity = PayloadIdentity.newBuilder()
                    .setNamespace(payloadNamespace)
                    .setProfile(policy.getPayloadStoreProfile())
                    .setArtifact(claim.getArtifact())
                    .setPayloadTypeName(claim.getPayloadTypeName())
                    .setDescriptorFingerprint(claim.getDescriptorFingerprint())
                    .build();
            return new Externalized(envelope,
                    acquire(identity, envelope, policy, ownerId, leaseExpiresAt));
        }
        if (!envelope.hasPayload()
                || policy.getPayloadStoreProfile().isBlank()
                || envelope.getPayload().getValue().size() <= policy.getInlineByteLimit()) {
            return new Externalized(envelope, null);
        }
        byte[] bytes = envelope.getPayload().getValue().toByteArray();
        var metadata = store.put(new PayloadStore.Put(namespace,
                policy.getPayloadStoreProfile(), envelope.getSchema().getTypeName(),
                envelope.getSchema().getDescriptorFingerprint(), bytes,
                envelope.getHeader().getPayloadDigest()));
        EntityEnvelope external = envelope.toBuilder()
                .clearPayload()
                .setClaimCheck(ClaimCheck.newBuilder()
                        .setArtifact(metadata.getIdentity().getArtifact())
                        .setPayloadTypeName(envelope.getSchema().getTypeName())
                        .setDescriptorFingerprint(
                                envelope.getSchema().getDescriptorFingerprint())
                        .setPayloadNamespace(namespace)
                        .setExpiresAt(RemoteValidation.timestamp(leaseExpiresAt)))
                .build();
        PayloadLeaseFrontier lease = acquire(metadata.getIdentity(), envelope, policy,
                ownerId, leaseExpiresAt);
        return new Externalized(external, lease);
    }

    private PayloadLeaseFrontier acquire(
            PayloadIdentity identity,
            EntityEnvelope envelope,
            ChannelPolicy policy,
            String ownerId,
            Instant leaseExpiresAt) {
        String leaseId = EntityEnvelopes.stableUuid("payload-lease\0"
                + identity.getArtifact().getSha256() + '\0' + ownerId);
        store.acquire(identity, ownerId, leaseId, leaseExpiresAt);
        return PayloadLeaseFrontier.newBuilder()
                .setPayloadSha256(identity.getArtifact().getSha256())
                .setLeaseId(leaseId)
                .setOwnerId(ownerId)
                .addAllDescendantMessageIds(List.of(
                        envelope.getHeader().getEntityId()))
                .setNamespace(identity.getNamespace())
                .setProfile(identity.getProfile())
                .setIdentity(identity)
                .setExpiresAt(RemoteValidation.timestamp(leaseExpiresAt))
                .setRetentionPolicyReference(policy.getRetentionPolicyReference())
                .setLegalHoldPolicyReference(policy.getLegalHoldPolicyReference())
                .build();
    }

    public record Externalized(EntityEnvelope envelope, PayloadLeaseFrontier lease) {
    }
}
