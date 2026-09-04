package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity;
import ai.protomolt.proto.mesh.runtime.v1.PayloadMetadata;

import java.time.Instant;

/** Namespace-scoped immutable protobuf-byte storage with fenced retention leases. */
public interface PayloadStore {

    int MAX_RANGE_BYTES = 16 * 1024 * 1024;

    PayloadMetadata put(Put request);

    byte[] get(PayloadIdentity identity, long offset, int length);

    PayloadMetadata head(PayloadIdentity identity);

    PayloadMetadata acquire(
            PayloadIdentity identity, String ownerId, String leaseId, Instant expiresAt);

    PayloadMetadata release(PayloadIdentity identity, String ownerId, String leaseId);

    PayloadMetadata markEligible(
            PayloadIdentity identity,
            Instant notBefore,
            String retentionPolicyReference,
            String legalHoldPolicyReference);

    PayloadMetadata hold(PayloadIdentity identity, boolean held);

    PayloadMetadata purge(PayloadIdentity identity, String reason, Instant now);

    record Put(
            String namespace,
            String profile,
            String payloadTypeName,
            String descriptorFingerprint,
            byte[] bytes,
            String expectedSha256) {
        public Put {
            if (namespace == null || namespace.isBlank() || namespace.length() > 256) {
                throw new IllegalArgumentException("payload-namespace-invalid");
            }
            if (profile == null || profile.isBlank() || profile.length() > 128) {
                throw new IllegalArgumentException("payload-profile-invalid");
            }
            if (payloadTypeName == null || payloadTypeName.isBlank()
                    || payloadTypeName.length() > 512) {
                throw new IllegalArgumentException("payload-type-name-invalid");
            }
            if (descriptorFingerprint == null
                    || !descriptorFingerprint.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("payload-descriptor-fingerprint-invalid");
            }
            bytes = bytes.clone();
            if (expectedSha256 == null) {
                expectedSha256 = "";
            }
            if (!expectedSha256.isEmpty() && !expectedSha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("payload-expected-digest-invalid");
            }
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }
}
