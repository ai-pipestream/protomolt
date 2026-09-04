package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.mesh.MeshDigest;
import ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity;
import ai.protomolt.proto.mesh.runtime.v1.PayloadMetadata;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Bounded payload store used by conformance tests and non-durable deployments. */
public final class InMemoryPayloadStore implements PayloadStore {

    private final Clock clock;
    private final int maxItems;
    private final long maxBytes;
    private final Map<Key, State> payloads = new LinkedHashMap<>();
    private long bytes;

    public InMemoryPayloadStore(Clock clock, int maxItems, long maxBytes) {
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxItems < 1 || maxBytes < 1) {
            throw new IllegalArgumentException("payload-store-bounds-invalid");
        }
        this.maxItems = maxItems;
        this.maxBytes = maxBytes;
    }

    @Override
    public synchronized PayloadMetadata put(Put request) {
        byte[] body = request.bytes();
        String digest = MeshDigest.sha256(body);
        if (!request.expectedSha256().isBlank()
                && !request.expectedSha256().equals(digest)) {
            throw new IllegalArgumentException("payload-digest-mismatch: put bytes");
        }
        PayloadIdentity identity = PayloadIdentity.newBuilder()
                .setNamespace(request.namespace())
                .setProfile(request.profile())
                .setArtifact(ArtifactReference.newBuilder()
                        .setSha256(digest)
                        .setMediaType("application/x-protobuf")
                        .setSizeBytes(body.length))
                .setPayloadTypeName(request.payloadTypeName())
                .setDescriptorFingerprint(request.descriptorFingerprint())
                .build();
        Key key = Key.of(identity);
        State existing = payloads.get(key);
        if (existing != null) {
            if (!existing.identity.equals(identity) || !Arrays.equals(existing.bytes, body)) {
                throw new IllegalArgumentException(
                        "payload-content-address-conflict: digest names different metadata or bytes");
            }
            return existing.metadata();
        }
        if (payloads.size() >= maxItems || bytes + body.length > maxBytes) {
            throw new IllegalStateException("payload-store-full: item or byte bound exceeded");
        }
        State created = new State(identity, body, clock.instant());
        payloads.put(key, created);
        bytes += body.length;
        return created.metadata();
    }

    @Override
    public synchronized byte[] get(PayloadIdentity identity, long offset, int length) {
        State state = require(identity);
        if (state.purged) {
            throw new IllegalArgumentException("payload-missing: payload was purged");
        }
        if (offset < 0 || length < 0 || length > MAX_RANGE_BYTES
                || offset > state.bytes.length
                || offset + length > state.bytes.length) {
            throw new IllegalArgumentException("payload-range-invalid");
        }
        return Arrays.copyOfRange(state.bytes, Math.toIntExact(offset),
                Math.toIntExact(offset + length));
    }

    @Override
    public synchronized PayloadMetadata head(PayloadIdentity identity) {
        return require(identity).metadata();
    }

    @Override
    public synchronized PayloadMetadata acquire(
            PayloadIdentity identity, String ownerId, String leaseId, Instant expiresAt) {
        State state = require(identity);
        validateLease(ownerId, leaseId, expiresAt, clock.instant());
        if (state.purged) {
            throw new IllegalStateException("payload-purged: cannot acquire lease");
        }
        Lease lease = new Lease(ownerId, expiresAt);
        Lease prior = state.leases.putIfAbsent(leaseId, lease);
        if (prior != null && !prior.equals(lease)) {
            throw new IllegalArgumentException("payload-lease-conflict: lease_id bytes differ");
        }
        return state.metadata();
    }

    @Override
    public synchronized PayloadMetadata release(
            PayloadIdentity identity, String ownerId, String leaseId) {
        State state = require(identity);
        Lease current = state.leases.get(leaseId);
        if (current == null) {
            return state.metadata();
        }
        if (!current.ownerId.equals(ownerId)) {
            throw new IllegalArgumentException("payload-lease-fence: owner does not match");
        }
        state.leases.remove(leaseId);
        return state.metadata();
    }

    @Override
    public synchronized PayloadMetadata markEligible(
            PayloadIdentity identity, Instant notBefore,
            String retentionPolicyReference, String legalHoldPolicyReference) {
        State state = require(identity);
        state.eligibleAt = Objects.requireNonNull(notBefore, "notBefore");
        state.retentionPolicy = bounded(retentionPolicyReference, 128);
        state.legalHoldPolicy = bounded(legalHoldPolicyReference, 128);
        return state.metadata();
    }

    @Override
    public synchronized PayloadMetadata hold(PayloadIdentity identity, boolean held) {
        State state = require(identity);
        state.hold = held;
        return state.metadata();
    }

    @Override
    public synchronized PayloadMetadata purge(
            PayloadIdentity identity, String reason, Instant now) {
        State state = require(identity);
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("payload-purge-reason-required");
        }
        expireLeases(state, now);
        if (state.hold) {
            throw new IllegalStateException("payload-retention-hold: purge refused");
        }
        if (state.eligibleAt == null || now.isBefore(state.eligibleAt)) {
            throw new IllegalStateException("payload-not-eligible: purge refused");
        }
        if (!state.leases.isEmpty()) {
            throw new IllegalStateException("payload-live-descendants: purge refused");
        }
        if (!state.purged) {
            bytes -= state.bytes.length;
            state.bytes = new byte[0];
            state.purged = true;
        }
        return state.metadata();
    }

    private State require(PayloadIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        State state = payloads.get(Key.of(identity));
        if (state == null || !state.identity.equals(identity)) {
            throw new IllegalArgumentException("payload-missing: exact identity not found");
        }
        expireLeases(state, clock.instant());
        return state;
    }

    private static void validateLease(
            String ownerId, String leaseId, Instant expiresAt, Instant now) {
        if (ownerId == null || ownerId.isBlank() || ownerId.length() > 256) {
            throw new IllegalArgumentException("payload-lease-owner-invalid");
        }
        RemoteValidation.uuid(leaseId, "lease_id");
        Objects.requireNonNull(expiresAt, "expiresAt");
        if (!expiresAt.isAfter(now)) {
            throw new IllegalArgumentException("payload-lease-expiry-invalid");
        }
    }

    private static void expireLeases(State state, Instant now) {
        state.leases.values().removeIf(lease -> !lease.expiresAt.isAfter(now));
    }

    private static String bounded(String value, int limit) {
        String normalized = value == null ? "" : value;
        if (normalized.length() > limit) {
            throw new IllegalArgumentException("payload policy reference is too long");
        }
        return normalized;
    }

    private record Key(String namespace, String profile, String digest) {
        private static Key of(PayloadIdentity identity) {
            return new Key(identity.getNamespace(), identity.getProfile(),
                    identity.getArtifact().getSha256());
        }
    }

    private record Lease(String ownerId, Instant expiresAt) {
    }

    private static final class State {
        private final PayloadIdentity identity;
        private byte[] bytes;
        private final Instant createdAt;
        private final Map<String, Lease> leases = new LinkedHashMap<>();
        private Instant eligibleAt;
        private String retentionPolicy = "";
        private String legalHoldPolicy = "";
        private boolean hold;
        private boolean purged;

        private State(PayloadIdentity identity, byte[] bytes, Instant createdAt) {
            this.identity = identity;
            this.bytes = bytes.clone();
            this.createdAt = createdAt;
        }

        private PayloadMetadata metadata() {
            PayloadMetadata.Builder result = PayloadMetadata.newBuilder()
                    .setIdentity(identity)
                    .setCreatedAt(RemoteValidation.timestamp(createdAt))
                    .setEligibleForDeletion(eligibleAt != null)
                    .setRetentionHold(hold)
                    .setActiveLeases(leases.size())
                    .setPurged(purged);
            if (eligibleAt != null) {
                result.setEligibleAt(RemoteValidation.timestamp(eligibleAt));
            }
            return result.build();
        }
    }
}
