package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.mesh.MeshDigest;
import ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLedgerEventKind;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLedgerRecord;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLeaseLedgerEntry;
import ai.protomolt.proto.mesh.runtime.v1.PayloadMetadata;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Payload-store adapter over the repository service's existing loose blobs. */
public final class RepositoryPayloadStore implements PayloadStore, AutoCloseable {

    private static final byte[] MAGIC = {'P', 'M', 'P', 'L', '0', '0', '0', '1'};
    private static final int MAX_LEDGER_RECORD = 16 * 1024 * 1024;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub repository;
    private final String driveName;
    private final String objectPrefix;
    private final Clock clock;
    private final FramedProtobufWal<PayloadLedgerRecord> ledger;
    private final Map<Key, State> states = new LinkedHashMap<>();

    public RepositoryPayloadStore(
            DocumentServiceGrpc.DocumentServiceBlockingStub repository,
            String driveName,
            String objectPrefix,
            Path ledgerPath,
            Clock clock) {
        this.repository = Objects.requireNonNull(repository, "repository");
        if (driveName == null || driveName.isBlank()) {
            throw new IllegalArgumentException("repository payload drive is required");
        }
        this.driveName = driveName;
        this.objectPrefix = normalizePrefix(objectPrefix);
        this.clock = Objects.requireNonNull(clock, "clock");
        try {
            this.ledger = new FramedProtobufWal<>(ledgerPath, MAGIC,
                    MAX_LEDGER_RECORD, PayloadLedgerRecord.parser());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open repository payload ledger", e);
        }
        for (PayloadLedgerRecord record : ledger.records()) {
            apply(record);
        }
    }

    @Override
    public synchronized PayloadMetadata put(Put request) {
        byte[] bytes = request.bytes();
        String digest = MeshDigest.sha256(bytes);
        if (!request.expectedSha256().isBlank()
                && !request.expectedSha256().equals(digest)) {
            throw new IllegalArgumentException("payload-digest-mismatch: put bytes");
        }
        PayloadIdentity identity = identity(request, digest, bytes.length);
        State existing = states.get(Key.of(identity));
        if (existing != null) {
            requireIdentity(existing.identity, identity);
            return metadata(existing);
        }
        var stored = repository.putBlob(PutBlobRequest.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(objectKey(identity))
                .setMimeType("application/x-protobuf")
                .setData(ByteString.copyFrom(bytes))
                .build());
        if (!stored.getSha256().equals(digest) || stored.getSizeBytes() != bytes.length) {
            throw new IllegalStateException(
                    "repository-payload-put-verification-failed: landed bytes differ");
        }
        State created = new State(identity, clock.instant());
        append(created, PayloadLedgerEventKind.PAYLOAD_LEDGER_EVENT_KIND_STORED,
                "stored", "", "");
        return metadata(states.get(Key.of(identity)));
    }

    @Override
    public synchronized byte[] get(PayloadIdentity identity, long offset, int length) {
        State state = require(identity);
        if (state.purged) {
            throw new IllegalArgumentException("payload-missing: payload was purged");
        }
        long total = identity.getArtifact().getSizeBytes();
        if (offset < 0 || length < 0 || length > MAX_RANGE_BYTES
                || offset > total || offset + length > total) {
            throw new IllegalArgumentException("payload-range-invalid");
        }
        byte[] bytes = repository.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(reference(identity)).build()).getData().toByteArray();
        if (bytes.length != total) {
            throw new IllegalArgumentException("payload-length-mismatch");
        }
        if (!MeshDigest.sha256(bytes).equals(identity.getArtifact().getSha256())) {
            throw new IllegalArgumentException("payload-digest-mismatch");
        }
        return Arrays.copyOfRange(bytes, Math.toIntExact(offset),
                Math.toIntExact(offset + length));
    }

    @Override
    public synchronized PayloadMetadata head(PayloadIdentity identity) {
        return metadata(require(identity));
    }

    @Override
    public synchronized PayloadMetadata acquire(PayloadIdentity identity,
            String ownerId, String leaseId, Instant expiresAt) {
        State state = require(identity);
        RemoteValidation.uuid(leaseId, "lease_id");
        if (ownerId == null || ownerId.isBlank() || ownerId.length() > 256
                || !expiresAt.isAfter(clock.instant())) {
            throw new IllegalArgumentException("payload-lease-invalid");
        }
        Lease lease = new Lease(ownerId, expiresAt);
        Lease prior = state.leases.get(leaseId);
        if (prior != null) {
            if (!prior.equals(lease)) {
                throw new IllegalArgumentException("payload-lease-conflict");
            }
            return metadata(state);
        }
        State next = state.copy();
        next.leases.put(leaseId, lease);
        append(next, PayloadLedgerEventKind.PAYLOAD_LEDGER_EVENT_KIND_LEASE_ACQUIRED,
                "lease acquired", state.retentionPolicy, state.legalHoldPolicy);
        return metadata(states.get(Key.of(identity)));
    }

    @Override
    public synchronized PayloadMetadata release(
            PayloadIdentity identity, String ownerId, String leaseId) {
        State state = require(identity);
        Lease lease = state.leases.get(leaseId);
        if (lease == null) {
            return metadata(state);
        }
        if (!lease.ownerId.equals(ownerId)) {
            throw new IllegalArgumentException("payload-lease-fence");
        }
        State next = state.copy();
        next.leases.remove(leaseId);
        append(next, PayloadLedgerEventKind.PAYLOAD_LEDGER_EVENT_KIND_LEASE_RELEASED,
                "lease released", state.retentionPolicy, state.legalHoldPolicy);
        return metadata(states.get(Key.of(identity)));
    }

    @Override
    public synchronized PayloadMetadata markEligible(PayloadIdentity identity,
            Instant notBefore, String retentionPolicyReference,
            String legalHoldPolicyReference) {
        State state = require(identity);
        State next = state.copy();
        next.eligibleAt = Objects.requireNonNull(notBefore, "notBefore");
        next.retentionPolicy = bounded(retentionPolicyReference, 128);
        next.legalHoldPolicy = bounded(legalHoldPolicyReference, 128);
        append(next, PayloadLedgerEventKind.PAYLOAD_LEDGER_EVENT_KIND_ELIGIBLE,
                "eligible", next.retentionPolicy, next.legalHoldPolicy);
        return metadata(states.get(Key.of(identity)));
    }

    @Override
    public synchronized PayloadMetadata hold(PayloadIdentity identity, boolean held) {
        State state = require(identity);
        State next = state.copy();
        next.hold = held;
        append(next, PayloadLedgerEventKind.PAYLOAD_LEDGER_EVENT_KIND_HOLD_CHANGED,
                held ? "hold enabled" : "hold released",
                state.retentionPolicy, state.legalHoldPolicy);
        return metadata(states.get(Key.of(identity)));
    }

    @Override
    public synchronized PayloadMetadata purge(
            PayloadIdentity identity, String reason, Instant now) {
        State state = require(identity);
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
            repository.deleteBlob(DeleteBlobRequest.newBuilder()
                    .setStorageRef(reference(identity)).build());
            State next = state.copy();
            next.purged = true;
            append(next, PayloadLedgerEventKind.PAYLOAD_LEDGER_EVENT_KIND_PURGED,
                    bounded(reason, 8192), state.retentionPolicy, state.legalHoldPolicy);
        }
        return metadata(states.get(Key.of(identity)));
    }

    @Override
    public synchronized void close() {
        try {
            ledger.close();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot close repository payload ledger", e);
        }
    }

    private void append(State state, PayloadLedgerEventKind kind, String reason,
            String retention, String holdPolicy) {
        PayloadLedgerRecord.Builder record = PayloadLedgerRecord.newBuilder()
                .setSequence(ledger.records().size() + 1L)
                .setRecordedAt(RemoteValidation.timestamp(clock.instant()))
                .setEventKind(kind)
                .setMetadata(metadata(state))
                .setReason(reason)
                .setRetentionPolicyReference(retention)
                .setLegalHoldPolicyReference(holdPolicy);
        state.leases.forEach((leaseId, lease) -> record.addLeases(
                PayloadLeaseLedgerEntry.newBuilder()
                        .setLeaseId(leaseId)
                        .setOwnerId(lease.ownerId)
                        .setExpiresAt(RemoteValidation.timestamp(lease.expiresAt))));
        try {
            PayloadLedgerRecord built = record.build();
            ledger.append(built);
            apply(built);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot append repository payload ledger", e);
        }
    }

    private void apply(PayloadLedgerRecord record) {
        if (record.getSequence() != statesSequence() + 1L
                || record.getEventKind()
                == PayloadLedgerEventKind.PAYLOAD_LEDGER_EVENT_KIND_UNSPECIFIED) {
            throw new IllegalArgumentException("payload-ledger-transition-invalid");
        }
        PayloadMetadata metadata = record.getMetadata();
        State state = new State(metadata.getIdentity(),
                RemoteValidation.instant(metadata.getCreatedAt()));
        state.eligibleAt = metadata.hasEligibleAt()
                ? RemoteValidation.instant(metadata.getEligibleAt()) : null;
        state.hold = metadata.getRetentionHold();
        state.purged = metadata.getPurged();
        state.retentionPolicy = record.getRetentionPolicyReference();
        state.legalHoldPolicy = record.getLegalHoldPolicyReference();
        record.getLeasesList().forEach(lease -> state.leases.put(lease.getLeaseId(),
                new Lease(lease.getOwnerId(), RemoteValidation.instant(lease.getExpiresAt()))));
        states.put(Key.of(state.identity), state);
        appliedSequence = record.getSequence();
    }

    private long appliedSequence;

    private long statesSequence() {
        return appliedSequence;
    }

    private State require(PayloadIdentity identity) {
        State state = states.get(Key.of(identity));
        if (state == null) {
            throw new IllegalArgumentException("payload-missing: exact identity not found");
        }
        requireIdentity(state.identity, identity);
        expireLeases(state, clock.instant());
        return state;
    }

    private static void requireIdentity(PayloadIdentity actual, PayloadIdentity expected) {
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException("payload-identity-mismatch");
        }
    }

    private static PayloadIdentity identity(Put request, String digest, int size) {
        return PayloadIdentity.newBuilder()
                .setNamespace(request.namespace()).setProfile(request.profile())
                .setArtifact(ArtifactReference.newBuilder()
                        .setSha256(digest).setMediaType("application/x-protobuf")
                        .setSizeBytes(size))
                .setPayloadTypeName(request.payloadTypeName())
                .setDescriptorFingerprint(request.descriptorFingerprint()).build();
    }

    private PayloadMetadata metadata(State state) {
        expireLeases(state, clock.instant());
        PayloadMetadata.Builder result = PayloadMetadata.newBuilder()
                .setIdentity(state.identity)
                .setCreatedAt(RemoteValidation.timestamp(state.createdAt))
                .setEligibleForDeletion(state.eligibleAt != null)
                .setRetentionHold(state.hold)
                .setActiveLeases(state.leases.size())
                .setPurged(state.purged);
        if (state.eligibleAt != null) {
            result.setEligibleAt(RemoteValidation.timestamp(state.eligibleAt));
        }
        return result.build();
    }

    private FileStorageReference reference(PayloadIdentity identity) {
        return FileStorageReference.newBuilder()
                .setDriveName(driveName).setObjectKey(objectKey(identity)).build();
    }

    private String objectKey(PayloadIdentity identity) {
        return objectPrefix + pathComponent(identity.getNamespace()) + "/"
                + pathComponent(identity.getProfile())
                + "/" + identity.getArtifact().getSha256() + ".pb";
    }

    private static String pathComponent(String value) {
        return MeshDigest.sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void expireLeases(State state, Instant now) {
        state.leases.values().removeIf(lease -> !lease.expiresAt.isAfter(now));
    }

    private static String normalizePrefix(String value) {
        String prefix = value == null || value.isBlank() ? "mesh/payloads/" : value;
        return prefix.endsWith("/") ? prefix : prefix + "/";
    }

    private static String bounded(String value, int maximum) {
        String normalized = value == null ? "" : value;
        if (normalized.length() > maximum) {
            throw new IllegalArgumentException("payload ledger value exceeds bound");
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
        private final Instant createdAt;
        private final Map<String, Lease> leases = new LinkedHashMap<>();
        private Instant eligibleAt;
        private String retentionPolicy = "";
        private String legalHoldPolicy = "";
        private boolean hold;
        private boolean purged;

        private State(PayloadIdentity identity, Instant createdAt) {
            this.identity = identity;
            this.createdAt = createdAt;
        }

        private State copy() {
            State copy = new State(identity, createdAt);
            copy.leases.putAll(leases);
            copy.eligibleAt = eligibleAt;
            copy.retentionPolicy = retentionPolicy;
            copy.legalHoldPolicy = legalHoldPolicy;
            copy.hold = hold;
            copy.purged = purged;
            return copy;
        }
    }
}
