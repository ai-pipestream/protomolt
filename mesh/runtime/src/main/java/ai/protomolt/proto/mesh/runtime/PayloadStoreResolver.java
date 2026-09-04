package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.MeshDigest;
import ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;

import java.util.Objects;
import java.util.function.Function;

/** Exact claim-check hydration with distinct identity and byte refusals. */
public final class PayloadStoreResolver implements PayloadResolver {

    private final DescriptorRegistry descriptors;
    private final PayloadStore store;
    private final Function<EntityEnvelope, String> namespaces;
    private final String profile;

    public PayloadStoreResolver(
            DescriptorRegistry descriptors,
            PayloadStore store,
            Function<EntityEnvelope, String> namespaces,
            String profile) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.store = Objects.requireNonNull(store, "store");
        this.namespaces = Objects.requireNonNull(namespaces, "namespaces");
        if (profile == null || profile.isBlank()) {
            throw new IllegalArgumentException("payload profile is required");
        }
        this.profile = profile;
    }

    @Override
    public DynamicMessage resolve(EntityEnvelope envelope) {
        if (envelope.hasPayload()) {
            return PayloadResolver.inlineOnly(descriptors).resolve(envelope);
        }
        if (!envelope.hasClaimCheck()) {
            throw new IllegalArgumentException("payload-form-missing");
        }
        var claim = envelope.getClaimCheck();
        if (!claim.getPayloadTypeName().equals(envelope.getSchema().getTypeName())) {
            throw new IllegalArgumentException("payload-type-name-mismatch");
        }
        if (!claim.getDescriptorFingerprint().equals(
                envelope.getSchema().getDescriptorFingerprint())) {
            throw new IllegalArgumentException("payload-descriptor-fingerprint-mismatch");
        }
        PayloadIdentity identity = PayloadIdentity.newBuilder()
                .setNamespace(claim.getPayloadNamespace().isBlank()
                        ? namespaces.apply(envelope) : claim.getPayloadNamespace())
                .setProfile(profile)
                .setArtifact(claim.getArtifact())
                .setPayloadTypeName(claim.getPayloadTypeName())
                .setDescriptorFingerprint(claim.getDescriptorFingerprint())
                .build();
        var metadata = store.head(identity);
        long expectedLength = envelope.getHeader().getPayloadLength();
        if (claim.getArtifact().getSizeBytes() != expectedLength
                || metadata.getIdentity().getArtifact().getSizeBytes() != expectedLength) {
            throw new IllegalArgumentException("payload-length-mismatch");
        }
        if (expectedLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("payload-length-unsupported");
        }
        byte[] bytes = new byte[Math.toIntExact(expectedLength)];
        int offset = 0;
        while (offset < bytes.length) {
            int length = Math.min(PayloadStore.MAX_RANGE_BYTES, bytes.length - offset);
            byte[] chunk = store.get(identity, offset, length);
            if (chunk.length != length) {
                throw new IllegalArgumentException("payload-length-mismatch");
            }
            System.arraycopy(chunk, 0, bytes, offset, length);
            offset += length;
        }
        String digest = MeshDigest.sha256(bytes);
        if (!digest.equals(claim.getArtifact().getSha256())
                || !digest.equals(envelope.getHeader().getPayloadDigest())) {
            throw new IllegalArgumentException("payload-digest-mismatch");
        }
        return RuntimeSchemas.unpack(descriptors, TypedPayload.newBuilder()
                .setSchema(envelope.getSchema())
                .setPayload(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/" + claim.getPayloadTypeName())
                        .setValue(ByteString.copyFrom(bytes)))
                .build());
    }
}
