package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.mesh.MeshDigest;
import ai.protomolt.proto.mesh.MeshValidation;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import ai.protomolt.proto.mesh.v1.EntityHeader;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Constructs digest-correct inline mesh envelopes without changing protobuf bytes. */
public final class EntityEnvelopes {

    private EntityEnvelopes() {
    }

    /** Builds a root entity for an in-process flow submission. */
    public static EntityEnvelope root(
            String entityId,
            String scopeId,
            Message message,
            Instant createdAt,
            Instant deadline,
            CompletionPolicy completionPolicy) {
        Objects.requireNonNull(message, "message");
        TypedPayload payload = RuntimeSchemas.pack(message);
        byte[] bytes = payload.getPayload().getValue().toByteArray();
        EntityHeader.Builder header = EntityHeader.newBuilder()
                .setEntityId(entityId)
                .setScopeId(scopeId)
                .setScopeDepth(0)
                .setContentType("application/x-protobuf")
                .setPayloadLength(bytes.length)
                .setPayloadDigest(MeshDigest.sha256(bytes))
                .setCreatedAt(timestamp(createdAt))
                .setCompletionPolicy(completionPolicy);
        if (deadline != null) {
            header.setDeadline(timestamp(deadline));
        }
        EntityEnvelope result = EntityEnvelope.newBuilder()
                .setHeader(header)
                .setSchema(payload.getSchema())
                .setPayload(payload.getPayload())
                .build();
        MeshValidation.validateStructure(result);
        return result;
    }

    static EntityEnvelope child(
            String runId,
            String invocationId,
            int outputOrdinal,
            EntityEnvelope parent,
            TypedPayload typed,
            Instant createdAt) {
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(typed, "typed");
        if (parent.getHeader().getScopeDepth() >= 64) {
            throw new IllegalArgumentException("cannot emit a child beyond scope depth 64");
        }
        byte[] bytes = typed.getPayload().getValue().toByteArray();
        String entityId = stableUuid(runId + '\0' + invocationId + '\0' + outputOrdinal
                + '\0' + typed.getSchema().getTypeName() + '\0'
                + MeshDigest.sha256(bytes));
        EntityHeader parentHeader = parent.getHeader();
        EntityHeader.Builder header = EntityHeader.newBuilder()
                .setEntityId(entityId)
                .setParentEntityId(parentHeader.getEntityId())
                .setScopeId(parentHeader.getScopeId())
                .setScopeDepth(parentHeader.getScopeDepth() + 1)
                .setDataLayer(parentHeader.getDataLayer())
                .setContentType("application/x-protobuf")
                .setPayloadLength(bytes.length)
                .setPayloadDigest(MeshDigest.sha256(bytes))
                .setCreatedAt(timestamp(createdAt))
                .setCompletionPolicy(parentHeader.getCompletionPolicy())
                .setSecurityPostureDigest(parentHeader.getSecurityPostureDigest())
                .setTraceId(parentHeader.getTraceId())
                .setEvidenceCorrelationId(parentHeader.getEvidenceCorrelationId());
        if (parentHeader.hasDeadline()) {
            header.setDeadline(parentHeader.getDeadline());
        }
        if (parentHeader.hasRouteProfile()) {
            header.setRouteProfile(parentHeader.getRouteProfile());
        }
        if (parentHeader.hasProcessingProfile()) {
            header.setProcessingProfile(parentHeader.getProcessingProfile());
        }
        if (parentHeader.hasSecurityPolicy()) {
            header.setSecurityPolicy(parentHeader.getSecurityPolicy());
        }
        EntityEnvelope result = EntityEnvelope.newBuilder()
                .setHeader(header)
                .setSchema(typed.getSchema())
                .setPayload(typed.getPayload())
                .build();
        MeshValidation.validateStructure(result);
        return result;
    }

    /** Re-admits exact payload bytes as a new replay root with a fresh deadline. */
    static EntityEnvelope replayRoot(
            String runId,
            long frontierSequence,
            EntityEnvelope source,
            Instant createdAt,
            Instant deadline) {
        Objects.requireNonNull(source, "source");
        EntityHeader original = source.getHeader();
        EntityHeader.Builder header = EntityHeader.newBuilder()
                .setEntityId(stableUuid("replay\0" + runId + '\0' + frontierSequence
                        + '\0' + original.getEntityId()))
                .setScopeId(stableUuid("replay-scope\0" + runId))
                .setScopeDepth(0)
                .setDataLayer(original.getDataLayer())
                .setContentType(original.getContentType())
                .setPayloadLength(original.getPayloadLength())
                .setPayloadDigest(original.getPayloadDigest())
                .setCreatedAt(timestamp(createdAt))
                .setDeadline(timestamp(deadline))
                .setCompletionPolicy(original.getCompletionPolicy())
                .setSecurityPostureDigest(original.getSecurityPostureDigest())
                .setTraceId(original.getTraceId())
                .setEvidenceCorrelationId(original.getEvidenceCorrelationId());
        if (original.hasRouteProfile()) {
            header.setRouteProfile(original.getRouteProfile());
        }
        if (original.hasProcessingProfile()) {
            header.setProcessingProfile(original.getProcessingProfile());
        }
        if (original.hasSecurityPolicy()) {
            header.setSecurityPolicy(original.getSecurityPolicy());
        }
        EntityEnvelope.Builder replay = EntityEnvelope.newBuilder()
                .setHeader(header)
                .setSchema(source.getSchema());
        if (source.hasPayload()) {
            replay.setPayload(source.getPayload());
        }
        if (source.hasClaimCheck()) {
            replay.setClaimCheck(source.getClaimCheck());
        }
        EntityEnvelope result = replay.build();
        MeshValidation.validateStructure(result);
        return result;
    }

    /** Exact schema reference for a message descriptor. */
    public static SchemaReference schemaOf(Message message) {
        DescriptorIdentity identity = DescriptorIdentity.of(message.getDescriptorForType());
        return SchemaReference.newBuilder()
                .setTypeName(identity.typeName())
                .setDescriptorFingerprint(identity.fingerprint())
                .build();
    }

    private static Timestamp timestamp(Instant instant) {
        Objects.requireNonNull(instant, "instant");
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    static String stableUuid(String material) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(material.getBytes(StandardCharsets.UTF_8));
            ByteBuffer bytes = ByteBuffer.wrap(digest);
            long most = bytes.getLong();
            long least = bytes.getLong();
            most = (most & 0xffffffffffff0fffl) | 0x0000000000005000L;
            least = (least & 0x3fffffffffffffffL) | 0x8000000000000000L;
            return new UUID(most, least).toString();
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("JDK does not provide SHA-256", e);
        }
    }
}
