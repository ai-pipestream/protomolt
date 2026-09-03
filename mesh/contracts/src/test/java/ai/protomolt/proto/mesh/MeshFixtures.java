package ai.protomolt.proto.mesh;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.mesh.v1.ClaimCheck;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import ai.protomolt.proto.mesh.v1.EntityHeader;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import ai.protomolt.proto.mesh.test.v1.TestDocument;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;

/** Builders for mesh contract test entities with every invariant already satisfied. */
final class MeshFixtures {

    static final String ENTITY_ID = "0f4a6a1e-3a21-4c9a-9d2a-4f3c0d2a7a01";
    static final String PARENT_ID = "1b5b7c2f-4b32-5dab-ae3b-5a4d1e3b8b12";
    static final String SCOPE_ID = "2c6c8d3a-5c43-6ebc-bf4c-6b5e2f4c9c23";
    static final Timestamp CREATED = Timestamp.newBuilder()
            .setSeconds(1_700_000_000L).build();
    static final Timestamp DEADLINE = Timestamp.newBuilder()
            .setSeconds(1_700_000_600L).build();
    static final String TYPE_NAME = "ai.pipestream.proto.mesh.test.v1.TestDocument";

    private MeshFixtures() {
    }

    /** The schema identity of the fixture TestDocument type. */
    static SchemaReference schema() {
        return SchemaReference.newBuilder()
                .setTypeName(TYPE_NAME)
                .setDescriptorFingerprint(MeshDigest.fingerprintOf(TestDocument.getDescriptor()))
                .build();
    }

    /** The fixture payload. */
    static TestDocument document() {
        return TestDocument.newBuilder()
                .setDocumentId("doc-1")
                .setInstruction("summarize")
                .setBody("the body text")
                .build();
    }

    /** The fixture payload packed as an Any. */
    static Any payload() {
        return Any.pack(document());
    }

    /** A header whose digest and length describe the given payload bytes. */
    static EntityHeader.Builder header(byte[] payloadBytes) {
        return EntityHeader.newBuilder()
                .setEntityId(ENTITY_ID)
                .setScopeId(SCOPE_ID)
                .setScopeDepth(0)
                .setPayloadLength(payloadBytes.length)
                .setPayloadDigest(MeshDigest.sha256(payloadBytes))
                .setCreatedAt(CREATED)
                .setDeadline(DEADLINE)
                .setCompletionPolicy(CompletionPolicy.COMPLETION_POLICY_STRICT);
    }

    /** A valid inline-payload entity. */
    static EntityEnvelope.Builder inlineEntity() {
        Any payload = payload();
        return EntityEnvelope.newBuilder()
                .setHeader(header(payload.getValue().toByteArray()))
                .setSchema(schema())
                .setPayload(payload);
    }

    /** A valid claim-check entity over the same fixture bytes. */
    static EntityEnvelope.Builder claimCheckEntity() {
        byte[] stored = payload().getValue().toByteArray();
        return EntityEnvelope.newBuilder()
                .setHeader(header(stored))
                .setSchema(schema())
                .setClaimCheck(ClaimCheck.newBuilder()
                        .setArtifact(ArtifactReference.newBuilder()
                                .setSha256(MeshDigest.sha256(stored))
                                .setMediaType("application/x-protobuf")
                                .setSizeBytes(stored.length)
                                .build())
                        .setPayloadTypeName(TYPE_NAME)
                        .setDescriptorFingerprint(
                                MeshDigest.fingerprintOf(TestDocument.getDescriptor())));
    }

    /** A valid child entity: parent set, depth 1. */
    static EntityEnvelope.Builder childEntity() {
        Any payload = payload();
        return EntityEnvelope.newBuilder()
                .setHeader(header(payload.getValue().toByteArray())
                        .setParentEntityId(PARENT_ID)
                        .setScopeDepth(1))
                .setSchema(schema())
                .setPayload(payload);
    }
}
