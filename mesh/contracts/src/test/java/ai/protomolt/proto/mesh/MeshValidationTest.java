package ai.protomolt.proto.mesh;

import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import ai.protomolt.proto.mesh.v1.EntityState;
import ai.protomolt.proto.mesh.v1.EntityStatus;
import ai.protomolt.proto.mesh.v1.TerminalState;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the contract gate fails fast: an invalid entity is rejected by the validation entry
 * point, before any routing, persistence, or processor execution could act on it. The byte-level
 * checks (digest, length, expiry) live here because annotations cannot express them.
 */
class MeshValidationTest {

    private static final Instant BEFORE_DEADLINE = Instant.ofEpochSecond(1_700_000_300L);
    private static final Instant AFTER_DEADLINE = Instant.ofEpochSecond(1_700_000_900L);

    @Test
    void aValidInlineEntityPassesTheGate() {
        assertThatCode(() -> MeshValidation.validate(
                MeshFixtures.inlineEntity().build(), BEFORE_DEADLINE))
                .doesNotThrowAnyException();
    }

    @Test
    void aValidClaimCheckEntityPassesTheGate() {
        assertThatCode(() -> MeshValidation.validate(
                MeshFixtures.claimCheckEntity().build(), BEFORE_DEADLINE))
                .doesNotThrowAnyException();
    }

    @Test
    void aPayloadDigestMismatchFailsFast() {
        Any payload = MeshFixtures.payload();
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setHeader(MeshFixtures.header(payload.getValue().toByteArray())
                        .setPayloadDigest("0".repeat(64)))
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload_digest");
    }

    @Test
    void aPayloadLengthMismatchFailsFast() {
        Any payload = MeshFixtures.payload();
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setHeader(MeshFixtures.header(payload.getValue().toByteArray())
                        .setPayloadLength(payload.getValue().size() + 1))
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload_length");
    }

    @Test
    void aClaimCheckArtifactDigestMismatchFailsFast() {
        EntityEnvelope entity = MeshFixtures.claimCheckEntity()
                .setHeader(MeshFixtures.claimCheckEntity().getHeader().toBuilder()
                        .setPayloadDigest("f".repeat(64)))
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("claim_check.artifact.sha256");
    }

    @Test
    void anExpiredDeadlineFailsFastAtTheBoundary() {
        EntityEnvelope entity = MeshFixtures.inlineEntity().build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, AFTER_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadline");
    }

    @Test
    void anInvalidTypeUrlFailsFast() {
        Any wrongType = Any.pack(Timestamp.newBuilder().setSeconds(1).build());
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setPayload(wrongType)
                .setHeader(MeshFixtures.header(wrongType.getValue().toByteArray()))
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload.type_url");
    }

    @Test
    void aTypeUrlWithoutASlashFailsFast() {
        byte[] bytes = MeshFixtures.payload().getValue().toByteArray();
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setPayload(Any.newBuilder()
                        .setTypeUrl("badurl")
                        .setValue(MeshFixtures.payload().getValue()))
                .setHeader(MeshFixtures.header(bytes))
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain a slash");
    }

    @Test
    void aLeadingSlashTypeUrlIsAccepted() {
        // An empty host is a valid URI reference; only the final segment matters.
        byte[] bytes = MeshFixtures.payload().getValue().toByteArray();
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setPayload(Any.newBuilder()
                        .setTypeUrl("/" + MeshFixtures.TYPE_NAME)
                        .setValue(MeshFixtures.payload().getValue()))
                .setHeader(MeshFixtures.header(bytes))
                .build();
        assertThatCode(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .doesNotThrowAnyException();
    }

    @Test
    void aTypeUrlWithAnEmptyTypeNameFailsFast() {
        byte[] bytes = MeshFixtures.payload().getValue().toByteArray();
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setPayload(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/")
                        .setValue(MeshFixtures.payload().getValue()))
                .setHeader(MeshFixtures.header(bytes))
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must end with the fully qualified type name");
    }

    @Test
    void anEntityWithBothBodiesIsRejectedBeforeAnyBoundary() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setClaimCheck(MeshFixtures.claimCheckEntity().getClaimCheck())
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly-one-body");
    }

    @Test
    void anInvalidSchemaIdentityIsRejectedBeforeAnyBoundary() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setSchema(MeshFixtures.schema().toBuilder()
                        .setDescriptorFingerprint("b".repeat(64)))
                .build();
        // Structurally valid annotations, but the fingerprint names a different schema:
        // the resolver is the gate that must refuse it.
        MeshValidation.validate(entity, BEFORE_DEADLINE);
        SchemaIdentityResolver resolver = new SchemaIdentityResolver(
                ai.protomolt.proto.descriptors.DescriptorRegistry.builder()
                        .registerFromMessage(MeshFixtures.document())
                        .build());
        assertThatThrownBy(() -> resolver.resolve(entity))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("descriptor_fingerprint mismatch");
    }

    @Test
    void aTerminalStatusRequiresTerminalFields() {
        EntityStatus status = EntityStatus.newBuilder()
                .setState(EntityState.ENTITY_STATE_TERMINAL)
                .setUpdatedAt(MeshFixtures.CREATED)
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(status))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("terminal_state");
    }

    @Test
    void anActiveStatusForbidsTerminalFields() {
        EntityStatus status = EntityStatus.newBuilder()
                .setState(EntityState.ENTITY_STATE_PROCESSING)
                .setProcessorId("nlp-1")
                .setTerminalState(TerminalState.TERMINAL_STATE_COMPLETED)
                .setUpdatedAt(MeshFixtures.CREATED)
                .build();
        assertThatThrownBy(() -> MeshValidation.validate(status))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("terminal_state");
    }

    @Test
    void aWellFormedTerminalStatusPasses() {
        EntityStatus status = EntityStatus.newBuilder()
                .setState(EntityState.ENTITY_STATE_TERMINAL)
                .setProcessorId("nlp-1")
                .setTerminalState(TerminalState.TERMINAL_STATE_COMPLETED)
                .setTerminalAt(MeshFixtures.DEADLINE)
                .setUpdatedAt(MeshFixtures.DEADLINE)
                .build();
        assertThatCode(() -> MeshValidation.validate(status)).doesNotThrowAnyException();
        assertThat(status.getTerminalState()).isEqualTo(TerminalState.TERMINAL_STATE_COMPLETED);
    }
}
