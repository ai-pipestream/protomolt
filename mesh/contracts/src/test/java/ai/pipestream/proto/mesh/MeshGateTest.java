package ai.pipestream.proto.mesh;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mesh.v1.EntityEnvelope;
import ai.pipestream.proto.mesh.test.v1.TestDocument;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the combined admission gate catches failures from every layer it composes: structural
 * validation, deadline validation, and schema-identity resolution. A boundary that admits through
 * {@link MeshGate} cannot accidentally run only a subset.
 */
class MeshGateTest {

    private static final Instant BEFORE_DEADLINE = Instant.ofEpochSecond(1_700_000_300L);
    private static final Instant AFTER_DEADLINE = Instant.ofEpochSecond(1_700_000_900L);

    private static MeshGate gate() {
        return new MeshGate(DescriptorRegistry.builder()
                .registerFromMessage(MeshFixtures.document())
                .build());
    }

    @Test
    void aValidEntityAdmitsAndYieldsTheExactDescriptor() {
        assertThat(gate().admit(MeshFixtures.inlineEntity().build(), BEFORE_DEADLINE))
                .isEqualTo(TestDocument.getDescriptor());
        assertThat(gate().admit(MeshFixtures.claimCheckEntity().build(), BEFORE_DEADLINE))
                .isEqualTo(TestDocument.getDescriptor());
    }

    @Test
    void theGateCatchesAStructuralFailure() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                        .setPayloadDigest("0".repeat(64)))
                .build();
        assertThatThrownBy(() -> gate().admit(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload_digest");
    }

    @Test
    void theGateCatchesAnAnnotationFailure() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setClaimCheck(MeshFixtures.claimCheckEntity().getClaimCheck())
                .build();
        assertThatThrownBy(() -> gate().admit(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly-one-body");
    }

    @Test
    void theGateCatchesAnExpiredDeadline() {
        assertThatThrownBy(() -> gate().admit(MeshFixtures.inlineEntity().build(), AFTER_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadline");
    }

    @Test
    void theGateCatchesASchemaFingerprintMismatch() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setSchema(MeshFixtures.schema().toBuilder()
                        .setDescriptorFingerprint("d".repeat(64)))
                .build();
        assertThatThrownBy(() -> gate().admit(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("descriptor_fingerprint mismatch");
    }

    @Test
    void theGateCatchesAnUnregisteredType() {
        MeshGate empty = new MeshGate(DescriptorRegistry.create());
        assertThatThrownBy(() -> empty.admit(MeshFixtures.inlineEntity().build(), BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a registered type");
    }
}
