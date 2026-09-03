package ai.protomolt.proto.mesh;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import ai.protomolt.proto.mesh.test.v1.TestDocument;
import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the resolver acceptance criterion: an {@code Any} type plus a canonical descriptor
 * fingerprint identify exactly one message definition, and every disagreement is rejected with
 * both identities named.
 */
class SchemaIdentityResolverTest {

    private static SchemaIdentityResolver resolver() {
        return new SchemaIdentityResolver(DescriptorRegistry.builder()
                .registerFromMessage(MeshFixtures.document())
                .build());
    }

    @Test
    void anInlineEntityResolvesToTheExactMessageDefinition() {
        Descriptor resolved = resolver().resolve(MeshFixtures.inlineEntity().build());
        assertThat(resolved).isEqualTo(TestDocument.getDescriptor());
    }

    @Test
    void aClaimCheckEntityResolvesToTheExactMessageDefinition() {
        Descriptor resolved = resolver().resolve(MeshFixtures.claimCheckEntity().build());
        assertThat(resolved).isEqualTo(TestDocument.getDescriptor());
    }

    @Test
    void aDriftedFingerprintIsRejectedWithBothFingerprintsNamed() {
        String wrong = "c".repeat(64);
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setSchema(MeshFixtures.schema().toBuilder().setDescriptorFingerprint(wrong))
                .build();
        assertThatThrownBy(() -> resolver().resolve(entity))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("descriptor_fingerprint mismatch")
                .hasMessageContaining(wrong)
                .hasMessageContaining(MeshDigest.fingerprintOf(TestDocument.getDescriptor()));
    }

    @Test
    void anUnknownTypeIsRejected() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setSchema(MeshFixtures.schema().toBuilder()
                        .setTypeName("ai.pipestream.proto.mesh.test.v1.NoSuchType"))
                .build();
        assertThatThrownBy(() -> resolver().resolve(entity.getSchema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a registered type");
    }

    @Test
    void aBodyThatDisagreesWithTheSchemaIsRejected() {
        EntityEnvelope entity = MeshFixtures.claimCheckEntity()
                .setClaimCheck(MeshFixtures.claimCheckEntity().getClaimCheck().toBuilder()
                        .setPayloadTypeName("ai.pipestream.proto.mesh.test.v1.TestResult")
                        .setDescriptorFingerprint(
                                MeshFixtures.schema().getDescriptorFingerprint()))
                .build();
        // The envelope annotations already reject this; the resolver stays a second gate.
        assertThatThrownBy(() -> resolver().resolve(entity))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("claim-check-names-schema");
    }

    @Test
    void resolvingRequiresARegistryThatKnowsTheType() {
        SchemaIdentityResolver empty = new SchemaIdentityResolver(DescriptorRegistry.create());
        assertThatThrownBy(() -> empty.resolve(MeshFixtures.schema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a registered type");
    }
}
