package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.DescriptorProtos;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the builtin {@code Document} container closure identity ({@code #335}
 * item 3.10).
 *
 * <p>The container identity is the closure digest: the host compares a
 * revision's retained {@code descriptor_sha256} with this host's compiled
 * closure, so a protoc or protobuf-java upgrade that changes the serialized
 * closure would make every earlier revision fail instead of decoding
 * (requirements section 11, section 15.14 retained container identity).
 * Pinning the digest here makes such an upgrade fail in CI, where the change
 * can be reviewed deliberately, instead of failing in reads.</p>
 *
 * <p>{@link BuiltinDocumentSchema} exposes no {@code asset()} accessor and no
 * numeric schema id. Its equivalent identity is
 * {@code definition().metadata()}: {@code artifact_sha256} is the
 * {@code descriptor_sha256} carried by retained schema references,
 * {@code schema.type_name} and {@code type_url} name the root type, and
 * {@code schema.descriptor_fingerprint} is the canonical (file-name-sorted)
 * closure fingerprint. This test pins all of them.</p>
 */
class BuiltinDocumentSchemaClosurePinTest {

    /**
     * SHA-256 of the serialized descriptor closure bytes
     * ({@code RepositorySchemaAsset.artifact_sha256}, the
     * {@code descriptor_sha256} of retained schema references), as 64
     * lowercase hex characters.
     */
    private static final String PINNED_DESCRIPTOR_SHA256 =
            "50dabaee70843163a0809aaaec00c1f7bb3edfa08e3cf78feb1a0ca845927071";

    /**
     * Canonical descriptor fingerprint of the same closure
     * ({@code PublicationSchemaCondition.descriptor_fingerprint}), as 64
     * lowercase hex characters.
     */
    private static final String PINNED_DESCRIPTOR_FINGERPRINT =
            "20b08edae6a912107a103f23acbb4230b0ea48b50f634f3bc8256baf86ab1d60";

    /** Fully qualified name of the builtin container root message. */
    private static final String EXPECTED_ROOT_TYPE_NAME =
            "ai.protomolt.proto.repo.v1.Document";

    /** Type URL naming {@link #EXPECTED_ROOT_TYPE_NAME} in the asset. */
    private static final String EXPECTED_TYPE_URL =
            "type.googleapis.com/" + EXPECTED_ROOT_TYPE_NAME;

    /**
     * Exact transitive files of the builtin closure, in the
     * dependency-first order produced by
     * {@link DescriptorFingerprints#closure}.
     */
    private static final List<String> EXPECTED_CLOSURE_FILES = List.of(
            "google/protobuf/descriptor.proto",
            "ai/protomolt/proto/meta/v1/metadata.proto",
            "ai/protomolt/proto/repo/v1/address.proto",
            "google/protobuf/timestamp.proto",
            "ai/protomolt/proto/repo/v1/manifest.proto",
            "ai/protomolt/proto/repo/v1/security.proto",
            "google/protobuf/duration.proto",
            "ai/protomolt/proto/validate/v1/validate.proto",
            "ai/protomolt/proto/repo/semantic/v1/semantic.proto",
            "google/protobuf/any.proto",
            "google/protobuf/struct.proto",
            "ai/protomolt/proto/repo/v1/document.proto");

    /** Explanation attached to a pin failure, citing its impact. */
    private static final String PIN_FAILURE_CONTEXT =
            "Builtin Document container closure identity changed"
                    + " (#335 item 3.10; requirements section 11 and"
                    + " section 15.14 retained container identity): container"
                    + " identity is this closure digest, so a changed digest"
                    + " makes earlier revisions fail instead of decoding."
                    + " Update this pin deliberately only after reviewing the"
                    + " protoc/protobuf or .proto change that caused it";

    /**
     * Pins the exact descriptor SHA-256 of the builtin closure asset.
     */
    @Test
    void pinsDescriptorSha256OfBuiltinClosure() {
        var definition = BuiltinDocumentSchema.definition();
        var metadata = definition.metadata();

        assertThat(metadata.getArtifactSha256())
                .as(PIN_FAILURE_CONTEXT)
                .matches("^[0-9a-f]{64}$")
                .isEqualTo(PINNED_DESCRIPTOR_SHA256);
        assertThat(metadata.getSchema().getDescriptorFingerprint())
                .as(PIN_FAILURE_CONTEXT)
                .matches("^[0-9a-f]{64}$")
                .isEqualTo(PINNED_DESCRIPTOR_FINGERPRINT);
        assertThat(metadata.getArtifactSha256())
                .as("pinned digest must be the SHA-256 of the closure bytes")
                .isEqualTo(DocumentPartCodec.sha256Hex(
                        definition.descriptors().toByteArray()));
    }

    /**
     * Pins the schema identity and root type named by the builtin asset.
     */
    @Test
    void pinsSchemaIdentityAndRootType() {
        var metadata = BuiltinDocumentSchema.definition().metadata();

        assertThat(metadata.getSchema().getTypeName())
                .as(PIN_FAILURE_CONTEXT)
                .isEqualTo(EXPECTED_ROOT_TYPE_NAME);
        assertThat(metadata.getTypeUrl())
                .as(PIN_FAILURE_CONTEXT)
                .isEqualTo(EXPECTED_TYPE_URL);
        assertThat(Document.getDescriptor().getFullName())
                .as(PIN_FAILURE_CONTEXT)
                .isEqualTo(EXPECTED_ROOT_TYPE_NAME);
    }

    /**
     * Verifies the closure and the builtin asset are byte-identical across
     * repeated calls, so the pinned digest is deterministic.
     */
    @Test
    void closureAndAssetAreDeterministicAcrossCalls() {
        var firstClosure = DescriptorFingerprints.closure(Document.getDescriptor());
        var secondClosure = DescriptorFingerprints.closure(Document.getDescriptor());

        assertThat(secondClosure.toByteString())
                .isEqualTo(firstClosure.toByteString());
        assertThat(secondClosure.toByteArray())
                .isEqualTo(firstClosure.toByteArray());
        assertThat(DescriptorFingerprints.fingerprint(secondClosure))
                .isEqualTo(PINNED_DESCRIPTOR_FINGERPRINT)
                .isEqualTo(DescriptorFingerprints.fingerprint(firstClosure));

        var firstDefinition = BuiltinDocumentSchema.definition();
        var secondDefinition = BuiltinDocumentSchema.definition();

        assertThat(secondDefinition).isSameAs(firstDefinition);
        assertThat(secondDefinition.descriptors())
                .isEqualTo(firstDefinition.descriptors())
                .isEqualTo(firstClosure.toByteString());
        assertThat(secondDefinition.metadata().toByteString())
                .isEqualTo(firstDefinition.metadata().toByteString());
        assertThat(secondDefinition.metadata().getArtifactSha256())
                .isEqualTo(PINNED_DESCRIPTOR_SHA256);
    }

    /**
     * Verifies the closure contains exactly the expected transitive files,
     * including {@code document.proto} and {@code semantic.proto}.
     */
    @Test
    void closureContainsExactlyExpectedTransitiveFiles() throws Exception {
        var closure = DescriptorFingerprints.closure(Document.getDescriptor());

        assertThat(closure.getFileList())
                .extracting(DescriptorProtos.FileDescriptorProto::getName)
                .as(PIN_FAILURE_CONTEXT)
                .containsExactlyElementsOf(EXPECTED_CLOSURE_FILES)
                .contains("ai/protomolt/proto/repo/v1/document.proto",
                        "ai/protomolt/proto/repo/semantic/v1/semantic.proto");

        var retained = DescriptorProtos.FileDescriptorSet.parseFrom(
                BuiltinDocumentSchema.definition().descriptors());
        assertThat(retained.getFileList())
                .extracting(DescriptorProtos.FileDescriptorProto::getName)
                .containsExactlyElementsOf(EXPECTED_CLOSURE_FILES);
    }
}
