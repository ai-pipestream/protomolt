package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentAdmissionPolicyTest {
    static DocumentSchemaPolicy policy() {
        return DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setValidationProfile(DocumentSchemaAdmission.PROFILE)
                .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED).setAnyResolvedSchema(true)
                .setRequireStructuredRoot(true).setLimits(DocumentSchemaPolicyLimits.newBuilder()
                        .setMaxFragments(32).setMaxFragmentBytes(4_000_000).setMaxRoots(100).setMaxEvidenceBytes(4_000_000)
                        .setMaxBindings(20).setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build();
    }

    @Test void normalizesTheEligibleSetAndRejectsDuplicateOrNoncanonicalStorage() throws Exception {
        var a = binding("a", "a"); var b = binding("b", "b");
        var first = policy().toBuilder().setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(b).addBindings(a)).build();
        var second = first.toBuilder().setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(a).addBindings(b)).build();
        var one = DocumentAdmissionPolicy.of(first, () -> {});
        var two = DocumentAdmissionPolicy.of(second, () -> {});
        assertThat(one.bytes()).isEqualTo(two.bytes());
        assertThat(one.sha256()).isEqualTo(two.sha256());
        assertThat(DocumentAdmissionPolicy.decode(DocumentAdmissionPolicy.CODEC, 1, one.bytes(), one.sha256(), () -> {}).definition())
                .isEqualTo(second);
        var unsorted = DocumentSchemaEvidenceCodec.encode(first, () -> {});
        assertThatThrownBy(() -> DocumentAdmissionPolicy.decode(DocumentAdmissionPolicy.CODEC, 1, unsorted.bytes(), unsorted.sha256(), () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("noncanonical policy schema ordering");
        var duplicate = first.toBuilder().setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(a).addBindings(a)).build();
        assertThatThrownBy(() -> DocumentAdmissionPolicy.of(duplicate, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate policy schema binding");
    }

    @Test void refusesUnknownContentAndUnsupportedStorageIdentity() {
        var snapshot = DocumentAdmissionPolicy.of(policy(), () -> {});
        assertThatThrownBy(() -> DocumentAdmissionPolicy.decode("other", 1, snapshot.bytes(), snapshot.sha256(), () -> {}))
                .hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentAdmissionPolicy.decode(DocumentAdmissionPolicy.CODEC, 2, snapshot.bytes(), snapshot.sha256(), () -> {}))
                .hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentAdmissionPolicy.decode(DocumentAdmissionPolicy.CODEC, 1, snapshot.bytes(), "f".repeat(64), () -> {}))
                .hasMessageContaining("digest mismatch");
        var unknown = policy().toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        assertThatThrownBy(() -> DocumentAdmissionPolicy.of(unknown, () -> {})).hasMessageContaining("unknown");
    }

    @Test void capsCanonicalPolicyBytesAndDirectFacadeLimits() {
        var large = DocumentSchemaPolicyAllowList.newBuilder();
        for (int i = 0; i < 200; i++) large.addBindings(binding("x".repeat(3900) + i, "a"));
        assertThatThrownBy(() -> DocumentAdmissionPolicy.of(policy().toBuilder().setAllowedSchemas(large).build(), () -> {}))
                .hasMessageContaining("policy bytes exceed limit");
        assertThatCode(() -> new DocumentSchemaAdmission.Limits(1, 256L * 1024 * 1024, 1, 1, 1, 1, 256 * 1024 * 1024))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new DocumentSchemaAdmission.Limits(1, 256L * 1024 * 1024 + 1, 1, 1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DocumentSchemaAdmission.Limits(1, 1, 1, 1, 1, 1, 256 * 1024 * 1024 + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static DocumentSchemaPolicyBinding binding(String prefix, String fingerprint) {
        return DocumentSchemaPolicyBinding.newBuilder().setTypeUrl(prefix + "/test.Payload")
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("test.Payload").setDescriptorFingerprint(fingerprint.repeat(64))).build();
    }
}
