package ai.protomolt.proto.mesh.v1;

import ai.protomolt.proto.mesh.test.v1.TestDocument;
import ai.protomolt.proto.mesh.test.v1.TestResult;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the mesh message and field options compile onto an annotated application contract and
 * survive a descriptor round trip: the contract gate's option carriers work before any runtime
 * consumes them.
 */
class MeshOptionsRoundTripTest {

    private static MeshMessageOptions messageOptions() {
        return TestDocument.getDescriptor().getOptions()
                .getExtension(MeshOptionsProto.message);
    }

    private static MeshFieldOptions fieldOptions(String message, String field) {
        Descriptor found = switch (message) {
            case "TestDocument" -> TestDocument.getDescriptor();
            case "TestResult" -> TestResult.getDescriptor();
            default -> throw new IllegalArgumentException(message);
        };
        FieldDescriptor fieldDescriptor = found.findFieldByName(field);
        assertThat(fieldDescriptor).as("field %s.%s", message, field).isNotNull();
        return fieldDescriptor.getOptions().getExtension(MeshOptionsProto.field);
    }

    @Test
    void theMessageOptionCarriesEveryDeclaredProfileAndPolicy() {
        MeshMessageOptions options = messageOptions();
        assertThat(options.getProcessingProfile().getName()).isEqualTo("nlp-standard");
        assertThat(options.getProcessingProfile().getVersion()).isEqualTo("1.2.0");
        assertThat(options.getResultType())
                .isEqualTo("ai.protomolt.proto.mesh.test.v1.TestResult");
        assertThat(options.getCapabilitiesList())
                .containsExactly("opennlp-ner", "java-build");
        assertThat(options.getRouteProfile().getName()).isEqualTo("default-routes");
        assertThat(options.getRecursion().getMaxDepth()).isEqualTo(4);
        assertThat(options.getRecursion().getMaxChildren()).isEqualTo(16);
        assertThat(options.getScatterProfile().getName()).isEqualTo("line-scatter");
        assertThat(options.getRehydrationProfile().getName()).isEqualTo("ordered-collect");
        assertThat(options.getLlmAllowed()).isTrue();
        assertThat(options.getPiiScanRequired()).isTrue();
        assertThat(options.getApprovalPolicy())
                .isEqualTo(ApprovalPolicy.APPROVAL_POLICY_ON_COMPLETION);
        assertThat(options.getEvidencePolicy())
                .isEqualTo(EvidencePolicy.EVIDENCE_POLICY_SUMMARY);
    }

    @Test
    void everyFieldRoleRoundTrips() {
        assertThat(fieldOptions("TestDocument", "document_id").getRoutingKey()).isTrue();
        assertThat(fieldOptions("TestDocument", "instruction").getInstruction()).isTrue();
        MeshFieldOptions body = fieldOptions("TestDocument", "body");
        assertThat(body.getGrounding()).isTrue();
        assertThat(body.getPiiScanTarget()).isTrue();
        assertThat(body.getRemoteDisclosureProhibited()).isFalse();
        assertThat(fieldOptions("TestDocument", "api_token").getRemoteDisclosureProhibited())
                .isTrue();
        assertThat(fieldOptions("TestDocument", "attachments").getAttachment()).isTrue();
        assertThat(fieldOptions("TestDocument", "scatter_lines").getScatterSource()).isTrue();
        assertThat(fieldOptions("TestResult", "summary").getResult()).isTrue();
        assertThat(fieldOptions("TestResult", "evidence_reference").getEvidence()).isTrue();
    }

    @Test
    void theOptionsSurviveADescriptorProtoRoundTrip() throws Exception {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        MeshOptionsProto.registerAllExtensions(registry);
        FileDescriptorProto proto = TestDocument.getDescriptor().getFile().toProto();
        FileDescriptorProto reparsed = FileDescriptorProto.parseFrom(
                proto.toByteArray(), registry);
        MeshMessageOptions options = reparsed.getMessageTypeList().stream()
                .filter(m -> m.getName().equals("TestDocument"))
                .findFirst()
                .orElseThrow()
                .getOptions()
                .getExtension(MeshOptionsProto.message);
        assertThat(options.getProcessingProfile().getName()).isEqualTo("nlp-standard");
        assertThat(options.getCapabilitiesList()).containsExactly("opennlp-ner", "java-build");
        assertThat(reparsed.getMessageTypeList().stream()
                .filter(m -> m.getName().equals("TestDocument"))
                .findFirst()
                .orElseThrow()
                .getFieldList().stream()
                .filter(f -> f.getName().equals("api_token"))
                .findFirst()
                .orElseThrow()
                .getOptions()
                .getExtension(MeshOptionsProto.field)
                .getRemoteDisclosureProhibited()).isTrue();
    }
}
