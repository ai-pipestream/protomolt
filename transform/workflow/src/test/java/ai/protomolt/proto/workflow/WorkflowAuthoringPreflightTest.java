package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringPreflightTest {

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.test;
            message Text { string text = 1; }
            service Echo { rpc Say(Text) returns (Text); }
            """;
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path temp;
    FileSystemArtifactRepository artifacts;
    ActionContext context;
    ObjectNode source;
    byte[] descriptors;
    Workflow durable;
    ArtifactReference workflowRef;
    ArtifactReference sourceRef;
    ArtifactReference descriptorRef;
    WorkflowAuthoringPreflight.Policy policy;

    @BeforeEach
    void setup() throws Exception {
        artifacts = new FileSystemArtifactRepository(temp);
        context = ActionContext.create();
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workflow/test/echo.proto", PROTO, "test").build());
        descriptors = compiled.descriptorSet().toByteArray();
        source = JSON.createObjectNode();
        source.put("name", "echo-text");
        source.put("validateContract", true);
        source.putObject("schema").put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(descriptors));
        source.put("inputType", "workflow.test.Text");
        ObjectNode step = source.putArray("steps").addObject();
        step.put("name", "echo");
        step.put("validate", true);
        step.put("target", "fixture:9090");
        step.put("method", "workflow.test.Echo/Say");
        step.putArray("rules").add("text = input.text");
        durable = WorkflowCompiler.compile(WorkflowJson.parse(source, context));
        workflowRef = artifacts.save(durable.toByteArray(), "application/x-protobuf", false);
        sourceRef = artifacts.save(source.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "application/json", false);
        descriptorRef = artifacts.save(descriptors, "application/x-protobuf", false);
        policy = new WorkflowAuthoringPreflight.Policy(descriptorRef,
                List.of(new WorkflowAuthoringPreflight.PermittedCall(
                        "fixture:9090", "workflow.test.Echo/Say", false)));
    }

    private WorkflowAuthoringPreflight.Result verify() throws Exception {
        return WorkflowAuthoringPreflight.verify(durable, workflowRef, sourceRef,
                descriptorRef, policy, context, artifacts);
    }

    @Test
    void acceptsExactlyPinnedSourceAndWorkflowWithoutInvokingServices() throws Exception {
        var result = verify();
        assertThat(result.workflow().name()).isEqualTo("echo-text");
        assertThat(result.sourceJson()).isEqualTo(source.toString());
    }

    @Test
    void refusesImportsThatTheGeneralResolverCouldFillFromBuiltIns() throws Exception {
        var set = FileDescriptorSet.parseFrom(descriptors);
        var missingImport = set.toBuilder().setFile(0, set.getFile(0).toBuilder()
                .addDependency("ai/protomolt/proto/validate/v1/validate.proto")).build();
        descriptors = missingImport.toByteArray();
        descriptorRef = artifacts.save(descriptors, "application/x-protobuf", false);
        policy = new WorkflowAuthoringPreflight.Policy(descriptorRef, policy.permittedCalls());
        source.withObject("schema").put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(descriptors));
        sourceRef = artifacts.save(source.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "application/json", false);
        // The general resolver accepts this, but authoring must pin every definition.
        durable = WorkflowCompiler.compile(WorkflowJson.parse(source, context));
        workflowRef = artifacts.save(durable.toByteArray(), "application/x-protobuf", false);
        assertThatThrownBy(this::verify).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("omits import 'ai/protomolt/proto/validate/v1/validate.proto'");
    }

    @Test
    void refusesSourceThatDisablesContractValidationEvenWithMatchingDurableWorkflow() throws Exception {
        source.put("validateContract", false);
        durable = WorkflowCompiler.compile(WorkflowJson.parse(source, context));
        workflowRef = artifacts.save(durable.toByteArray(), "application/x-protobuf", false);
        sourceRef = artifacts.save(source.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "application/json", false);
        assertThatThrownBy(this::verify).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must enable contract validation");
    }

    @Test
    void refusesChangedSourceAndDurableWorkflow() throws Exception {
        ObjectNode altered = source.deepCopy();
        ((ObjectNode) altered.withArray("steps").get(0)).put("target", "other:9090");
        ArtifactReference alteredRef = artifacts.save(altered.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8), "application/json", false);
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                alteredRef, descriptorRef, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("caller policy");

        altered = source.deepCopy();
        altered.put("name", "other-name");
        ArtifactReference changedName = artifacts.save(altered.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8), "application/json", false);
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                changedName, descriptorRef, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different workflow content");

        Workflow different = durable.toBuilder().setDescription("different").build();
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(different, workflowRef,
                sourceRef, descriptorRef, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("workflow artifact differs");
    }

    @Test
    void refusesChangedDescriptorsAndArtifactMetadata() throws Exception {
        ArtifactReference wrongDescriptors = artifacts.save(new byte[] {1, 2, 3},
                "application/x-protobuf", false);
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                sourceRef, wrongDescriptors, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("caller policy");

        ArtifactReference wrongMetadata = sourceRef.toBuilder().setRedacted(true).build();
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                wrongMetadata, descriptorRef, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("metadata differs");
    }

    @Test
    void refusesUnapprovedTransportModeAndSourceDescriptorBytes() throws Exception {
        var tlsPolicy = new WorkflowAuthoringPreflight.Policy(descriptorRef,
                List.of(new WorkflowAuthoringPreflight.PermittedCall(
                        "fixture:9090", "workflow.test.Echo/Say", true)));
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                sourceRef, descriptorRef, tlsPolicy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TLS mode");

        ObjectNode altered = source.deepCopy();
        altered.withObject("schema").put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(new byte[] {1, 2, 3}));
        ArtifactReference alteredRef = artifacts.save(altered.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8), "application/json", false);
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                alteredRef, descriptorRef, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("descriptor set differs");
    }

    @Test
    void refusesMutableSchemaAndUnsupportedExecutionKinds() throws Exception {
        ObjectNode mutable = source.deepCopy();
        mutable.withObject("schema").remove("descriptorSetBase64");
        mutable.withObject("schema").put("type", "workflow.test.Text");
        ArtifactReference mutableRef = artifacts.save(mutable.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8), "application/json", false);
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                mutableRef, descriptorRef, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("descriptor set only");

        ObjectNode external = source.deepCopy();
        ((ObjectNode) external.withArray("steps").get(0)).put("completion", "external");
        ArtifactReference externalRef = artifacts.save(external.toString().getBytes(
                java.nio.charset.StandardCharsets.UTF_8), "application/json", false);
        assertThatThrownBy(() -> WorkflowAuthoringPreflight.verify(durable, workflowRef,
                externalRef, descriptorRef, policy, context, artifacts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unsupported");
    }
}
