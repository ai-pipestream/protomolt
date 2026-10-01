package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Read-only admission of an authored executable source before any service invocation. */
public final class WorkflowAuthoringPreflight {

    private static final int MAX_SOURCE_BYTES = 4 * 1024 * 1024;
    private static final int MAX_DESCRIPTOR_BYTES = 4 * 1024 * 1024;

    private WorkflowAuthoringPreflight() {
    }

    /** A concrete call approved by the task's trusted caller. */
    public record PermittedCall(String target, String method, boolean tls) {
        public PermittedCall {
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(method, "method");
        }
    }

    /** Policy obtained from the immutable offer or operator configuration, never the candidate. */
    public record Policy(ArtifactReference descriptors, List<PermittedCall> permittedCalls) {
        public Policy {
            Objects.requireNonNull(descriptors, "descriptors");
            permittedCalls = List.copyOf(permittedCalls);
            if (permittedCalls.isEmpty()) {
                throw new IllegalArgumentException("policy permits no service calls");
            }
        }
    }

    /** The checked runtime definition and exact submitted source text. */
    public static final class Result {
        private final CompiledWorkflow workflow;
        private final String sourceJson;

        private Result(CompiledWorkflow workflow, String sourceJson) {
            this.workflow = workflow;
            this.sourceJson = sourceJson;
        }

        public CompiledWorkflow workflow() { return workflow; }
        public String sourceJson() { return sourceJson; }
    }

    /**
     * Resolves exact artifact references, pins the descriptor closure, checks the
     * source and durable compilation, and authorizes every step. Performs no calls.
     */
    public static Result verify(Workflow durable, ArtifactReference workflowArtifact,
            ArtifactReference executableSource, ArtifactReference descriptorArtifact,
            Policy policy, ActionContext context, ArtifactRepository artifacts) throws IOException {
        Objects.requireNonNull(durable, "durable");
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(artifacts, "artifacts");
        WorkflowValidation.validate(durable);
        byte[] workflowBytes = resolve(artifacts, workflowArtifact,
                WorkflowValidation.MAX_WORKFLOW_BYTES, "workflow");
        if (!Arrays.equals(workflowBytes, durable.toByteArray())) {
            throw new IllegalArgumentException("workflow artifact differs from inline workflow");
        }
        if (!descriptorArtifact.equals(policy.descriptors())) {
            throw new IllegalArgumentException("descriptor reference differs from caller policy");
        }
        byte[] descriptorBytes = resolve(artifacts, descriptorArtifact,
                MAX_DESCRIPTOR_BYTES, "descriptors");
        requireCompleteImports(descriptorBytes);
        if (!"application/json".equals(executableSource.getMediaType())) {
            throw new IllegalArgumentException("executable source must be application/json");
        }
        byte[] sourceBytes = resolve(artifacts, executableSource,
                MAX_SOURCE_BYTES, "executable source");
        String sourceJson;
        try {
            sourceJson = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(sourceBytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("executable source is not UTF-8", e);
        }
        Message source;
        try {
            JsonNode tree = context.objectMapper().readTree(sourceJson);
            if (!(tree instanceof ObjectNode object)) {
                throw new IllegalArgumentException("executable source must be a JSON object");
            }
            source = CatalogContract.read(object, CatalogContract.request("CompiledWorkflow"),
                    "executable source");
            CatalogContract.validate(source, source.getDescriptorForType(), "executable source");
            Message schema = Fields.message(source, "schema");
            if (!Fields.string(schema, "type").isEmpty()
                    || !Fields.map(schema, "sources").isEmpty()
                    || !Fields.string(schema, "root").isEmpty()) {
                throw new IllegalArgumentException("executable source must pin a descriptor set only");
            }
            String encoded = Fields.string(schema, "descriptorSetBase64");
            if (encoded.isEmpty() || !Arrays.equals(Base64.getDecoder().decode(encoded),
                    descriptorBytes)) {
                throw new IllegalArgumentException("source descriptor set differs from caller policy");
            }
        } catch (ActionException | IOException e) {
            throw new IllegalArgumentException("invalid executable source: " + e.getMessage(), e);
        }
        CompiledWorkflow parsed;
        try {
            parsed = WorkflowJson.parse(source, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            throw new IllegalArgumentException("executable source does not parse: " + e.getMessage(), e);
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(parsed);
        if (!findings.isEmpty()) {
            throw new IllegalArgumentException("executable source does not verify: " + findings);
        }
        Set<PermittedCall> permitted = new HashSet<>(policy.permittedCalls());
        for (CompiledWorkflow.Step step : parsed.steps()) {
            if (!step.validate()) {
                throw new IllegalArgumentException("step '" + step.name()
                        + "' must enable response validation for subsequent execution");
            }
            if (step.structured() != null || step.external() || step.fanOut() != null) {
                throw new IllegalArgumentException("unsupported authoring fixture step '"
                        + step.name() + "': structured, external and fan-out steps are not admitted");
            }
            PermittedCall call = new PermittedCall(step.target(),
                    step.method().getService().getFullName() + "/" + step.method().getName(),
                    step.tls());
            if (!permitted.contains(call)) {
                throw new IllegalArgumentException("step '" + step.name()
                        + "' calls a target, method or TLS mode absent from caller policy");
            }
        }
        Workflow compiled = WorkflowCompiler.compile(parsed);
        if (!Arrays.equals(compiled.toByteArray(), durable.toByteArray())) {
            throw new IllegalArgumentException("executable source compiles to different workflow content");
        }
        return new Result(parsed, sourceJson);
    }

    private static byte[] resolve(ArtifactRepository artifacts, ArtifactReference reference,
            int maxBytes, String label) throws IOException {
        WorkflowValidation.validate(reference);
        if (Long.compareUnsigned(reference.getSizeBytes(), maxBytes) > 0) {
            throw new IllegalArgumentException(label + " artifact exceeds " + maxBytes + " bytes");
        }
        ArtifactRepository.StoredArtifact stored = artifacts.find(reference.getSha256())
                .orElseThrow(() -> new IllegalArgumentException("missing " + label + " artifact"));
        if (!stored.reference().equals(reference)) {
            throw new IllegalArgumentException(label + " artifact reference metadata differs");
        }
        byte[] bytes = stored.content();
        if (bytes.length > maxBytes) {
            throw new IllegalArgumentException(label + " artifact exceeds " + maxBytes + " bytes");
        }
        return bytes;
    }

    private static void requireCompleteImports(byte[] bytes) throws IOException {
        FileDescriptorSet descriptors = FileDescriptorSet.parseFrom(bytes);
        var names = new HashSet<String>();
        for (var file : descriptors.getFileList()) {
            if (file.getName().isBlank() || !names.add(file.getName())) {
                throw new IllegalArgumentException("descriptor set has empty or duplicate file names");
            }
        }
        for (var file : descriptors.getFileList()) {
            for (String dependency : file.getDependencyList()) {
                if (!names.contains(dependency)) {
                    throw new IllegalArgumentException("descriptor set omits import '" + dependency
                            + "' required by '" + file.getName() + "'");
                }
            }
        }
    }
}
