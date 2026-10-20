package ai.protomolt.proto.workflow;

import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.CelMappingRule;
import ai.protomolt.proto.grpc.workflow.v1.StructuredGenerationEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StructuredGenerationSpec;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.inference.v1.GenerateStructuredRequest;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.shapes.MessageScope;
import ai.protomolt.proto.shapes.ScopedProtoMapper;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The pure re-derivations and fixture reads offline replay compares against: loading a
 * recorded artifact, re-mapping a message from the recorded scope, the recorder's
 * redaction pass, and the per-step verdicts that need no scope bookkeeping. Each check
 * returns the mismatch detail, or null when it holds; {@link WorkflowReplay} owns the
 * verdict list, the scope, and the order the checks run in.
 */
final class ReplayFixtures {

    /** The sensitivity tags the recorder removes before an artifact is stored. */
    private static final Set<String> REDACTED = Set.of("pii", "secret");

    private ReplayFixtures() {
    }

    /** Loads one fixture; the store has already re-hashed it, so bytes are authentic. */
    static DynamicMessage parse(ArtifactRepository artifacts, ArtifactReference reference,
                                Descriptor type, String what) throws IOException {
        ArtifactRepository.StoredArtifact artifact = artifacts.find(reference.getSha256())
                .orElseThrow(() -> new IOException(
                        "evidence references missing artifact " + reference.getSha256()
                                + " (" + what + ")"));
        try {
            return DynamicMessage.parseFrom(type, artifact.content());
        } catch (Exception e) {
            throw new IOException("recorded " + what + " does not parse as "
                    + type.getFullName(), e);
        }
    }

    /** The same scoped mapping the live runner uses, rule for rule. */
    static DynamicMessage buildMessage(ScopedProtoMapper mapper, Map<String, Message> scope,
                                       Descriptor type, List<String> rules,
                                       List<CelMappingRule> celRules) throws Exception {
        MessageScope messageScope = ScopedMapping.scope(scope);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        ScopedMapping.apply(mapper, messageScope, scope, builder, rules,
                celRules.stream().map(ScopedMapping::celRule).toList(),
                () -> ScopedMapping.evaluator(scope, type));
        return builder.build();
    }

    /** The recorder's redaction pass, applied to a re-derived value. */
    static Message mask(Message message) {
        return SensitivityMasker.mask(message, REDACTED,
                SensitivityMasker.Strategy.REMOVE).message();
    }

    /** The redacted request a structured step without an edge sends for its spec. */
    static Message structuredRequest(StructuredGenerationSpec spec) {
        return mask(GenerateStructuredRequest.newBuilder()
                .setTargetType(spec.getTargetType())
                .setModel(spec.getModel())
                .setMaxAttempts(spec.getMaxAttempts())
                .setMode(spec.getMode())
                .build());
    }

    /** True when {@code message} passes its own declared validation rules. */
    static boolean valid(Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message).valid();
    }

    /** The recorded structured identity must name the spec's target type and model. */
    static String structuredIdentityMismatch(StructuredGenerationEvidence provenance,
                                             StructuredGenerationSpec spec) {
        if (!provenance.getTargetType().equals(spec.getTargetType())) {
            return "recorded target type " + provenance.getTargetType()
                    + " differs from the workflow's " + spec.getTargetType();
        }
        if (!provenance.getModel().equals(spec.getModel())) {
            return "recorded model " + provenance.getModel()
                    + " differs from the workflow's " + spec.getModel();
        }
        return null;
    }

    /** A skipped step must have a gate, and the gate must evaluate false on the scope. */
    static String verifySkip(WorkflowStep step, Map<String, Message> scope,
                             MethodDescriptor method) {
        if (step.getWhen().isBlank()) {
            return "step is recorded skipped but declares no gate";
        }
        try {
            CelEvaluator gate = ScopedMapping.evaluator(scope, method.getInputType());
            boolean go = gate.evaluateBooleanOrFail(step.getWhen(), Map.copyOf(scope));
            return go ? "step is recorded skipped but its gate evaluates true" : null;
        } catch (Exception e) {
            return "gate could not be re-evaluated: " + e.getMessage();
        }
    }
}
