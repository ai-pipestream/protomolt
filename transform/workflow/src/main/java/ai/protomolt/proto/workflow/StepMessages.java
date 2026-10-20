package ai.protomolt.proto.workflow;

import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.inference.v1.GenerateStructuredRequest;
import ai.protomolt.proto.inference.v1.GenerateStructuredResponse;
import ai.protomolt.proto.mapper.cel.CelMappingRule;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.ProjectionException;
import ai.protomolt.proto.shapes.MessageScope;
import ai.protomolt.proto.shapes.ScopedProtoMapper;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.workflow.WorkflowRunner.FailureKind;
import ai.protomolt.proto.workflow.WorkflowRunner.WorkflowExecutionException;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Map;

/**
 * The pure message work of one live step: building and checking the messages a step
 * sends, receives, and binds. Nothing here touches a channel, the clock, the scope map,
 * or the checkpoint list; {@link WorkflowRunner} owns those and calls these in its own
 * order. Every failure is a {@link WorkflowExecutionException} carrying the kind the
 * jobs worker's retry decision reads.
 */
final class StepMessages {

    private StepMessages() {
    }

    /** Maps the scope into a new {@code type} message; a rule failure is {@code MAPPING}. */
    static DynamicMessage buildMessage(ScopedProtoMapper mapper, CelEvaluator evaluator,
                                       Map<String, Message> values, Descriptor type,
                                       List<String> rules, List<CelMappingRule> celRules,
                                       String where) throws WorkflowExecutionException {
        MessageScope scope = ScopedMapping.scope(values);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        try {
            ScopedMapping.apply(mapper, scope, values, builder, rules, celRules,
                    () -> evaluator);
        } catch (Exception e) {
            throw new WorkflowExecutionException(where, FailureKind.MAPPING, null,
                    "mapping failed: " + e.getMessage(), e);
        }
        return builder.build();
    }

    /** The step's structured-generation request, with grounding when supplied. */
    static GenerateStructuredRequest structuredRequest(CompiledWorkflow.Step step,
                                                       Any grounding) {
        GenerateStructuredRequest.Builder request = GenerateStructuredRequest.newBuilder()
                .setTargetType(step.structured().targetType().getFullName())
                .setModel(step.structured().model())
                .setMaxAttempts(step.structured().maxAttempts())
                .setMode(step.structured().mode());
        if (grounding != null) {
            request.setGrounding(grounding);
        }
        return request.build();
    }

    /** The coordinator's packed output as the step's typed message. */
    static Message unpackStructured(CompiledWorkflow.Step step,
                                    GenerateStructuredResponse generated)
            throws WorkflowExecutionException {
        try {
            return DynamicMessage.parseFrom(step.structured().targetType(),
                    generated.getMessage().getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.WORKFLOW, null,
                    "the coordinator's packed output does not parse as the step's "
                            + "target type", e);
        }
    }

    /** Projects one edge value to its consumer-visible form. */
    static Message projectValue(CompiledWorkflow.Step step, Descriptor projectTo,
                                Message value, DescriptorRegistry registry)
            throws WorkflowExecutionException {
        try {
            return MessageProjection
                    .forTarget(projectTo, registry)
                    .orElseThrow(() -> new ProjectionException("projection target "
                            + projectTo.getFullName() + " declares no projection sources"))
                    .project(value);
        } catch (ProjectionException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "edge projection failed: " + e.getMessage(), e);
        }
    }

    /** Runs the response's declared validation rules when the step asks for them. */
    static void validateResponse(CompiledWorkflow.Step step, DynamicMessage response,
                                 boolean validateContract)
            throws WorkflowExecutionException {
        if (step.validate() || validateContract) {
            ValidationResult result = ProtoValidator
                    .forMessageType(step.method().getOutputType())
                    .validate(response);
            if (!result.valid()) {
                throw new WorkflowExecutionException(step.name(), FailureKind.VALIDATION,
                        null, "response failed validation: " + summary(result), null);
            }
        }
    }

    /**
     * Re-parses {@code message} against the workflow's pinned {@code type} and validates
     * it, so a Java caller cannot swap in a descriptor with weaker rules.
     */
    static DynamicMessage contractValue(Message message, Descriptor type, String where)
            throws WorkflowExecutionException {
        if (!message.getDescriptorForType().getFullName().equals(type.getFullName())) {
            throw new WorkflowExecutionException(where, FailureKind.VALIDATION, null,
                    where + " does not name the workflow's declared type", null);
        }
        try {
            // Java callers cannot replace the pinned rules with a weaker descriptor.
            DynamicMessage pinned = DynamicMessage.parseFrom(type, message.toByteArray());
            validateContractMessage(pinned, where);
            return pinned;
        } catch (InvalidProtocolBufferException invalid) {
            throw new WorkflowExecutionException(where, FailureKind.VALIDATION, null,
                    where + " does not parse as the workflow's declared type", invalid);
        }
    }

    /** Validates {@code message} against its own declared rules under a strict contract. */
    static void validateContractMessage(Message message, String where)
            throws WorkflowExecutionException {
        ValidationResult result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) {
            throw new WorkflowExecutionException(where, FailureKind.VALIDATION, null,
                    where + " contract validation failed: " + summary(result), null);
        }
    }

    /** The type a step's response binds under its name, gRPC, structured, or collected. */
    static Descriptor outputTypeOf(CompiledWorkflow.Step step) {
        if (step.fanOut() != null) {
            return step.fanOut().collectType();
        }
        return step.structured() != null
                ? step.structured().targetType()
                : step.method().getOutputType();
    }

    /** Every violation as {@code path: message}, joined by {@code "; "}. */
    static String summary(ValidationResult result) {
        StringBuilder out = new StringBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            if (!out.isEmpty()) {
                out.append("; ");
            }
            out.append(violation.path()).append(": ").append(violation.message());
        }
        return out.toString();
    }
}
