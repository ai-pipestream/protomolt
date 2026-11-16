package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.pipeline.PipelineExecutor.BranchOutcome;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import ai.protomolt.proto.pipeline.PipelineExecutor.StepOutcome;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Turns a saved {@link Checkpoint} back into the binding its step produced, so a later segment
 * resumes after it without calling transport again. This is also where an external-completion
 * step resumes: the coordinator supplies the completion as that step's checkpoint, and it is
 * accepted here under the same rules as a checkpoint the engine wrote itself.
 *
 * <p>A checkpoint is untrusted input. It must name this pipeline, this input and this step, carry
 * the step's declared type and cardinality, stay inside the stream and fan-out bounds, and pass
 * the step's declared response validation. Anything else stops the run with
 * {@link FailureKind#PIPELINE}.
 */
final class CheckpointReplay {

    /** What every checkpoint of one segment is checked against. */
    record Context(String pipelineFingerprint, String inputFingerprint,
                   DescriptorRegistry registry, List<FileDescriptor> files, int streamLimit) {
    }

    /** The type, cardinality and fan-out a step's output is declared to have. */
    private record Shape(Descriptor type, EdgeCardinality cardinality, MethodDescriptor method,
                         FanOutSpec fanOut) {
    }

    private CheckpointReplay() {
    }

    static PipelineBinding restore(PipelineStep step, Checkpoint saved,
                                   Map<String, PipelineBinding> scope, Context context)
            throws PipelineExecutionException {
        requireIdentity(step, saved, context);
        Shape shape = declaredShape(step, scope, context);
        requireOutcome(step, saved, shape, context.streamLimit());
        requireBranchEvidence(step, saved, shape.fanOut(), context.streamLimit());
        List<DynamicMessage> values = decodeMessages(step, saved, shape);
        if (shape.fanOut() != null && !saved.outcome().skipped()) {
            requireCollectedBranches(step, saved, shape, values.getFirst());
        }
        if (step.hasCollect()) {
            scope.remove(step.getCollect().getSource());
        } else if (shape.method() != null) {
            PipelineBinding.consumeRequestStream(step.getGrpcCall(), shape.method(), scope);
        }
        return new PipelineBinding(shape.type(), shape.cardinality(), values);
    }

    private static void requireIdentity(PipelineStep step, Checkpoint saved, Context context)
            throws PipelineExecutionException {
        if (!saved.pipelineFingerprint().equals(context.pipelineFingerprint())
                || !saved.inputFingerprint().equals(context.inputFingerprint())
                || !saved.outcome().name().equals(step.getName())) {
            throw invalid(step,
                    "checkpoint identity does not match the pipeline, input and ordered step");
        }
    }

    private static Shape declaredShape(PipelineStep step, Map<String, PipelineBinding> scope,
                                       Context context) {
        DescriptorRegistry registry = context.registry();
        if (step.hasGrpcCall()) {
            MethodDescriptor method = DescriptorSets.resolveMethod(
                    context.files(), step.getGrpcCall().getMethod());
            boolean fansOut = step.getGrpcCall().hasFanOut();
            Descriptor type = fansOut
                    ? registry.findDescriptorByFullName(
                            step.getGrpcCall().getFanOut().getCollectType())
                    : method.getOutputType();
            return new Shape(type, step.getGrpcCall().getOutputCardinality(), method,
                    fansOut ? step.getGrpcCall().getFanOut() : null);
        }
        if (step.hasStructured()) {
            boolean fansOut = step.getStructured().hasFanOut();
            Descriptor type = registry.findDescriptorByFullName(fansOut
                    ? step.getStructured().getFanOut().getCollectType()
                    : step.getStructured().getSpec().getTargetType());
            return new Shape(type, EdgeCardinality.EDGE_CARDINALITY_ONE, null,
                    fansOut ? step.getStructured().getFanOut() : null);
        }
        if (step.hasCollect()) {
            return new Shape(
                    registry.findDescriptorByFullName(step.getCollect().getCollectType()),
                    EdgeCardinality.EDGE_CARDINALITY_ONE, null, null);
        }
        Descriptor unnested = StreamPaths.repeatedField(
                scope.get(step.getUnnest().getSource()).type(),
                step.getUnnest().getPath()).getMessageType();
        return new Shape(unnested, EdgeCardinality.EDGE_CARDINALITY_MANY, null, null);
    }

    private static void requireOutcome(PipelineStep step, Checkpoint saved, Shape shape,
                                       int limit) throws PipelineExecutionException {
        StepOutcome outcome = saved.outcome();
        EdgeCardinality cardinality = shape.cardinality();
        boolean externalStep = step.hasGrpcCall()
                && step.getGrpcCall().getCompletion() == StepCompletion.STEP_COMPLETION_EXTERNAL;
        if (saved.cardinality() != cardinality
                || saved.messages().size() > limit
                || (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE
                        && saved.messages().size() != 1)
                || outcome.requestCount() < 0
                || outcome.responseCount() < 0
                || ((!step.hasGrpcCall() || externalStep) && outcome.requestCount() != 0)
                || (!outcome.skipped() && outcome.responseCount() != saved.messages().size())
                || (outcome.skipped()
                        && (!step.hasGrpcCall() || step.getWhen().isBlank()
                                || outcome.requestCount() != 0
                                || outcome.responseCount() != 0))) {
            throw invalid(step, "invalid checkpoint cardinality or outcome");
        }
        if (outcome.skipped()
                && cardinality == EdgeCardinality.EDGE_CARDINALITY_MANY
                && !saved.messages().isEmpty()) {
            throw invalid(step, "skipped stream checkpoint must be empty");
        }
    }

    private static void requireBranchEvidence(PipelineStep step, Checkpoint saved,
                                              FanOutSpec fanOut, int limit)
            throws PipelineExecutionException {
        if (fanOut == null || saved.outcome().skipped()) {
            if (!saved.branches().isEmpty()) {
                throw invalid(step,
                        "checkpoint has branch evidence for a step without executed fan-out");
            }
            return;
        }
        if (saved.branches().size() > fanOut.getMaxItems() || saved.branches().size() > limit) {
            throw invalid(step, "checkpoint branch evidence exceeds fan-out bounds");
        }
        boolean failFast = fanOut.getFailurePolicy()
                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST;
        for (int i = 0; i < saved.branches().size(); i++) {
            BranchOutcome branch = saved.branches().get(i);
            if (branch.index() != i
                    || !(step.getName() + "#" + i).equals(branch.id())
                    || branch.error() == null
                    || (branch.succeeded()
                            && (branch.kind() != null || !branch.error().isEmpty()))
                    || (!branch.succeeded()
                            && (branch.kind() == null || branch.error().isBlank() || failFast))) {
                throw invalid(step, "invalid checkpoint branch evidence");
            }
        }
    }

    private static List<DynamicMessage> decodeMessages(PipelineStep step, Checkpoint saved,
                                                       Shape shape)
            throws PipelineExecutionException {
        Descriptor type = shape.type();
        boolean skipped = saved.outcome().skipped();
        boolean validateResponse = !skipped
                && step.hasGrpcCall()
                && step.getGrpcCall().getValidateResponse()
                && !step.getGrpcCall().hasFanOut();
        List<DynamicMessage> values = new ArrayList<>(saved.messages().size());
        for (DynamicMessage message : saved.messages()) {
            if (!message.getDescriptorForType().getFullName().equals(type.getFullName())
                    || !DescriptorFingerprints.fingerprintOf(message.getDescriptorForType())
                            .equals(DescriptorFingerprints.fingerprintOf(type))) {
                throw invalid(step,
                        "checkpoint response descriptor does not match " + type.getFullName());
            }
            if (skipped && !message.equals(
                    DynamicMessage.getDefaultInstance(message.getDescriptorForType()))) {
                throw invalid(step, "skipped checkpoint contains a response");
            }
            DynamicMessage restored;
            try {
                restored = DynamicMessage.parseFrom(type, StepValues.canonicalBytes(message));
            } catch (InvalidProtocolBufferException e) {
                throw PipelineExecutor.failure(step.getName(), FailureKind.PIPELINE,
                        "checkpoint response cannot be decoded", e);
            }
            if (validateResponse) {
                StepValues.validate(restored, step.getName(), "checkpoint response");
            }
            values.add(restored);
        }
        return values;
    }

    private static void requireCollectedBranches(PipelineStep step, Checkpoint saved,
                                                 Shape shape, DynamicMessage collectedHolder)
            throws PipelineExecutionException {
        long successes = saved.branches().stream().filter(BranchOutcome::succeeded).count();
        FieldDescriptor collectedField =
                shape.type().findFieldByName(shape.fanOut().getCollectInto());
        int collected = collectedHolder.getRepeatedFieldCount(collectedField);
        if (step.hasGrpcCall() && step.getGrpcCall().getValidateResponse()) {
            for (int i = 0; i < collected; i++) {
                StepValues.validate(
                        StepValues.dynamic(
                                (Message) collectedHolder.getRepeatedField(collectedField, i)),
                        step.getName(), "checkpoint branch response");
            }
        }
        if (successes != collected
                || (step.hasGrpcCall()
                        && saved.outcome().requestCount() != saved.branches().size())) {
            throw invalid(step, "checkpoint branches do not match the collected responses");
        }
    }

    private static PipelineExecutionException invalid(PipelineStep step, String message) {
        return PipelineExecutor.failure(step.getName(), FailureKind.PIPELINE, message, null);
    }
}
