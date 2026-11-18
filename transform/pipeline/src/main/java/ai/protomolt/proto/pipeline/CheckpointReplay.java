package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
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
import java.util.Arrays;
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
        requireGateAgreement(step, saved, shape, scope);
        requireRequestCount(step, saved, shape, scope);
        requireBranchEvidence(step, saved, shape.fanOut(), context.streamLimit());
        List<DynamicMessage> values = decodeMessages(step, saved, shape);
        if (shape.fanOut() != null && !saved.outcome().skipped()) {
            requireEveryItemBranched(step, saved, shape.fanOut(), scope, context.registry());
            requireCollectedBranches(step, saved, shape, values.getFirst());
        }
        if (step.hasUnnest()) {
            requireRecomputed(step, values,
                    LocalSteps.unnest(step, scope, context.streamLimit()));
        }
        if (step.hasCollect()) {
            requireRecomputed(step, values,
                    LocalSteps.collect(step, scope, context.registry()));
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

    /**
     * A gate is a pure function of the bindings in scope, so replay evaluates it again: a
     * checkpoint may claim a skip only where the gate is false, and a response only where it
     * is true.
     */
    private static void requireGateAgreement(PipelineStep step, Checkpoint saved, Shape shape,
                                             Map<String, PipelineBinding> scope)
            throws PipelineExecutionException {
        if (!step.hasGrpcCall() || step.getWhen().isBlank()) {
            return;
        }
        boolean gatePasses = EdgeMapping.evaluateGate(
                step, scope, shape.method().getInputType());
        if (saved.outcome().skipped() == gatePasses) {
            throw invalid(step, gatePasses
                    ? "checkpoint claims a skip but the step's gate passes"
                    : "checkpoint claims a response but the step's gate is false");
        }
    }

    /**
     * A called step sends one request per item of its request stream, or exactly one when no
     * source is a stream. Fan-out counts are checked against the branch evidence instead.
     */
    private static void requireRequestCount(PipelineStep step, Checkpoint saved, Shape shape,
                                            Map<String, PipelineBinding> scope)
            throws PipelineExecutionException {
        if (!step.hasGrpcCall() || saved.outcome().skipped() || shape.fanOut() != null
                || step.getGrpcCall().getCompletion()
                        == StepCompletion.STEP_COMPLETION_EXTERNAL) {
            return;
        }
        int expected = 1;
        for (String source : step.getGrpcCall().getEdge().getSourcesList()) {
            PipelineBinding binding = scope.get(source);
            if (binding.cardinality() == EdgeCardinality.EDGE_CARDINALITY_MANY) {
                expected = binding.values().size();
            }
        }
        if (saved.outcome().requestCount() != expected) {
            throw invalid(step, "checkpoint request count " + saved.outcome().requestCount()
                    + " does not match the " + expected + " request(s) the step sends");
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
        boolean validateResponse = !skipped && shape.fanOut() == null
                && validatesResponses(step);
        List<DynamicMessage> values = new ArrayList<>(saved.messages().size());
        for (DynamicMessage message : saved.messages()) {
            if (!message.getDescriptorForType().getFullName().equals(type.getFullName())
                    || !DescriptorFingerprints.fingerprintOf(message.getDescriptorForType())
                            .equals(DescriptorFingerprints.fingerprintOf(type))) {
                throw invalid(step,
                        "checkpoint response descriptor does not match " + type.getFullName());
            }
            if (skipped) {
                if (!message.equals(
                        DynamicMessage.getDefaultInstance(message.getDescriptorForType()))) {
                    throw invalid(step, "skipped checkpoint contains a response");
                }
                // The placeholder of a skipped step is the empty message. It is not parsed
                // back, because a proto2 type with required fields refuses to parse empty.
                values.add(DynamicMessage.getDefaultInstance(type));
                continue;
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

    /**
     * Whether a live run validates this step's responses: a gRPC step when it declares
     * {@code validate_response}, and a structured step always, because generation returns
     * only a form that passed its target type's rules.
     */
    private static boolean validatesResponses(PipelineStep step) {
        return step.hasStructured()
                || (step.hasGrpcCall() && step.getGrpcCall().getValidateResponse());
    }

    /**
     * A fan-out that completes records one branch per item its edge maps, so the items are
     * mapped again from the replayed scope and the evidence must cover every one of them.
     */
    private static void requireEveryItemBranched(PipelineStep step, Checkpoint saved,
                                                 FanOutSpec fanOut,
                                                 Map<String, PipelineBinding> scope,
                                                 DescriptorRegistry registry)
            throws PipelineExecutionException {
        TypedEdge edge = step.hasGrpcCall()
                ? step.getGrpcCall().getEdge() : step.getStructured().getEdge();
        DynamicMessage holder = EdgeMapping.mapEdge(edge, scope, registry, step.getName())
                .getFirst();
        int items = EdgeMapping.prepareFanOut(step.getName(), holder, edge, fanOut, registry)
                .size();
        if (saved.branches().size() != items) {
            throw invalid(step, "checkpoint has " + saved.branches().size()
                    + " branch(es) for the " + items + " item(s) the step fans out over");
        }
    }

    private static void requireCollectedBranches(PipelineStep step, Checkpoint saved,
                                                 Shape shape, DynamicMessage collectedHolder)
            throws PipelineExecutionException {
        long successes = saved.branches().stream().filter(BranchOutcome::succeeded).count();
        FieldDescriptor collectedField =
                shape.type().findFieldByName(shape.fanOut().getCollectInto());
        int collected = collectedHolder.getRepeatedFieldCount(collectedField);
        if (validatesResponses(step)) {
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

    /**
     * An unnest or collect step calls nothing and depends only on the replayed scope, so its
     * checkpoint must carry exactly what the step computes again.
     */
    private static void requireRecomputed(PipelineStep step, List<DynamicMessage> saved,
                                          PipelineBinding recomputed)
            throws PipelineExecutionException {
        boolean same = saved.size() == recomputed.values().size();
        for (int i = 0; same && i < saved.size(); i++) {
            same = Arrays.equals(StepValues.canonicalBytes(saved.get(i)),
                    StepValues.canonicalBytes(recomputed.values().get(i)));
        }
        if (!same) {
            throw invalid(step, "checkpoint differs from the value recomputed from its source");
        }
    }

    private static PipelineExecutionException invalid(PipelineStep step, String message) {
        return PipelineExecutor.failure(step.getName(), FailureKind.PIPELINE, message, null);
    }
}
