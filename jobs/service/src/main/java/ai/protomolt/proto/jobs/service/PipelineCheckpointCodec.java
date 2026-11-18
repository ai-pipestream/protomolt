package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.Definition;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.pipeline.PipelineExecutor.BranchOutcome;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.Result;
import ai.protomolt.proto.pipeline.PipelineExecutor.StepOutcome;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineBranchOutcome;
import ai.protomolt.proto.pipeline.v1.PipelineFailureKind;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoint;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoints;
import ai.protomolt.proto.pipeline.v1.PipelineRunResult;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.pipeline.v1.PipelineStepOutcome;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts executor checkpoints and results to and from the protobuf messages a durable
 * pipeline run stores in {@code workflow_run.pipeline_checkpoints_pb} and
 * {@code pipeline_result_pb}. Step messages are stored as deterministic bytes and decoded
 * against the step's output type from the run's own descriptor set.
 */
final class PipelineCheckpointCodec {

    private PipelineCheckpointCodec() {
    }

    static List<Checkpoint> readCheckpoints(WorkflowRunRecord job, Definition definition) {
        if (job.pipelineCheckpoints == null) {
            return List.of();
        }
        PipelineRunCheckpoints stored;
        try {
            stored = PipelineRunCheckpoints.parseFrom(job.pipelineCheckpoints);
        } catch (InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "stored checkpoints are not a readable PipelineRunCheckpoints: "
                            + e.getMessage(), e);
        }
        return readCheckpoints(stored, definition);
    }

    static List<Checkpoint> readCheckpoints(PipelineRunCheckpoints stored,
                                            Definition definition) {
        Pipeline pipeline = definition.pipeline();
        if (stored.getCheckpointsCount() > pipeline.getStepsCount()) {
            throw new PipelineDefinitionException("",
                    "stored checkpoints (" + stored.getCheckpointsCount()
                            + ") outnumber pipeline steps (" + pipeline.getStepsCount() + ")");
        }
        List<Descriptor> outputTypes = stepOutputTypes(definition);
        List<Checkpoint> restored = new ArrayList<>(stored.getCheckpointsCount());
        for (int i = 0; i < stored.getCheckpointsCount(); i++) {
            restored.add(fromProto(
                    stored.getCheckpoints(i), i, pipeline.getSteps(i), outputTypes.get(i)));
        }
        return List.copyOf(restored);
    }

    static PipelineRunCheckpoints writeCheckpoints(List<Checkpoint> checkpoints) {
        PipelineRunCheckpoints.Builder stored = PipelineRunCheckpoints.newBuilder();
        for (Checkpoint checkpoint : checkpoints) {
            stored.addCheckpoints(toProto(checkpoint));
        }
        return stored.build();
    }

    static PipelineRunCheckpoint toProto(Checkpoint checkpoint) {
        PipelineRunCheckpoint.Builder stored = PipelineRunCheckpoint.newBuilder()
                .setPipelineFingerprint(checkpoint.pipelineFingerprint())
                .setInputFingerprint(checkpoint.inputFingerprint())
                .setOutcome(toProto(checkpoint.outcome()))
                .setCardinality(checkpoint.cardinality());
        for (DynamicMessage message : checkpoint.messages()) {
            stored.addMessages(ByteString.copyFrom(
                    MessageFingerprints.deterministicBytes(message)));
        }
        for (BranchOutcome branch : checkpoint.branches()) {
            stored.addBranches(toProto(branch));
        }
        return stored.build();
    }

    static PipelineRunResult writeResult(Result result, Definition definition) {
        PipelineRunResult.Builder stored = PipelineRunResult.newBuilder()
                .setCardinality(result.cardinality())
                .setOutputType(finalOutputType(definition).getFullName());
        for (DynamicMessage message : result.messages()) {
            stored.addMessages(ByteString.copyFrom(
                    MessageFingerprints.deterministicBytes(message)));
        }
        for (StepOutcome step : result.steps()) {
            stored.addSteps(toProto(step));
        }
        for (BranchOutcome branch : result.branches()) {
            stored.addBranches(toProto(branch));
        }
        return stored.build();
    }

    /** The messages of a stored result, decoded against the run's own descriptor set. */
    static List<DynamicMessage> readResultMessages(PipelineRunResult stored,
                                                   Definition definition) {
        Descriptor outputType = PipelineRunCodec.buildRegistry(definition.files(), null)
                .findDescriptorByFullName(stored.getOutputType());
        if (outputType == null) {
            throw new PipelineDefinitionException("",
                    "stored result type '" + stored.getOutputType()
                            + "' is absent from the run's descriptor set");
        }
        return decodeMessages(stored.getMessagesList(), outputType, "");
    }

    /** The type of the run's final binding: the declared output, or the last step's output. */
    static Descriptor finalOutputType(Definition definition) {
        Pipeline pipeline = definition.pipeline();
        if (pipeline.hasOutput()) {
            return PipelineRunCodec.buildRegistry(definition.files(), null)
                    .findDescriptorByFullName(pipeline.getOutput().getType());
        }
        if (pipeline.getStepsCount() == 0) {
            return definition.input().getDescriptorForType();
        }
        return stepOutputTypes(definition).getLast();
    }

    /** Each step's output type, in step order. */
    static List<Descriptor> stepOutputTypes(Definition definition) {
        DescriptorRegistry registry = PipelineRunCodec.buildRegistry(definition.files(), null);
        Map<String, Descriptor> scopeTypes = new LinkedHashMap<>();
        scopeTypes.put("input", definition.input().getDescriptorForType());
        List<Descriptor> types = new ArrayList<>(definition.pipeline().getStepsCount());
        for (PipelineStep step : definition.pipeline().getStepsList()) {
            Descriptor type = stepOutputType(step, definition.files(), registry, scopeTypes);
            scopeTypes.put(step.getName(), type);
            types.add(type);
        }
        return types;
    }

    private static Checkpoint fromProto(PipelineRunCheckpoint stored, int index,
                                        PipelineStep step, Descriptor stepOutputType) {
        String stepName = stored.getOutcome().getStep();
        if (!step.getName().equals(stepName)) {
            throw new PipelineDefinitionException(step.getName(),
                    "checkpoint " + index + " belongs to step '" + stepName
                            + "' but pipeline step " + index + " is '" + step.getName() + "'");
        }
        EdgeCardinality cardinality = stored.getCardinality();
        if (cardinality != EdgeCardinality.EDGE_CARDINALITY_ONE
                && cardinality != EdgeCardinality.EDGE_CARDINALITY_MANY) {
            throw new PipelineDefinitionException(stepName,
                    "invalid checkpoint cardinality '" + cardinality + "'");
        }
        List<BranchOutcome> branches = new ArrayList<>(stored.getBranchesCount());
        for (PipelineBranchOutcome branch : stored.getBranchesList()) {
            branches.add(fromProto(branch, stepName));
        }
        return new Checkpoint(
                stored.getPipelineFingerprint(),
                stored.getInputFingerprint(),
                new StepOutcome(stepName, stored.getOutcome().getSkipped(),
                        stored.getOutcome().getRequestCount(),
                        stored.getOutcome().getResponseCount()),
                cardinality,
                decodeMessages(stored.getMessagesList(), stepOutputType, stepName),
                branches);
    }

    private static List<DynamicMessage> decodeMessages(List<ByteString> stored,
                                                       Descriptor type, String stepName) {
        List<DynamicMessage> messages = new ArrayList<>(stored.size());
        for (ByteString bytes : stored) {
            if (bytes.isEmpty()) {
                // No bytes is the default instance, which is what a skipped step stores as
                // its placeholder. It is not parsed, because a proto2 type with required
                // fields refuses to parse empty; replay still refuses it for a step that ran.
                messages.add(DynamicMessage.getDefaultInstance(type));
                continue;
            }
            try {
                messages.add(DynamicMessage.parseFrom(type, bytes));
            } catch (InvalidProtocolBufferException e) {
                throw new PipelineDefinitionException(stepName,
                        "stored message cannot be decoded as " + type.getFullName() + ": "
                                + e.getMessage(), e);
            }
        }
        return messages;
    }

    private static PipelineStepOutcome toProto(StepOutcome outcome) {
        return PipelineStepOutcome.newBuilder()
                .setStep(outcome.name())
                .setSkipped(outcome.skipped())
                .setRequestCount(outcome.requestCount())
                .setResponseCount(outcome.responseCount())
                .build();
    }

    private static PipelineBranchOutcome toProto(BranchOutcome branch) {
        PipelineBranchOutcome.Builder stored = PipelineBranchOutcome.newBuilder()
                .setId(branch.id())
                .setIndex(branch.index())
                .setSucceeded(branch.succeeded())
                .setError(branch.error());
        if (branch.kind() != null) {
            stored.setKind(PipelineFailureKind.valueOf(
                    "PIPELINE_FAILURE_KIND_" + branch.kind().name()));
        }
        return stored.build();
    }

    private static BranchOutcome fromProto(PipelineBranchOutcome stored, String stepName) {
        FailureKind kind = null;
        if (stored.getKind() != PipelineFailureKind.PIPELINE_FAILURE_KIND_UNSPECIFIED) {
            if (stored.getKind() == PipelineFailureKind.UNRECOGNIZED) {
                throw new PipelineDefinitionException(stepName,
                        "invalid branch failure kind " + stored.getKindValue());
            }
            kind = FailureKind.valueOf(stored.getKind().name()
                    .substring("PIPELINE_FAILURE_KIND_".length()));
        }
        return new BranchOutcome(
                stored.getId(), stored.getIndex(), stored.getSucceeded(), kind,
                stored.getError());
    }

    private static Descriptor stepOutputType(PipelineStep step,
                                             List<FileDescriptor> files,
                                             DescriptorRegistry registry,
                                             Map<String, Descriptor> scopeTypes) {
        if (step.hasGrpcCall()) {
            if (step.getGrpcCall().hasFanOut()) {
                return registry.findDescriptorByFullName(
                        step.getGrpcCall().getFanOut().getCollectType());
            }
            return PipelineRunCodec.resolveMethod(
                    files, step.getGrpcCall().getMethod()).getOutputType();
        }
        if (step.hasStructured()) {
            return registry.findDescriptorByFullName(step.getStructured().hasFanOut()
                    ? step.getStructured().getFanOut().getCollectType()
                    : step.getStructured().getSpec().getTargetType());
        }
        if (step.hasCollect()) {
            return registry.findDescriptorByFullName(step.getCollect().getCollectType());
        }
        Descriptor source = scopeTypes.get(step.getUnnest().getSource());
        if (source == null) {
            throw new PipelineDefinitionException(step.getName(),
                    "unnest source '" + step.getUnnest().getSource() + "' is not in scope");
        }
        return resolveUnnestElementType(source, step.getUnnest().getPath(), step.getName());
    }

    private static Descriptor resolveUnnestElementType(Descriptor root, String path,
                                                       String stepName) {
        Descriptor current = root;
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            String segment = dot < 0 ? path.substring(start) : path.substring(start, dot);
            FieldDescriptor field = current.findFieldByName(segment);
            if (field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new PipelineDefinitionException(stepName,
                        "unnest path '" + path + "' does not resolve on " + root.getFullName());
            }
            current = field.getMessageType();
            if (dot < 0) {
                return current;
            }
            start = dot + 1;
        }
    }
}
