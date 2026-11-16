package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.http.json.ProtobufJsonException;
import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
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
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Package-private binary-faithful JSONB codec for durable pipeline checkpoints, branch
 * evidence, input previews, and results stored in {@code workflow_run}.
 */
final class PipelineCheckpointCodec {

    static final String PIPELINE_CHECKPOINT_KEY = "pipelineCheckpoint";
    static final String ITEMS_KEY = "items";
    static final String BINARY_BASE64_FALLBACK_KEY = "binaryBase64";

    private static final String NAME_KEY = "name";
    private static final String SKIPPED_KEY = "skipped";
    private static final String RESPONSE_KEY = "response";
    private static final String CARDINALITY_KEY = "cardinality";
    private static final String REQUEST_COUNT_KEY = "requestCount";
    private static final String RESPONSE_COUNT_KEY = "responseCount";
    private static final String MESSAGES_BASE64_KEY = "messagesBase64";
    private static final String BRANCHES_KEY = "branches";
    private static final String BRANCH_ID_KEY = "id";
    private static final String BRANCH_INDEX_KEY = "index";
    private static final String BRANCH_SUCCEEDED_KEY = "succeeded";
    private static final String BRANCH_KIND_KEY = "kind";
    private static final String BRANCH_ERROR_KEY = "error";

    private static final ObjectMapper JSON = new ObjectMapper();

    private PipelineCheckpointCodec() {
    }

    static List<Checkpoint> readCheckpoints(WorkflowRunRecord job, Definition definition) {
        String raw = job.checkpoints == null ? "[]" : job.checkpoints;
        JsonNode tree;
        try {
            tree = JSON.readTree(raw);
        } catch (JsonProcessingException e) {
            throw new PipelineDefinitionException("",
                    "stored checkpoints are not readable JSON: " + e.getMessage(), e);
        }
        if (!(tree instanceof ArrayNode array)) {
            throw new PipelineDefinitionException("", "stored checkpoints are not an array");
        }
        Pipeline pipeline = definition.pipeline();
        if (array.size() > pipeline.getStepsCount()) {
            throw new PipelineDefinitionException("",
                    "stored checkpoints (" + array.size() + ") outnumber pipeline steps ("
                            + pipeline.getStepsCount() + ")");
        }
        DescriptorRegistry registry = PipelineRunCodec.buildRegistry(definition.files(), null);
        Map<String, Descriptor> scopeTypes = new LinkedHashMap<>();
        scopeTypes.put("input", definition.input().getDescriptorForType());

        List<Checkpoint> restored = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            PipelineStep step = pipeline.getSteps(i);
            Descriptor stepOutputType = stepOutputType(
                    step, definition.files(), registry, scopeTypes);
            scopeTypes.put(step.getName(), stepOutputType);
            restored.add(readCheckpointEntry(array.get(i), i, step, stepOutputType));
        }
        return List.copyOf(restored);
    }

    static String writeCheckpoints(List<Checkpoint> checkpoints, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(
                PipelineRunCodec.buildRegistry(files, null));
        ArrayNode array = JSON.createArrayNode();
        for (Checkpoint checkpoint : checkpoints) {
            array.add(checkpointEntryNode(checkpoint, transcoder));
        }
        return array.toString();
    }

    static String writeCheckpointEntry(Checkpoint checkpoint, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(
                PipelineRunCodec.buildRegistry(files, null));
        return checkpointEntryNode(checkpoint, transcoder).toString();
    }

    static ObjectNode checkpointEntryNode(Checkpoint checkpoint,
                                          ProtobufJsonTranscoder transcoder) {
        ObjectNode entry = JSON.createObjectNode();
        entry.put(NAME_KEY, checkpoint.outcome().name());
        entry.put(SKIPPED_KEY, checkpoint.outcome().skipped());
        if (!checkpoint.outcome().skipped()) {
            if (checkpoint.cardinality() == EdgeCardinality.EDGE_CARDINALITY_ONE) {
                entry.set(RESPONSE_KEY, messageJsonObject(
                        checkpoint.messages().getFirst(), transcoder, JSON));
            } else {
                ObjectNode streamWrapper = JSON.createObjectNode();
                ArrayNode items = streamWrapper.putArray(ITEMS_KEY);
                for (DynamicMessage message : checkpoint.messages()) {
                    items.add(messageJsonObject(message, transcoder, JSON));
                }
                entry.set(RESPONSE_KEY, streamWrapper);
            }
        }
        ObjectNode meta = entry.putObject(PIPELINE_CHECKPOINT_KEY);
        meta.put(PipelineRunCodec.PIPELINE_FINGERPRINT_KEY, checkpoint.pipelineFingerprint());
        meta.put(PipelineRunCodec.INPUT_FINGERPRINT_KEY, checkpoint.inputFingerprint());
        meta.put(CARDINALITY_KEY, checkpoint.cardinality().name());
        meta.put(REQUEST_COUNT_KEY, checkpoint.outcome().requestCount());
        meta.put(RESPONSE_COUNT_KEY, checkpoint.outcome().responseCount());
        ArrayNode messagesBase64 = meta.putArray(MESSAGES_BASE64_KEY);
        for (DynamicMessage message : checkpoint.messages()) {
            messagesBase64.add(Base64.getEncoder().encodeToString(
                    MessageFingerprints.deterministicBytes(message)));
        }
        ArrayNode branchesNode = meta.putArray(BRANCHES_KEY);
        for (BranchOutcome branch : checkpoint.branches()) {
            ObjectNode b = branchesNode.addObject();
            b.put(BRANCH_ID_KEY, branch.id());
            b.put(BRANCH_INDEX_KEY, branch.index());
            b.put(BRANCH_SUCCEEDED_KEY, branch.succeeded());
            if (branch.kind() != null) {
                b.put(BRANCH_KIND_KEY, branch.kind().name());
            }
            b.put(BRANCH_ERROR_KEY, branch.error());
        }
        return entry;
    }

    static String writeResult(Result result, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(
                PipelineRunCodec.buildRegistry(files, null));
        if (result.cardinality() == EdgeCardinality.EDGE_CARDINALITY_ONE) {
            return messageJsonObject(result.message(), transcoder, JSON).toString();
        }
        ObjectNode streamWrapper = JSON.createObjectNode();
        ArrayNode items = streamWrapper.putArray(ITEMS_KEY);
        for (DynamicMessage message : result.messages()) {
            items.add(messageJsonObject(message, transcoder, JSON));
        }
        return streamWrapper.toString();
    }

    static String renderInputJson(DynamicMessage input, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(
                PipelineRunCodec.buildRegistry(files, null));
        return messageJsonObject(input, transcoder, JSON).toString();
    }

    static ObjectNode messageJsonObject(DynamicMessage message,
                                        ProtobufJsonTranscoder transcoder,
                                        ObjectMapper mapper) {
        String preview;
        try {
            preview = transcoder.toJson(message);
        } catch (ProtobufJsonException unrenderable) {
            // proto3 JSON cannot print an Any whose type is outside the pipeline's descriptor
            // closure; authoritative wire bytes are carried in Base64.
            ObjectNode fallback = mapper.createObjectNode();
            fallback.put(BINARY_BASE64_FALLBACK_KEY, Base64.getEncoder().encodeToString(
                    MessageFingerprints.deterministicBytes(message)));
            return fallback;
        }
        JsonNode tree;
        try {
            tree = mapper.readTree(preview);
        } catch (JsonProcessingException notJson) {
            throw new IllegalStateException(
                    "proto3 JSON printer produced text that is not JSON for "
                            + message.getDescriptorForType().getFullName(), notJson);
        }
        if (tree instanceof ObjectNode objectNode) {
            return objectNode;
        }
        throw new IllegalStateException(
                "proto3 JSON printer produced a non-object node for "
                        + message.getDescriptorForType().getFullName());
    }

    private static Checkpoint readCheckpointEntry(JsonNode entry, int index,
                                                  PipelineStep step,
                                                  Descriptor stepOutputType) {
        if (entry == null || !entry.isObject()) {
            throw new PipelineDefinitionException(step.getName(),
                    "checkpoint " + index + " is not a JSON object");
        }
        String stepName = entry.path(NAME_KEY).asText("");
        if (!step.getName().equals(stepName)) {
            throw new PipelineDefinitionException(step.getName(),
                    "checkpoint " + index + " belongs to step '" + stepName
                            + "' but pipeline step " + index + " is '" + step.getName() + "'");
        }
        boolean skipped = entry.path(SKIPPED_KEY).asBoolean(false);
        JsonNode meta = entry.get(PIPELINE_CHECKPOINT_KEY);
        if (meta == null || !meta.isObject()) {
            throw new PipelineDefinitionException(stepName,
                    "checkpoint '" + stepName + "' is missing pipelineCheckpoint metadata");
        }
        String cpPipelineFp = meta.path(PipelineRunCodec.PIPELINE_FINGERPRINT_KEY).asText("");
        String cpInputFp = meta.path(PipelineRunCodec.INPUT_FINGERPRINT_KEY).asText("");
        EdgeCardinality cardinality = parseCardinality(
                meta.path(CARDINALITY_KEY).asText(""), stepName);
        int requestCount = requireInt(meta, REQUEST_COUNT_KEY, stepName);
        int responseCount = requireInt(meta, RESPONSE_COUNT_KEY, stepName);
        List<DynamicMessage> messages = decodeMessages(
                meta.get(MESSAGES_BASE64_KEY), stepOutputType, stepName);
        List<BranchOutcome> branches = readBranches(meta.get(BRANCHES_KEY), stepName);
        return new Checkpoint(
                cpPipelineFp,
                cpInputFp,
                new StepOutcome(stepName, skipped, requestCount, responseCount),
                cardinality,
                messages,
                branches);
    }

    private static List<DynamicMessage> decodeMessages(JsonNode rawMessages,
                                                       Descriptor stepOutputType,
                                                       String stepName) {
        if (!(rawMessages instanceof ArrayNode messagesArray)) {
            throw new PipelineDefinitionException(stepName,
                    "checkpoint '" + stepName + "' messagesBase64 is not an array");
        }
        List<DynamicMessage> messages = new ArrayList<>(messagesArray.size());
        for (JsonNode item : messagesArray) {
            if (!item.isTextual()) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' message entry is not base64 text");
            }
            try {
                byte[] decoded = Base64.getDecoder().decode(item.asText());
                messages.add(DynamicMessage.parseFrom(stepOutputType, decoded));
            } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' message cannot be decoded as "
                                + stepOutputType.getFullName() + ": " + e.getMessage(), e);
            }
        }
        return messages;
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

    private static EdgeCardinality parseCardinality(String value, String stepName) {
        try {
            EdgeCardinality cardinality = EdgeCardinality.valueOf(value);
            if (cardinality == EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED
                    || cardinality == EdgeCardinality.UNRECOGNIZED) {
                throw new PipelineDefinitionException(stepName,
                        "invalid checkpoint cardinality '" + value + "'");
            }
            return cardinality;
        } catch (IllegalArgumentException e) {
            throw new PipelineDefinitionException(stepName,
                    "invalid checkpoint cardinality '" + value + "'", e);
        }
    }

    private static int requireInt(JsonNode parent, String field, String stepName) {
        JsonNode value = parent.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new PipelineDefinitionException(stepName,
                    "checkpoint '" + stepName + "' field '" + field + "' is not an int");
        }
        return value.intValue();
    }

    private static List<BranchOutcome> readBranches(JsonNode rawBranches, String stepName) {
        if (rawBranches == null || rawBranches.isNull()) {
            return List.of();
        }
        if (!(rawBranches instanceof ArrayNode array)) {
            throw new PipelineDefinitionException(stepName,
                    "checkpoint '" + stepName + "' branches is not an array");
        }
        List<BranchOutcome> branches = new ArrayList<>(array.size());
        for (JsonNode node : array) {
            if (!node.isObject()) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' branch entry is not an object");
            }
            String id = node.path(BRANCH_ID_KEY).asText("");
            int index = requireInt(node, BRANCH_INDEX_KEY, stepName);
            boolean succeeded = node.path(BRANCH_SUCCEEDED_KEY).asBoolean(false);
            FailureKind kind = null;
            JsonNode kindNode = node.get(BRANCH_KIND_KEY);
            if (kindNode != null && !kindNode.isNull()) {
                try {
                    kind = FailureKind.valueOf(kindNode.asText(""));
                } catch (IllegalArgumentException e) {
                    throw new PipelineDefinitionException(stepName,
                            "invalid branch failure kind '" + kindNode.asText("") + "'", e);
                }
            }
            JsonNode errorNode = node.get(BRANCH_ERROR_KEY);
            if (errorNode == null || !errorNode.isTextual()) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' branch error is missing");
            }
            branches.add(new BranchOutcome(id, index, succeeded, kind, errorNode.asText()));
        }
        return List.copyOf(branches);
    }
}
