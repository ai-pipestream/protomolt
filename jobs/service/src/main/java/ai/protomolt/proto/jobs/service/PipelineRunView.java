package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.http.json.ProtobufJsonException;
import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.Definition;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.pipeline.PipelineExecutor.BranchOutcome;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.PipelineRunResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;

/**
 * Renders a stored pipeline run as JSON for the {@code get-job} and {@code list-jobs} action
 * replies. This is one-way output: nothing here is read back, and the stored protobuf stays
 * the only record of the run.
 */
final class PipelineRunView {

    static final String ITEMS_KEY = "items";
    static final String UNRENDERABLE_TYPE_KEY = "unrenderableType";

    private static final ObjectMapper JSON = new ObjectMapper();

    private PipelineRunView() {
    }

    /**
     * The JSON view of one run.
     *
     * @param input the run's input message
     * @param checkpoints one entry per executed step
     * @param result the final output, or null until the run completes
     */
    record Documents(ObjectNode input, ArrayNode checkpoints, JsonNode result) {
    }

    static Documents render(WorkflowRunRecord job) {
        Definition definition = PipelineRunCodec.readDefinition(job);
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(
                PipelineRunCodec.buildRegistry(definition.files(), null));
        ArrayNode checkpoints = JSON.createArrayNode();
        for (Checkpoint checkpoint : PipelineCheckpointCodec.readCheckpoints(job, definition)) {
            checkpoints.add(checkpoint(checkpoint, transcoder));
        }
        return new Documents(
                message(definition.input(), transcoder),
                checkpoints,
                job.pipelineResult == null ? null : result(job, definition, transcoder));
    }

    private static ObjectNode checkpoint(Checkpoint checkpoint,
                                         ProtobufJsonTranscoder transcoder) {
        ObjectNode entry = JSON.createObjectNode();
        entry.put("name", checkpoint.outcome().name());
        entry.put("skipped", checkpoint.outcome().skipped());
        if (!checkpoint.outcome().skipped()) {
            entry.set("response",
                    binding(checkpoint.cardinality(), checkpoint.messages(), transcoder));
        }
        entry.put("cardinality", checkpoint.cardinality().name());
        entry.put("requestCount", checkpoint.outcome().requestCount());
        entry.put("responseCount", checkpoint.outcome().responseCount());
        ArrayNode branches = entry.putArray("branches");
        for (BranchOutcome branch : checkpoint.branches()) {
            ObjectNode node = branches.addObject();
            node.put("id", branch.id());
            node.put("index", branch.index());
            node.put("succeeded", branch.succeeded());
            if (branch.kind() != null) {
                node.put("kind", branch.kind().name());
            }
            node.put("error", branch.error());
        }
        return entry;
    }

    private static JsonNode result(WorkflowRunRecord job, Definition definition,
                                   ProtobufJsonTranscoder transcoder) {
        PipelineRunResult stored;
        try {
            stored = PipelineRunResult.parseFrom(job.pipelineResult);
        } catch (InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "stored result is not a readable PipelineRunResult: " + e.getMessage(), e);
        }
        return binding(stored.getCardinality(),
                PipelineCheckpointCodec.readResultMessages(stored, definition), transcoder);
    }

    /** One message as an object, or a stream as {@code {"items": [...]}}. */
    private static ObjectNode binding(EdgeCardinality cardinality,
                                      List<DynamicMessage> messages,
                                      ProtobufJsonTranscoder transcoder) {
        if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE) {
            return message(messages.getFirst(), transcoder);
        }
        ObjectNode stream = JSON.createObjectNode();
        ArrayNode items = stream.putArray(ITEMS_KEY);
        for (DynamicMessage message : messages) {
            items.add(message(message, transcoder));
        }
        return stream;
    }

    private static ObjectNode message(DynamicMessage message,
                                      ProtobufJsonTranscoder transcoder) {
        String printed;
        try {
            printed = transcoder.toJson(message);
        } catch (ProtobufJsonException unrenderable) {
            // proto3 JSON cannot print an Any whose type is outside the run's descriptor set.
            // The view says so; the stored bytes are unaffected.
            ObjectNode marker = JSON.createObjectNode();
            marker.put(UNRENDERABLE_TYPE_KEY, message.getDescriptorForType().getFullName());
            return marker;
        }
        JsonNode tree;
        try {
            tree = JSON.readTree(printed);
        } catch (JsonProcessingException notJson) {
            throw new IllegalStateException(
                    "proto3 JSON printer produced text that is not JSON for "
                            + message.getDescriptorForType().getFullName(), notJson);
        }
        if (tree instanceof ObjectNode object) {
            return object;
        }
        throw new IllegalStateException(
                "proto3 JSON printer produced a non-object node for "
                        + message.getDescriptorForType().getFullName());
    }
}
