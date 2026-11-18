package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
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
import com.google.protobuf.Message;
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
    record Documents(ObjectNode input, ArrayNode checkpoints, ObjectNode result) {
    }

    /** Prints a run's messages as proto3 JSON, resolving {@code Any} against its registry. */
    record Printer(DescriptorRegistry registry, ProtobufJsonTranscoder transcoder) {

        static Printer over(DescriptorRegistry registry) {
            return new Printer(registry, new ProtobufJsonTranscoder(registry));
        }
    }

    static Documents render(WorkflowRunRecord job) {
        Definition definition = PipelineRunCodec.readDefinition(job);
        Printer printer = Printer.over(
                PipelineRunCodec.buildRegistry(definition.files(), null));
        ArrayNode checkpoints = JSON.createArrayNode();
        for (Checkpoint checkpoint : PipelineCheckpointCodec.readCheckpoints(job, definition)) {
            checkpoints.add(checkpoint(checkpoint, printer));
        }
        return new Documents(
                asObject(definition.input(), printer),
                checkpoints,
                job.pipelineResult == null ? null : result(job, definition, printer));
    }

    private static ObjectNode checkpoint(Checkpoint checkpoint,
                                         Printer printer) {
        ObjectNode entry = JSON.createObjectNode();
        entry.put("name", checkpoint.outcome().name());
        entry.put("skipped", checkpoint.outcome().skipped());
        if (!checkpoint.outcome().skipped()) {
            entry.set("response",
                    binding(checkpoint.cardinality(), checkpoint.messages(), printer));
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

    private static ObjectNode result(WorkflowRunRecord job, Definition definition,
                                   Printer printer) {
        PipelineRunResult stored;
        try {
            stored = PipelineRunResult.parseFrom(job.pipelineResult);
        } catch (InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "stored result is not a readable PipelineRunResult: " + e.getMessage(), e);
        }
        return binding(stored.getCardinality(),
                PipelineCheckpointCodec.readResultMessages(stored, definition), printer);
    }

    /** One message as an object, or a stream as {@code {"items": [...]}}. */
    private static ObjectNode binding(EdgeCardinality cardinality,
                                      List<DynamicMessage> messages,
                                      Printer printer) {
        if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE) {
            return asObject(messages.getFirst(), printer);
        }
        ObjectNode stream = JSON.createObjectNode();
        ArrayNode items = stream.putArray(ITEMS_KEY);
        for (DynamicMessage message : messages) {
            items.add(message(message, printer));
        }
        return stream;
    }

    /**
     * One message where the reply needs an object: its proto3 JSON when that is an object,
     * and {@code {"value": <its proto3 JSON>}} for a type that prints as anything else.
     */
    static ObjectNode asObject(DynamicMessage message, Printer printer) {
        JsonNode printed = message(message, printer);
        if (printed instanceof ObjectNode object
                && (Proto3JsonShape.printsAsObject(message.getDescriptorForType())
                        || object.has(UNRENDERABLE_TYPE_KEY))) {
            return object;
        }
        ObjectNode carried = JSON.createObjectNode();
        carried.set(Proto3JsonShape.VALUE_KEY, printed);
        return carried;
    }

    /** A message's proto3 JSON, whatever JSON type its message type prints as. */
    static JsonNode message(DynamicMessage message, Printer printer) {
        String printed;
        try {
            printed = printer.transcoder().toJson(message);
        } catch (ProtobufJsonException unprintable) {
            // proto3 JSON cannot print an Any whose type is outside the run's descriptor set.
            // When that alone is why the message does not print, the view says so; the stored
            // bytes are unaffected. Any other printing failure is raised.
            Message withoutUndefinedAny = UnresolvedAnyTypes
                    .without(message, printer.registry())
                    .orElseThrow(() -> unprintable);
            try {
                printer.transcoder().toJson(withoutUndefinedAny);
            } catch (ProtobufJsonException alsoUnprintable) {
                unprintable.addSuppressed(alsoUnprintable);
                throw unprintable;
            }
            ObjectNode marker = JSON.createObjectNode();
            marker.put(UNRENDERABLE_TYPE_KEY, message.getDescriptorForType().getFullName());
            return marker;
        }
        try {
            return JSON.readTree(printed);
        } catch (JsonProcessingException notJson) {
            throw new IllegalStateException(
                    "proto3 JSON printer produced text that is not JSON for "
                            + message.getDescriptorForType().getFullName(), notJson);
        }
    }
}
