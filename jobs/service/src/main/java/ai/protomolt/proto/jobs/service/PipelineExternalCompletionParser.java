package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Package-private proto3 JSON response parser for external-completion pipeline steps,
 * enforcing declared output cardinality, stream-size ceilings, and object element shapes.
 */
final class PipelineExternalCompletionParser {

    private PipelineExternalCompletionParser() {
    }

    static List<DynamicMessage> parseResponses(ObjectNode response,
                                               EdgeCardinality cardinality,
                                               Descriptor outputType,
                                               int maxStreamMessages,
                                               String stepName,
                                               List<FileDescriptor> files,
                                               DescriptorRegistry extraRegistry)
            throws MalformedProtobufJsonException {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(
                PipelineRunCodec.buildRegistry(files, extraRegistry));
        if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE) {
            return List.of(transcoder.fromJsonDynamic(response.toString(), outputType));
        }
        JsonNode itemsNode = response.get(PipelineCheckpointCodec.ITEMS_KEY);
        if (!(itemsNode instanceof ArrayNode items)) {
            throw new IllegalArgumentException(
                    "streaming external step '" + stepName
                            + "' requires 'response' object {\"items\": [...]}");
        }
        if (items.size() > maxStreamMessages) {
            throw new IllegalArgumentException(
                    "streaming external step '" + stepName + "' supplied " + items.size()
                            + " items, exceeding max_stream_messages " + maxStreamMessages);
        }
        List<DynamicMessage> parsed = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            JsonNode element = items.get(i);
            if (!element.isObject()) {
                throw new IllegalArgumentException(
                        "streaming external step '" + stepName + "' item " + i
                                + " must be a JSON object");
            }
            parsed.add(transcoder.fromJsonDynamic(element.toString(), outputType));
        }
        return List.copyOf(parsed);
    }
}
