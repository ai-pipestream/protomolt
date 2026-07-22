package ai.pipestream.proto.actions;

import ai.pipestream.proto.shapes.SchemaInferrer;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Struct-to-proto: reverse-engineers a message type from data-rich JSON samples and returns
 * it exactly like the other shape verbs — registrable proto source plus the linked
 * descriptor set.
 */
final class InferSchemaAction implements ProtoAction {

    @Override
    public String name() {
        return "infer-schema";
    }

    @Override
    public String description() {
        return "Infers a proto definition from one or more JSON sample documents: objects "
                + "become nested messages, arrays become repeated fields with element "
                + "inference, numbers become int64 when integral across every sample and "
                + "double otherwise, and anything genuinely dynamic (mixed types, empty "
                + "arrays, null-only keys) falls back to google.protobuf.Value. Keys are "
                + "sanitized to field names with json_name preserving the original, so the "
                + "inferred schema parses the very documents it came from. Returns proto "
                + "source and a descriptor set, ready to register or feed other verbs.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("name")
                .put("type", "string")
                .put("description", "Fully qualified name for the inferred message, e.g. "
                        + "'inferred.v1.Event'.");
        ObjectNode samples = properties.putObject("samples");
        samples.put("type", "array");
        samples.put("description", "Sample documents (JSON objects). More samples make a "
                + "better schema: keys union and the numeric heuristic sees every "
                + "occurrence.");
        samples.putObject("items").put("type", "object");
        ActionJson.required(schema, "name", "samples");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String name = Inputs.requireString(input, "name");
        ArrayNode samplesNode = Inputs.optionalArray(input, "samples");
        if (samplesNode == null || samplesNode.isEmpty()) {
            throw Inputs.invalidInput("'samples' must be a non-empty array", "/samples");
        }
        List<Struct> samples = new ArrayList<>(samplesNode.size());
        for (int i = 0; i < samplesNode.size(); i++) {
            JsonNode node = samplesNode.get(i);
            if (!node.isObject()) {
                throw Inputs.invalidInput("Each sample must be a JSON object", "/samples/" + i);
            }
            Struct.Builder struct = Struct.newBuilder();
            try {
                JsonFormat.parser().merge(node.toString(), struct);
            } catch (Exception e) {
                throw Inputs.invalidInput("Sample does not parse: " + e.getMessage(),
                        "/samples/" + i);
            }
            samples.add(struct.build());
        }
        ShapeSynthesizer.SynthesizedShape shape;
        try {
            shape = new SchemaInferrer().infer(name, samples);
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput(e.getMessage(), "/samples");
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("type", shape.type().getFullName());
        output.put("file", shape.file().getName());
        output.put("protoSource", shape.protoSource());
        output.put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(shape.descriptorSet().toByteArray()));
        return output;
    }
}
