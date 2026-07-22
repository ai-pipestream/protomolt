package ai.pipestream.proto.actions;

import ai.pipestream.proto.jsonschema.ProtoJsonSchemaGenerator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.Map;

/** Renders the JSON Schema document for a protobuf message type. */
final class RenderJsonSchemaAction implements ProtoAction {

    @Override
    public String name() {
        return "render-json-schema";
    }

    @Override
    public String description() {
        return "Renders a JSON Schema document describing the canonical proto3 JSON shape of a "
                + "protobuf message type, folding declared validation rules into JSON Schema "
                + "constraints where they translate.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.set("type", ActionJson.typeProperty(
                "Fully qualified message type to render; required unless the schema already "
                        + "identifies a single message."));
        ActionJson.required(schema, "schema");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");
        Map<String, Object> document = ProtoJsonSchemaGenerator.create().generate(descriptor);
        return context.objectMapper().valueToTree(document);
    }
}
