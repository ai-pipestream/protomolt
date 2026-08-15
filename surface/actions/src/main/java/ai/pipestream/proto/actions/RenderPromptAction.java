package ai.pipestream.proto.actions;

import ai.pipestream.proto.json.MalformedProtobufJsonException;
import ai.pipestream.proto.prompt.Persona;
import ai.pipestream.proto.prompt.PromptPacket;
import ai.pipestream.proto.prompt.PromptRenderException;
import ai.pipestream.proto.prompt.PromptRenderer;
import ai.pipestream.proto.prompt.RenderPromptRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;

/** Renders the prompt packet for a protobuf message type: the complete LLM form-filling briefing. */
final class RenderPromptAction implements ProtoAction {

    @Override
    public String name() {
        return "render-prompt";
    }

    @Override
    public String description() {
        return "Renders a prompt packet for a protobuf message type: instruction prose built "
                + "from the schema's metadata, validation, quality and llm.v1 annotations, plus "
                + "the JSON Schema decoder constraint, plus an optional resolved persona. The "
                + "packet is the complete briefing for a model asked to fill the form.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.set("type", ActionJson.typeProperty(
                "Fully qualified message type to render; required unless the schema already "
                        + "identifies a single message."));
        properties.putObject("persona")
                .put("type", "object")
                .put("description", "The resolved persona to render for, as canonical proto3 "
                        + "JSON of ai.pipestream.proto.prompt.v1.Persona. Absent renders "
                        + "persona-free (schema instructions only).");
        properties.putObject("descriptor_set_ref")
                .put("type", "string")
                .put("description", "Opaque registry reference echoed into the packet so the "
                        + "receiver can rebuild the descriptor set; empty when the caller has "
                        + "no registry context.");
        ActionJson.required(schema, "schema");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");

        RenderPromptRequest.Builder request = RenderPromptRequest.newBuilder()
                .setTargetType(descriptor.getFullName());
        if (input.has("persona")) {
            ObjectNode personaNode = Inputs.requireObject(input, "persona");
            try {
                request.setPersona(context.transcoder()
                        .fromJson(personaNode.toString(), Persona.class));
            } catch (MalformedProtobufJsonException e) {
                ObjectNode details = JsonNodeFactory.instance.objectNode();
                details.put("pointer", "/persona");
                details.put("detail",
                        e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                throw new ActionException("invalid-persona",
                        "persona is not valid proto3 JSON for "
                                + Persona.getDescriptor().getFullName() + ": "
                                + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()),
                        details);
            }
        }

        String descriptorSetRef = Inputs.optionalString(input, "descriptor_set_ref");
        if (descriptorSetRef == null) {
            descriptorSetRef = "";
        }
        PromptPacket packet;
        try {
            packet = PromptRenderer.create().render(descriptor, request.build(), descriptorSetRef);
        } catch (PromptRenderException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("type", descriptor.getFullName());
            throw new ActionException("render-failed", e.getMessage(), details);
        }

        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("target_type", packet.getTargetType());
        output.put("descriptor_set_ref", packet.getDescriptorSetRef());
        output.put("instructions", packet.getInstructions());
        try {
            output.set("response_json_schema",
                    context.objectMapper().readTree(packet.getResponseJsonSchema()));
            if (packet.hasPersona()) {
                output.set("persona", context.objectMapper()
                        .readTree(context.transcoder().toJson(packet.getPersona())));
            }
            ArrayNode fewShot = output.putArray("few_shot");
            for (Any example : packet.getFewShotList()) {
                fewShot.add(context.objectMapper()
                        .readTree(context.transcoder().toJson(example)));
            }
        } catch (JsonProcessingException | MalformedProtobufJsonException e) {
            throw new ActionException("render-failed",
                    "rendered packet could not be serialized to JSON: " + e.getMessage());
        }
        return output;
    }
}
