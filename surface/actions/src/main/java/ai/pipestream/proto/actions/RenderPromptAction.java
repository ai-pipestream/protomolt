package ai.pipestream.proto.actions;

import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import ai.pipestream.proto.prompt.Persona;
import ai.pipestream.proto.prompt.PromptPacket;
import ai.pipestream.proto.prompt.PromptRenderException;
import ai.pipestream.proto.prompt.PromptRenderer;
import ai.pipestream.proto.prompt.RenderPromptRequest;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/** Renders the prompt packet for a protobuf message type: the complete LLM form-filling briefing. */
final class RenderPromptAction implements ProtoAction {

    @Override
    public String name() {
        return "render-prompt";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Renders a prompt packet for a protobuf message type: instruction prose built "
                + "from the schema's metadata, validation, quality and llm.v1 annotations, plus "
                + "the JSON Schema decoder constraint, plus an optional resolved persona. The "
                + "packet is the complete briefing for a model asked to fill the form.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("RenderPromptRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("RenderPromptResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(
                SynthesizeShapeAction.named(input, "type"), "/type");

        RenderPromptRequest.Builder request = RenderPromptRequest.newBuilder()
                .setTargetType(descriptor.getFullName());
        if (Fields.has(input, "persona")) {
            // The persona is a structure here: prompt.v1 owns its shape, not this contract.
            try {
                request.setPersona(context.transcoder()
                        .fromJson(Fields.json(input, "persona").toString(), Persona.class));
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

        String descriptorSetRef = Fields.string(input, "descriptorSetRef");
        PromptPacket packet;
        try {
            packet = PromptRenderer.create().render(descriptor, request.build(), descriptorSetRef);
        } catch (PromptRenderException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("type", descriptor.getFullName());
            throw new ActionException("render-failed", e.getMessage(), details);
        }

        Reply output = Reply.of(responseType())
                .set("targetType", packet.getTargetType())
                .set("descriptorSetRef", packet.getDescriptorSetRef())
                .set("instructions", packet.getInstructions());
        try {
            output.set("responseJsonSchema", packet.getResponseJsonSchema());
            if (packet.hasPersona()) {
                output.set("persona", context.transcoder().toJson(packet.getPersona()));
            }
            for (Any example : packet.getFewShotList()) {
                output.add("fewShot", context.transcoder().toJson(example));
            }
        } catch (MalformedProtobufJsonException e) {
            throw new ActionException("render-failed",
                    "rendered packet could not be serialized to JSON: " + e.getMessage());
        }
        return output.build();
    }
}
