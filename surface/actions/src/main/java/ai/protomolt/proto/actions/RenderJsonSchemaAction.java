package ai.protomolt.proto.actions;

import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Map;

/** Renders the JSON Schema document for a protobuf message type. */
final class RenderJsonSchemaAction implements ProtoAction {

    @Override
    public String name() {
        return "render-json-schema";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Renders a JSON Schema document describing the canonical proto3 JSON shape of a "
                + "protobuf message type, folding declared validation rules into JSON Schema "
                + "constraints where they translate.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("RenderJsonSchemaRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("RenderJsonSchemaResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(type(input), "/type");
        Map<String, Object> document = ProtoJsonSchemaGenerator.create().generate(descriptor);
        // The schema document is nested under a named field rather than returned as the whole
        // envelope, so the response has a declared protobuf contract. A JSON Schema has no
        // protobuf shape of its own, so the document itself stays a structure.
        return Reply.of(responseType())
                .set("schema", context.objectMapper().valueToTree(document))
                .build();
    }

    /** The named type, or null when the caller left the schema's own default to apply. */
    private static String type(Message input) {
        String type = Fields.string(input, "type");
        return type.isEmpty() ? null : type;
    }
}
