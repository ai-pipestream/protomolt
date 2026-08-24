package ai.pipestream.proto.actions;

import ai.pipestream.proto.meta.DescriptorMetadata;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.Map;
import java.util.TreeMap;

/** Extracts the descriptive metadata bag declared on a message and its fields. */
final class ExtractMetadataAction implements ProtoAction {

    @Override
    public String name() {
        return "extract-metadata";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Extracts the descriptive metadata (ai.pipestream.proto.meta.v1 options: "
                + "description, owner, sensitivity, display name, labels) declared on a protobuf "
                + "message and its fields as a flat bag keyed 'message.*' and 'field.<name>.*'.";
    }

    @Override
    public ObjectNode inputSchema() {
        return CatalogContract.schemaFor("ExtractMetadataRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CatalogContract.check(input, "ExtractMetadataRequest", name());
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");
        Map<String, Object> bag = DescriptorMetadata.asBag(descriptor);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("type", descriptor.getFullName());
        // Map.copyOf in asBag drops insertion order; sort for a deterministic document.
        output.set("metadata", context.objectMapper().valueToTree(new TreeMap<>(bag)));
        return output;
    }
}
