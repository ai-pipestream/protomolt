package ai.pipestream.proto.actions;

import ai.pipestream.proto.meta.DescriptorMetadata;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
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
    public Descriptor requestType() {
        return CatalogContract.request("ExtractMetadataRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("ExtractMetadataResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        String named = Fields.string(input, "type");
        Descriptor descriptor = schema.message(named.isEmpty() ? null : named, "/type");
        Map<String, Object> bag = DescriptorMetadata.asBag(descriptor);
        return Reply.of(responseType())
                .set("type", descriptor.getFullName())
                // Map.copyOf in asBag drops insertion order; sort for a deterministic document.
                .set("metadata", context.objectMapper().valueToTree(new TreeMap<>(bag)))
                .build();
    }
}
