package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Lists the types a schema (or the shared registry) declares — the LLM's grounding verb. */
final class ListTypesAction implements ProtoAction {

    @Override
    public String name() {
        return "list-types";
    }

    @Override
    public String description() {
        return "Lists the protobuf types available in a schema — or, with no schema, in the shared "
                + "descriptor registry — as {fullName, file, kind} entries with field shapes for "
                + "messages; use it first to discover exact type names for the other actions.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.putObject("filter")
                .put("type", "string")
                .put("description",
                        "Optional case-insensitive substring filter on the fully qualified type name.");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String filter = Inputs.optionalString(input, "filter");
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        if (input.has("schema")) {
            SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
            for (FileDescriptor file : schema.files()) {
                files.putIfAbsent(file.getName(), file);
            }
        } else {
            for (Descriptor descriptor : context.registry().registeredDescriptors()) {
                files.putIfAbsent(descriptor.getFile().getName(), descriptor.getFile());
            }
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        ArrayNode types = output.putArray("types");
        for (FileDescriptor file : files.values()) {
            for (Descriptor message : file.getMessageTypes()) {
                addMessage(message, filter, types);
            }
            for (EnumDescriptor enumType : file.getEnumTypes()) {
                addNamed(enumType.getFullName(), file, "enum", filter, types);
            }
            for (ServiceDescriptor service : file.getServices()) {
                addNamed(service.getFullName(), file, "service", filter, types);
            }
        }
        return output;
    }

    private static void addMessage(Descriptor message, String filter, ArrayNode types) {
        if (message.getOptions().getMapEntry()) {
            return;
        }
        if (matches(message.getFullName(), filter)) {
            ObjectNode entry = types.addObject();
            entry.put("fullName", message.getFullName());
            entry.put("file", message.getFile().getName());
            entry.put("kind", "message");
            ArrayNode fields = entry.putArray("fields");
            for (FieldDescriptor field : message.getFields()) {
                ObjectNode fieldNode = fields.addObject();
                fieldNode.put("name", field.getName());
                fieldNode.put("number", field.getNumber());
                fieldNode.put("type", field.getType().name().toLowerCase(Locale.ROOT));
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    fieldNode.put("typeName", field.getMessageType().getFullName());
                } else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    fieldNode.put("typeName", field.getEnumType().getFullName());
                }
                fieldNode.put("label", label(field));
            }
        }
        for (Descriptor nested : message.getNestedTypes()) {
            addMessage(nested, filter, types);
        }
        for (EnumDescriptor enumType : message.getEnumTypes()) {
            addNamed(enumType.getFullName(), message.getFile(), "enum", filter, types);
        }
    }

    private static void addNamed(String fullName, FileDescriptor file, String kind,
                                 String filter, ArrayNode types) {
        if (!matches(fullName, filter)) {
            return;
        }
        ObjectNode entry = types.addObject();
        entry.put("fullName", fullName);
        entry.put("file", file.getName());
        entry.put("kind", kind);
    }

    private static boolean matches(String fullName, String filter) {
        return filter == null
                || fullName.toLowerCase(Locale.ROOT).contains(filter.toLowerCase(Locale.ROOT));
    }

    private static String label(FieldDescriptor field) {
        return switch (field.toProto().getLabel()) {
            case LABEL_REPEATED -> "repeated";
            case LABEL_REQUIRED -> "required";
            case LABEL_OPTIONAL -> "optional";
        };
    }
}
