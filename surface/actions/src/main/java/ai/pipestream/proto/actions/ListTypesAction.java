package ai.pipestream.proto.actions;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;
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
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Lists the protobuf types available in a schema — or, with no schema, in the shared "
                + "descriptor registry — as {fullName, file, kind} entries with field shapes for "
                + "messages; use it first to discover exact type names for the other actions.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("ListTypesRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("ListTypesResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // An omitted filter arrives as the empty string, which matches everything.
        String filter = Fields.string(input, "filter");
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        if (Fields.has(input, "schema")) {
            SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
            for (FileDescriptor file : schema.files()) {
                files.putIfAbsent(file.getName(), file);
            }
        } else {
            for (Descriptor descriptor : context.registry().registeredDescriptors()) {
                files.putIfAbsent(descriptor.getFile().getName(), descriptor.getFile());
            }
        }
        Reply output = Reply.of(responseType());
        for (FileDescriptor file : files.values()) {
            for (Descriptor message : file.getMessageTypes()) {
                addMessage(message, filter, output);
            }
            for (EnumDescriptor enumType : file.getEnumTypes()) {
                addNamed(enumType.getFullName(), file, "enum", filter, output);
            }
            for (ServiceDescriptor service : file.getServices()) {
                addNamed(service.getFullName(), file, "service", filter, output);
            }
        }
        return output.build();
    }

    private static void addMessage(Descriptor message, String filter, Reply types) {
        if (message.getOptions().getMapEntry()) {
            return;
        }
        if (matches(message.getFullName(), filter)) {
            Reply entry = types.append("types")
                    .set("fullName", message.getFullName())
                    .set("file", message.getFile().getName())
                    .set("kind", "message");
            for (FieldDescriptor field : message.getFields()) {
                Reply fieldEntry = entry.append("fields")
                        .set("name", field.getName())
                        .set("number", field.getNumber())
                        .set("type", field.getType().name().toLowerCase(Locale.ROOT))
                        .set("label", label(field));
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    fieldEntry.set("typeName", field.getMessageType().getFullName());
                } else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    fieldEntry.set("typeName", field.getEnumType().getFullName());
                }
                fieldEntry.build();
            }
            entry.build();
        }
        for (Descriptor nested : message.getNestedTypes()) {
            addMessage(nested, filter, types);
        }
        for (EnumDescriptor enumType : message.getEnumTypes()) {
            addNamed(enumType.getFullName(), message.getFile(), "enum", filter, types);
        }
    }

    private static void addNamed(String fullName, FileDescriptor file, String kind,
                                 String filter, Reply types) {
        if (!matches(fullName, filter)) {
            return;
        }
        types.append("types")
                .set("fullName", fullName)
                .set("file", file.getName())
                .set("kind", kind)
                .build();
    }

    private static boolean matches(String fullName, String filter) {
        return filter.isEmpty()
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
