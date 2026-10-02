package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** Schema-source facade. Install protomolt-actions-schema to compile and link schemas. */
public final class SchemaResolver {
    private SchemaResolver() {}

    public static ResolvedSchema resolve(ObjectNode input, String field, ActionContext context)
            throws ActionException {
        return provider().resolve(input, field, context);
    }

    public static ResolvedSchema resolve(Message request, String field, ActionContext context)
            throws ActionException {
        return resolveSource(Fields.message(request, field), "/" + field, context);
    }

    public static ResolvedSchema resolveSource(Message schema, String pointer, ActionContext context)
            throws ActionException {
        return provider().resolveSource(schema, pointer, context);
    }

    private static SchemaResolverProvider provider() {
        var providers = ServiceLoader.load(SchemaResolverProvider.class).stream().toList();
        if (providers.size() != 1) {
            throw new IllegalStateException("Schema resolution requires exactly one SchemaResolverProvider; found "
                    + providers.size() + ". Install protomolt-actions-schema or select one implementation.");
        }
        return providers.getFirst().get();
    }

    static ActionException unknownType(String typeName, List<String> available, String pointer) {
        String simpleName = simpleName(typeName);
        List<String> suggestions = available.stream()
                .filter(name -> simpleName(name).equalsIgnoreCase(simpleName))
                .distinct()
                .sorted()
                .toList();
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("type", typeName);
        details.put("pointer", pointer);
        ArrayNode suggestionsNode = details.putArray("suggestions");
        suggestions.forEach(suggestionsNode::add);
        String message = "Unknown type '" + typeName + "'"
                + (suggestions.isEmpty()
                        ? ""
                        : "; did you mean " + String.join(", ", suggestions) + "?");
        return new ActionException("unknown-type", message, details);
    }

    private static String simpleName(String fullName) {
        return fullName.substring(fullName.lastIndexOf('.') + 1);
    }

    static List<Descriptor> allMessages(List<FileDescriptor> files) {
        List<Descriptor> messages = new ArrayList<>();
        for (FileDescriptor file : files) {
            for (Descriptor message : file.getMessageTypes()) {
                addMessages(message, messages);
            }
        }
        return messages;
    }

    private static void addMessages(Descriptor message, List<Descriptor> out) {
        if (message.getOptions().getMapEntry()) {
            return;
        }
        out.add(message);
        for (Descriptor nested : message.getNestedTypes()) {
            addMessages(nested, out);
        }
    }

    /**
     * A resolved schema: the encoded descriptor set (diff/compat input), the linked files, and
     * the default message when the schema unambiguously targets one.
     */
    public record ResolvedSchema(FileDescriptorSet descriptorSet,
                                 List<FileDescriptor> files,
                                 Descriptor defaultMessage) {

        public ResolvedSchema {
            files = List.copyOf(files);
        }

        /**
         * The message descriptor a message-level action should operate on: {@code typeName} when
         * given, otherwise the schema's unambiguous default.
         *
         * @throws ActionException {@code unknown-type} (with same-simple-name suggestions) or
         *         {@code invalid-input} when no type was given and none is implied
         */
        public Descriptor message(String typeName, String pointer) throws ActionException {
            if (typeName == null) {
                if (defaultMessage != null) {
                    return defaultMessage;
                }
                throw Inputs.invalidInput(
                        "A message 'type' is required because the schema does not identify a single message",
                        pointer);
            }
            Descriptor found = findMessage(typeName);
            if (found != null) {
                return found;
            }
            throw unknownType(typeName,
                    allMessages(files).stream().map(Descriptor::getFullName).toList(), pointer);
        }

        /**
         * The descriptor for a type this schema carries, or null when it does not. Unlike
         * {@link #message}, an absent type is an answer rather than an error: callers
         * resolving a packed {@code Any} payload are asking whether the schema happens to
         * describe it.
         */
        public Descriptor findMessage(String typeName) {
            for (Descriptor message : allMessages(files)) {
                if (message.getFullName().equals(typeName)) {
                    return message;
                }
            }
            return null;
        }
    }
}
