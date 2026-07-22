package ai.pipestream.proto.actions;

import ai.pipestream.proto.index.spi.ProtoOptionsIndexingHintSource;
import ai.pipestream.proto.meta.DescriptorMetadata;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoCompilationException;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The shared schema-source convention: wherever an action needs a schema it accepts a
 * {@code schema} object with exactly one of
 * <ul>
 *   <li>{@code {"type": "fully.qualified.Name"}} — resolved via the context registry,</li>
 *   <li>{@code {"sources": {"path.proto": "syntax = ...", ...}, "root": "path.proto"}} —
 *       compiled inline per call,</li>
 *   <li>{@code {"descriptorSetBase64": "..."}} — a serialized
 *       {@code google.protobuf.FileDescriptorSet}.</li>
 * </ul>
 *
 * <p>Inline and descriptor-set forms are re-parsed with the toolkit's option extensions
 * (validation rules, descriptor metadata, indexing hints) registered, so custom options carried
 * as unknown fields become readable extensions and every option-driven action behaves the same
 * regardless of how the schema arrived.</p>
 */
public final class SchemaResolver {

    private static final ExtensionRegistry EXTENSIONS = extensions();
    private static final Map<String, FileDescriptor> WELL_KNOWN = wellKnown();

    private SchemaResolver() {
    }

    /** Resolves the schema object held in {@code input.<field>}. */
    public static ResolvedSchema resolve(ObjectNode input, String field, ActionContext context)
            throws ActionException {
        JsonNode node = input.get(field);
        return resolveNode(node, "/" + field, context);
    }

    static ResolvedSchema resolveNode(JsonNode node, String pointer, ActionContext context)
            throws ActionException {
        if (node == null || !node.isObject()) {
            throw Inputs.invalidInput(
                    "Schema must be an object with exactly one of 'type', 'sources', 'descriptorSetBase64'",
                    pointer);
        }
        ObjectNode schema = (ObjectNode) node;
        int forms = (schema.has("type") ? 1 : 0)
                + (schema.has("sources") ? 1 : 0)
                + (schema.has("descriptorSetBase64") ? 1 : 0);
        if (forms != 1) {
            throw Inputs.invalidInput(
                    "Schema must contain exactly one of 'type', 'sources', 'descriptorSetBase64' but had "
                            + forms,
                    pointer);
        }
        if (schema.has("type")) {
            return fromRegistry(Inputs.requireString(schema, "type"), pointer + "/type", context);
        }
        if (schema.has("sources")) {
            return fromSources(schema, pointer, context);
        }
        return fromDescriptorSet(Inputs.requireString(schema, "descriptorSetBase64"),
                pointer + "/descriptorSetBase64");
    }

    // ---- the three source forms ----

    private static ResolvedSchema fromRegistry(String typeName, String pointer, ActionContext context)
            throws ActionException {
        Descriptor descriptor = context.registry().findDescriptorByFullName(typeName);
        if (descriptor == null) {
            List<String> available = context.registry().registeredDescriptors().stream()
                    .map(Descriptor::getFullName)
                    .toList();
            throw unknownType(typeName, available, pointer);
        }
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        collect(descriptor.getFile(), byName);
        FileDescriptorSet set = FileDescriptorSet.newBuilder().addAllFile(byName.values()).build();
        List<FileDescriptor> files = transitiveFiles(descriptor.getFile());
        return new ResolvedSchema(set, files, descriptor);
    }

    private static ResolvedSchema fromSources(ObjectNode schema, String pointer, ActionContext context)
            throws ActionException {
        JsonNode sourcesNode = schema.get("sources");
        if (!sourcesNode.isObject()) {
            throw Inputs.invalidInput("'sources' must map proto import paths to file contents",
                    pointer + "/sources");
        }
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        for (Map.Entry<String, JsonNode> entry : sourcesNode.properties()) {
            if (!entry.getValue().isTextual()) {
                throw Inputs.invalidInput("Source file contents must be strings",
                        pointer + "/sources/" + entry.getKey());
            }
            builder.add(entry.getKey(), entry.getValue().asText(), "inline");
        }
        ProtoSourceSet sources = builder.build();
        if (sources.isEmpty()) {
            throw Inputs.invalidInput("'sources' must contain at least one proto file",
                    pointer + "/sources");
        }
        String root = Inputs.optionalString(schema, "root");
        if (root != null) {
            if (!sources.contains(root)) {
                throw Inputs.invalidInput("'root' must name a file present in 'sources'",
                        pointer + "/root");
            }
            sources = sources.reachableFrom(root);
        }
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(sources);
        } catch (ProtoCompilationException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("pointer", pointer + "/sources");
            details.put("error", e.getMessage());
            throw new ActionException("compile-failed",
                    "Failed to compile proto sources: " + e.getMessage(), details);
        }
        // Re-parse with the option extensions registered so custom options (validation rules,
        // metadata, indexing hints) are readable extensions instead of unknown fields.
        FileDescriptorSet normalized = reparse(compiled.descriptorSet().toByteArray(), pointer);
        List<FileDescriptor> files = link(normalized, pointer);
        return new ResolvedSchema(normalized, files, defaultMessage(files, root));
    }

    private static ResolvedSchema fromDescriptorSet(String base64, String pointer)
            throws ActionException {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput("'descriptorSetBase64' is not valid base64: " + e.getMessage(),
                    pointer);
        }
        FileDescriptorSet set = reparse(bytes, pointer);
        List<FileDescriptor> files = link(set, pointer);
        return new ResolvedSchema(set, files, defaultMessage(files, null));
    }

    // ---- shared machinery ----

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

    private static FileDescriptorSet reparse(byte[] bytes, String pointer) throws ActionException {
        try {
            // Materializing meta.v1 json_name annotations restores original JSON keys
            // that source-level round-trips drop from the descriptor's own json_name.
            return DescriptorMetadata.materializeJsonNames(
                    FileDescriptorSet.parseFrom(bytes, EXTENSIONS));
        } catch (InvalidProtocolBufferException e) {
            throw Inputs.invalidInput(
                    "Not a serialized google.protobuf.FileDescriptorSet: " + e.getMessage(), pointer);
        }
    }

    /** Links every file in the set, resolving missing imports from the well-known fallback. */
    private static List<FileDescriptor> link(FileDescriptorSet set, String pointer)
            throws ActionException {
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            byName.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            build(proto, byName, built, new LinkedHashSet<>(), pointer);
        }
        return List.copyOf(built.values());
    }

    private static FileDescriptor build(
            FileDescriptorProto proto,
            Map<String, FileDescriptorProto> byName,
            Map<String, FileDescriptor> built,
            Set<String> building,
            String pointer) throws ActionException {
        FileDescriptor done = built.get(proto.getName());
        if (done != null) {
            return done;
        }
        if (!building.add(proto.getName())) {
            throw Inputs.invalidInput("Cyclic proto import involving '" + proto.getName() + "'",
                    pointer);
        }
        List<FileDescriptor> dependencies = new ArrayList<>();
        for (String dependency : proto.getDependencyList()) {
            FileDescriptorProto dependencyProto = byName.get(dependency);
            if (dependencyProto != null) {
                dependencies.add(build(dependencyProto, byName, built, building, pointer));
            } else {
                FileDescriptor known = WELL_KNOWN.get(dependency);
                if (known == null) {
                    throw Inputs.invalidInput("Unresolved import '" + dependency + "' required by '"
                            + proto.getName() + "'", pointer);
                }
                dependencies.add(known);
            }
        }
        try {
            FileDescriptor file = FileDescriptor.buildFrom(
                    proto, dependencies.toArray(new FileDescriptor[0]));
            built.put(proto.getName(), file);
            return file;
        } catch (DescriptorValidationException e) {
            throw Inputs.invalidInput("Descriptor set does not link: " + e.getMessage(), pointer);
        }
    }

    private static List<FileDescriptor> transitiveFiles(FileDescriptor file) {
        Map<String, FileDescriptor> out = new LinkedHashMap<>();
        collectFiles(file, out);
        return List.copyOf(out.values());
    }

    private static void collectFiles(FileDescriptor file, Map<String, FileDescriptor> out) {
        if (out.putIfAbsent(file.getName(), file) != null) {
            return;
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collectFiles(dependency, out);
        }
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptorProto> out) {
        if (out.putIfAbsent(file.getName(), file.toProto()) != null) {
            return;
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collect(dependency, out);
        }
    }

    /**
     * The message a type-less action call targets: the sole top-level message of the root file,
     * or — with no root — the sole user message in the whole schema. {@code null} when ambiguous.
     */
    private static Descriptor defaultMessage(List<FileDescriptor> files, String root) {
        List<Descriptor> candidates = new ArrayList<>();
        for (FileDescriptor file : files) {
            if (root != null ? !file.getName().equals(root) : file.getName().startsWith("google/protobuf/")) {
                continue;
            }
            candidates.addAll(file.getMessageTypes());
        }
        return candidates.size() == 1 ? candidates.get(0) : null;
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

    private static ExtensionRegistry extensions() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ValidationResult.registerExtensions(registry);
        DescriptorMetadata.registerExtensions(registry);
        ProtoOptionsIndexingHintSource.registerExtensions(registry);
        return registry;
    }

    private static Map<String, FileDescriptor> wellKnown() {
        Map<String, FileDescriptor> map = new LinkedHashMap<>();
        for (FileDescriptor file : List.of(
                com.google.protobuf.Any.getDescriptor().getFile(),
                com.google.protobuf.Struct.getDescriptor().getFile(),
                com.google.protobuf.Timestamp.getDescriptor().getFile(),
                com.google.protobuf.Duration.getDescriptor().getFile(),
                com.google.protobuf.Empty.getDescriptor().getFile(),
                com.google.protobuf.FieldMask.getDescriptor().getFile(),
                com.google.protobuf.StringValue.getDescriptor().getFile(),
                com.google.protobuf.DescriptorProtos.getDescriptor(),
                ai.pipestream.proto.validate.ValidateProto.getDescriptor(),
                ai.pipestream.proto.meta.MetadataProto.getDescriptor(),
                ai.pipestream.proto.index.hints.IndexingHintsProto.getDescriptor())) {
            map.put(file.getName(), file);
        }
        return Map.copyOf(map);
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
