package ai.pipestream.proto.emit.okf;

import ai.pipestream.proto.emit.Bundle;
import ai.pipestream.proto.meta.DescriptorMetadata;
import ai.pipestream.proto.meta.FieldMeta;
import ai.pipestream.proto.meta.MessageMeta;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Renders protobuf schemas as an Open Knowledge Format (OKF v0.1) bundle: plain markdown
 * files with YAML frontmatter, one concept per message, enum, and service, cross-linked into
 * the knowledge graph agents and catalogs consume. The schema is the source of truth — the
 * {@code ai.pipestream.proto.meta.v1} annotations become frontmatter (description, owner,
 * labels) and schema-table columns (per-field descriptions, sensitivity classes), and every
 * message-typed field links to its type's concept document.
 *
 * <p>Bundle layout: a root {@code index.md} (declaring {@code okf_version}), one directory
 * per concept kind ({@code messages/}, {@code enums/}, {@code services/}, and
 * {@code subjects/} for registry renders), each with its own {@code index.md}. Concept
 * files are named by protobuf full name, so links are stable across renders.</p>
 */
public final class OkfRenderer {

    /**
     * @param title the root index heading
     * @param registryUrl base URL of the registry the bundle describes; fills subject
     *                    {@code resource} frontmatter when present
     */
    public record Options(String title, String registryUrl) {

        public Options {
            title = title == null || title.isBlank() ? "Schema knowledge bundle" : title;
        }

        public static Options defaults() {
            return new Options(null, null);
        }
    }

    /** Option-carrier packages whose types describe schemas rather than being the schema. */
    private static final List<String> INTERNAL_PACKAGES = List.of(
            "google.protobuf",
            "ai.pipestream.proto.meta.v1",
            "ai.pipestream.proto.validate.v1",
            "ai.pipestream.proto.index.v1");

    /** Renders every user-defined type and service in {@code files} as one bundle. */
    public Bundle render(List<FileDescriptor> files, Options options) {
        Objects.requireNonNull(options, "options");
        Model model = Model.of(files);
        Bundle.Builder bundle = Bundle.builder();
        bundle.add("index.md", rootIndex(model, options, Map.of()));
        renderKinds(model, bundle);
        return bundle.build();
    }

    Bundle render(Model model, Options options, Map<String, String> subjectDocs) {
        Bundle.Builder bundle = Bundle.builder();
        bundle.add("index.md", rootIndex(model, options, subjectDocs));
        if (!subjectDocs.isEmpty()) {
            StringBuilder index = new StringBuilder("# Subjects\n\n");
            subjectDocs.forEach((path, content) -> {
                bundle.add(path, content);
                index.append("* [").append(titleOf(content)).append("](/").append(path)
                        .append(") - ").append(descriptionOf(content)).append('\n');
            });
            bundle.add("subjects/index.md", index.toString());
        }
        renderKinds(model, bundle);
        return bundle.build();
    }

    private void renderKinds(Model model, Bundle.Builder bundle) {
        if (!model.messages.isEmpty()) {
            bundle.add("messages/index.md", kindIndex("Messages", model.messages.values(),
                    d -> conceptPath(d.getFullName(), "messages"),
                    d -> DescriptorMetadata.message(d).map(MessageMeta::getDescription)
                            .filter(s -> !s.isBlank()).orElse("Protobuf message type.")));
            for (Descriptor message : model.messages.values()) {
                bundle.add(conceptPath(message.getFullName(), "messages"),
                        messageConcept(message, model));
            }
        }
        if (!model.enums.isEmpty()) {
            bundle.add("enums/index.md", kindIndex("Enums", model.enums.values(),
                    e -> conceptPath(e.getFullName(), "enums"),
                    e -> "Protobuf enum type."));
            for (EnumDescriptor enumType : model.enums.values()) {
                bundle.add(conceptPath(enumType.getFullName(), "enums"), enumConcept(enumType));
            }
        }
        if (!model.services.isEmpty()) {
            bundle.add("services/index.md", kindIndex("Services", model.services.values(),
                    s -> conceptPath(s.getFullName(), "services"),
                    s -> "gRPC service."));
            for (ServiceDescriptor service : model.services.values()) {
                bundle.add(conceptPath(service.getFullName(), "services"),
                        serviceConcept(service, model));
            }
        }
    }

    // ------------------------------------------------------------------ concept documents

    private String messageConcept(Descriptor message, Model model) {
        Optional<MessageMeta> meta = DescriptorMetadata.message(message);
        StringBuilder doc = new StringBuilder();
        frontmatter(doc, "Protobuf Message", message.getName(),
                meta.map(MessageMeta::getDescription).orElse(""),
                meta.map(MessageMeta::getLabelsMap).orElse(Map.of()),
                meta.map(MessageMeta::getOwner).orElse(""), null);

        doc.append("Full name `").append(message.getFullName()).append("`, declared in `")
                .append(message.getFile().getName()).append("`.\n");
        meta.map(MessageMeta::getSensitivity).filter(s -> !s.isBlank()).ifPresent(s ->
                doc.append("\nMessage sensitivity class: `").append(s).append("`.\n"));

        boolean anySensitivity = message.getFields().stream().anyMatch(f ->
                DescriptorMetadata.field(f).map(FieldMeta::getSensitivity)
                        .filter(s -> !s.isBlank()).isPresent());

        doc.append("\n# Schema\n\n");
        doc.append("| Field | Type | Label | Description |");
        if (anySensitivity) {
            doc.append(" Sensitivity |");
        }
        doc.append('\n');
        doc.append("|---|---|---|---|").append(anySensitivity ? "---|" : "").append('\n');
        for (FieldDescriptor field : message.getFields()) {
            Optional<FieldMeta> fieldMeta = DescriptorMetadata.field(field);
            doc.append("| `").append(field.getName()).append("` | ")
                    .append(typeCell(field, model)).append(" | ")
                    .append(label(field)).append(" | ")
                    .append(cell(fieldMeta.map(FieldMeta::getDescription).orElse("")))
                    .append(" |");
            if (anySensitivity) {
                doc.append(' ').append(fieldMeta.map(FieldMeta::getSensitivity)
                        .filter(s -> !s.isBlank()).map(s -> "`" + s + "`").orElse(""))
                        .append(" |");
            }
            doc.append('\n');
        }
        return doc.toString();
    }

    private String enumConcept(EnumDescriptor enumType) {
        StringBuilder doc = new StringBuilder();
        frontmatter(doc, "Protobuf Enum", enumType.getName(), "", Map.of(), "", null);
        doc.append("Full name `").append(enumType.getFullName()).append("`, declared in `")
                .append(enumType.getFile().getName()).append("`.\n");
        doc.append("\n# Values\n\n| Value | Number |\n|---|---|\n");
        for (EnumValueDescriptor value : enumType.getValues()) {
            doc.append("| `").append(value.getName()).append("` | ")
                    .append(value.getNumber()).append(" |\n");
        }
        return doc.toString();
    }

    private String serviceConcept(ServiceDescriptor service, Model model) {
        StringBuilder doc = new StringBuilder();
        frontmatter(doc, "gRPC Service", service.getName(), "", Map.of(), "", null);
        doc.append("Full name `").append(service.getFullName()).append("`, declared in `")
                .append(service.getFile().getName()).append("`.\n");
        doc.append("\n# Methods\n\n| Method | Request | Response | Streaming |\n|---|---|---|---|\n");
        for (MethodDescriptor method : service.getMethods()) {
            String streaming = method.isClientStreaming() && method.isServerStreaming()
                    ? "bidirectional"
                    : method.isClientStreaming() ? "client"
                    : method.isServerStreaming() ? "server" : "unary";
            doc.append("| `").append(method.getName()).append("` | ")
                    .append(typeLink(method.getInputType().getFullName(), model)).append(" | ")
                    .append(typeLink(method.getOutputType().getFullName(), model)).append(" | ")
                    .append(streaming).append(" |\n");
        }
        return doc.toString();
    }

    // ------------------------------------------------------------------ shared rendering

    static void frontmatter(StringBuilder doc, String type, String title, String description,
                            Map<String, String> labels, String owner, String resource) {
        doc.append("---\n");
        doc.append("type: ").append(yaml(type)).append('\n');
        doc.append("title: ").append(yaml(title)).append('\n');
        if (description != null && !description.isBlank()) {
            doc.append("description: ").append(yaml(firstSentence(description))).append('\n');
        }
        if (resource != null && !resource.isBlank()) {
            doc.append("resource: ").append(yaml(resource)).append('\n');
        }
        List<String> tags = new ArrayList<>();
        if (owner != null && !owner.isBlank()) {
            tags.add("owner:" + owner);
        }
        labels.forEach((key, value) -> tags.add(key + ":" + value));
        if (!tags.isEmpty()) {
            doc.append("tags: [").append(String.join(", ", tags.stream().map(OkfRenderer::yaml)
                    .toList())).append("]\n");
        }
        doc.append("---\n\n");
    }

    private <T> String kindIndex(String heading, Iterable<T> concepts,
                                 java.util.function.Function<T, String> path,
                                 java.util.function.Function<T, String> description) {
        StringBuilder index = new StringBuilder("# ").append(heading).append("\n\n");
        for (T concept : concepts) {
            String conceptPath = path.apply(concept);
            String name = conceptPath.substring(conceptPath.lastIndexOf('/') + 1,
                    conceptPath.length() - ".md".length());
            index.append("* [").append(name).append("](/").append(conceptPath).append(") - ")
                    .append(firstSentence(description.apply(concept))).append('\n');
        }
        return index.toString();
    }

    private String rootIndex(Model model, Options options, Map<String, String> subjectDocs) {
        StringBuilder index = new StringBuilder();
        index.append("---\nokf_version: \"0.1\"\n---\n\n");
        index.append("# ").append(options.title()).append("\n\n");
        if (!subjectDocs.isEmpty()) {
            index.append("* [Subjects](/subjects/index.md) - ").append(subjectDocs.size())
                    .append(" registry subject(s) and their version history\n");
        }
        if (!model.messages.isEmpty()) {
            index.append("* [Messages](/messages/index.md) - ").append(model.messages.size())
                    .append(" message type(s) with schema tables\n");
        }
        if (!model.enums.isEmpty()) {
            index.append("* [Enums](/enums/index.md) - ").append(model.enums.size())
                    .append(" enum type(s)\n");
        }
        if (!model.services.isEmpty()) {
            index.append("* [Services](/services/index.md) - ").append(model.services.size())
                    .append(" gRPC service(s)\n");
        }
        return index.toString();
    }

    private String typeCell(FieldDescriptor field, Model model) {
        if (field.isMapField()) {
            FieldDescriptor key = field.getMessageType().findFieldByName("key");
            FieldDescriptor value = field.getMessageType().findFieldByName("value");
            return "map<`" + scalarName(key) + "`, " + valueCell(value, model) + ">";
        }
        return valueCell(field, model);
    }

    private String valueCell(FieldDescriptor field, Model model) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return typeLink(field.getMessageType().getFullName(), model);
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
            String fullName = field.getEnumType().getFullName();
            return model.enums.containsKey(fullName)
                    ? "[`" + fullName + "`](/" + conceptPath(fullName, "enums") + ")"
                    : "`" + fullName + "`";
        }
        return "`" + scalarName(field) + "`";
    }

    private String typeLink(String fullName, Model model) {
        return model.messages.containsKey(fullName)
                ? "[`" + fullName + "`](/" + conceptPath(fullName, "messages") + ")"
                : "`" + fullName + "`";
    }

    private static String scalarName(FieldDescriptor field) {
        return field.getType().name().toLowerCase(java.util.Locale.ROOT);
    }

    private static String label(FieldDescriptor field) {
        if (field.isMapField()) {
            return "map";
        }
        if (field.isRepeated()) {
            return "repeated";
        }
        return field.toProto().getProto3Optional() ? "optional" : "";
    }

    static String conceptPath(String fullName, String kind) {
        return kind + "/" + fullName + ".md";
    }

    static String cell(String text) {
        return text == null ? "" : text.replace("|", "\\|").replace("\n", " ").trim();
    }

    static String yaml(String value) {
        // A plain scalar is safe without quotes; "key: value"-looking content is not.
        if (value.matches("[A-Za-z0-9 ._:/-]*") && !value.isBlank()
                && !value.contains(": ") && !value.startsWith(" ") && !value.endsWith(" ")) {
            return value;
        }
        return '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    static String firstSentence(String text) {
        String flattened = text.replace('\n', ' ').trim();
        int period = flattened.indexOf(". ");
        return period > 0 ? flattened.substring(0, period + 1) : flattened;
    }

    private static String titleOf(String conceptDoc) {
        return frontmatterField(conceptDoc, "title");
    }

    private static String descriptionOf(String conceptDoc) {
        String description = frontmatterField(conceptDoc, "description");
        return description.isBlank() ? "Registry subject." : description;
    }

    private static String frontmatterField(String doc, String key) {
        boolean inFrontmatter = false;
        for (String line : doc.split("\n")) {
            if (line.equals("---")) {
                if (inFrontmatter) {
                    break; // the block ended without the key
                }
                inFrontmatter = true;
                continue;
            }
            if (inFrontmatter && line.startsWith(key + ": ")) {
                String value = line.substring(key.length() + 2).trim();
                if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
                    value = value.substring(1, value.length() - 1)
                            .replace("\\\"", "\"").replace("\\\\", "\\");
                }
                return value;
            }
        }
        return "";
    }

    /** The renderable types and services, in file order, internal option carriers excluded. */
    static final class Model {
        final Map<String, Descriptor> messages = new LinkedHashMap<>();
        final Map<String, EnumDescriptor> enums = new LinkedHashMap<>();
        final Map<String, ServiceDescriptor> services = new LinkedHashMap<>();

        static Model of(List<FileDescriptor> files) {
            Model model = new Model();
            Set<String> seenFiles = new LinkedHashSet<>();
            for (FileDescriptor file : files) {
                model.collect(file, seenFiles);
            }
            return model;
        }

        private void collect(FileDescriptor file, Set<String> seenFiles) {
            if (!seenFiles.add(file.getName()) || internal(file.getPackage())) {
                return;
            }
            for (FileDescriptor dependency : file.getDependencies()) {
                collect(dependency, seenFiles);
            }
            file.getMessageTypes().forEach(this::collectMessage);
            file.getEnumTypes().forEach(e -> enums.putIfAbsent(e.getFullName(), e));
            file.getServices().forEach(s -> services.putIfAbsent(s.getFullName(), s));
        }

        private void collectMessage(Descriptor message) {
            if (message.getOptions().getMapEntry()) {
                return;
            }
            messages.putIfAbsent(message.getFullName(), message);
            message.getNestedTypes().forEach(this::collectMessage);
            message.getEnumTypes().forEach(e -> enums.putIfAbsent(e.getFullName(), e));
        }

        private static boolean internal(String packageName) {
            return INTERNAL_PACKAGES.stream().anyMatch(prefix ->
                    packageName.equals(prefix) || packageName.startsWith(prefix + "."));
        }
    }
}
