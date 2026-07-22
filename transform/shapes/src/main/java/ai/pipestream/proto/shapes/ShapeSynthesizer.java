package ai.pipestream.proto.shapes;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Synthesizes real message types at runtime — the output shapes of joins and unions. Three
 * shapes are derivable from source descriptors alone:
 *
 * <ul>
 *   <li><b>envelope</b> — one message field per named source, each source intact;</li>
 *   <li><b>projection</b> — a flat message whose fields take their types from scoped source
 *       paths (the SELECT list becomes the schema);</li>
 *   <li><b>tagged union</b> — a {@code oneof} over the source types, protobuf's native
 *       union.</li>
 * </ul>
 *
 * <p>The shape is built as a {@code FileDescriptorProto} depending on the sources' files and
 * linked in-process, so it is immediately usable; the equivalent {@code .proto} source is
 * emitted with the sources' true import paths, so the derived schema can be registered in a
 * schema registry (with references) like any hand-written one.</p>
 */
public final class ShapeSynthesizer {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** A named source type in shape order; names become field names. */
    public record NamedType(String name, Descriptor type) {
        public NamedType {
            requireIdentifier(name, "source name");
            Objects.requireNonNull(type, "type");
        }
    }

    /** A projected field: its type is inferred from the scoped source path. */
    public record ProjectedField(String name, String sourcePath) {
        public ProjectedField {
            requireIdentifier(name, "field name");
            Objects.requireNonNull(sourcePath, "sourcePath");
        }
    }

    /** A synthesized shape: linked descriptor, emitted source, and the rules it implies. */
    public record SynthesizedShape(FileDescriptor file, Descriptor type, String protoSource,
                                   List<String> impliedRules) {

        /** The shape plus its transitive dependencies, dependencies first. */
        public FileDescriptorSet descriptorSet() {
            Map<String, FileDescriptorProto> ordered = new LinkedHashMap<>();
            visit(file, ordered);
            return FileDescriptorSet.newBuilder().addAllFile(ordered.values()).build();
        }

        private static void visit(FileDescriptor file, Map<String, FileDescriptorProto> out) {
            if (out.containsKey(file.getName())) {
                return;
            }
            for (FileDescriptor dependency : file.getDependencies()) {
                visit(dependency, out);
            }
            out.put(file.getName(), file.toProto());
        }
    }

    /** One message field per source, each source intact — lossless, zero authoring. */
    public SynthesizedShape envelope(String fullName, List<NamedType> parts) {
        requireUniqueNames(parts);
        DescriptorProto.Builder message = DescriptorProto.newBuilder()
                .setName(simpleName(fullName));
        List<FileDescriptor> dependencies = new ArrayList<>();
        List<String> impliedRules = new ArrayList<>();
        int number = 1;
        for (NamedType part : parts) {
            message.addField(FieldDescriptorProto.newBuilder()
                    .setName(part.name())
                    .setNumber(number++)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName("." + part.type().getFullName()));
            dependencies.add(part.type().getFile());
            impliedRules.add(part.name() + " = " + part.name());
        }
        return link(fullName, message, dependencies, impliedRules);
    }

    /** A flat message whose field types are inferred from scoped source paths. */
    public SynthesizedShape projection(String fullName, List<NamedType> sources,
                                       List<ProjectedField> fields) {
        requireUniqueNames(sources);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("A projection needs at least one field");
        }
        Map<String, Descriptor> byName = new LinkedHashMap<>();
        sources.forEach(source -> byName.put(source.name(), source.type()));
        List<NamedField> named = new ArrayList<>(fields.size());
        List<String> impliedRules = new ArrayList<>(fields.size());
        Set<String> seen = new LinkedHashSet<>();
        for (ProjectedField projected : fields) {
            if (!seen.add(projected.name())) {
                throw new IllegalArgumentException("Duplicate projected field: "
                        + projected.name());
            }
            named.add(new NamedField(projected.name(), resolve(byName, projected.sourcePath())));
            impliedRules.add(projected.name() + " = " + projected.sourcePath());
        }
        return fromFields(fullName, named, impliedRules);
    }

    /** A field of a synthesized flat message, typed by the source field it comes from. */
    public record NamedField(String name, FieldDescriptor typedBy) {
        public NamedField {
            requireIdentifier(name, "field name");
            Objects.requireNonNull(typedBy, "typedBy");
        }
    }

    /** Builds a flat message from named fields, each typed by a source field descriptor. */
    public SynthesizedShape fromFields(String fullName, List<NamedField> fields,
                                       List<String> impliedRules) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("A shape needs at least one field");
        }
        DescriptorProto.Builder message = DescriptorProto.newBuilder()
                .setName(simpleName(fullName));
        List<FileDescriptor> dependencies = new ArrayList<>();
        int number = 1;
        for (NamedField named : fields) {
            FieldDescriptor typedBy = named.typedBy();
            if (typedBy.isMapField()) {
                throw new IllegalArgumentException("Map field '" + typedBy.getName()
                        + "' cannot be carried into a synthesized shape yet");
            }
            FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder()
                    .setName(named.name())
                    .setNumber(number++)
                    .setLabel(typedBy.isRepeated()
                            ? FieldDescriptorProto.Label.LABEL_REPEATED
                            : FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(typedBy.getType().toProto());
            if (typedBy.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                field.setTypeName("." + typedBy.getMessageType().getFullName());
                dependencies.add(typedBy.getMessageType().getFile());
            } else if (typedBy.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                field.setTypeName("." + typedBy.getEnumType().getFullName());
                dependencies.add(typedBy.getEnumType().getFile());
            }
            message.addField(field);
        }
        return link(fullName, message, dependencies, impliedRules);
    }

    /** Protobuf's native union: a {@code oneof} over the source types. */
    public SynthesizedShape taggedUnion(String fullName, List<NamedType> cases) {
        requireUniqueNames(cases);
        DescriptorProto.Builder message = DescriptorProto.newBuilder()
                .setName(simpleName(fullName))
                .addOneofDecl(OneofDescriptorProto.newBuilder().setName("value"));
        List<FileDescriptor> dependencies = new ArrayList<>();
        int number = 1;
        for (NamedType unionCase : cases) {
            message.addField(FieldDescriptorProto.newBuilder()
                    .setName(unionCase.name())
                    .setNumber(number++)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName("." + unionCase.type().getFullName())
                    .setOneofIndex(0));
            dependencies.add(unionCase.type().getFile());
        }
        return link(fullName, message, dependencies, List.of());
    }

    /** Resolves a scoped path against the source descriptors to the final field. */
    private static FieldDescriptor resolve(Map<String, Descriptor> sources, String scopedPath) {
        int dot = scopedPath.indexOf('.');
        String sourceName = dot < 0 ? scopedPath : scopedPath.substring(0, dot);
        Descriptor current = sources.get(sourceName);
        if (current == null) {
            throw new IllegalArgumentException("Unknown source '" + sourceName + "' in '"
                    + scopedPath + "'; sources are " + sources.keySet());
        }
        if (dot < 0) {
            throw new IllegalArgumentException("Projection path '" + scopedPath
                    + "' names a whole source; project a field of it, or use an envelope");
        }
        String[] segments = scopedPath.substring(dot + 1).split("\\.");
        FieldDescriptor field = null;
        for (int i = 0; i < segments.length; i++) {
            field = current.findFieldByName(segments[i]);
            if (field == null) {
                throw new IllegalArgumentException("No field '" + segments[i] + "' on "
                        + current.getFullName() + " (resolving '" + scopedPath + "')");
            }
            if (field.isMapField()) {
                throw new IllegalArgumentException("Map field '" + segments[i]
                        + "' cannot be projected ('" + scopedPath + "')");
            }
            boolean last = i == segments.length - 1;
            if (!last) {
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                        || field.isRepeated()) {
                    throw new IllegalArgumentException("Field '" + segments[i] + "' on "
                            + current.getFullName()
                            + " is not a singular message; cannot descend ('"
                            + scopedPath + "')");
                }
                current = field.getMessageType();
            }
        }
        return field;
    }

    /** Links a fully-authored message proto (nested types included) as a shape. */
    SynthesizedShape linkSynthetic(String fullName, DescriptorProto.Builder message,
                                   List<FileDescriptor> dependencies,
                                   List<String> impliedRules) {
        return link(fullName, message, dependencies, impliedRules);
    }

    private SynthesizedShape link(String fullName, DescriptorProto.Builder message,
                                  List<FileDescriptor> dependencies,
                                  List<String> impliedRules) {
        String pkg = packageName(fullName);
        LinkedHashMap<String, FileDescriptor> uniqueDeps = new LinkedHashMap<>();
        dependencies.forEach(dependency -> uniqueDeps.putIfAbsent(
                dependency.getName(), dependency));
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder()
                .setName(fileName(fullName))
                .setSyntax("proto3")
                .addAllDependency(uniqueDeps.keySet())
                .addMessageType(message);
        if (!pkg.isEmpty()) {
            file.setPackage(pkg);
        }
        FileDescriptor linked;
        try {
            linked = FileDescriptor.buildFrom(file.build(),
                    uniqueDeps.values().toArray(FileDescriptor[]::new));
        } catch (DescriptorValidationException e) {
            throw new IllegalArgumentException("Synthesized shape does not link: "
                    + e.getMessage(), e);
        }
        Descriptor type = linked.findMessageTypeByName(simpleName(fullName));
        return new SynthesizedShape(linked, type,
                emitSource(linked.toProto(), type), List.copyOf(impliedRules));
    }

    /** The shape as {@code .proto} source, imports carrying the sources' true paths. */
    private static String emitSource(FileDescriptorProto file, Descriptor type) {
        StringBuilder out = new StringBuilder("syntax = \"proto3\";\n\n");
        if (!file.getPackage().isEmpty()) {
            out.append("package ").append(file.getPackage()).append(";\n\n");
        }
        for (String dependency : file.getDependencyList()) {
            out.append("import \"").append(dependency).append("\";\n");
        }
        if (file.getDependencyCount() > 0) {
            out.append('\n');
        }
        emitMessage(out, type, "");
        return out.toString();
    }

    private static void emitMessage(StringBuilder out, Descriptor type, String indent) {
        out.append(indent).append("message ").append(type.getName()).append(" {\n");
        for (Descriptor nested : type.getNestedTypes()) {
            emitMessage(out, nested, indent + "  ");
        }
        boolean oneof = !type.getOneofs().isEmpty();
        if (oneof) {
            out.append(indent).append("  oneof ")
                    .append(type.getOneofs().get(0).getName()).append(" {\n");
        }
        String fieldIndent = indent + (oneof ? "    " : "  ");
        for (FieldDescriptor field : type.getFields()) {
            out.append(fieldIndent);
            if (field.isRepeated()) {
                out.append("repeated ");
            }
            out.append(typeKeyword(field)).append(' ').append(field.getName())
                    .append(" = ").append(field.getNumber());
            // json_name is set only when a sanitized name must round-trip the original;
            // the meta.v1 annotation carries the same value through compilers that drop
            // json_name from source (ProtoMolt loaders materialize it back).
            java.util.List<String> brackets = new java.util.ArrayList<>();
            if (field.toProto().hasJsonName()) {
                brackets.add("json_name = \"" + field.toProto().getJsonName() + "\"");
            }
            if (field.toProto().getOptions().hasExtension(
                    ai.pipestream.proto.meta.MetadataProto.field)) {
                String original = field.toProto().getOptions()
                        .getExtension(ai.pipestream.proto.meta.MetadataProto.field)
                        .getJsonName();
                if (!original.isEmpty()) {
                    brackets.add("(ai.pipestream.proto.meta.v1.field) = {json_name: \""
                            + original + "\"}");
                }
            }
            if (!brackets.isEmpty()) {
                out.append(" [").append(String.join(", ", brackets)).append(']');
            }
            out.append(";\n");
        }
        if (oneof) {
            out.append(indent).append("  }\n");
        }
        out.append(indent).append("}\n");
    }

    /** The proto keyword or full type name a field declares — also used in clash reports. */
    static String typeKeyword(FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case MESSAGE -> field.getMessageType().getFullName();
            case ENUM -> field.getEnumType().getFullName();
            default -> field.getType().name().toLowerCase(Locale.ROOT);
        };
    }

    private static String simpleName(String fullName) {
        int dot = fullName.lastIndexOf('.');
        String simple = dot < 0 ? fullName : fullName.substring(dot + 1);
        requireIdentifier(simple, "message name");
        return simple;
    }

    private static String packageName(String fullName) {
        int dot = fullName.lastIndexOf('.');
        return dot < 0 ? "" : fullName.substring(0, dot);
    }

    private static String fileName(String fullName) {
        String pkg = packageName(fullName);
        String base = simpleName(fullName)
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .toLowerCase(Locale.ROOT);
        return pkg.isEmpty() ? base + ".proto" : pkg.replace('.', '/') + "/" + base + ".proto";
    }

    private static void requireUniqueNames(List<NamedType> named) {
        if (named.isEmpty()) {
            throw new IllegalArgumentException("A shape needs at least one source");
        }
        Set<String> names = new LinkedHashSet<>();
        for (NamedType type : named) {
            if (!names.add(type.name())) {
                throw new IllegalArgumentException("Duplicate source name: " + type.name());
            }
        }
    }

    private static void requireIdentifier(String name, String what) {
        Objects.requireNonNull(name, what);
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Not a valid " + what + ": '" + name + "'");
        }
    }
}
