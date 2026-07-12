package ai.pipestream.proto.validate.conformance;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Links a {@link FileDescriptorSet} (as delivered in a conformance request) into runtime
 * {@link FileDescriptor}s, resolving imports in dependency order.
 *
 * <p>Any import not present in the set is resolved from the well-known descriptors already compiled
 * onto the classpath (the protobuf runtime types plus {@code buf/validate/validate.proto}), so a
 * request that omits those still links.
 */
final class DescriptorSets {

    private static final Map<String, FileDescriptor> WELL_KNOWN = wellKnown();

    private DescriptorSets() {
    }

    /** The linked result of a {@link FileDescriptorSet}: message types by full name and all files. */
    record Linked(Map<String, Descriptor> types, List<FileDescriptor> files) {
    }

    /** Builds every file in {@code set}, returning a full {@code messageFullName -> Descriptor} map. */
    static Map<String, Descriptor> messageTypes(FileDescriptorSet set) throws DescriptorValidationException {
        return link(set).types();
    }

    /** Builds every file in {@code set}, returning both the message-type map and the built files. */
    static Linked link(FileDescriptorSet set) throws DescriptorValidationException {
        Map<String, FileDescriptorProto> byName = new HashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            byName.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new HashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            build(proto, byName, built, new LinkedHashSet<>());
        }
        Map<String, Descriptor> types = new HashMap<>();
        for (FileDescriptor file : built.values()) {
            collect(file.getMessageTypes(), types);
        }
        return new Linked(types, List.copyOf(built.values()));
    }

    private static FileDescriptor build(
            FileDescriptorProto proto,
            Map<String, FileDescriptorProto> byName,
            Map<String, FileDescriptor> built,
            Set<String> building) throws DescriptorValidationException {
        String name = proto.getName();
        FileDescriptor done = built.get(name);
        if (done != null) {
            return done;
        }
        if (!building.add(name)) {
            throw new IllegalStateException("cyclic proto import involving " + name);
        }
        List<FileDescriptor> deps = new java.util.ArrayList<>();
        for (String dep : proto.getDependencyList()) {
            FileDescriptorProto depProto = byName.get(dep);
            if (depProto != null) {
                deps.add(build(depProto, byName, built, building));
            } else {
                FileDescriptor known = WELL_KNOWN.get(dep);
                if (known == null) {
                    throw new IllegalArgumentException(
                            "unresolved import '" + dep + "' required by " + name);
                }
                deps.add(known);
            }
        }
        FileDescriptor file = FileDescriptor.buildFrom(proto, deps.toArray(new FileDescriptor[0]));
        built.put(name, file);
        building.remove(name);
        return file;
    }

    private static void collect(List<Descriptor> messages, Map<String, Descriptor> out) {
        for (Descriptor message : messages) {
            out.put(message.getFullName(), message);
            collect(message.getNestedTypes(), out);
        }
    }

    private static Map<String, FileDescriptor> wellKnown() {
        Map<String, FileDescriptor> map = new HashMap<>();
        for (FileDescriptor file : List.of(
                com.google.protobuf.DescriptorProtos.getDescriptor(),
                com.google.protobuf.AnyProto.getDescriptor(),
                com.google.protobuf.DurationProto.getDescriptor(),
                com.google.protobuf.TimestampProto.getDescriptor(),
                com.google.protobuf.StructProto.getDescriptor(),
                com.google.protobuf.WrappersProto.getDescriptor(),
                com.google.protobuf.FieldMaskProto.getDescriptor(),
                com.google.protobuf.EmptyProto.getDescriptor(),
                com.google.protobuf.ApiProto.getDescriptor(),
                com.google.protobuf.TypeProto.getDescriptor(),
                com.google.protobuf.SourceContextProto.getDescriptor(),
                build.buf.validate.ValidateProto.getDescriptor())) {
            map.put(file.getName(), file);
        }
        return map;
    }
}
