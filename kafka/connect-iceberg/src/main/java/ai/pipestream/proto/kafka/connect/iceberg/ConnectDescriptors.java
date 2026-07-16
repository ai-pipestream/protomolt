package ai.pipestream.proto.kafka.connect.iceberg;

import ai.pipestream.proto.meta.DescriptorMetadata;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Linking a configured descriptor set and resolving a message type against it. The parse
 * registers ProtoMolt's metadata extensions so declared options (partition hints, json_name)
 * are readable rather than unknown fields.
 */
final class ConnectDescriptors {

    private ConnectDescriptors() {
    }

    /** Parses and links the configured descriptor set. */
    static List<FileDescriptor> linkedFiles(String descriptorSetBase64) {
        FileDescriptorSet set;
        try {
            ExtensionRegistry extensions = ExtensionRegistry.newInstance();
            DescriptorMetadata.registerExtensions(extensions);
            set = DescriptorMetadata.materializeJsonNames(FileDescriptorSet.parseFrom(
                    Base64.getDecoder().decode(descriptorSetBase64), extensions));
        } catch (Exception e) {
            throw new ConnectException("'schema.descriptor.set.base64' is not a base64 "
                    + "serialized FileDescriptorSet: " + e.getMessage(), e);
        }
        return link(set);
    }

    /** Finds a message type (nested types included) across the linked files. */
    static Descriptor messageType(List<FileDescriptor> files, String fullName) {
        for (FileDescriptor file : files) {
            Descriptor found = findMessage(file, fullName);
            if (found != null) {
                return found;
            }
        }
        throw new ConnectException("Message type '" + fullName
                + "' not found in the configured descriptor set");
    }

    private static Descriptor findMessage(FileDescriptor file, String fullName) {
        String pkg = file.getPackage();
        if (!pkg.isEmpty() && !fullName.startsWith(pkg + ".")) {
            return null;
        }
        String relative = pkg.isEmpty() ? fullName : fullName.substring(pkg.length() + 1);
        String[] parts = relative.split("\\.");
        Descriptor current = file.findMessageTypeByName(parts[0]);
        for (int i = 1; current != null && i < parts.length; i++) {
            current = current.findNestedTypeByName(parts[i]);
        }
        return current;
    }

    private static List<FileDescriptor> link(FileDescriptorSet set) {
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            byName.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new LinkedHashMap<>();
        try {
            for (FileDescriptorProto proto : set.getFileList()) {
                build(proto, byName, built);
            }
        } catch (Descriptors.DescriptorValidationException e) {
            throw new ConnectException("Descriptor set does not link: " + e.getMessage(), e);
        }
        return List.copyOf(built.values());
    }

    private static FileDescriptor build(FileDescriptorProto proto,
                                        Map<String, FileDescriptorProto> byName,
                                        Map<String, FileDescriptor> built)
            throws Descriptors.DescriptorValidationException {
        FileDescriptor existing = built.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            FileDescriptorProto dep = byName.get(proto.getDependency(i));
            if (dep == null) {
                throw new ConnectException("Descriptor set is missing the import '"
                        + proto.getDependency(i) + "'");
            }
            dependencies[i] = build(dep, byName, built);
        }
        FileDescriptor file = FileDescriptor.buildFrom(proto, dependencies);
        built.put(proto.getName(), file);
        return file;
    }
}
