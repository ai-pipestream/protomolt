package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.meta.DescriptorMetadata;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Descriptors from a descriptor set the deployment already has: a base64 config value or, more
 * usefully, a resource on the classpath, which is where a build that compiled the protos already
 * put one.
 *
 * <p>The parse registers ProtoMolt's metadata and validation extensions. This is load-bearing
 * rather than cosmetic: declared rules live in the descriptor's options, and options parsed
 * without their extensions land as unknown fields, which would leave the validator seeing a
 * schema with no rules and reporting every message as clean.</p>
 */
final class SerdeDescriptors {

    private SerdeDescriptors() {
    }

    /** Reads a serialized FileDescriptorSet from the classpath. */
    static List<FileDescriptor> fromClasspath(String resource, ClassLoader loader) {
        try (InputStream in = loader.getResourceAsStream(resource)) {
            if (in == null) {
                throw new KafkaException("Descriptor set resource not found on the classpath: "
                        + resource);
            }
            return link(parse(in.readAllBytes(), "classpath resource " + resource));
        } catch (IOException e) {
            throw new KafkaException("Could not read the descriptor set resource " + resource
                    + ": " + e.getMessage(), e);
        }
    }

    /** Reads a serialized FileDescriptorSet from a base64 config value. */
    static List<FileDescriptor> fromBase64(String base64) {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw new KafkaException("The configured descriptor set is not valid base64", e);
        }
        return link(parse(bytes, "configured descriptor set"));
    }

    private static FileDescriptorSet parse(byte[] bytes, String what) {
        try {
            ExtensionRegistry extensions = ExtensionRegistry.newInstance();
            ValidationResult.registerExtensions(extensions);
            DescriptorMetadata.registerExtensions(extensions);
            return DescriptorMetadata.materializeJsonNames(
                    FileDescriptorSet.parseFrom(bytes, extensions));
        } catch (Exception e) {
            throw new KafkaException("The " + what
                    + " is not a serialized FileDescriptorSet: " + e.getMessage(), e);
        }
    }

    /** Finds a message type by full name, nested types included. */
    static Descriptor messageType(List<FileDescriptor> files, String fullName) {
        for (FileDescriptor file : files) {
            Descriptor found = findMessage(file, fullName);
            if (found != null) {
                return found;
            }
        }
        throw new KafkaException("Message type '" + fullName
                + "' is not in the configured descriptor set");
    }

    private static Descriptor findMessage(FileDescriptor file, String fullName) {
        String pkg = file.getPackage();
        if (!pkg.isEmpty()) {
            if (!fullName.startsWith(pkg + ".")) {
                return null;
            }
            return file.findMessageTypeByName(fullName.substring(pkg.length() + 1));
        }
        return file.findMessageTypeByName(fullName);
    }

    private static List<FileDescriptor> link(FileDescriptorSet set) {
        Map<String, FileDescriptorProto> protos = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            protos.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            build(proto, protos, built);
        }
        return List.copyOf(built.values());
    }

    private static FileDescriptor build(FileDescriptorProto proto,
                                        Map<String, FileDescriptorProto> protos,
                                        Map<String, FileDescriptor> built) {
        FileDescriptor done = built.get(proto.getName());
        if (done != null) {
            return done;
        }
        List<FileDescriptor> dependencies = new java.util.ArrayList<>();
        for (String dependency : proto.getDependencyList()) {
            FileDescriptorProto next = protos.get(dependency);
            if (next == null) {
                throw new KafkaException("The descriptor set is missing an imported file: "
                        + dependency);
            }
            dependencies.add(build(next, protos, built));
        }
        try {
            FileDescriptor file = FileDescriptor.buildFrom(proto,
                    dependencies.toArray(new FileDescriptor[0]));
            built.put(proto.getName(), file);
            return file;
        } catch (Descriptors.DescriptorValidationException e) {
            throw new KafkaException("The descriptor set does not link: " + e.getMessage(), e);
        }
    }
}
