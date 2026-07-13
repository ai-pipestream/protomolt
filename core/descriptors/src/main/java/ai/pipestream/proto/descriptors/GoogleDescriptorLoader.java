package ai.pipestream.proto.descriptors;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads Protocol Buffer descriptors from Google descriptor set files (.dsc).
 */
public class GoogleDescriptorLoader implements DescriptorLoader {

    private final String descriptorPath;
    private final ClassLoader classLoader;

    public static final String DEFAULT_DESCRIPTOR_PATH = "META-INF/grpc/services.dsc";

    public GoogleDescriptorLoader() {
        this(DEFAULT_DESCRIPTOR_PATH);
    }

    public GoogleDescriptorLoader(String descriptorPath) {
        this(descriptorPath, Thread.currentThread().getContextClassLoader());
    }

    public GoogleDescriptorLoader(String descriptorPath, ClassLoader classLoader) {
        this.descriptorPath = descriptorPath;
        this.classLoader = classLoader != null ? classLoader : ClassLoader.getSystemClassLoader();
    }

    /**
     * Gets the path to the descriptor file.
     *
     * @return the descriptor path
     */
    public String getDescriptorPath() {
        return descriptorPath;
    }

    /**
     * Searches for a descriptor file in the specified paths and returns a loader for the first one found.
     * If none are found, returns a loader for the first path.
     *
     * @param paths the paths to search; at least one is required
     * @return a descriptor loader
     * @throws IllegalArgumentException if no paths are given
     */
    public static GoogleDescriptorLoader searchPaths(String... paths) {
        if (paths == null || paths.length == 0) {
            throw new IllegalArgumentException("searchPaths requires at least one path");
        }
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = GoogleDescriptorLoader.class.getClassLoader();
        }
        for (String path : paths) {
            if (cl.getResource(path) != null) {
                return new GoogleDescriptorLoader(path, cl);
            }
        }
        return new GoogleDescriptorLoader(paths[0], cl);
    }

    @Override
    public List<FileDescriptor> loadDescriptors() throws DescriptorLoadException {
        try (InputStream inputStream = classLoader.getResourceAsStream(descriptorPath)) {
            if (inputStream == null) {
                throw new DescriptorLoadException(
                    "Descriptor file not found on classpath: " + descriptorPath);
            }

            FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(inputStream);
            return buildFileDescriptors(descriptorSet);
        } catch (IOException e) {
            throw new DescriptorLoadException(
                "Failed to read descriptor file: " + descriptorPath, e);
        } catch (DescriptorValidationException e) {
            throw new DescriptorLoadException(
                "Invalid descriptor in file: " + descriptorPath, e);
        } catch (RuntimeException e) {
            throw new DescriptorLoadException(
                "Failed to build descriptors from file: " + descriptorPath, e);
        }
    }

    @Override
    public FileDescriptor loadDescriptor(String fileName) throws DescriptorLoadException {
        List<FileDescriptor> allDescriptors = loadDescriptors();
        return allDescriptors.stream()
            .filter(fd -> fd.getName().equals(fileName))
            .findFirst()
            .orElse(null);
    }

    /**
     * Builds runtime descriptors from a serialized descriptor set. This is useful for
     * registry clients that transport descriptor data outside the classpath.
     */
    public static List<FileDescriptor> fromDescriptorSet(FileDescriptorSet descriptorSet)
            throws DescriptorLoadException {
        try {
            return new GoogleDescriptorLoader().buildFileDescriptors(descriptorSet);
        } catch (DescriptorValidationException | RuntimeException e) {
            throw new DescriptorLoadException("Failed to build descriptors from descriptor set", e);
        }
    }

    @Override
    public boolean isAvailable() {
        InputStream stream = classLoader.getResourceAsStream(descriptorPath);
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                // Ignore
            }
            return true;
        }
        return false;
    }

    @Override
    public String getLoaderType() {
        return "Google Descriptor File";
    }

    private List<FileDescriptor> buildFileDescriptors(FileDescriptorSet descriptorSet)
            throws DescriptorValidationException, DescriptorLoadException {

        Map<String, FileDescriptor> descriptorMap = new HashMap<>();
        Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> protoMap = new HashMap<>();

        for (com.google.protobuf.DescriptorProtos.FileDescriptorProto proto : descriptorSet.getFileList()) {
            protoMap.put(proto.getName(), proto);
        }

        for (com.google.protobuf.DescriptorProtos.FileDescriptorProto proto : descriptorSet.getFileList()) {
            if (!descriptorMap.containsKey(proto.getName())) {
                buildFileDescriptor(proto, protoMap, descriptorMap, new LinkedHashSet<>());
            }
        }

        return new ArrayList<>(descriptorMap.values());
    }

    private FileDescriptor buildFileDescriptor(
            com.google.protobuf.DescriptorProtos.FileDescriptorProto proto,
            Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> protoMap,
            Map<String, FileDescriptor> descriptorMap,
            Set<String> inProgress) throws DescriptorValidationException, DescriptorLoadException {

        if (descriptorMap.containsKey(proto.getName())) {
            return descriptorMap.get(proto.getName());
        }

        // Guard against cyclic dependency declarations (a.proto -> a.proto, or a <-> b), which
        // would otherwise recurse until StackOverflowError. The insertion-ordered set doubles
        // as the dependency chain for the error message.
        if (!inProgress.add(proto.getName())) {
            throw new DescriptorLoadException(
                "dependency cycle: " + String.join(" -> ", inProgress) + " -> " + proto.getName());
        }

        List<FileDescriptor> dependencies = new ArrayList<>();
        for (String dependency : proto.getDependencyList()) {
            FileDescriptor depDescriptor = descriptorMap.get(dependency);
            if (depDescriptor == null) {
                com.google.protobuf.DescriptorProtos.FileDescriptorProto depProto = protoMap.get(dependency);
                if (depProto != null) {
                    depDescriptor = buildFileDescriptor(depProto, protoMap, descriptorMap, inProgress);
                } else {
                    depDescriptor = tryGetWellKnownType(dependency);
                    if (depDescriptor == null) {
                        throw new IllegalStateException(
                            "Missing dependency: " + dependency + " for " + proto.getName());
                    }
                }
            }
            dependencies.add(depDescriptor);
        }

        FileDescriptor descriptor = FileDescriptor.buildFrom(
            proto,
            dependencies.toArray(new FileDescriptor[0])
        );

        descriptorMap.put(proto.getName(), descriptor);
        inProgress.remove(proto.getName());
        return descriptor;
    }

    private FileDescriptor tryGetWellKnownType(String fileName) {
        try {
            return switch (fileName) {
                case "google/protobuf/any.proto" -> com.google.protobuf.Any.getDescriptor().getFile();
                case "google/protobuf/struct.proto" -> com.google.protobuf.Struct.getDescriptor().getFile();
                case "google/protobuf/timestamp.proto" -> com.google.protobuf.Timestamp.getDescriptor().getFile();
                case "google/protobuf/duration.proto" -> com.google.protobuf.Duration.getDescriptor().getFile();
                case "google/protobuf/empty.proto" -> com.google.protobuf.Empty.getDescriptor().getFile();
                case "google/protobuf/field_mask.proto" -> com.google.protobuf.FieldMask.getDescriptor().getFile();
                case "google/protobuf/wrappers.proto" -> com.google.protobuf.StringValue.getDescriptor().getFile();
                default -> null;
            };
        } catch (Exception e) {
            return null;
        }
    }
}
