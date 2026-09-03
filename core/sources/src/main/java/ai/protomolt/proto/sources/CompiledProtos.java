package ai.protomolt.proto.sources;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Result of compiling a {@link ProtoSourceSet}: the encoded {@link FileDescriptorSet} plus
 * linked runtime descriptors, indexed by import path.
 *
 * <p>{@link #descriptorSet()} may contain more files than the source set — Wire supplies
 * {@code google/protobuf/*} imports it bundles — while descriptors for well-known types the
 * encoder cannot supply are linked from protobuf-java's runtime instead and appear only in
 * {@link #fileDescriptors()}.</p>
 */
public record CompiledProtos(FileDescriptorSet descriptorSet,
                             List<FileDescriptor> fileDescriptors,
                             Map<String, FileDescriptor> byPath) {

    public CompiledProtos {
        fileDescriptors = List.copyOf(fileDescriptors);
        byPath = Map.copyOf(byPath);
    }

    /** The linked descriptor for the given import path, when it was part of the compilation. */
    public Optional<FileDescriptor> descriptorFor(String path) {
        return Optional.ofNullable(byPath.get(path));
    }
}
