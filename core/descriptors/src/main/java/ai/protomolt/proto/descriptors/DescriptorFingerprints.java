package ai.protomolt.proto.descriptors;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Existing platform descriptor identity, independent of mesh or registry clients.
 * Files are sorted by name; file contents, source information, options and unknown
 * fields inside each file remain unchanged. Set-envelope unknown fields are not
 * included, matching the original mesh algorithm. This hashes identity only:
 * callers must separately validate closure, duplicate names and resource limits.
 */
public final class DescriptorFingerprints {
    private DescriptorFingerprints() {}

    public static String fingerprint(FileDescriptorSet set) {
        var canonical = FileDescriptorSet.newBuilder().addAllFile(set.getFileList().stream()
                .sorted(Comparator.comparing(FileDescriptorProto::getName)).toList()).build();
        return MessageFingerprints.sha256Hex(canonical.toByteArray());
    }

    public static String fingerprint(List<FileDescriptor> files) {
        return fingerprint(FileDescriptorSet.newBuilder()
                .addAllFile(files.stream().map(FileDescriptor::toProto).toList()).build());
    }

    /** Dependency-first closure of an already linked message descriptor. */
    public static FileDescriptorSet closure(Descriptor type) {
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        collect(type.getFile(), byName);
        return FileDescriptorSet.newBuilder().addAllFile(byName.values()).build();
    }

    public static String fingerprintOf(Descriptor type) { return fingerprint(closure(type)); }

    private static void collect(FileDescriptor file, Map<String, FileDescriptorProto> byName) {
        if (byName.containsKey(file.getName())) return;
        for (FileDescriptor dependency : file.getDependencies()) collect(dependency, byName);
        for (FileDescriptor dependency : file.getPublicDependencies()) collect(dependency, byName);
        byName.put(file.getName(), file.toProto());
    }
}
