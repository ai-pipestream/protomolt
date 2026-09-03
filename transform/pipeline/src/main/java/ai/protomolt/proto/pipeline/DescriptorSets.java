package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Shared descriptor-set mechanics for the compiler and the checker: the canonical
 * fingerprint every pipeline reference binds to, and method resolution across the set.
 */
final class DescriptorSets {

    private DescriptorSets() {
    }

    /**
     * The descriptor set's stable identity: files serialized in name order, hashed with
     * SHA-256, so the fingerprint depends on content alone. This is exactly the workflow
     * compiler's convention, so a workflow's dependency fingerprints bind to the same
     * descriptor set its compiled pipeline declares.
     */
    static String fingerprint(List<FileDescriptor> files) {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addAllFile(files.stream()
                        .map(FileDescriptor::toProto)
                        .sorted(Comparator.comparing(proto -> proto.getName()))
                        .toList())
                .build();
        return ServiceProfileValidation.sha256(set.toByteArray());
    }

    /**
     * Resolves {@code package.Service/Method} across the set.
     *
     * @return the method, or null when the service or method is absent
     * @throws IllegalArgumentException when {@code qualified} is not in Service/Method form
     */
    static MethodDescriptor resolveMethod(List<FileDescriptor> files, String qualified) {
        int slash = qualified.indexOf('/');
        if (slash <= 0 || slash == qualified.length() - 1) {
            throw new IllegalArgumentException(
                    "method must be 'package.Service/Method'; got '" + qualified + "'");
        }
        String serviceName = qualified.substring(0, slash);
        String methodName = qualified.substring(slash + 1);
        for (FileDescriptor file : files) {
            for (ServiceDescriptor service : file.getServices()) {
                if (service.getFullName().equals(serviceName)) {
                    MethodDescriptor method = service.findMethodByName(methodName);
                    if (method != null) {
                        return method;
                    }
                }
            }
        }
        return null;
    }

    /** The comma-separated file names of the set, for descriptor-precise messages. */
    static String fileNames(List<FileDescriptor> files) {
        return files.stream().map(FileDescriptor::getName).sorted()
                .collect(Collectors.joining(", "));
    }
}
