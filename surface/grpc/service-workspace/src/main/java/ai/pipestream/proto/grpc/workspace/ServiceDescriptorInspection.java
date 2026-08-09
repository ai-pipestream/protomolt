package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;

import java.io.IOException;
import java.util.List;

/** Renders agent-sized service and method contracts from a stored descriptor artifact. */
public final class ServiceDescriptorInspection {

    private static final int MAX_SERVICES = 1_024;
    private static final int MAX_METHODS = 4_096;
    private static final int MAX_FIELDS_PER_MESSAGE = 2_048;

    private ServiceDescriptorInspection() {
    }

    /** Inspects every non-reflection service in a profile's descriptor artifact. */
    public static ArrayNode services(ServiceProfile profile, ServiceProfileRepository repository,
                                     ObjectMapper mapper) throws IOException {
        String fingerprint = profile.getSchemaSource().getDescriptorFingerprint();
        DescriptorArtifact artifact = repository.findDescriptorArtifact(fingerprint)
                .orElseThrow(() -> new IOException("descriptor artifact '" + fingerprint
                        + "' for service '" + profile.getName() + "' was not found"));
        FileDescriptorSet set = FileDescriptorSet.parseFrom(artifact.getDescriptorSet());
        List<FileDescriptor> files;
        try {
            files = GoogleDescriptorLoader.fromDescriptorSet(set);
        } catch (Exception e) {
            throw new IOException("descriptor artifact '" + fingerprint + "' cannot be linked", e);
        }
        ArrayNode services = mapper.createArrayNode();
        int methodCount = 0;
        for (FileDescriptor file : files) {
            for (var service : file.getServices()) {
                if (service.getFullName().startsWith("grpc.reflection.")) {
                    continue;
                }
                if (services.size() == MAX_SERVICES) {
                    throw new IOException("descriptor exceeds the service inspection limit of "
                            + MAX_SERVICES);
                }
                ObjectNode serviceNode = services.addObject();
                serviceNode.put("name", service.getFullName());
                ArrayNode methods = serviceNode.putArray("methods");
                for (MethodDescriptor method : service.getMethods()) {
                    if (methodCount++ == MAX_METHODS) {
                        throw new IOException("descriptor exceeds the method inspection limit of "
                                + MAX_METHODS);
                    }
                    methods.add(method(method, mapper));
                }
            }
        }
        return services;
    }

    private static ObjectNode method(MethodDescriptor method, ObjectMapper mapper)
            throws IOException {
        ObjectNode node = mapper.createObjectNode();
        node.put("name", method.getName());
        node.put("fullName", method.getService().getFullName() + "/" + method.getName());
        node.put("inputType", method.getInputType().getFullName());
        node.put("outputType", method.getOutputType().getFullName());
        node.put("clientStreaming", method.isClientStreaming());
        node.put("serverStreaming", method.isServerStreaming());
        node.set("inputFields", fields(method.getInputType(), mapper));
        node.set("outputFields", fields(method.getOutputType(), mapper));
        return node;
    }

    private static ArrayNode fields(Descriptor descriptor, ObjectMapper mapper) throws IOException {
        if (descriptor.getFields().size() > MAX_FIELDS_PER_MESSAGE) {
            throw new IOException("message '" + descriptor.getFullName()
                    + "' exceeds the field inspection limit of " + MAX_FIELDS_PER_MESSAGE);
        }
        ArrayNode fields = mapper.createArrayNode();
        for (FieldDescriptor field : descriptor.getFields()) {
            ObjectNode node = fields.addObject();
            node.put("name", field.getName());
            node.put("jsonName", field.getJsonName());
            node.put("number", field.getNumber());
            node.put("type", field.getType().name().toLowerCase(java.util.Locale.ROOT));
            node.put("cardinality", field.isRepeated() ? "repeated" : "singular");
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                node.put("typeName", field.getMessageType().getFullName());
            } else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                node.put("typeName", field.getEnumType().getFullName());
            }
        }
        return fields;
    }
}
