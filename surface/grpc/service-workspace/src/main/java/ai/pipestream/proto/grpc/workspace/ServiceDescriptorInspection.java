package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.util.List;

/** Renders agent-sized service and method contracts from a stored descriptor artifact. */
public final class ServiceDescriptorInspection {

    private static final int MAX_SERVICES = 1_024;
    private static final int MAX_METHODS = 4_096;
    private static final int MAX_FIELDS_PER_MESSAGE = 2_048;

    private ServiceDescriptorInspection() {
    }

    /**
     * The inspection as JSON, for a front that speaks it.
     *
     * <p>Rendered from the same messages the verb answers with, so the MCP resource and the
     * service-inspect reply cannot describe a profile differently.
     */
    public static ArrayNode servicesJson(ServiceProfile profile,
                                         ServiceProfileRepository repository,
                                         SchemaRegistryStore registry, ObjectMapper mapper)
            throws IOException {
        Reply reply = Reply.of(CatalogContract.response("ServiceInspectResponse"));
        writeServices(reply, "services", profile, repository, registry);
        try {
            JsonNode rendered = mapper.readTree(
                    JsonFormat.printer().print(reply.build()));
            JsonNode services = rendered.get("services");
            return services == null ? mapper.createArrayNode() : (ArrayNode) services;
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("service inspection does not render as JSON", e);
        }
    }

    /**
     * Inspects every non-reflection service in a profile's descriptor artifact, appending
     * each to {@code services} on {@code reply}.
     */
    public static void writeServices(Reply reply, String field, ServiceProfile profile,
                                     ServiceProfileRepository repository,
                                     SchemaRegistryStore registry)
            throws IOException {
        String fingerprint = profile.getSchemaSource().getDescriptorFingerprint();
        DescriptorArtifact artifact = ServiceActionSupport.descriptorArtifact(
                profile, repository, registry);
        FileDescriptorSet set = FileDescriptorSet.parseFrom(artifact.getDescriptorSet());
        List<FileDescriptor> files;
        try {
            files = GoogleDescriptorLoader.fromDescriptorSet(set);
        } catch (Exception e) {
            throw new IOException("descriptor artifact '" + fingerprint + "' cannot be linked", e);
        }
        int serviceCount = 0;
        int methodCount = 0;
        for (FileDescriptor file : files) {
            for (var service : file.getServices()) {
                if (service.getFullName().startsWith("grpc.reflection.")) {
                    continue;
                }
                if (serviceCount++ == MAX_SERVICES) {
                    throw new IOException("descriptor exceeds the service inspection limit of "
                            + MAX_SERVICES);
                }
                Reply serviceReply = reply.append(field).set("name", service.getFullName());
                for (MethodDescriptor method : service.getMethods()) {
                    if (methodCount++ == MAX_METHODS) {
                        throw new IOException("descriptor exceeds the method inspection limit of "
                                + MAX_METHODS);
                    }
                    writeMethod(serviceReply.append("methods"), method);
                }
                serviceReply.build();
            }
        }
    }

    private static void writeMethod(Reply reply, MethodDescriptor method) throws IOException {
        reply.set("name", method.getName())
                .set("fullName", method.getService().getFullName() + "/" + method.getName())
                .set("inputType", method.getInputType().getFullName())
                .set("outputType", method.getOutputType().getFullName())
                .set("clientStreaming", method.isClientStreaming())
                .set("serverStreaming", method.isServerStreaming());
        writeFields(reply, "inputFields", method.getInputType());
        writeFields(reply, "outputFields", method.getOutputType());
        reply.build();
    }

    private static void writeFields(Reply reply, String into, Descriptor descriptor)
            throws IOException {
        if (descriptor.getFields().size() > MAX_FIELDS_PER_MESSAGE) {
            throw new IOException("message '" + descriptor.getFullName()
                    + "' exceeds the field inspection limit of " + MAX_FIELDS_PER_MESSAGE);
        }
        for (FieldDescriptor field : descriptor.getFields()) {
            Reply node = reply.append(into)
                    .set("name", field.getName())
                    .set("jsonName", field.getJsonName())
                    .set("number", field.getNumber())
                    .set("type", field.getType().name().toLowerCase(java.util.Locale.ROOT))
                    .set("cardinality", field.isRepeated() ? "repeated" : "singular");
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                node.set("typeName", field.getMessageType().getFullName());
            } else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                node.set("typeName", field.getEnumType().getFullName());
            }
            node.build();
        }
    }
}
