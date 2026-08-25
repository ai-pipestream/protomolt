package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Binds the methods of a registered service as catalog verbs.
 *
 * <p>A service reaches ProtoMolt by reflection: {@code service-register} discovers its
 * descriptors and stores them. From that point the runtime knows every method's request and
 * response type, which is all a verb has to declare, so each method can be registered as one.
 * A verb bound this way is a verb like any other: an RPC on the gRPC surface, a method on the
 * REST mount, and an MCP tool whose input schema is derived from the request message. No code
 * is generated and nothing restarts.
 *
 * <p>This is what dispatching on messages buys. While a verb was a JSON handler, binding a
 * reflected method meant writing the two conversions by hand for a type nobody had seen at
 * build time, so it could not be done at all.
 */
public final class ReflectedServiceActions {

    /** Reflection services describe the server rather than its work; they are not verbs. */
    private static final String REFLECTION_PREFIX = "grpc.reflection.";

    private ReflectedServiceActions() {
    }

    /**
     * Registers every unary method of {@code profile} as a verb on {@code catalog}.
     *
     * <p>Client-streaming and bidirectional methods are left out: a verb takes one request,
     * and a method that expects a stream of them cannot be driven by one. Server-streaming
     * methods are bound and answer with their first reply.
     *
     * @return the verb names registered, in the order the descriptors declare them
     * @throws ActionException when the profile's descriptors cannot be read
     */
    public static List<String> register(ActionCatalog catalog, ServiceProfile profile,
                                        ServiceProfileRepository repository,
                                        SchemaRegistryStore registry, ChannelFactory channels)
            throws ActionException {
        List<MethodDescriptor> methods = unaryMethods(profile, repository, registry);
        Map<String, String> names = verbNames(profile, methods);
        List<String> registered = new ArrayList<>(methods.size());
        for (MethodDescriptor method : methods) {
            String verb = names.get(method.getFullName());
            catalog.replace(new ReflectedMethodAction(
                    verb, profile.getName(), method, repository, registry, channels));
            registered.add(verb);
        }
        return List.copyOf(registered);
    }

    /** Every method the profile's descriptors declare that one request can drive. */
    private static List<MethodDescriptor> unaryMethods(ServiceProfile profile,
                                                       ServiceProfileRepository repository,
                                                       SchemaRegistryStore registry)
            throws ActionException {
        DescriptorArtifact artifact;
        List<FileDescriptor> files;
        try {
            artifact = ServiceActionSupport.descriptorArtifact(profile, repository, registry);
            files = GoogleDescriptorLoader.fromDescriptorSet(
                    FileDescriptorSet.parseFrom(artifact.getDescriptorSet()));
        } catch (IOException | IllegalArgumentException | DescriptorLoadException e) {
            throw new ActionException("invalid-descriptor", e.getMessage());
        }
        List<MethodDescriptor> methods = new ArrayList<>();
        for (FileDescriptor file : files) {
            for (ServiceDescriptor service : file.getServices()) {
                if (service.getFullName().startsWith(REFLECTION_PREFIX)) {
                    continue;
                }
                for (MethodDescriptor method : service.getMethods()) {
                    if (!method.isClientStreaming()) {
                        methods.add(method);
                    }
                }
            }
        }
        return methods;
    }

    /**
     * A verb name per method: the profile and the method, as {@code billing-charge}.
     *
     * <p>Two services in one profile can declare the same method name, and then neither name
     * says which was meant, so both take the service as well. Only the ambiguous ones do,
     * because a name a caller has to type is worth keeping short.
     */
    private static Map<String, String> verbNames(ServiceProfile profile,
                                                 List<MethodDescriptor> methods) {
        Map<String, Integer> byMethod = new LinkedHashMap<>();
        for (MethodDescriptor method : methods) {
            byMethod.merge(kebab(method.getName()), 1, Integer::sum);
        }
        Map<String, String> names = new LinkedHashMap<>();
        for (MethodDescriptor method : methods) {
            String simple = kebab(method.getName());
            names.put(method.getFullName(), byMethod.get(simple) == 1
                    ? kebab(profile.getName()) + "-" + simple
                    : kebab(profile.getName()) + "-" + kebab(method.getService().getName())
                            + "-" + simple);
        }
        return names;
    }

    /** {@code ListOrders} becomes {@code list-orders}; a name already kebab stays as it is. */
    static String kebab(String name) {
        StringBuilder out = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char character = name.charAt(i);
            if (character == '_' || character == '.') {
                out.append('-');
            } else if (Character.isUpperCase(character)) {
                if (i > 0 && out.charAt(out.length() - 1) != '-') {
                    out.append('-');
                }
                out.append(Character.toLowerCase(character));
            } else {
                out.append(character);
            }
        }
        return out.toString().toLowerCase(Locale.ROOT);
    }
}
