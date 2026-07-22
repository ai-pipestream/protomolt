package ai.pipestream.proto.rest;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Registry of protobuf RPCs exposed over JSON/REST.
 * Framework-agnostic counterpart to Micronaut's {@code GrpcServiceRegistry}.
 *
 * <p>Routes are identified by the URL pair {@code service/method}; a second registration of
 * the same pair is rejected at startup rather than silently replacing the first — otherwise
 * the winner would depend on registration order. When descriptors are present the error
 * names both protobuf service full names, the usual culprit being same-named services from
 * different packages colliding on the simple-name URL segment.</p>
 */
public final class ProtoRestMethodRegistry {

    private final Map<String, Map<String, ProtoRestMethod>> methods = new ConcurrentHashMap<>();

    /**
     * @throws IllegalStateException when {@code service/method} is already registered
     */
    public void register(ProtoRestMethod method) {
        ProtoRestMethod existing = methods
                .computeIfAbsent(method.serviceName(), k -> new ConcurrentHashMap<>())
                .putIfAbsent(method.methodName(), method);
        if (existing != null) {
            throw new IllegalStateException("Route " + method.routeKey()
                    + " is already registered" + collisionDetail(existing, method)
                    + "; routes are rejected instead of silently replaced");
        }
    }

    private static String collisionDetail(ProtoRestMethod existing, ProtoRestMethod added) {
        String existingFullName = existing.serviceDescriptor() == null
                ? null : existing.serviceDescriptor().getFullName();
        String addedFullName = added.serviceDescriptor() == null
                ? null : added.serviceDescriptor().getFullName();
        if (existingFullName == null || existingFullName.equals(addedFullName)) {
            return "";
        }
        return " (existing: " + existingFullName + ", added: "
                + (addedFullName == null ? "typed registration" : addedFullName) + ")";
    }

    public ProtoRestMethod register(
            String serviceName,
            String methodName,
            Class<? extends Message> requestType,
            Function<Message, Message> invoker) {
        ProtoRestMethod method = ProtoRestMethod.builder(serviceName, methodName, invoker)
                .requestType(requestType)
                .build();
        register(method);
        return method;
    }

    public ProtoRestMethod register(
            ServiceDescriptor service,
            MethodDescriptor method,
            Function<Message, Message> invoker,
            ApiTokenRequirement apiToken) {
        ProtoRestMethod restMethod = ProtoRestMethod.builder(service.getName(), method.getName(), invoker)
                .serviceDescriptor(service)
                .methodDescriptor(method)
                .apiToken(apiToken)
                .build();
        register(restMethod);
        return restMethod;
    }

    public Optional<ProtoRestMethod> find(String serviceName, String methodName) {
        Map<String, ProtoRestMethod> byMethod = methods.get(serviceName);
        if (byMethod == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(byMethod.get(methodName));
    }

    public boolean hasService(String serviceName) {
        return methods.containsKey(serviceName);
    }

    public Collection<ProtoRestMethod> all() {
        return methods.values().stream()
                .flatMap(m -> m.values().stream())
                .toList();
    }

    public void clear() {
        methods.clear();
    }
}
