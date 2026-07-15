package ai.pipestream.proto.rest;

import com.google.protobuf.Message;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Registers protobuf-shaped methods annotated with {@link ProtoRestExposed}
 * (and optional {@link ProtoApiToken}) onto a {@link ProtoRestMethodRegistry}.
 *
 * <p>Framework glue (Spring/Quarkus) can call {@link #register(Object)} for each bean;
 * plain Java samples can do the same without a DI container.
 */
public final class ProtoRestAnnotationRegistrar {

    private final ProtoRestMethodRegistry registry;

    public ProtoRestAnnotationRegistrar(ProtoRestMethodRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public List<ProtoRestMethod> register(Object bean) {
        Objects.requireNonNull(bean, "bean");
        Class<?> type = bean.getClass();
        ProtoRestExposed typeExposed = type.getAnnotation(ProtoRestExposed.class);
        ProtoApiToken typeToken = type.getAnnotation(ProtoApiToken.class);

        List<ProtoRestMethod> registered = new ArrayList<>();
        for (Method method : type.getMethods()) {
            if (!isCandidate(method)) {
                continue;
            }
            ProtoRestExposed methodExposed = method.getAnnotation(ProtoRestExposed.class);
            // Methods must opt in; type-level @ProtoRestExposed / @ProtoApiToken supply defaults.
            if (methodExposed == null) {
                continue;
            }
            ProtoApiToken methodToken = method.getAnnotation(ProtoApiToken.class);
            ProtoRestMethod restMethod = toRestMethod(
                    bean, type, method, typeExposed, methodExposed, typeToken, methodToken);
            registry.register(restMethod);
            registered.add(restMethod);
        }
        return registered;
    }

    public List<ProtoRestMethod> registerAll(Iterable<?> beans) {
        List<ProtoRestMethod> all = new ArrayList<>();
        for (Object bean : beans) {
            all.addAll(register(bean));
        }
        return all;
    }

    private static boolean isCandidate(Method method) {
        if (method.getDeclaringClass() == Object.class) {
            return false;
        }
        int mods = method.getModifiers();
        if (!Modifier.isPublic(mods) || Modifier.isStatic(mods)) {
            return false;
        }
        if (method.getParameterCount() != 1) {
            return false;
        }
        Class<?> param = method.getParameterTypes()[0];
        Class<?> ret = method.getReturnType();
        return Message.class.isAssignableFrom(param) && Message.class.isAssignableFrom(ret);
    }

    @SuppressWarnings("unchecked")
    private static ProtoRestMethod toRestMethod(
            Object bean,
            Class<?> type,
            Method method,
            ProtoRestExposed typeExposed,
            ProtoRestExposed methodExposed,
            ProtoApiToken typeToken,
            ProtoApiToken methodToken) {
        String serviceName = resolveServiceName(type, typeExposed, methodExposed);
        String methodName = method.getName();
        Class<? extends Message> requestType = (Class<? extends Message>) method.getParameterTypes()[0];
        Function<Message, Message> invoker = request -> invoke(bean, method, request);

        ProtoRestMethod.Builder builder = ProtoRestMethod.builder(serviceName, methodName, invoker)
                .requestType(requestType);

        ProtoRestExposed exposed = methodExposed;
        builder.exposed(exposed);
        if (!exposed.path().isBlank()) {
            // No host routes per-method custom paths; registering one would publish an
            // OpenAPI route that 404s. Fail at startup instead of lying in the contract.
            throw new IllegalStateException("@ProtoRestExposed(path=...) on method "
                    + type.getName() + "#" + method.getName() + " is not supported: methods "
                    + "are served at {service}/{method}; use a class-level path to rename "
                    + "the service segment");
        }
        if (!exposed.summary().isBlank()) {
            builder.summary(exposed.summary());
        } else if (typeExposed != null && !typeExposed.summary().isBlank()) {
            builder.summary(typeExposed.summary());
        }
        if (!exposed.description().isBlank()) {
            builder.description(exposed.description());
        } else if (typeExposed != null && !typeExposed.description().isBlank()) {
            builder.description(typeExposed.description());
        }
        if (exposed.httpMethods().length > 0) {
            builder.httpMethods(exposed.httpMethods());
        } else if (typeExposed != null && typeExposed.httpMethods().length > 0) {
            builder.httpMethods(typeExposed.httpMethods());
        }

        ProtoApiToken token = methodToken != null ? methodToken : typeToken;
        if (token != null) {
            builder.apiToken(token);
        }

        return builder.build();
    }

    private static String resolveServiceName(
            Class<?> type,
            ProtoRestExposed typeExposed,
            ProtoRestExposed methodExposed) {
        // Prefer an explicit class-level path segment if it looks like a service name (no slashes).
        if (typeExposed != null && !typeExposed.path().isBlank() && !typeExposed.path().contains("/")) {
            return stripLeadingSlash(typeExposed.path());
        }
        String simple = type.getSimpleName();
        if (simple.endsWith("Service") && simple.length() > "Service".length()) {
            return simple.substring(0, simple.length() - "Service".length());
        }
        if (simple.endsWith("Impl") && simple.length() > "Impl".length()) {
            return simple.substring(0, simple.length() - "Impl".length());
        }
        return simple;
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private static Message invoke(Object bean, Method method, Message request) {
        try {
            Object result = method.invoke(bean, request);
            if (result == null) {
                throw new ProtoRestInvocationException(method.getName() + " returned null");
            }
            return (Message) result;
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new ProtoRestInvocationException(
                    "Failed invoking " + method.getDeclaringClass().getSimpleName() + "." + method.getName(),
                    cause);
        }
    }
}
