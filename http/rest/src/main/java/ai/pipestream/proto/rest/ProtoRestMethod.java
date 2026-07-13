package ai.pipestream.proto.rest;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * A registered unary (or unary-shaped) protobuf RPC available over JSON/REST.
 *
 * @param serviceName simple or full service name used in the URL
 * @param methodName method name used in the URL
 * @param serviceDescriptor protobuf service descriptor (may be null for typed-only registrations)
 * @param methodDescriptor protobuf method descriptor (may be null for typed-only registrations)
 * @param requestType generated request class, or empty for dynamic-only
 * @param invoker request message → response message
 * @param apiToken optional API token requirement
 * @param exposed metadata from {@link ProtoRestExposed} (path/summary/http methods)
 * @param path optional path override (takes precedence over {@link ProtoRestExposed#path()})
 * @param summary optional OpenAPI summary
 * @param description optional OpenAPI description
 * @param httpMethods declared HTTP verbs (uppercase); empty = unset, which the gateway treats
 *        as allowing all standard verbs (see {@link #allowedHttpVerbs()})
 */
public record ProtoRestMethod(
        String serviceName,
        String methodName,
        ServiceDescriptor serviceDescriptor,
        MethodDescriptor methodDescriptor,
        Optional<Class<? extends Message>> requestType,
        Function<Message, Message> invoker,
        Optional<ApiTokenRequirement> apiToken,
        Optional<ProtoRestExposed> exposed,
        Optional<String> path,
        Optional<String> summary,
        Optional<String> description,
        String[] httpMethods) {

    /** Verbs allowed when {@link #httpMethods()} is unset (backward-compatible default). */
    public static final List<String> DEFAULT_HTTP_VERBS = List.of("GET", "POST", "PUT", "PATCH", "DELETE");

    public ProtoRestMethod {
        Objects.requireNonNull(serviceName, "serviceName");
        Objects.requireNonNull(methodName, "methodName");
        Objects.requireNonNull(invoker, "invoker");
        requestType = requestType == null ? Optional.empty() : requestType;
        apiToken = apiToken == null ? Optional.empty() : apiToken;
        exposed = exposed == null ? Optional.empty() : exposed;
        path = path == null ? Optional.empty() : path;
        summary = summary == null ? Optional.empty() : summary;
        description = description == null ? Optional.empty() : description;
        // Defensive copy, normalized to uppercase; empty means "unset".
        httpMethods = httpMethods == null
                ? new String[0]
                : Arrays.stream(httpMethods)
                        .filter(Objects::nonNull)
                        .map(m -> m.toUpperCase(Locale.ROOT))
                        .toArray(String[]::new);
    }

    /** @return a defensive copy of the declared verbs (empty when unset) */
    @Override
    public String[] httpMethods() {
        return httpMethods.clone();
    }

    /**
     * @return the verbs this method accepts: the declared {@link #httpMethods()}, or
     *         {@link #DEFAULT_HTTP_VERBS} when none were declared
     */
    public List<String> allowedHttpVerbs() {
        return httpMethods.length == 0 ? DEFAULT_HTTP_VERBS : List.of(httpMethods);
    }

    public String routeKey() {
        return serviceName + "/" + methodName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProtoRestMethod that)) {
            return false;
        }
        return serviceName.equals(that.serviceName)
                && methodName.equals(that.methodName)
                && Objects.equals(serviceDescriptor, that.serviceDescriptor)
                && Objects.equals(methodDescriptor, that.methodDescriptor)
                && requestType.equals(that.requestType)
                && invoker.equals(that.invoker)
                && apiToken.equals(that.apiToken)
                && exposed.equals(that.exposed)
                && path.equals(that.path)
                && summary.equals(that.summary)
                && description.equals(that.description)
                && Arrays.equals(httpMethods, that.httpMethods);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, methodName, serviceDescriptor, methodDescriptor,
                requestType, invoker, apiToken, exposed, path, summary, description);
        return 31 * result + Arrays.hashCode(httpMethods);
    }

    @Override
    public String toString() {
        return "ProtoRestMethod[" + routeKey() + ", httpMethods=" + Arrays.toString(httpMethods) + "]";
    }

    public static Builder builder(String serviceName, String methodName, Function<Message, Message> invoker) {
        return new Builder(serviceName, methodName, invoker);
    }

    public static final class Builder {
        private final String serviceName;
        private final String methodName;
        private final Function<Message, Message> invoker;
        private ServiceDescriptor serviceDescriptor;
        private MethodDescriptor methodDescriptor;
        private Class<? extends Message> requestType;
        private ApiTokenRequirement apiToken;
        private ProtoRestExposed exposed;
        private String path;
        private String summary;
        private String description;
        private String[] httpMethods = {};

        private Builder(String serviceName, String methodName, Function<Message, Message> invoker) {
            this.serviceName = serviceName;
            this.methodName = methodName;
            this.invoker = invoker;
        }

        public Builder serviceDescriptor(ServiceDescriptor serviceDescriptor) {
            this.serviceDescriptor = serviceDescriptor;
            return this;
        }

        public Builder methodDescriptor(MethodDescriptor methodDescriptor) {
            this.methodDescriptor = methodDescriptor;
            return this;
        }

        public Builder requestType(Class<? extends Message> requestType) {
            this.requestType = requestType;
            return this;
        }

        public Builder apiToken(ApiTokenRequirement apiToken) {
            this.apiToken = apiToken;
            return this;
        }

        public Builder apiToken(ProtoApiToken annotation) {
            this.apiToken = annotation == null ? null : ApiTokenRequirement.from(annotation);
            return this;
        }

        public Builder exposed(ProtoRestExposed exposed) {
            this.exposed = exposed;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder summary(String summary) {
            this.summary = summary;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder httpMethods(String... httpMethods) {
            this.httpMethods = httpMethods;
            return this;
        }

        public ProtoRestMethod build() {
            return new ProtoRestMethod(
                    serviceName,
                    methodName,
                    serviceDescriptor,
                    methodDescriptor,
                    Optional.ofNullable(requestType),
                    invoker,
                    Optional.ofNullable(apiToken),
                    Optional.ofNullable(exposed),
                    Optional.ofNullable(path),
                    Optional.ofNullable(summary),
                    Optional.ofNullable(description),
                    httpMethods);
        }
    }
}
