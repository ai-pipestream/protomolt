package ai.pipestream.proto.rest;

import ai.pipestream.proto.json.ProtobufJsonException;
import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Framework-agnostic JSON/REST → protobuf RPC gateway.
 *
 * <p>Mirrors Micronaut {@code GrpcProxyService}: {@code POST /{service}/{method}} with a JSON body.
 * Descriptor resolution for dynamic requests goes through the transcoder's
 * {@link ai.pipestream.proto.descriptors.DescriptorRegistry} (Apicurio / Confluent / classpath plugins).
 */
public final class ProtoRestGateway {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoRestGateway.class);

    private final ProtoRestMethodRegistry registry;
    private final ProtobufJsonTranscoder transcoder;
    private final ProtoApiTokenValidator tokenValidator;

    /**
     * Creates a gateway with the fail-closed default validator: every method that carries a
     * required token responds 401 until a real {@link ProtoApiTokenValidator} is supplied.
     */
    public ProtoRestGateway(ProtoRestMethodRegistry registry, ProtobufJsonTranscoder transcoder) {
        this(registry, transcoder, ProtoApiTokenValidator.denyAll());
        warnIfTokenProtectedWithoutValidator();
    }

    public ProtoRestGateway(
            ProtoRestMethodRegistry registry,
            ProtobufJsonTranscoder transcoder,
            ProtoApiTokenValidator tokenValidator) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.transcoder = Objects.requireNonNull(transcoder, "transcoder");
        this.tokenValidator = Objects.requireNonNull(tokenValidator, "tokenValidator");
    }

    private void warnIfTokenProtectedWithoutValidator() {
        long protectedMethods = registry.all().stream()
                .filter(m -> m.apiToken().map(ApiTokenRequirement::required).orElse(false))
                .count();
        if (protectedMethods > 0) {
            LOG.warn("{} registered method(s) require an API token but no ProtoApiTokenValidator "
                            + "was supplied; they will respond 401 until one is configured "
                            + "(e.g. ProtoApiTokenValidator.sharedSecret(...))",
                    protectedMethods);
        }
    }

    public String invoke(String serviceName, String methodName, String jsonRequest) {
        return invoke(serviceName, methodName, jsonRequest, Map.of(), Map.of());
    }

    public String invoke(
            String serviceName,
            String methodName,
            String jsonRequest,
            Map<String, String> headers,
            Map<String, String> queryParams) {
        return invoke(null, serviceName, methodName, jsonRequest, headers, queryParams);
    }

    /**
     * Invokes a registered method, enforcing its declared HTTP verbs.
     *
     * @param httpMethod the request's HTTP verb, or {@code null} to skip verb enforcement
     * @throws HttpMethodNotAllowedException when the verb is not among the method's
     *         {@link ProtoRestMethod#allowedHttpVerbs()} (an empty declaration means POST only)
     */
    public String invoke(
            String httpMethod,
            String serviceName,
            String methodName,
            String jsonRequest,
            Map<String, String> headers,
            Map<String, String> queryParams) {
        Objects.requireNonNull(serviceName, "serviceName");
        Objects.requireNonNull(methodName, "methodName");
        Objects.requireNonNull(jsonRequest, "jsonRequest");

        Map<String, String> normalizedHeaders = normalizeHeaders(headers);
        Map<String, String> safeQuery = queryParams == null ? Map.of() : queryParams;

        if (!registry.hasService(serviceName)) {
            throw new ServiceNotFoundException(serviceName);
        }

        ProtoRestMethod method = registry.find(serviceName, methodName)
                .orElseThrow(() -> new MethodNotFoundException(serviceName, methodName));

        if (httpMethod != null) {
            String verb = httpMethod.toUpperCase(Locale.ROOT);
            List<String> allowed = method.allowedHttpVerbs();
            if (!allowed.contains(verb)) {
                throw new HttpMethodNotAllowedException(verb, allowed);
            }
        }

        method.apiToken().ifPresent(token -> {
            if (token.required()) {
                tokenValidator.validate(token, normalizedHeaders, safeQuery)
                        .ifPresent(reason -> {
                            throw new UnauthorizedProtoRestException(reason);
                        });
            }
        });

        try {
            Message request = decodeRequest(method, jsonRequest);
            Message response = method.invoker().apply(request);
            if (response == null) {
                return "{}";
            }
            return transcoder.toJson(response);
        } catch (ProtobufJsonException | ProtoRestException e) {
            // Both vocabularies already carry a status mapping (e.g. a MalformedRequestException
            // thrown by an invoker for a client-repairable failure); pass them through unwrapped.
            throw e;
        } catch (RuntimeException e) {
            LOG.error("Invocation failed for {}/{}", serviceName, methodName, e);
            throw new ProtoRestInvocationException(
                    "Failed invoking " + serviceName + "/" + methodName + ": " + e.getMessage(), e);
        }
    }

    private Message decodeRequest(ProtoRestMethod method, String jsonRequest) {
        if (method.requestType().isPresent()) {
            return transcoder.fromJson(jsonRequest, method.requestType().get());
        }
        if (method.methodDescriptor() != null) {
            Descriptor input = method.methodDescriptor().getInputType();
            return transcoder.fromJsonDynamic(jsonRequest, input);
        }
        throw new ProtoRestInvocationException(
                "Method " + method.routeKey() + " has no request type or method descriptor");
    }

    private static Map<String, String> normalizeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Collections.emptyMap();
        }
        return headers.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getValue() != null)
                .collect(Collectors.toUnmodifiableMap(
                        e -> e.getKey().toLowerCase(Locale.ROOT),
                        Map.Entry::getValue,
                        // Keep the first value for duplicate keys, matching the host contract.
                        (a, b) -> a));
    }

    public ProtoRestMethodRegistry getRegistry() {
        return registry;
    }

    public ProtobufJsonTranscoder getTranscoder() {
        return transcoder;
    }
}
