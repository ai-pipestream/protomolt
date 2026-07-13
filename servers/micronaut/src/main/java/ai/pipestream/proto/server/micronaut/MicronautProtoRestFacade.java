package ai.pipestream.proto.server.micronaut;

import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.server.ProtoRestHttpSupport;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Micronaut-oriented gateway facade (annotation wiring lives in the Micronaut app).
 * Serves JSON over gRPC methods at POST /{service}/{method}.
 */
public final class MicronautProtoRestFacade {

    public static final String ENGINE_ID = "micronaut";

    private static final Logger LOG = LoggerFactory.getLogger(MicronautProtoRestFacade.class);

    private final ProtoRestGateway gateway;
    private final ProtoToolsServerConfig config;
    private final ProtoOpenApiGenerator openApiGenerator;
    private volatile String cachedOpenApiJson;

    public MicronautProtoRestFacade(ProtoRestGateway gateway, ProtoToolsServerConfig config) {
        this.gateway = gateway;
        this.config = config;
        this.openApiGenerator = new ProtoOpenApiGenerator(
                "Protobuf REST Gateway", "1.0.0", "/", config.restPathPrefix());
    }

    public String engineId() {
        return ENGINE_ID;
    }

    public String healthJson() {
        return "{\"status\":\"UP\"}";
    }

    public String openApiJson() {
        String cached = cachedOpenApiJson;
        if (cached == null) {
            cached = openApiGenerator.generateJson(gateway.getRegistry());
            cachedOpenApiJson = cached;
        }
        return cached;
    }

    public void invalidateOpenApiCache() {
        cachedOpenApiJson = null;
    }

    /** Invokes without HTTP verb enforcement (legacy signature). */
    public Result invoke(String service, String method, String body, Map<String, String> headers, Map<String, String> query) {
        return invoke(null, service, method, body, headers, query);
    }

    /**
     * Invokes with HTTP verb enforcement; a verb outside the method's declared
     * {@code httpMethods} yields 405 with an {@code Allow} header in {@link Result#headers()}.
     */
    public Result invoke(String httpMethod, String service, String method, String body,
            Map<String, String> headers, Map<String, String> query) {
        try {
            ProtoRestHttpSupport.checkBodySize(body, config.maxRequestBytes());
            Map<String, String> normalized = headers == null ? Map.of() : headers.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().toLowerCase(Locale.ROOT),
                            Map.Entry::getValue,
                            // Keep the first value for duplicate keys, matching the host contract.
                            (a, b) -> a));
            String json = gateway.invoke(
                    httpMethod,
                    service,
                    method,
                    ProtoRestHttpSupport.bodyOrEmptyJson(body),
                    normalized,
                    query == null ? Map.of() : query);
            return new Result(200, json);
        } catch (Throwable err) {
            ProtoRestHttpSupport.logIfServerError(LOG, err);
            Map<String, String> responseHeaders = ProtoRestHttpSupport.allowHeaderFor(err)
                    .map(allow -> Map.of("Allow", allow))
                    .orElse(Map.of());
            return new Result(ProtoRestHttpSupport.statusFor(err), ProtoRestHttpSupport.errorJson(err),
                    responseHeaders);
        }
    }

    public ProtoToolsServerConfig config() {
        return config;
    }

    /**
     * @param headers extra response headers the host must emit (e.g. {@code Allow} on 405)
     */
    public record Result(int status, String body, Map<String, String> headers) {
        public Result {
            headers = headers == null ? Map.of() : Map.copyOf(headers);
        }

        public Result(int status, String body) {
            this(status, body, Map.of());
        }
    }
}
