package ai.pipestream.proto.server.quarkus;

import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.server.ProtoRestHttpSupport;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Quarkus (Vert.x 4 era) adapter — JAX-RS / RESTEasy resources call this bean.
 *
 * <p>Quarkus is not yet on Vert.x 5; until it is, roll REST here rather than using
 * {@code servers/vertx}. When Quarkus moves to Vert.x 5, prefer mounting
 * {@code VertxProtoRestServer#createRouter()}.
 */
@ApplicationScoped
public class QuarkusProtoRestFacade {

    public static final String ENGINE_ID = "quarkus";

    private static final Logger LOG = LoggerFactory.getLogger(QuarkusProtoRestFacade.class);

    private final ProtoRestGateway gateway;
    private final ProtoToolsServerConfig config;
    private final ProtoOpenApiGenerator openApiGenerator;
    private volatile String cachedOpenApiJson;

    public QuarkusProtoRestFacade(ProtoRestGateway gateway) {
        this(gateway, ProtoToolsServerConfig.defaults());
    }

    @Inject
    public QuarkusProtoRestFacade(ProtoRestGateway gateway, ProtoToolsServerConfig config) {
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
