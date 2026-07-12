package ai.pipestream.proto.server.micronaut;

import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.server.ProtoRestHttpSupport;
import ai.pipestream.proto.server.ProtoToolsServerConfig;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Micronaut-oriented gateway facade (annotation wiring lives in the Micronaut app).
 * Serves JSON over gRPC methods at POST /{service}/{method}.
 */
public final class MicronautProtoRestFacade {

    public static final String ENGINE_ID = "micronaut";

    private final ProtoRestGateway gateway;
    private final ProtoToolsServerConfig config;
    private final ProtoOpenApiGenerator openApiGenerator;

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
        return openApiGenerator.generateJson(gateway.getRegistry());
    }

    public Result invoke(String service, String method, String body, Map<String, String> headers, Map<String, String> query) {
        try {
            Map<String, String> normalized = headers == null ? Map.of() : headers.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().toLowerCase(Locale.ROOT),
                            Map.Entry::getValue,
                            (a, b) -> b));
            String json = gateway.invoke(
                    service,
                    method,
                    ProtoRestHttpSupport.bodyOrEmptyJson(body),
                    normalized,
                    query == null ? Map.of() : query);
            return new Result(200, json);
        } catch (Throwable err) {
            return new Result(ProtoRestHttpSupport.statusFor(err), ProtoRestHttpSupport.errorJson(err));
        }
    }

    public ProtoToolsServerConfig config() {
        return config;
    }

    public record Result(int status, String body) {
    }
}
