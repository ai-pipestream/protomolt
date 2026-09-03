package ai.protomolt.proto.server.micronaut;

import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoApiToken;
import ai.protomolt.proto.http.rest.ProtoApiTokenValidator;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.http.rest.ProtoRestMethod;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.server.ProtoToolsServerConfig;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MicronautProtoRestFacadeTest {

    private MicronautProtoRestFacade facade;

    @BeforeEach
    void setUp() {
        ProtoRestGateway gateway = new ProtoRestGateway(
                newRegistry(),
                new ProtobufJsonTranscoder(),
                ProtoApiTokenValidator.sharedSecret("secret-token"));
        facade = new MicronautProtoRestFacade(gateway, ProtoToolsServerConfig.defaults());
    }

    private static ProtoRestMethodRegistry newRegistry() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("EchoService", "Echo", request -> {
                    Struct in = (Struct) request;
                    String name = in.getFieldsOrDefault("name", Value.getDefaultInstance()).getStringValue();
                    return Struct.newBuilder()
                            .putFields("message", Value.newBuilder().setStringValue("hello " + name).build())
                            .build();
                })
                .requestType(Struct.class)
                .build());
        registry.register(ProtoRestMethod.builder("SecureService", "Ping", request ->
                        Struct.newBuilder()
                                .putFields("ok", Value.newBuilder().setBoolValue(true).build())
                                .build())
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.apiKeyHeader("api_token"))
                .build());
        registry.register(ProtoRestMethod.builder("RestrictedService", "PostOnly",
                        request -> Struct.getDefaultInstance())
                .requestType(Struct.class)
                .httpMethods("POST")
                .build());
        registry.register(ProtoRestMethod.builder("BoomService", "Boom", request -> {
                    throw new RuntimeException("kaboom-secret-detail");
                })
                .requestType(Struct.class)
                .build());
        registry.register(ProtoRestMethod.builder("QueryService", "Check",
                        request -> Struct.getDefaultInstance())
                .requestType(Struct.class)
                .apiToken(new ApiTokenRequirement("tok", ProtoApiToken.In.QUERY,
                        ProtoApiToken.Scheme.API_KEY, "bearer", true, "query token"))
                .build());
        return registry;
    }

    @Test
    void invokesAndReportsEngine() {
        assertThat(facade.engineId()).isEqualTo("micronaut");
        assertThat(facade.healthJson()).contains("UP");
        assertThat(facade.openApiJson()).contains("EchoService");

        MicronautProtoRestFacade.Result ok = facade.invoke(
                "EchoService", "Echo", "{\"name\":\"world\"}", Map.of(), Map.of());
        assertThat(ok.status()).isEqualTo(200);
        assertThat(ok.body()).contains("hello world");
    }

    @Test
    void dispatchesEmptyJsonWhenBodyAbsent() {
        MicronautProtoRestFacade.Result ok = facade.invoke("EchoService", "Echo", null, Map.of(), Map.of());
        assertThat(ok.status()).isEqualTo(200);
        assertThat(ok.body()).contains("hello ");
    }

    @Test
    void mapsMissingServiceAndUnauthorized() {
        assertThat(facade.invoke("Missing", "Echo", "{}", null, null).status()).isEqualTo(404);
        assertThat(facade.invoke("SecureService", "Ping", "{}", Map.of(), Map.of()).status()).isEqualTo(401);
        assertThat(facade.invoke(
                "SecureService", "Ping", "{}", Map.of("api_token", "secret-token"), Map.of()).status())
                .isEqualTo(200);
    }

    @Test
    void declaredHttpMethodsAreEnforcedWith405AndAllow() {
        MicronautProtoRestFacade.Result viaGet = facade.invoke(
                "GET", "RestrictedService", "PostOnly", "{}", Map.of(), Map.of());
        assertThat(viaGet.status()).isEqualTo(405);
        assertThat(viaGet.headers()).containsEntry("Allow", "POST");

        assertThat(facade.invoke("POST", "RestrictedService", "PostOnly", "{}", Map.of(), Map.of()).status())
                .isEqualTo(200);
        // Undeclared verbs default to POST only, matching the OpenAPI contract.
        MicronautProtoRestFacade.Result viaDelete = facade.invoke(
                "DELETE", "EchoService", "Echo", "{}", Map.of(), Map.of());
        assertThat(viaDelete.status()).isEqualTo(405);
        assertThat(viaDelete.headers()).containsEntry("Allow", "POST");
    }

    @Test
    void oversizedBodyIs413() {
        MicronautProtoRestFacade small = new MicronautProtoRestFacade(
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder(),
                        ProtoApiTokenValidator.sharedSecret("secret-token")),
                ProtoToolsServerConfig.defaults().withMaxRequestBytes(64));
        MicronautProtoRestFacade.Result res = small.invoke(
                "POST", "EchoService", "Echo",
                "{\"name\":\"" + "x".repeat(256) + "\"}", Map.of(), Map.of());
        assertThat(res.status()).isEqualTo(413);
    }

    @Test
    void serverErrorBodyIsGeneric() {
        MicronautProtoRestFacade.Result res = facade.invoke(
                "POST", "BoomService", "Boom", "{}", Map.of(), Map.of());
        assertThat(res.status()).isEqualTo(500);
        assertThat(res.body()).contains("Internal server error");
        assertThat(res.body()).doesNotContain("kaboom-secret-detail");
    }

    @Test
    void defaultGatewayFailsClosedForTokenProtectedMethods() {
        MicronautProtoRestFacade failClosed = new MicronautProtoRestFacade(
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder()),
                ProtoToolsServerConfig.defaults());
        assertThat(failClosed.invoke(
                "POST", "SecureService", "Ping", "{}",
                Map.of("api_token", "any-junk-token"), Map.of()).status())
                .isEqualTo(401);
    }

    @Test
    void openApiResponseIsCachedUntilInvalidated() {
        ProtoRestMethodRegistry registry = newRegistry();
        MicronautProtoRestFacade caching = new MicronautProtoRestFacade(
                new ProtoRestGateway(registry, new ProtobufJsonTranscoder(),
                        ProtoApiTokenValidator.sharedSecret("secret-token")),
                ProtoToolsServerConfig.defaults());
        String first = caching.openApiJson();
        assertThat(first).contains("EchoService");

        registry.register(ProtoRestMethod.builder("LateService", "Late",
                        request -> Struct.getDefaultInstance())
                .requestType(Struct.class)
                .build());
        // The cached document is served until explicitly invalidated.
        assertThat(caching.openApiJson()).isSameAs(first);
        assertThat(caching.openApiJson()).doesNotContain("LateService");

        caching.invalidateOpenApiCache();
        assertThat(caching.openApiJson()).contains("LateService");
    }

    @Test
    void headerNamesAreLowercasedBeforeValidation() {
        assertThat(facade.invoke("POST", "SecureService", "Ping", "{}",
                Map.of("API_TOKEN", "secret-token"), Map.of()).status())
                .isEqualTo(200);

        // Two keys that differ only by case collapse to one; the first value wins.
        Map<String, String> dupes = new LinkedHashMap<>();
        dupes.put("Api_Token", "secret-token");
        dupes.put("API_TOKEN", "junk");
        assertThat(facade.invoke("POST", "SecureService", "Ping", "{}", dupes, Map.of()).status())
                .isEqualTo(200);
    }

    @Test
    void legacyInvokeSkipsVerbEnforcement() {
        // The 5-arg legacy signature passes a null verb, so declared httpMethods are not enforced.
        MicronautProtoRestFacade.Result res = facade.invoke(
                "RestrictedService", "PostOnly", "{}", Map.of(), Map.of());
        assertThat(res.status()).isEqualTo(200);
        assertThat(res.headers()).isEmpty();
    }

    @Test
    void blankBodyIsCoercedToEmptyJson() {
        MicronautProtoRestFacade.Result res = facade.invoke(
                "POST", "EchoService", "Echo", "   ", Map.of(), Map.of());
        assertThat(res.status()).isEqualTo(200);
        assertThat(res.body()).contains("hello ");
    }

    @Test
    void malformedJsonBodyIs400() {
        MicronautProtoRestFacade.Result res = facade.invoke(
                "POST", "EchoService", "Echo", "{not-json", Map.of(), Map.of());
        assertThat(res.status()).isEqualTo(400);
        assertThat(res.body()).contains("\"status\":400");
    }

    @Test
    void tokenAcceptedViaQueryParam() {
        assertThat(facade.invoke("POST", "QueryService", "Check", "{}", Map.of(), Map.of()).status())
                .isEqualTo(401);
        assertThat(facade.invoke("POST", "QueryService", "Check", "{}",
                Map.of(), Map.of("tok", "secret-token")).status())
                .isEqualTo(200);
    }

    @Test
    void bodyExactlyAtSizeLimitIsAccepted() {
        MicronautProtoRestFacade small = new MicronautProtoRestFacade(
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder(),
                        ProtoApiTokenValidator.sharedSecret("secret-token")),
                ProtoToolsServerConfig.defaults().withMaxRequestBytes(64));
        // ASCII bodies: 9 + 53 + 2 = 64 bytes exactly, one more byte tips over the limit.
        String exact = "{\"name\":\"" + "x".repeat(53) + "\"}";
        assertThat(small.invoke("POST", "EchoService", "Echo", exact, Map.of(), Map.of()).status())
                .isEqualTo(200);
        String over = "{\"name\":\"" + "x".repeat(54) + "\"}";
        assertThat(small.invoke("POST", "EchoService", "Echo", over, Map.of(), Map.of()).status())
                .isEqualTo(413);
    }

    @Test
    void configAccessorReturnsInjectedConfig() {
        assertThat(facade.config()).isEqualTo(ProtoToolsServerConfig.defaults());
        assertThat(facade.config().maxRequestBytes())
                .isEqualTo(ProtoToolsServerConfig.DEFAULT_MAX_REQUEST_BYTES);
    }

    @Test
    void resultRecordNormalizesAndDefensivelyCopiesHeaders() {
        MicronautProtoRestFacade.Result nullHeaders =
                new MicronautProtoRestFacade.Result(200, "ok", null);
        assertThat(nullHeaders.headers()).isEmpty();

        MicronautProtoRestFacade.Result twoArg = new MicronautProtoRestFacade.Result(200, "ok");
        assertThat(twoArg.headers()).isEmpty();

        Map<String, String> mutable = new HashMap<>();
        mutable.put("Allow", "POST");
        MicronautProtoRestFacade.Result copied = new MicronautProtoRestFacade.Result(405, "err", mutable);
        mutable.put("X-Evil", "1");
        assertThat(copied.headers()).containsExactly(Map.entry("Allow", "POST"));
        assertThatThrownBy(() -> copied.headers().put("a", "b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
