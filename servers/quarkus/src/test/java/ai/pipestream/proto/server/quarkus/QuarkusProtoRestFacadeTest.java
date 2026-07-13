package ai.pipestream.proto.server.quarkus;

import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethod;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QuarkusProtoRestFacadeTest {

    private QuarkusProtoRestFacade facade;
    private ProtoRestGateway gateway;

    @BeforeEach
    void setUp() {
        gateway = new ProtoRestGateway(
                newRegistry(),
                new ProtobufJsonTranscoder(),
                ProtoApiTokenValidator.acceptNonBlank());
        facade = new QuarkusProtoRestFacade(gateway, ProtoToolsServerConfig.defaults());
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
        registry.register(ProtoRestMethod.builder("SecureService", "Ping", request ->
                        Struct.newBuilder()
                                .putFields("ok", Value.newBuilder().setBoolValue(true).build())
                                .build())
                .requestType(Struct.class)
                .apiToken(ai.pipestream.proto.rest.ApiTokenRequirement.apiKeyHeader("api_token"))
                .build());
        return registry;
    }

    @Test
    void invokesWithNullBodyCoerced() {
        assertThat(facade.engineId()).isEqualTo("quarkus");
        assertThat(facade.healthJson()).contains("UP");
        assertThat(facade.openApiJson()).contains("EchoService");

        QuarkusProtoRestFacade.Result ok = facade.invoke(
                "EchoService", "Echo", "{\"name\":\"Ada\"}", Map.of(), Map.of());
        assertThat(ok.status()).isEqualTo(200);
        assertThat(ok.body()).contains("hello Ada");

        QuarkusProtoRestFacade.Result missing = facade.invoke("Nope", "Echo", null, null, null);
        assertThat(missing.status()).isEqualTo(404);

        QuarkusProtoRestFacade.Result noBody = facade.invoke("EchoService", "Echo", null, Map.of(), Map.of());
        assertThat(noBody.status()).isEqualTo(200);
        assertThat(noBody.body()).contains("hello ");
    }

    @Test
    void honorsInjectedConfigPrefix() throws Exception {
        ProtoToolsServerConfig custom = new ProtoToolsServerConfig(
                "0.0.0.0", 8080, "/custom-json", "/openapi.json", "/health");
        QuarkusProtoRestFacade customFacade = new QuarkusProtoRestFacade(gateway, custom);
        assertThat(customFacade.config().restPathPrefix()).isEqualTo("/custom-json");
        assertThat(customFacade.openApiJson()).contains("/custom-json/EchoService/Echo");

        // CDI must wire the config, not hardcode defaults.
        assertThat(QuarkusProtoRestFacade.class
                .getConstructor(ai.pipestream.proto.rest.ProtoRestGateway.class, ProtoToolsServerConfig.class)
                .isAnnotationPresent(jakarta.inject.Inject.class)).isTrue();
    }

    @Test
    void declaredHttpMethodsAreEnforcedWith405AndAllow() {
        QuarkusProtoRestFacade.Result viaGet = facade.invoke(
                "GET", "RestrictedService", "PostOnly", "{}", Map.of(), Map.of());
        assertThat(viaGet.status()).isEqualTo(405);
        assertThat(viaGet.headers()).containsEntry("Allow", "POST");

        assertThat(facade.invoke("POST", "RestrictedService", "PostOnly", "{}", Map.of(), Map.of()).status())
                .isEqualTo(200);
        // Undeclared verbs allow all standard verbs.
        assertThat(facade.invoke("DELETE", "EchoService", "Echo", "{}", Map.of(), Map.of()).status())
                .isEqualTo(200);
    }

    @Test
    void oversizedBodyIs413() {
        QuarkusProtoRestFacade small = new QuarkusProtoRestFacade(
                gateway, ProtoToolsServerConfig.defaults().withMaxRequestBytes(64));
        QuarkusProtoRestFacade.Result res = small.invoke(
                "POST", "EchoService", "Echo",
                "{\"name\":\"" + "x".repeat(256) + "\"}", Map.of(), Map.of());
        assertThat(res.status()).isEqualTo(413);
    }

    @Test
    void serverErrorBodyIsGeneric() {
        QuarkusProtoRestFacade.Result res = facade.invoke(
                "POST", "BoomService", "Boom", "{}", Map.of(), Map.of());
        assertThat(res.status()).isEqualTo(500);
        assertThat(res.body()).contains("Internal server error");
        assertThat(res.body()).doesNotContain("kaboom-secret-detail");
    }

    @Test
    void defaultGatewayFailsClosedForTokenProtectedMethods() {
        QuarkusProtoRestFacade failClosed = new QuarkusProtoRestFacade(
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder()),
                ProtoToolsServerConfig.defaults());
        assertThat(failClosed.invoke(
                "POST", "SecureService", "Ping", "{}",
                Map.of("api_token", "any-junk-token"), Map.of()).status())
                .isEqualTo(401);
    }
}
