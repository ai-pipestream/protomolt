package ai.pipestream.proto.server.micronaut;

import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.rest.ApiTokenRequirement;
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
        // Undeclared verbs allow all standard verbs.
        assertThat(facade.invoke("DELETE", "EchoService", "Echo", "{}", Map.of(), Map.of()).status())
                .isEqualTo(200);
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
}
