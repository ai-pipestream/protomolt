package ai.pipestream.proto.rest;

import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoRestGatewayTest {

    private ProtoRestMethodRegistry registry;
    private ProtoRestGateway gateway;

    @BeforeEach
    void setUp() {
        registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("EchoService", "Echo", request -> {
                    Struct in = (Struct) request;
                    String name = in.getFieldsOrDefault("name", Value.getDefaultInstance()).getStringValue();
                    return Struct.newBuilder()
                            .putFields("message", Value.newBuilder().setStringValue("hello " + name).build())
                            .build();
                })
                .requestType(Struct.class)
                .summary("Echo a name")
                .build());

        registry.register(ProtoRestMethod.builder("SecureService", "Ping", request ->
                        Struct.newBuilder()
                                .putFields("ok", Value.newBuilder().setBoolValue(true).build())
                                .build())
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.apiKeyHeader("api_token"))
                .build());

        gateway = new ProtoRestGateway(
                registry,
                new ProtobufJsonTranscoder(),
                ProtoApiTokenValidator.sharedSecret("secret-token"));
    }

    @Test
    void invokesTypedMethod() {
        String json = gateway.invoke("EchoService", "Echo", "{\"name\":\"world\"}");
        assertThat(json).contains("hello world");
    }

    @Test
    void missingService() {
        assertThatThrownBy(() -> gateway.invoke("Missing", "Echo", "{}"))
                .isInstanceOf(ServiceNotFoundException.class);
    }

    @Test
    void missingMethod() {
        assertThatThrownBy(() -> gateway.invoke("EchoService", "Nope", "{}"))
                .isInstanceOf(MethodNotFoundException.class);
    }

    @Test
    void defaultConstructorFailsClosedForTokenProtectedMethods() {
        // No validator supplied: token-protected methods must reject ANY token, junk or not.
        ProtoRestGateway failClosed = new ProtoRestGateway(registry, new ProtobufJsonTranscoder());

        assertThatThrownBy(() -> failClosed.invoke(
                "SecureService", "Ping", "{}", Map.of("api_token", "any-junk-token"), Map.of()))
                .isInstanceOf(UnauthorizedProtoRestException.class);
        assertThatThrownBy(() -> failClosed.invoke("SecureService", "Ping", "{}"))
                .isInstanceOf(UnauthorizedProtoRestException.class);

        // Methods without a token requirement keep working.
        assertThat(failClosed.invoke("EchoService", "Echo", "{\"name\":\"open\"}"))
                .contains("hello open");
    }

    @Test
    void enforcesDeclaredHttpMethods() {
        registry.register(ProtoRestMethod.builder("WriteService", "Put", request -> Struct.getDefaultInstance())
                .requestType(Struct.class)
                .httpMethods("POST", "PUT")
                .build());

        assertThat(gateway.invoke("PUT", "WriteService", "Put", "{}", Map.of(), Map.of()))
                .isNotBlank();
        assertThat(gateway.invoke("post", "WriteService", "Put", "{}", Map.of(), Map.of()))
                .isNotBlank();

        assertThatThrownBy(() -> gateway.invoke("GET", "WriteService", "Put", "{}", Map.of(), Map.of()))
                .isInstanceOf(HttpMethodNotAllowedException.class)
                .satisfies(e -> assertThat(((HttpMethodNotAllowedException) e).allowedMethods())
                        .containsExactly("POST", "PUT"));
    }

    @Test
    void undeclaredHttpMethodsAllowAllStandardVerbs() {
        // EchoService.Echo declares no verbs: all five standard verbs pass.
        for (String verb : ProtoRestMethod.DEFAULT_HTTP_VERBS) {
            assertThat(gateway.invoke(verb, "EchoService", "Echo", "{\"name\":\"v\"}", Map.of(), Map.of()))
                    .contains("hello v");
        }
        // Null verb skips enforcement (legacy signature).
        assertThat(gateway.invoke(null, "EchoService", "Echo", "{\"name\":\"v\"}", Map.of(), Map.of()))
                .contains("hello v");
    }

    @Test
    void requiresApiToken() {
        assertThatThrownBy(() -> gateway.invoke("SecureService", "Ping", "{}"))
                .isInstanceOf(UnauthorizedProtoRestException.class);

        String ok = gateway.invoke(
                "SecureService",
                "Ping",
                "{}",
                Map.of("api_token", "secret-token"),
                Map.of());
        assertThat(ok).contains("\"ok\"");
    }
}
