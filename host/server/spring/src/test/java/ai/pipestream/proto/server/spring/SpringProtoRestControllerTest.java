package ai.pipestream.proto.server.spring;

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
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class SpringProtoRestControllerTest {

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ProtoRestGateway gateway = new ProtoRestGateway(
                newRegistry(),
                new ProtobufJsonTranscoder(),
                ProtoApiTokenValidator.sharedSecret("secret-token"));
        mockMvc = mockMvcFor(gateway, ProtoToolsServerConfig.defaults());
        assertThat(SpringProtoRestController.ENGINE_ID).isEqualTo("spring");
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
                .httpMethods("GET", "POST", "PUT", "PATCH", "DELETE")
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

    private static MockMvc mockMvcFor(ProtoRestGateway gateway, ProtoToolsServerConfig config) {
        SpringProtoRestController controller = new SpringProtoRestController(gateway, config);
        return MockMvcBuilders.standaloneSetup(controller)
                .addPlaceholderValue("pipestream.proto.rest.health-path", "/health")
                .addPlaceholderValue("pipestream.proto.rest.openapi-path", "/openapi.json")
                .addPlaceholderValue("pipestream.proto.rest.path-prefix", "/grpc-json")
                .build();
    }

    @Test
    void healthAndOpenApi() throws Exception {
        String health = mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
        assertThat(health).contains("UP");

        String openApi = mockMvc.perform(get("/openapi.json"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andReturn()
                .getResponse()
                .getContentAsString();
        assertThat(openApi).contains("EchoService");
    }

    @Test
    void invokesViaGetAndDeleteWithoutBody() throws Exception {
        String viaGet = mockMvc.perform(get("/grpc-json/EchoService/Echo"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
        assertThat(viaGet).contains("hello ");

        String viaDelete = mockMvc.perform(delete("/grpc-json/EchoService/Echo"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
        assertThat(viaDelete).contains("hello ");
    }

    @Test
    void invokeEchoAndAuthFailures() throws Exception {
        String body = mockMvc.perform(post("/grpc-json/EchoService/Echo")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Ada\"}"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
        assertThat(body).contains("hello Ada");

        mockMvc.perform(post("/grpc-json/SecureService/Ping")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(post("/grpc-json/Missing/Echo")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void acceptsNonJsonContentTypes() throws Exception {
        // No consumes restriction: other hosts accept any content-type, Spring must too.
        mockMvc.perform(post("/grpc-json/EchoService/Echo")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("{\"name\":\"Ada\"}"))
                .andExpect(status().isOk());
    }

    @Test
    void declaredHttpMethodsAreEnforcedWith405AndAllow() throws Exception {
        String allow = mockMvc.perform(get("/grpc-json/RestrictedService/PostOnly"))
                .andExpect(status().isMethodNotAllowed())
                .andReturn()
                .getResponse()
                .getHeader("Allow");
        assertThat(allow).isEqualTo("POST");

        mockMvc.perform(post("/grpc-json/RestrictedService/PostOnly")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isOk());
    }

    @Test
    void trailingSlashOnInvokeRouteIs404() throws Exception {
        mockMvc.perform(get("/grpc-json/EchoService/Echo/"))
                .andExpect(status().isNotFound());
    }

    @Test
    void serverErrorBodyIsGeneric() throws Exception {
        String body = mockMvc.perform(post("/grpc-json/BoomService/Boom")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isInternalServerError())
                .andReturn()
                .getResponse()
                .getContentAsString();
        assertThat(body).contains("Internal server error");
        assertThat(body).doesNotContain("kaboom-secret-detail");
    }

    @Test
    void oversizedBodyIs413() throws Exception {
        MockMvc small = mockMvcFor(
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder(),
                        ProtoApiTokenValidator.sharedSecret("secret-token")),
                ProtoToolsServerConfig.defaults().withMaxRequestBytes(64));
        small.perform(post("/grpc-json/EchoService/Echo")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"" + "x".repeat(256) + "\"}"))
                .andExpect(status().isPayloadTooLarge());
    }

    @Test
    void defaultGatewayFailsClosedForTokenProtectedMethods() throws Exception {
        MockMvc failClosed = mockMvcFor(
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder()),
                ProtoToolsServerConfig.defaults());
        failClosed.perform(post("/grpc-json/SecureService/Ping")
                        .header("api_token", "any-junk-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isUnauthorized());
    }
}
