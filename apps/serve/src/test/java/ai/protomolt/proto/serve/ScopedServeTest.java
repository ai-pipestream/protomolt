package ai.protomolt.proto.serve;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.authz.ScopeBudget;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The serve process under a mounted access policy: gRPC scope refusals by name, the MCP
 * session pinned to its scoped caller with a filtered catalog, REST still operator-only,
 * and a policy without the operator token refused at construction.
 */
class ScopedServeTest {

    private static final String TOKEN = "operator-sekret";
    private static final String READER = "reader-sekret";
    private static final String METERED = "metered-sekret";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The schema-read slice of the serve catalog (44 typed verbs + delegation + mesh). */
    private static final int READER_TOOLS = 21;

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;
    private static Path policyFile;

    @BeforeAll
    static void start() throws Exception {
        policyFile = Files.createTempDirectory("protomolt-authz").resolve("policy.json");
        AccessPolicy policy = AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("ci-reader")
                        .addCredentialSha256(AccessPolicyCallers.sha256Hex(READER))
                        .addScopes(Scopes.SCHEMA_READ))
                .addPrincipals(Principal.newBuilder()
                        .setName("metered")
                        .addCredentialSha256(AccessPolicyCallers.sha256Hex(METERED))
                        .addScopes(Scopes.SCHEMA_READ)
                        .addBudgets(ScopeBudget.newBuilder()
                                .setScope(Scopes.SCHEMA_READ)
                                .setRequestsPerMinute(2)))
                .build();
        Files.writeString(policyFile, JsonFormat.printer().print(policy));
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, TOKEN, false, null, null,
                List.of(), null, null, null, null, null, null, policyFile));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static Object grpcCall(String method, String credential) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", serve.grpcPort())
                .usePlaintext()
                .build();
        try {
            var descriptor = ProtoMoltServiceSchema.service().findMethodByName(method);
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    credential);
            return DynamicGrpcCalls.call(channel, descriptor,
                    DynamicMessage.newBuilder(descriptor.getInputType()).build(),
                    CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS), headers, 4);
        } catch (StatusRuntimeException e) {
            return e;
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void grpcRefusesThePrincipalOutsideItsScopeByName() {
        Object denied = grpcCall("GetJob", READER);
        assertThat(denied).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(e.getStatus().getDescription())
                    .contains("ci-reader").contains(Scopes.SERVICE_INVOKE);
        });
        assertThat(grpcCall("ListTypes", READER)).isInstanceOf(List.class);
    }

    @Test
    void aBudgetedPrincipalExhaustsItsRateAsResourceExhausted() {
        assertThat(grpcCall("ListTypes", METERED)).isInstanceOf(List.class);
        assertThat(grpcCall("ListTypes", METERED)).isInstanceOf(List.class);
        Object exhausted = grpcCall("ListTypes", METERED);
        assertThat(exhausted).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(e.getStatus().getDescription())
                    .contains("metered").contains("2-per-minute")
                    .contains(Scopes.SCHEMA_READ);
        });
    }

    @Test
    void grpcKeepsUnknownCredentialsUnauthenticated() {
        Object refused = grpcCall("ListTypes", "guessed");
        assertThat(refused).isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void theOperatorTokenKeepsEveryScope() {
        Object outcome = grpcCall("GetJob", TOKEN);
        if (outcome instanceof StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.UNAUTHENTICATED);
        }
    }

    private static HttpResponse<String> post(String path, String body, String... headers)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void theMcpSessionIsPinnedToTheScopedCallerAndFiltered() throws Exception {
        String initialize = """
                {"jsonrpc":"2.0","id":1,"method":"initialize","params":
                 {"protocolVersion":"2025-06-18","capabilities":{},
                  "clientInfo":{"name":"scope-test","version":"0"}}}
                """;
        HttpResponse<String> initialized = post("/mcp", initialize, "api_token", READER);
        assertThat(initialized.statusCode()).isEqualTo(200);
        JsonNode meta = MAPPER.readTree(initialized.body()).path("result").path("_meta");
        assertThat(meta.path("ai.pipestream.protomolt/toolCount").asInt())
                .isEqualTo(READER_TOOLS);

        String session = initialized.headers().firstValue("Mcp-Session-Id").orElseThrow();
        String version = MAPPER.readTree(initialized.body()).path("result")
                .path("protocolVersion").asText();
        post("/mcp", "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                "api_token", READER, "Mcp-Session-Id", session,
                "MCP-Protocol-Version", version);

        HttpResponse<String> listed = post("/mcp",
                "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}",
                "api_token", READER, "Mcp-Session-Id", session,
                "MCP-Protocol-Version", version);
        assertThat(MAPPER.readTree(listed.body()).path("result").path("tools"))
                .hasSize(READER_TOOLS);

        HttpResponse<String> called = post("/mcp", """
                {"jsonrpc":"2.0","id":3,"method":"tools/call",
                 "params":{"name":"get-job","arguments":{}}}
                """, "api_token", READER, "Mcp-Session-Id", session,
                "MCP-Protocol-Version", version);
        JsonNode result = MAPPER.readTree(called.body()).path("result");
        assertThat(result.path("isError").asBoolean()).isTrue();
        assertThat(result.path("content").toString())
                .contains("permission-denied").contains(Scopes.SERVICE_INVOKE);
    }

    @Test
    void restResolvesThePrincipalAndRefusesOutsideItsScope() throws Exception {
        HttpResponse<String> allowed = post("/grpc-json/ProtoMoltService/ListTypes",
                "{}", "api_token", READER);
        assertThat(allowed.statusCode()).isEqualTo(200);

        HttpResponse<String> denied = post("/grpc-json/ProtoMoltService/GetJob",
                "{}", "api_token", READER);
        assertThat(denied.statusCode()).isEqualTo(403);
        assertThat(denied.body())
                .contains("permission-denied").contains(Scopes.SERVICE_INVOKE)
                .contains("ci-reader");

        assertThat(post("/grpc-json/ProtoMoltService/ListTypes", "{}",
                "api_token", "guessed").statusCode()).isEqualTo(401);
        assertThat(post("/grpc-json/ProtoMoltService/ListTypes", "{}",
                "api_token", TOKEN).statusCode()).isEqualTo(200);
    }

    @Test
    void anAccessPolicyWithoutTheOperatorTokenRefusesAtConstruction() {
        assertThatThrownBy(() -> new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, null, false, null, null,
                List.of(), null, null, null, null, null, null, policyFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--api-token");
    }

    @Test
    void aMissingPolicyFileFailsStartupLoudly() {
        ProtoMoltServe.Options options = new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, TOKEN, false, null, null,
                List.of(), null, null, null, null, null, null,
                policyFile.resolveSibling("absent.json"));
        assertThatThrownBy(() -> ProtoMoltServe.start(options))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("access policy");
    }
}
