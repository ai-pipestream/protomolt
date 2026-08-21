package ai.pipestream.proto.serve;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.service.ProtoMoltServiceSchema;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import com.google.protobuf.DynamicMessage;
import com.sun.net.httpserver.HttpServer;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The serve process with an OIDC identity store mounted: IdP-issued tokens resolve to
 * bounded principals and are scope-checked exactly like policy principals, unknown and
 * inactive tokens stay unauthenticated, and identity stores without the operator token
 * are refused at construction.
 */
class IdentityStoreServeTest {

    private static final String TOKEN = "operator-sekret";

    /** token → canned introspection answer. */
    static final Map<String, String> answers = new ConcurrentHashMap<>();

    private static HttpServer idp;
    private static ProtoMoltServe serve;

    @BeforeAll
    static void start() throws Exception {
        idp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        idp.createContext("/introspect", exchange -> {
            String body = new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String token = URLDecoder.decode(
                    body.replaceFirst("^token=", ""), StandardCharsets.UTF_8);
            byte[] payload = answers
                    .getOrDefault(token, "{\"active\": false}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, payload.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(payload);
            }
        });
        idp.start();
        answers.put("idp-reader-token", """
                {"active": true, "username": "idp-reader",
                 "protomolt_scopes": ["schema-read"]}""");
        ProtoMoltServe.IdentityStoreOptions stores = new ProtoMoltServe.IdentityStoreOptions(
                URI.create("http://127.0.0.1:" + idp.getAddress().getPort() + "/introspect"),
                "serve-resolver", "resolver-secret", null);
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, TOKEN, false, null, null,
                List.of(), null, null, null, null, null, null, null, stores));
    }

    @AfterAll
    static void stop() {
        serve.close();
        idp.stop(0);
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
    void anIdpTokenResolvesAndIsScopeCheckedLikeAnyPrincipal() {
        assertThat(grpcCall("ListTypes", "idp-reader-token")).isInstanceOf(List.class);
        Object denied = grpcCall("GetJob", "idp-reader-token");
        assertThat(denied).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(e.getStatus().getDescription())
                    .contains("idp-reader").contains(Scopes.SERVICE_INVOKE);
        });
    }

    @Test
    void inactiveAndUnknownTokensStayUnauthenticated() {
        answers.put("stale-token", "{\"active\": false}");
        for (String credential : new String[] {"stale-token", "never-issued"}) {
            Object refused = grpcCall("ListTypes", credential);
            assertThat(refused).isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.UNAUTHENTICATED));
        }
    }

    @Test
    void theOperatorTokenKeepsEveryScopeBesideTheStore() {
        Object outcome = grpcCall("GetJob", TOKEN);
        if (outcome instanceof StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.UNAUTHENTICATED);
        }
    }

    @Test
    void identityStoresWithoutTheOperatorTokenRefuseAtConstruction() {
        ProtoMoltServe.IdentityStoreOptions stores = new ProtoMoltServe.IdentityStoreOptions(
                URI.create("http://127.0.0.1:1/introspect"), "id", "secret", null);
        assertThatThrownBy(() -> new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, null, false, null, null,
                List.of(), null, null, null, null, null, null, null, stores))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operator api token");
    }

    @Test
    void identityStoreOptionsValidateTheirShape() {
        assertThatThrownBy(() ->
                new ProtoMoltServe.IdentityStoreOptions(null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProtoMoltServe.IdentityStoreOptions(
                URI.create("http://idp/introspect"), null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOMOLT_AUTHZ_OIDC_CLIENT_ID");
    }
}
