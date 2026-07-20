package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The authentication boundary in front of every RPC. The server-wide interceptor is exercised
 * through a real in-process call, so what is pinned is what a client actually observes: the
 * status code, the description, and — for a wrong token — that the rejection carries nothing
 * an attacker could use to recover the expected secret.
 */
class ApiTokenServerInterceptorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String TOKEN = "sh4red-secret-token";

    private static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Order { string id = 1; }
            """;

    private static Server server;
    private static ManagedChannel channel;

    @BeforeAll
    static void start() throws Exception {
        server = InProcessServerBuilder.forName("protomolt-auth-test")
                .intercept(new ApiTokenServerInterceptor(TOKEN))
                .addService(ProtoMoltGrpcService.definition(
                        ProtoMoltCatalog.full(ActionContext.create())))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("protomolt-auth-test").build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    /** Issues a Compile call carrying {@code headers}; the response proves the handler ran. */
    private static DynamicMessage compile(Metadata headers) throws Exception {
        MethodDescriptor method = ProtoMoltServiceSchema.service().findMethodByName("Compile");
        DynamicMessage.Builder request = DynamicMessage.newBuilder(method.getInputType());
        JsonFormat.parser().merge("""
                {"sources": {"shop/v1/order.proto": %s}}
                """.formatted(MAPPER.writeValueAsString(ORDER_PROTO)), request);
        List<DynamicMessage> responses = DynamicGrpcCalls.call(
                channel, method, request.build(),
                CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS), headers, 4);
        assertThat(responses).hasSize(1);
        return responses.getFirst();
    }

    private static Metadata metadata(Metadata.Key<String> key, String value) {
        Metadata headers = new Metadata();
        headers.put(key, value);
        return headers;
    }

    @Test
    void callWithTheExpectedTokenReachesTheHandler() throws Exception {
        DynamicMessage response = compile(metadata(API_TOKEN, TOKEN));

        String json = JsonFormat.printer().print(response);
        assertThat(json).contains("\"ok\": true");
    }

    @Test
    void bearerAuthorizationHeaderIsAcceptedAsWell() throws Exception {
        DynamicMessage response = compile(metadata(AUTHORIZATION, "Bearer " + TOKEN));

        assertThat(JsonFormat.printer().print(response)).contains("\"ok\": true");
    }

    /** The scheme is matched case-insensitively, so {@code bearer} must work too. */
    @Test
    void bearerSchemeMatchIsCaseInsensitive() throws Exception {
        DynamicMessage response = compile(metadata(AUTHORIZATION, "bearer " + TOKEN));

        assertThat(JsonFormat.printer().print(response)).contains("\"ok\": true");
    }

    @Test
    void callWithNoTokenIsUnauthenticated() {
        assertThatThrownBy(() -> compile(new Metadata()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
                    assertThat(e.getStatus().getDescription())
                            .isEqualTo("Missing API token 'api_token'");
                });
    }

    @Test
    void blankTokenIsTreatedAsMissing() {
        assertThatThrownBy(() -> compile(metadata(API_TOKEN, "   ")))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
                    assertThat(e.getStatus().getDescription())
                            .isEqualTo("Missing API token 'api_token'");
                });
    }

    @Test
    void authorizationHeaderWithoutTheBearerSchemeIsTreatedAsMissing() {
        assertThatThrownBy(() -> compile(metadata(AUTHORIZATION, "Basic " + TOKEN)))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
                    assertThat(e.getStatus().getDescription())
                            .isEqualTo("Missing API token 'api_token'");
                });
    }

    @Test
    void callWithTheWrongTokenIsUnauthenticated() {
        assertThatThrownBy(() -> compile(metadata(API_TOKEN, "not-the-token")))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
                    assertThat(e.getStatus().getDescription()).isEqualTo("Invalid API token");
                });
    }

    /**
     * A prefix of the expected token must be rejected outright — no partial credit — and the
     * rejection must be indistinguishable from any other wrong-token rejection.
     */
    @Test
    void tokenPrefixIsRejectedLikeAnyOtherWrongToken() {
        assertThatThrownBy(() -> compile(metadata(API_TOKEN, TOKEN.substring(0, TOKEN.length() - 1))))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
                    assertThat(e.getStatus().getDescription()).isEqualTo("Invalid API token");
                });
    }

    /**
     * The rejection must not disclose the expected token, its length, or any part of it —
     * neither in the description nor in the trailers.
     */
    @Test
    void rejectionDoesNotLeakTheExpectedToken() {
        StatusRuntimeException e = catchStatus(metadata(API_TOKEN, "not-the-token"));

        assertThat(e.getStatus().getDescription()).doesNotContain(TOKEN);
        assertThat(e.getStatus().getDescription()).doesNotContain(TOKEN.substring(0, 5));
        assertThat(e.getMessage()).doesNotContain(TOKEN.substring(0, 5));
        Metadata trailers = e.getTrailers();
        if (trailers != null) {
            assertThat(trailers.keys()).allSatisfy(key -> {
                String value = trailers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
                assertThat(value).doesNotContain(TOKEN.substring(0, 5));
            });
        }
    }

    /** Missing and wrong tokens must not be distinguishable by status code. */
    @Test
    void missingAndWrongTokensShareTheSameStatusCode() {
        assertThat(catchStatus(new Metadata()).getStatus().getCode())
                .isEqualTo(catchStatus(metadata(API_TOKEN, "wrong")).getStatus().getCode());
    }

    @Test
    void nullExpectedTokenIsRejectedAtConstruction() {
        assertThatThrownBy(() -> new ApiTokenServerInterceptor(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("expectedToken");
    }

    private static StatusRuntimeException catchStatus(Metadata headers) {
        try {
            compile(headers);
            throw new AssertionError("expected the call to be rejected");
        } catch (StatusRuntimeException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError("expected StatusRuntimeException, got " + e, e);
        }
    }
}
