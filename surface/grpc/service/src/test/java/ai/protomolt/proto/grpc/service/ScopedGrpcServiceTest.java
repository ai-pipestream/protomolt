package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The scope check behind the authentication boundary, observed as a client does: a policy
 * credential authenticates as its principal and is refused per method by name; the operator
 * token keeps every scope; an unknown credential stays indistinguishable from a wrong one.
 */
class ScopedGrpcServiceTest {

    private static final String OPERATOR = "operator-secret";
    private static final String READER = "reader-credential";

    private static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);

    private static Server server;
    private static ManagedChannel channel;

    @BeforeAll
    static void start() throws Exception {
        CallerResolver resolver = credential -> READER.equals(credential)
                ? Optional.of(Caller.scoped("ci-reader", Set.of(Scopes.SCHEMA_READ)))
                : Optional.empty();
        server = InProcessServerBuilder.forName("protomolt-scope-test")
                .intercept(new ApiTokenServerInterceptor(OPERATOR, resolver))
                .addService(ProtoMoltGrpcService.definition(
                        ProtoMoltCatalog.full(ActionContext.create())))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("protomolt-scope-test").build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private static List<DynamicMessage> call(String method, String credential) {
        MethodDescriptor descriptor =
                ProtoMoltServiceSchema.service().findMethodByName(method);
        Metadata headers = new Metadata();
        headers.put(API_TOKEN, credential);
        return DynamicGrpcCalls.call(channel, descriptor,
                DynamicMessage.newBuilder(descriptor.getInputType()).build(),
                CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS), headers, 4);
    }

    private static StatusRuntimeException refusal(String method, String credential) {
        try {
            call(method, credential);
            throw new AssertionError("expected " + method + " to be refused");
        } catch (StatusRuntimeException e) {
            return e;
        }
    }

    @Test
    void aPolicyCredentialReachesTheHandlerWithinItsScope() {
        assertThat(call("ListTypes", READER)).hasSize(1);
    }

    @Test
    void outsideItsScopeThePrincipalIsRefusedByName() {
        StatusRuntimeException denied = refusal("GetJob", READER);
        assertThat(denied.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(denied.getStatus().getDescription())
                .contains("ci-reader").contains(Scopes.SERVICE_INVOKE).contains("get-job");
        assertThat(denied.getTrailers().get(CatalogBridge.ERROR_CODE_KEY))
                .isEqualTo("permission-denied");
    }

    @Test
    void theOperatorTokenKeepsEveryScope() {
        StatusRuntimeException failure = refusal("GetJob", OPERATOR);
        assertThat(failure.getStatus().getCode()).isNotEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(failure.getStatus().getCode()).isNotEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void anUnknownCredentialStaysUnauthenticated() {
        StatusRuntimeException refused = refusal("ListTypes", "guessed-credential");
        assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
        assertThat(refused.getStatus().getDescription()).isEqualTo("Invalid API token");
    }
}
