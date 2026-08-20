package ai.pipestream.proto.authz.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.CallerResolver;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.MetadataUtils;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The scope table for hand-built gRPC surfaces, observed through real calls against the
 * health service: service defaults, method overrides, the open set, the refuse-unmapped
 * rule, operator pass-through, and authentication running before the table.
 */
class ScopeServerInterceptorTest {

    private static final String OPERATOR = "door-operator";
    private static final String ANALYST = "analyst-credential";
    private static final String HEALTH = "grpc.health.v1.Health";

    private static Server server;
    private static ManagedChannel channel;

    @BeforeAll
    static void start() throws Exception {
        CallerResolver resolver = credential -> ANALYST.equals(credential)
                ? Optional.of(Caller.scoped("analyst", Set.of(Scopes.METRICS_QUERY)))
                : Optional.empty();
        HealthStatusManager health = new HealthStatusManager();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .intercept(new ScopeServerInterceptor(
                        Map.of(HEALTH, Scopes.METRICS_QUERY),
                        Map.of(HEALTH + "/Watch", Scopes.METRICS_REBUILD),
                        Set.of()))
                .intercept(new ApiTokenServerInterceptor(OPERATOR, resolver))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private static HealthGrpc.HealthBlockingStub stub(String credential) {
        Metadata headers = new Metadata();
        if (credential != null) {
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    credential);
        }
        return HealthGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @Test
    void theServiceDefaultAdmitsTheHoldingCaller() {
        assertThat(stub(ANALYST).check(HealthCheckRequest.getDefaultInstance())
                .getStatus().getNumber()).isEqualTo(1);
    }

    @Test
    void aMethodOverrideBeatsItsServiceDefault() {
        assertThatThrownBy(() -> stub(ANALYST)
                .watch(HealthCheckRequest.getDefaultInstance()).next())
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("analyst").contains(Scopes.METRICS_REBUILD);
                });
    }

    @Test
    void anUnmappedServiceRefusesScopedCallersByName() throws Exception {
        HealthStatusManager health = new HealthStatusManager();
        String name = InProcessServerBuilder.generateName();
        Server unmapped = InProcessServerBuilder.forName(name)
                .addService(health.getHealthService())
                .intercept(new ScopeServerInterceptor(Map.of(), Map.of(), Set.of()))
                .intercept(new ApiTokenServerInterceptor(OPERATOR, credential ->
                        Optional.of(Caller.scoped("analyst", Set.of(Scopes.METRICS_QUERY)))))
                .build()
                .start();
        ManagedChannel unmappedChannel = InProcessChannelBuilder.forName(name).build();
        try {
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    "any-policy-credential");
            assertThatThrownBy(() -> HealthGrpc.newBlockingStub(unmappedChannel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                    .check(HealthCheckRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.PERMISSION_DENIED);
                        assertThat(e.getStatus().getDescription())
                                .contains("no scope is declared");
                    });

            // The refuse-unmapped rule gates scoped callers only; the operator reaches
            // plumbing the table never named.
            Metadata operator = new Metadata();
            operator.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    OPERATOR);
            assertThat(HealthGrpc.newBlockingStub(unmappedChannel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(operator))
                    .check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
        } finally {
            unmappedChannel.shutdownNow();
            unmapped.shutdownNow();
        }
    }

    @Test
    void theOperatorPassesEveryTable() {
        assertThat(stub(OPERATOR).check(HealthCheckRequest.getDefaultInstance())
                .getStatus().getNumber()).isEqualTo(1);
    }

    @Test
    void authenticationRunsBeforeTheTable() {
        assertThatThrownBy(() -> stub(null).check(HealthCheckRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void anOpenServiceStaysBehindAuthenticationAlone() throws Exception {
        HealthStatusManager health = new HealthStatusManager();
        String name = InProcessServerBuilder.generateName();
        Server open = InProcessServerBuilder.forName(name)
                .addService(health.getHealthService())
                .intercept(new ScopeServerInterceptor(Map.of(), Map.of(), Set.of(HEALTH)))
                .intercept(new ApiTokenServerInterceptor(OPERATOR, credential ->
                        Optional.of(Caller.scoped("anyone", Set.of(Scopes.SCHEMA_READ)))))
                .build()
                .start();
        ManagedChannel openChannel = InProcessChannelBuilder.forName(name).build();
        try {
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    "anything");
            assertThat(HealthGrpc.newBlockingStub(openChannel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                    .check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
        } finally {
            openChannel.shutdownNow();
            open.shutdownNow();
        }
    }

    @Test
    void anUnknownScopeInTheTableRefusesAtConstruction() {
        assertThatThrownBy(() -> new ScopeServerInterceptor(
                Map.of(HEALTH, "schema-red"), Map.of(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema-red");
    }
}
