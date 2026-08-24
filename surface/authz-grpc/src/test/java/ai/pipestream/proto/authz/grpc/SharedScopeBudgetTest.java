package ai.pipestream.proto.authz.grpc;

import ai.pipestream.proto.actions.JsonAction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.ScopeBudgets;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.CallerResolver;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Struct;
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
import io.grpc.stub.MetadataUtils;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * One node, one ledger. A principal budgeted on a scope has that budget enforced across
 * every enforcement point the node wires, so alternating transports (the action catalog
 * behind the actions route, MCP and the CLI; the scope interceptor on a gRPC surface)
 * spends the same allowance instead of one allowance each.
 */
class SharedScopeBudgetTest {

    private static final String OPERATOR = "test-operator";
    private static final String CREDENTIAL = "metered-credential";
    private static final String HEALTH = "grpc.health.v1.Health";
    private static final String PRINCIPAL = "meterme";

    /** Two requests a minute on the scope both enforcement points guard. */
    private static final Caller METERED = Caller.scoped(PRINCIPAL,
            Set.of(Scopes.METRICS_QUERY),
            Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 0)));

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void stop() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void aSpendOnTheCatalogIsVisibleToTheScopeInterceptor() throws Exception {
        ScopeBudgets ledger = new ScopeBudgets();
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create(), ledger)
                .register(new MeteredAction());
        HealthGrpc.HealthBlockingStub stub = grpcSurface(ledger);

        // One of the two admissions spends on the catalog: the JSON route, MCP, the CLI.
        assertThat(catalog.execute("metered-action", envelope(), METERED)
                .get("ok").asBoolean()).isTrue();
        // The second spends on gRPC, which the split ledgers admitted for free.
        assertThat(stub.check(HealthCheckRequest.getDefaultInstance())
                .getStatus().getNumber()).isEqualTo(1);

        // The budget is exhausted now, whichever transport asks next.
        assertThatThrownBy(() -> stub.check(HealthCheckRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                    assertThat(e.getStatus().getDescription())
                            .contains(PRINCIPAL).contains("2-per-minute")
                            .contains(Scopes.METRICS_QUERY);
                });
        assertThatThrownBy(() -> catalog.execute("metered-action", envelope(), METERED))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("resource-exhausted");
                    assertThat(e.getMessage()).contains(PRINCIPAL).contains("2-per-minute");
                });
    }

    @Test
    void aSpendOnTheScopeInterceptorIsVisibleToTheCatalog() throws Exception {
        ScopeBudgets ledger = new ScopeBudgets();
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create(), ledger)
                .register(new MeteredAction());
        HealthGrpc.HealthBlockingStub stub = grpcSurface(ledger);

        for (int spent = 0; spent < 2; spent++) {
            assertThat(stub.check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
        }
        assertThatThrownBy(() -> catalog.execute("metered-action", envelope(), METERED))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("resource-exhausted");
                    assertThat(e.getMessage()).contains(PRINCIPAL).contains("2-per-minute")
                            .contains(Scopes.METRICS_QUERY);
                });
    }

    @Test
    void aForkedCatalogSpendsTheSameLedger() throws Exception {
        ScopeBudgets ledger = new ScopeBudgets();
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create(), ledger)
                .register(new MeteredAction());
        ActionCatalog fork = catalog.fork();

        assertThat(catalog.execute("metered-action", envelope(), METERED)).isNotNull();
        assertThat(fork.execute("metered-action", envelope(), METERED)).isNotNull();
        assertThatThrownBy(() -> fork.execute("metered-action", envelope(), METERED))
                .isInstanceOf(ActionException.class);
    }

    /** A gRPC surface guarding the same scope on {@code ledger}. */
    private HealthGrpc.HealthBlockingStub grpcSurface(ScopeBudgets ledger) throws Exception {
        CallerResolver resolver = credential -> CREDENTIAL.equals(credential)
                ? Optional.of(METERED) : Optional.empty();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new HealthStatusManager().getHealthService())
                .intercept(new ScopeServerInterceptor(
                        Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of(), ledger))
                .intercept(new ApiTokenServerInterceptor(OPERATOR, resolver))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                CREDENTIAL);
        return HealthGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static ObjectNode envelope() {
        return JsonNodeFactory.instance.objectNode();
    }

    /** A verb requiring the same scope the gRPC table requires. */
    private static final class MeteredAction implements JsonAction {

        @Override
        public String name() {
            return "metered-action";
        }

        @Override
        public String description() {
            return "Spends the caller's metrics-query budget and answers.";
        }

        @Override
        public String requiredScope() {
            return Scopes.METRICS_QUERY;
        }

        @Override
        public Descriptor requestType() {
            // Struct accepts any JSON object, so a fixture is not constrained by a
            // contract it is not testing.
            return Struct.getDescriptor();
        }

        @Override
        public Descriptor responseType() {
            // Struct accepts any JSON object, so a fixture is not constrained by a
            // contract it is not testing.
            return Struct.getDescriptor();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context) {
            return JsonNodeFactory.instance.objectNode().put("ok", true);
        }
    }
}
