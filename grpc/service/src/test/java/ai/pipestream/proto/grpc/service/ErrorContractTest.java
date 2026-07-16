package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The error-sanitization boundary every verb shares. Action failures map onto stable gRPC codes
 * with the kebab-case error code in a trailer; an <em>unexpected</em> backend throwable — anything
 * that is not an {@link ActionException} — is logged server-side but never handed to the client,
 * which only ever sees {@code "internal-error: <Rpc> failed"} and never the raw exception text.
 * That anti-leak guard, and the full code&#8594;status mapping table, is what this pins.
 */
class ErrorContractTest {

    // A password-shaped secret standing in for anything a backend exception might carry.
    private static final String SECRET = "jdbc://svc:hunter2@internal-db.corp/ledger";

    // ---- CatalogBridge.toStatus: the code -> status mapping table (pure) ----

    @Test
    void internalErrorMapsToInternal() {
        StatusRuntimeException e = CatalogBridge.toStatus(
                new ActionException("internal-error", "boom"));
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(e.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo("internal-error");
    }

    @Test
    void unknownActionMapsToUnimplemented() {
        StatusRuntimeException e = CatalogBridge.toStatus(
                new ActionException("unknown-action", "no such verb"));
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
        assertThat(e.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo("unknown-action");
    }

    @Test
    void everyOtherCodeIsAClientRepairableInvalidArgument() {
        for (String code : new String[]{"invalid-input", "no-such-type", "unresolved-reference"}) {
            StatusRuntimeException e = CatalogBridge.toStatus(new ActionException(code, "x"));
            assertThat(e.getStatus().getCode())
                    .as("code %s", code).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(e.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo(code);
        }
    }

    @Test
    void theDescriptionCarriesOnlyTheActionsOwnCodeAndMessage() {
        // The description is the only free text a client receives; it must be exactly the action's
        // own code + message, never a wrapped stack trace, and no cause travels with it.
        StatusRuntimeException e = CatalogBridge.toStatus(
                new ActionException("invalid-input", "field 'qty' must be non-negative"));
        assertThat(e.getStatus().getDescription())
                .isEqualTo("invalid-input: field 'qty' must be non-negative");
        assertThat(e.getStatus().getCause()).as("no server-side cause is chained onto the status")
                .isNull();
    }

    // ---- the anti-leak guard: an unexpected RuntimeException never reaches the client ----

    private static Server server;
    private static ManagedChannel channel;

    @BeforeAll
    static void start() throws Exception {
        // A full catalog with one verb swapped for a stand-in that throws a raw RuntimeException
        // carrying a secret — exactly the backend detail the guard must contain. A raw
        // RuntimeException (not an ActionException) is the path that reaches the service's
        // last-resort catch; ActionCatalog.execute does not wrap it.
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create())
                .replace(alwaysThrows("list-types", new IllegalStateException(SECRET)));
        server = InProcessServerBuilder.forName("protomolt-error-contract-test")
                .addService(ProtoMoltGrpcService.definition(catalog))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("protomolt-error-contract-test").build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void anUnexpectedBackendExceptionIsSanitizedNotLeaked() {
        MethodDescriptor method = ProtoMoltServiceSchema.service().findMethodByName("ListTypes");
        assertThatThrownBy(() -> DynamicGrpcCalls.call(channel, method,
                DynamicMessage.newBuilder(method.getInputType()).build(),
                CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS), new Metadata(), 4))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
                    // Names the RPC, and nothing from the exception.
                    assertThat(e.getStatus().getDescription())
                            .isEqualTo("internal-error: ListTypes failed");
                    assertThat(e.getStatus().getDescription())
                            .doesNotContain("hunter2").doesNotContain("internal-db")
                            .doesNotContain(SECRET).doesNotContain("IllegalStateException");
                    // No error-code trailer on this path — it is deliberately opaque.
                    assertThat(e.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isNull();
                });
    }

    private static ProtoAction alwaysThrows(String name, RuntimeException toThrow) {
        return new ProtoAction() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String description() {
                return "test stand-in that always throws";
            }

            @Override
            public ObjectNode inputSchema() {
                return JsonNodeFactory.instance.objectNode();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                throw toThrow;
            }
        };
    }
}
