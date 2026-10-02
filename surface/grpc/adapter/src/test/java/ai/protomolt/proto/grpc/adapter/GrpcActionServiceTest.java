package ai.protomolt.proto.grpc.adapter;

import ai.protomolt.proto.actions.*;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.*;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/** A real gRPC server with only adapter dependencies: no full platform or optional providers. */
class GrpcActionServiceTest {
    private final ServiceDescriptor service = service(false);
    private final Descriptor type = service.findMethodByName("Echo").getInputType();
    private final AtomicInteger calls = new AtomicInteger();

    @Test void minimalCatalogServesValidRequestsWithoutPlatformProviders() throws Exception {
        assertThat(ServiceLoader.load(ActionProvider.class)).isEmpty();
        assertThat(ServiceLoader.load(SchemaResolverProvider.class)).isEmpty();
        assertThat(ServiceLoader.load(ActionContractProvider.class)).isEmpty();
        try (var rpc = new Rpc(catalog(false), Caller.operator())) {
            assertThat(rpc.call(message("Ada"))).isEqualTo(message("Ada"));
            assertThat(calls).hasValue(1);
        }
    }

    @Test void invalidRequestCannotReachTheAction() throws Exception {
        try (var rpc = new Rpc(catalog(false), Caller.operator())) {
            assertThatThrownBy(() -> rpc.call(message("x")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
            assertThat(calls).hasValue(0);
        }
    }

    @Test void authorizationPrecedesValidation() throws Exception {
        try (var rpc = new Rpc(catalog(false), Caller.scoped("reader", Set.of()))) {
            assertThatThrownBy(() -> rpc.call(message("x")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));
            assertThat(calls).hasValue(0);
        }
    }

    @Test void invalidSuccessfulResponseBecomesDataLoss() throws Exception {
        try (var rpc = new Rpc(catalog(true), Caller.operator())) {
            assertThatThrownBy(() -> rpc.call(message("Ada")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS);
                        assertThat(e.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo("invalid-response");
                    });
            assertThat(calls).hasValue(1);
        }
    }

    @Test void missingExtraAndWrongTypeBindingsFailBeforeServing() {
        var catalog = catalog(false);
        assertThatThrownBy(() -> GrpcActionService.bind(catalog, service, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exactly");
        assertThatThrownBy(() -> GrpcActionService.bind(catalog, service, Map.of("Echo", "echo", "Extra", "echo")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exactly");
        assertThatThrownBy(() -> GrpcActionService.bind(catalog, service, Map.of("Echo", "missing")))
                .isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(ActionException.class);
        assertThatThrownBy(() -> GrpcActionService.bind(catalog, service, Map.of("Echo", " ")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blank");
        catalog.register(new ProtoAction() {
            public String name() { return "wrong"; }
            public String description() { return "Wrong output contract"; }
            public Descriptor requestType() { return type; }
            public Descriptor responseType() { return com.google.protobuf.Empty.getDescriptor(); }
            public Message execute(Message request, ActionContext context) { throw new AssertionError("Not invoked"); }
        });
        assertThatThrownBy(() -> GrpcActionService.bind(catalog, service, Map.of("Echo", "wrong")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("type differs");
    }

    @Test void unavailableRpcRequiresAnExplicitPolicyAndReturnsUnimplemented() throws Exception {
        var empty = ActionCatalog.empty(ActionContext.create());
        try (var rpc = new Rpc(empty, Caller.operator(), GrpcActionService.MissingActionPolicy.EXPOSE_UNIMPLEMENTED)) {
            assertThatThrownBy(() -> rpc.call(message("Ada")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
                        assertThat(e.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo("unknown-action");
                    });
            assertThat(calls).hasValue(0);
        }
    }

    @Test void unsupportedStreamingFailsDuringBinding() {
        assertThatThrownBy(() -> GrpcActionService.bind(catalog(false), service(true), Map.of("Echo", "echo")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Only unary");
    }

    @Test void automaticBindingRejectsAmbiguityAndExplicitPolicyExcludesActions() {
        var catalog = catalog(false);
        catalog.register(action("second", false));
        assertThatThrownBy(() -> ContractActionBindings.mounted(catalog, service))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Ambiguous");
        assertThat(ContractActionBindings.mounted(catalog, service, Map.of(), a -> a.name().equals("echo")).values())
                .containsExactly("echo");
        assertThatThrownBy(() -> ContractActionBindings.mounted(catalog, service,
                Map.of(service.getFullName(), Map.of("Echo", "second")), a -> a.name().equals("echo")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("excluded");
    }

    private ActionCatalog catalog(boolean invalidResponse) {
        return ActionCatalog.empty(ActionContext.create()).register(action("echo", invalidResponse));
    }

    private ProtoAction action(String name, boolean invalidResponse) {
        return new ProtoAction() {
            public String name() { return name; }
            public String description() { return "Echo a validated name"; }
            public String requiredScope() { return Scopes.SCHEMA_READ; }
            public Descriptor requestType() { return type; }
            public Descriptor responseType() { return type; }
            public Message execute(Message request, ActionContext context) {
                calls.incrementAndGet();
                return invalidResponse ? message("x") : request;
            }
        };
    }

    private DynamicMessage message(String value) {
        return DynamicMessage.newBuilder(type).setField(type.findFieldByName("name"), value).build();
    }

    private final class Rpc implements AutoCloseable {
        private final Server server;
        private final ManagedChannel channel;
        private final io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> method;
        Rpc(ActionCatalog catalog, Caller caller) throws Exception {
            this(catalog, caller, GrpcActionService.MissingActionPolicy.REJECT);
        }
        @SuppressWarnings("unchecked")
        Rpc(ActionCatalog catalog, Caller caller, GrpcActionService.MissingActionPolicy policy) throws Exception {
            var definition = GrpcActionService.bind(catalog, service, Map.of("Echo", "echo"), policy);
            method = (io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage>) definition.getMethods()
                    .iterator().next().getMethodDescriptor();
            String name = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(definition).intercept(new ServerInterceptor() {
                        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                            return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER, caller), call, headers, next);
                        }
                    }).build().start();
            channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        }
        DynamicMessage call(DynamicMessage request) {
            return ClientCalls.blockingUnaryCall(channel, method, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), request);
        }
        public void close() throws Exception {
            channel.shutdownNow();
            server.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static ServiceDescriptor service(boolean streaming) {
        var field = FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setOptions(FieldOptions.newBuilder().setExtension(ValidateProto.field,
                        FieldRules.newBuilder().setString(StringRules.newBuilder().setMinLen(3)).build()));
        var file = FileDescriptorProto.newBuilder().setName("adapter.proto").setPackage("adapter.v1").setSyntax("proto3")
                .addDependency(ValidateProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Name").addField(field))
                .addService(ServiceDescriptorProto.newBuilder().setName("Names")
                        .addMethod(MethodDescriptorProto.newBuilder().setName("Echo")
                                .setInputType(".adapter.v1.Name").setOutputType(".adapter.v1.Name")
                                .setServerStreaming(streaming))).build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[]{ValidateProto.getDescriptor()}).findServiceByName("Names");
        } catch (DescriptorValidationException e) { throw new IllegalStateException(e); }
    }
}
