package ai.protomolt.proto.authz.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCalls;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The scope table for hand-built gRPC surfaces, observed through real calls against the
 * health service: service defaults, method overrides, the open set, the refuse-unmapped
 * rule, operator pass-through, and authentication running before the table.
 */
class ScopeServerInterceptorTest {

    private static final String OPERATOR = "test-operator";
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

    @Test
    void aBudgetedCallerExhaustsItsRateAndItsPayloadCap() throws Exception {
        CallerResolver resolver = credential -> switch (credential) {
            case "metered-credential" -> Optional.of(Caller.scoped("meterme",
                    Set.of(Scopes.METRICS_QUERY),
                    Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 0))));
            case "capped-credential" -> Optional.of(Caller.scoped("capped",
                    Set.of(Scopes.METRICS_QUERY),
                    Map.of(Scopes.METRICS_QUERY, new Caller.Budget(0, 4))));
            default -> Optional.empty();
        };
        HealthStatusManager health = new HealthStatusManager();
        String name = InProcessServerBuilder.generateName();
        Server budgeted = InProcessServerBuilder.forName(name)
                .addService(health.getHealthService())
                .intercept(new ScopeServerInterceptor(
                        Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of()))
                .intercept(new ApiTokenServerInterceptor(OPERATOR, resolver))
                .build()
                .start();
        ManagedChannel budgetedChannel = InProcessChannelBuilder.forName(name).build();
        try {
            HealthGrpc.HealthBlockingStub metered = stubOn(budgetedChannel,
                    "metered-credential");
            assertThat(metered.check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
            assertThat(metered.check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
            assertThatThrownBy(() ->
                    metered.check(HealthCheckRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                        assertThat(e.getStatus().getDescription())
                                .contains("meterme").contains("2-per-minute")
                                .contains(Scopes.METRICS_QUERY);
                    });

            HealthGrpc.HealthBlockingStub capped = stubOn(budgetedChannel,
                    "capped-credential");
            assertThat(capped.check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
            assertThatThrownBy(() -> capped.check(HealthCheckRequest.newBuilder()
                    .setService("a-service-name-past-the-cap").build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                        assertThat(e.getStatus().getDescription())
                                .contains("capped").contains("4-byte");
                    });
        } finally {
            budgetedChannel.shutdownNow();
            budgeted.shutdownNow();
        }
    }

    @Test
    void payloadCapRefusesOverCapFrameBeforeHalfCloseWithoutDecoding() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        MethodDescriptor<HealthCheckRequest, HealthCheckResponse> baseMethod =
                HealthGrpc.getCheckMethod();
        MethodDescriptor.Marshaller<HealthCheckRequest> baseMarshaller =
                baseMethod.getRequestMarshaller();
        MethodDescriptor<HealthCheckRequest, HealthCheckResponse> countingMethod =
                baseMethod.toBuilder(
                        new MethodDescriptor.Marshaller<HealthCheckRequest>() {
                            @Override
                            public InputStream stream(HealthCheckRequest value) {
                                return baseMarshaller.stream(value);
                            }

                            @Override
                            public HealthCheckRequest parse(InputStream stream) {
                                parses.incrementAndGet();
                                return baseMarshaller.parse(stream);
                            }
                        },
                        baseMethod.getResponseMarshaller()).build();
        ServerServiceDefinition countingService = ServerServiceDefinition.builder(HEALTH)
                .addMethod(countingMethod, ServerCalls.asyncUnaryCall((request, observer) -> {
                    observer.onNext(HealthCheckResponse.newBuilder()
                            .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                            .build());
                    observer.onCompleted();
                }))
                .build();
        CallerResolver resolver = credential -> "capped-credential".equals(credential)
                ? Optional.of(Caller.scoped("capped",
                        Set.of(Scopes.METRICS_QUERY),
                        Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 4))))
                : Optional.empty();
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of());
        NettyServerBuilder builder = NettyServerBuilder.forPort(0)
                .addService(countingService);
        scopes.configure(builder);
        builder.intercept(new ApiTokenServerInterceptor(OPERATOR, resolver));
        Server nettyServer = builder.build().start();
        ManagedChannel nettyChannel = NettyChannelBuilder
                .forAddress("localhost", nettyServer.getPort())
                .usePlaintext()
                .build();
        try {
            CountDownLatch closed = new CountDownLatch(1);
            AtomicReference<Status> closeStatus = new AtomicReference<>();
            ClientCall<HealthCheckRequest, HealthCheckResponse> call =
                    nettyChannel.newCall(HealthGrpc.getCheckMethod(), CallOptions.DEFAULT);
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    "capped-credential");
            call.start(new ClientCall.Listener<>() {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    closeStatus.set(status);
                    closed.countDown();
                }
            }, headers);
            call.request(1);
            call.sendMessage(HealthCheckRequest.newBuilder()
                    .setService("a-service-name-past-the-cap").build());
            // Do NOT call halfClose(): gRPC 1.85.1 parks the unary frame until halfClose,
            // so the stream tracer + triggerEvent path must refuse the frame immediately.
            assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closeStatus.get().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(closeStatus.get().getDescription())
                    .contains("capped").contains("4-byte");
            assertThat(parses.get()).isZero();

            // A small frame within the 4-byte cap succeeds and spends only 1 of the 2-per-minute
            // rate slots (the stream tracer evaluation does not double-spend in onMessage).
            HealthGrpc.HealthBlockingStub capped = stubOn(nettyChannel, "capped-credential");
            assertThat(capped.check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
            assertThat(parses.get()).isEqualTo(1);
            assertThat(capped.check(HealthCheckRequest.getDefaultInstance())
                    .getStatus().getNumber()).isEqualTo(1);
            assertThat(parses.get()).isEqualTo(2);
            assertThatThrownBy(() ->
                    capped.check(HealthCheckRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        } finally {
            nettyChannel.shutdownNow();
            nettyServer.shutdownNow();
        }
    }

    private static HealthGrpc.HealthBlockingStub stubOn(ManagedChannel target,
            String credential) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                credential);
        return HealthGrpc.newBlockingStub(target)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static ServerServiceDefinition countingHealthService(AtomicInteger parses) {
        MethodDescriptor<HealthCheckRequest, HealthCheckResponse> baseMethod =
                HealthGrpc.getCheckMethod();
        MethodDescriptor.Marshaller<HealthCheckRequest> baseMarshaller =
                baseMethod.getRequestMarshaller();
        MethodDescriptor<HealthCheckRequest, HealthCheckResponse> countingMethod =
                baseMethod.toBuilder(
                        new MethodDescriptor.Marshaller<HealthCheckRequest>() {
                            @Override
                            public InputStream stream(HealthCheckRequest value) {
                                return baseMarshaller.stream(value);
                            }

                            @Override
                            public HealthCheckRequest parse(InputStream stream) {
                                parses.incrementAndGet();
                                return baseMarshaller.parse(stream);
                            }
                        },
                        baseMethod.getResponseMarshaller()).build();
        return ServerServiceDefinition.builder(HEALTH)
                .addMethod(countingMethod, ServerCalls.asyncUnaryCall((request, observer) -> {
                    observer.onNext(HealthCheckResponse.newBuilder()
                            .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                            .build());
                    observer.onCompleted();
                }))
                .build();
    }

    @Test
    void reviewerCompressedSmallMessageWithinCapMustSucceed() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        CallerResolver resolver = credential -> "capped-credential".equals(credential)
                ? Optional.of(Caller.scoped("capped",
                        Set.of(Scopes.METRICS_QUERY),
                        Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 4))))
                : Optional.empty();
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of());
        NettyServerBuilder builder = NettyServerBuilder.forPort(0)
                .addService(countingHealthService(parses));
        scopes.configure(builder);
        builder.intercept(new ApiTokenServerInterceptor(OPERATOR, resolver));
        Server nettyServer = builder.build().start();
        ManagedChannel nettyChannel = NettyChannelBuilder
                .forAddress("localhost", nettyServer.getPort())
                .usePlaintext()
                .build();
        try {
            HealthCheckRequest small = HealthCheckRequest.newBuilder().setService("a").build();
            assertThat(small.getSerializedSize()).isEqualTo(3);
            HealthCheckResponse response = stubOn(nettyChannel, "capped-credential")
                    .withCompression("gzip")
                    .check(small);
            assertThat(response.getStatus())
                    .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
            assertThat(parses.get()).isEqualTo(1);
        } finally {
            nettyChannel.shutdownNow();
            nettyServer.shutdownNow();
        }
    }

    @Test
    void reviewerCompressedPayloadCapBeforeHalfClose() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        CallerResolver resolver = credential -> "capped-credential".equals(credential)
                ? Optional.of(Caller.scoped("capped",
                        Set.of(Scopes.METRICS_QUERY),
                        Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 100))))
                : Optional.empty();
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of());
        NettyServerBuilder builder = NettyServerBuilder.forPort(0)
                .addService(countingHealthService(parses));
        scopes.configure(builder);
        builder.intercept(new ApiTokenServerInterceptor(OPERATOR, resolver));
        Server nettyServer = builder.build().start();
        ManagedChannel nettyChannel = NettyChannelBuilder
                .forAddress("localhost", nettyServer.getPort())
                .usePlaintext()
                .build();
        try {
            CountDownLatch closed = new CountDownLatch(1);
            AtomicReference<Status> closeStatus = new AtomicReference<>();
            ClientCall<HealthCheckRequest, HealthCheckResponse> call =
                    nettyChannel.newCall(
                            HealthGrpc.getCheckMethod(),
                            CallOptions.DEFAULT.withCompression("gzip"));
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    "capped-credential");
            call.start(new ClientCall.Listener<>() {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    closeStatus.set(status);
                    closed.countDown();
                }
            }, headers);
            call.request(1);
            call.sendMessage(HealthCheckRequest.newBuilder()
                    .setService("x".repeat(1024))
                    .build());
            assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closeStatus.get().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(closeStatus.get().getDescription())
                    .contains("capped").contains("101 bytes").contains("100-byte");
            assertThat(parses.get()).isZero();
        } finally {
            nettyChannel.shutdownNow();
            nettyServer.shutdownNow();
        }
    }

    @Test
    void reviewerCompressedEmptyMessageWithinCapMustSucceed() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        CallerResolver resolver = credential -> "capped-credential".equals(credential)
                ? Optional.of(Caller.scoped("capped",
                        Set.of(Scopes.METRICS_QUERY),
                        Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 4))))
                : Optional.empty();
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of());
        NettyServerBuilder builder = NettyServerBuilder.forPort(0)
                .addService(countingHealthService(parses));
        scopes.configure(builder);
        builder.intercept(new ApiTokenServerInterceptor(OPERATOR, resolver));
        Server nettyServer = builder.build().start();
        ManagedChannel nettyChannel = NettyChannelBuilder
                .forAddress("localhost", nettyServer.getPort())
                .usePlaintext()
                .build();
        try {
            HealthCheckResponse response = stubOn(nettyChannel, "capped-credential")
                    .withCompression("gzip")
                    .check(HealthCheckRequest.getDefaultInstance());
            assertThat(response.getStatus())
                    .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
            assertThat(parses.get()).isEqualTo(1);
        } finally {
            nettyChannel.shutdownNow();
            nettyServer.shutdownNow();
        }
    }

    @Test
    void reviewerDirectExecutorCompressedPayloadCapBeforeHalfClose() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        CallerResolver resolver = credential -> "capped-credential".equals(credential)
                ? Optional.of(Caller.scoped("capped",
                        Set.of(Scopes.METRICS_QUERY),
                        Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 100))))
                : Optional.empty();
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of());
        NettyServerBuilder builder = NettyServerBuilder.forPort(0)
                .directExecutor()
                .addService(countingHealthService(parses));
        scopes.configure(builder);
        builder.intercept(new ApiTokenServerInterceptor(OPERATOR, resolver));
        Server nettyServer = builder.build().start();
        ManagedChannel nettyChannel = NettyChannelBuilder
                .forAddress("localhost", nettyServer.getPort())
                .usePlaintext()
                .build();
        try {
            CountDownLatch closed = new CountDownLatch(1);
            AtomicReference<Status> closeStatus = new AtomicReference<>();
            ClientCall<HealthCheckRequest, HealthCheckResponse> call =
                    nettyChannel.newCall(
                            HealthGrpc.getCheckMethod(),
                            CallOptions.DEFAULT.withCompression("gzip"));
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    "capped-credential");
            call.start(new ClientCall.Listener<>() {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    closeStatus.set(status);
                    closed.countDown();
                }
            }, headers);
            call.request(1);
            call.sendMessage(HealthCheckRequest.newBuilder()
                    .setService("x".repeat(1024))
                    .build());
            assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closeStatus.get().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(closeStatus.get().getDescription())
                    .contains("capped").contains("101 bytes").contains("100-byte");
            assertThat(parses.get()).isZero();

            HealthCheckRequest small = HealthCheckRequest.newBuilder().setService("a").build();
            HealthCheckResponse response = stubOn(nettyChannel, "capped-credential")
                    .withCompression("gzip")
                    .check(small);
            assertThat(response.getStatus())
                    .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
            assertThat(parses.get()).isEqualTo(1);
        } finally {
            nettyChannel.shutdownNow();
            nettyServer.shutdownNow();
        }
    }

    @Test
    void transparentCallWrapperMustNotDisableCompressedAdmission() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        AtomicInteger wrappedCalls = new AtomicInteger();
        CallerResolver resolver = credential -> "capped-credential".equals(credential)
                ? Optional.of(Caller.scoped("capped",
                        Set.of(Scopes.METRICS_QUERY),
                        Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 100))))
                : Optional.empty();
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of());
        ServerInterceptor wrapperInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    io.grpc.ServerCallHandler<ReqT, RespT> next) {
                wrappedCalls.incrementAndGet();
                return next.startCall(new NonForwardingServerCall<>(call), headers);
            }
        };
        NettyServerBuilder builder = NettyServerBuilder.forPort(0)
                .directExecutor()
                .addService(countingHealthService(parses));
        scopes.configure(builder);
        builder.intercept(wrapperInterceptor);
        builder.intercept(new ApiTokenServerInterceptor(OPERATOR, resolver));
        Server nettyServer = builder.build().start();
        ManagedChannel nettyChannel = NettyChannelBuilder
                .forAddress("localhost", nettyServer.getPort())
                .usePlaintext()
                .build();
        try {
            CountDownLatch closed = new CountDownLatch(1);
            AtomicReference<Status> closeStatus = new AtomicReference<>();
            ClientCall<HealthCheckRequest, HealthCheckResponse> call =
                    nettyChannel.newCall(
                            HealthGrpc.getCheckMethod(),
                            CallOptions.DEFAULT.withCompression("gzip"));
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    "capped-credential");
            call.start(new ClientCall.Listener<>() {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    closeStatus.set(status);
                    closed.countDown();
                }
            }, headers);
            call.request(1);
            call.sendMessage(HealthCheckRequest.newBuilder()
                    .setService("x".repeat(1024))
                    .build());
            assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closeStatus.get().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(closeStatus.get().getDescription())
                    .contains("capped").contains("101 bytes").contains("100-byte");
            assertThat(parses.get()).isZero();
            assertThat(wrappedCalls.get()).isEqualTo(1);

            HealthCheckRequest small = HealthCheckRequest.newBuilder().setService("a").build();
            HealthCheckResponse response = stubOn(nettyChannel, "capped-credential")
                    .withCompression("gzip")
                    .check(small);
            assertThat(response.getStatus())
                    .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
            assertThat(parses.get()).isEqualTo(1);
            assertThat(wrappedCalls.get()).isEqualTo(2);
        } finally {
            nettyChannel.shutdownNow();
            nettyServer.shutdownNow();
        }
    }

    @Test
    void syntheticServerCallWithoutDeframerFailsClosedOnCompressedFrameBeforeHalfClose()
            throws Exception {
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                Map.of(HEALTH, Scopes.METRICS_QUERY), Map.of(), Set.of());
        Caller capped = Caller.scoped("capped",
                Set.of(Scopes.METRICS_QUERY),
                Map.of(Scopes.METRICS_QUERY, new Caller.Budget(2, 100)));
        Metadata headers = new Metadata();
        ServerStreamTracer tracer = scopes.streamTracerFactory()
                .newServerStreamTracer(HealthGrpc.getCheckMethod().getFullMethodName(), headers);
        Context ctx = tracer.filterContext(Context.ROOT)
                .withValue(CallerContexts.CALLER, capped);

        AtomicReference<Status> closedStatus = new AtomicReference<>();
        AtomicReference<ServerCall.Listener<HealthCheckRequest>> listenerRef =
                new AtomicReference<>();
        ServerCall<HealthCheckRequest, HealthCheckResponse> syntheticCall =
                new ServerCall<>() {
                    @Override
                    public void request(int numMessages) {
                    }

                    @Override
                    public void sendHeaders(Metadata headers) {
                    }

                    @Override
                    public void sendMessage(HealthCheckResponse message) {
                    }

                    @Override
                    public void close(Status status, Metadata trailers) {
                        closedStatus.set(status);
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public MethodDescriptor<HealthCheckRequest, HealthCheckResponse>
                            getMethodDescriptor() {
                        return HealthGrpc.getCheckMethod();
                    }

                    @Override
                    public void triggerEvent(Object event) {
                        ServerCall.Listener<HealthCheckRequest> listener = listenerRef.get();
                        if (listener != null) {
                            listener.onEvent(event);
                        }
                    }
                };

        ctx.call(() -> {
            ServerCall.Listener<HealthCheckRequest> listener = scopes.interceptCall(
                    syntheticCall,
                    headers,
                    (call, callHeaders) -> new ServerCall.Listener<>() { });
            listenerRef.set(listener);
            tracer.inboundMessageRead(0, 48L, -1L);
            return null;
        });

        assertThat(closedStatus.get()).isNotNull();
        assertThat(closedStatus.get().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(closedStatus.get().getDescription())
                .contains("capped")
                .contains("100-byte cap");
    }

    private static final class NonForwardingServerCall<ReqT, RespT>
            extends ServerCall<ReqT, RespT> {
        private final ServerCall<ReqT, RespT> underlying;

        private NonForwardingServerCall(ServerCall<ReqT, RespT> underlying) {
            this.underlying = underlying;
        }

        @Override
        public void request(int numMessages) {
            underlying.request(numMessages);
        }

        @Override
        public void sendHeaders(Metadata headers) {
            underlying.sendHeaders(headers);
        }

        @Override
        public void sendMessage(RespT message) {
            underlying.sendMessage(message);
        }

        @Override
        public boolean isReady() {
            return underlying.isReady();
        }

        @Override
        public void close(Status status, Metadata trailers) {
            underlying.close(status, trailers);
        }

        @Override
        public boolean isCancelled() {
            return underlying.isCancelled();
        }

        @Override
        public void setMessageCompression(boolean enabled) {
            underlying.setMessageCompression(enabled);
        }

        @Override
        public void setCompression(String compressor) {
            underlying.setCompression(compressor);
        }

        @Override
        public Attributes getAttributes() {
            return underlying.getAttributes();
        }

        @Override
        public String getAuthority() {
            return underlying.getAuthority();
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return underlying.getMethodDescriptor();
        }

        @Override
        public void triggerEvent(Object event) {
            underlying.triggerEvent(event);
        }
    }
}
