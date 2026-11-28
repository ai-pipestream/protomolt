package ai.protomolt.proto.grpc.invoke;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.reflection.v1.ErrorResponse;
import io.grpc.reflection.v1.ListServiceResponse;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.reflection.v1.ServiceResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReflectionClientTest {

    @Test
    void rejectsUnboundedTimeoutBeforeOpeningAReflectionStream() {
        assertThatThrownBy(() -> ReflectionClient.discover(null,
                ReflectionClient.MAX_TIMEOUT_MS + 1))
                .isInstanceOf(ReflectionException.class)
                .hasMessageContaining("must be from 1 to");
    }

    @Test
    void stalledServerReflectionStreamTimesOutAndCancelsServerCall() throws Exception {
        CountDownLatch cancelledOnServer = new CountDownLatch(1);
        String serverName = InProcessServerBuilder.generateName();
        Server stalled = InProcessServerBuilder.forName(serverName)
                .addService(new ServerReflectionGrpc.ServerReflectionImplBase() {
                    @Override
                    public StreamObserver<ServerReflectionRequest> serverReflectionInfo(
                            StreamObserver<ServerReflectionResponse> responseObserver) {
                        Context.current().addListener(
                                ctx -> cancelledOnServer.countDown(),
                                Runnable::run);
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(ServerReflectionRequest value) {
                                // Intentionally stall and never respond.
                            }

                            @Override
                            public void onError(Throwable t) {
                                cancelledOnServer.countDown();
                            }

                            @Override
                            public void onCompleted() {
                            }
                        };
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThatThrownBy(() -> ReflectionClient.discover(channel, 50))
                    .isInstanceOf(ReflectionException.class)
                    .hasMessageContaining("Reflection timed out after 50ms");
            assertThat(cancelledOnServer.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            channel.shutdownNow();
            stalled.shutdownNow();
        }
    }

    @Test
    void v1ErrorResponseAttachesCanonicalGrpcStatusAndDoesNotDowngradeToV1Alpha() throws Exception {
        AtomicBoolean v1AlphaCalled = new AtomicBoolean();
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .addService(new ServerReflectionGrpc.ServerReflectionImplBase() {
                    @Override
                    public StreamObserver<ServerReflectionRequest> serverReflectionInfo(
                            StreamObserver<ServerReflectionResponse> responseObserver) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(ServerReflectionRequest value) {
                                responseObserver.onNext(ServerReflectionResponse.newBuilder()
                                        .setErrorResponse(ErrorResponse.newBuilder()
                                                .setErrorCode(Status.Code.PERMISSION_DENIED.value())
                                                .setErrorMessage("reflection denied for caller"))
                                        .build());
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                            }
                        };
                    }
                })
                .addService(new io.grpc.reflection.v1alpha.ServerReflectionGrpc
                        .ServerReflectionImplBase() {
                    @Override
                    public StreamObserver<io.grpc.reflection.v1alpha.ServerReflectionRequest>
                            serverReflectionInfo(StreamObserver<
                                    io.grpc.reflection.v1alpha.ServerReflectionResponse> out) {
                        v1AlphaCalled.set(true);
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(
                                    io.grpc.reflection.v1alpha.ServerReflectionRequest value) {
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                out.onCompleted();
                            }
                        };
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThatThrownBy(() -> ReflectionClient.discover(channel, 5_000))
                    .isInstanceOfSatisfying(ReflectionException.class, ex -> {
                        assertThat(ex).hasMessage(
                                "Reflection error 7: reflection denied for caller");
                        Status status = Status.fromThrowable(ex);
                        assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
                        assertThat(status.getDescription())
                                .isEqualTo("reflection denied for caller");
                    });
            assertThat(v1AlphaCalled).isFalse();
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void v1SymbolLookupErrorResponsePreservesNotFoundStatus() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .addService(new ServerReflectionGrpc.ServerReflectionImplBase() {
                    @Override
                    public StreamObserver<ServerReflectionRequest> serverReflectionInfo(
                            StreamObserver<ServerReflectionResponse> responseObserver) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(ServerReflectionRequest value) {
                                if (value.hasListServices()) {
                                    responseObserver.onNext(ServerReflectionResponse.newBuilder()
                                            .setListServicesResponse(
                                                    ListServiceResponse.newBuilder()
                                                            .addService(ServiceResponse.newBuilder()
                                                                    .setName("missing.Service")))
                                            .build());
                                } else {
                                    responseObserver.onNext(ServerReflectionResponse.newBuilder()
                                            .setErrorResponse(ErrorResponse.newBuilder()
                                                    .setErrorCode(Status.Code.NOT_FOUND.value())
                                                    .setErrorMessage(
                                                            "Symbol not found: missing.Service"))
                                            .build());
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                            }
                        };
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThatThrownBy(() -> ReflectionClient.discover(channel, 5_000))
                    .isInstanceOfSatisfying(ReflectionException.class, ex -> {
                        assertThat(ex).hasMessage(
                                "Reflection error 5: Symbol not found: missing.Service");
                        Status status = Status.fromThrowable(ex);
                        assertThat(status.getCode()).isEqualTo(Status.Code.NOT_FOUND);
                        assertThat(status.getDescription())
                                .isEqualTo("Symbol not found: missing.Service");
                    });
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void v1AlphaErrorResponseAttachesCanonicalGrpcStatus() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .addService(new io.grpc.reflection.v1alpha.ServerReflectionGrpc
                        .ServerReflectionImplBase() {
                    @Override
                    public StreamObserver<io.grpc.reflection.v1alpha.ServerReflectionRequest>
                            serverReflectionInfo(StreamObserver<
                                    io.grpc.reflection.v1alpha.ServerReflectionResponse> out) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(
                                    io.grpc.reflection.v1alpha.ServerReflectionRequest value) {
                                out.onNext(io.grpc.reflection.v1alpha.ServerReflectionResponse
                                        .newBuilder()
                                        .setErrorResponse(
                                                io.grpc.reflection.v1alpha.ErrorResponse
                                                        .newBuilder()
                                                        .setErrorCode(Status.Code.UNAUTHENTICATED
                                                                .value())
                                                        .setErrorMessage("expired bearer token"))
                                        .build());
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                out.onCompleted();
                            }
                        };
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThatThrownBy(() -> ReflectionClient.discover(channel, 5_000))
                    .isInstanceOfSatisfying(ReflectionException.class, ex -> {
                        assertThat(ex).hasMessage("Reflection error 16: expired bearer token");
                        Status status = Status.fromThrowable(ex);
                        assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
                        assertThat(status.getDescription()).isEqualTo("expired bearer token");
                    });
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void errorResponseWithUnsetZeroCodeMapsToUnknownRatherThanOk() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .addService(new ServerReflectionGrpc.ServerReflectionImplBase() {
                    @Override
                    public StreamObserver<ServerReflectionRequest> serverReflectionInfo(
                            StreamObserver<ServerReflectionResponse> responseObserver) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(ServerReflectionRequest value) {
                                responseObserver.onNext(ServerReflectionResponse.newBuilder()
                                        .setErrorResponse(ErrorResponse.newBuilder()
                                                .setErrorCode(Status.Code.OK.value())
                                                .setErrorMessage("malformed upstream error"))
                                        .build());
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                            }
                        };
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThatThrownBy(() -> ReflectionClient.discover(channel, 5_000))
                    .isInstanceOfSatisfying(ReflectionException.class, ex -> {
                        assertThat(Status.fromThrowable(ex).getCode())
                                .isEqualTo(Status.Code.UNKNOWN);
                    });
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void closureOfASymbolIsItsFileAndEveryDependencyAndLinks() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .addService(io.grpc.protobuf.services.ProtoReflectionServiceV1.newInstance())
                .addService(new io.grpc.protobuf.services.HealthStatusManager().getHealthService())
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            // A message, not a service: discover() would only reach it through a service.
            var closure = ReflectionClient.closure(channel, "grpc.health.v1.HealthCheckResponse", 5_000);

            assertThat(closure).isPresent();
            var expected = io.grpc.health.v1.HealthCheckResponse.getDescriptor().getFile();
            assertThat(closure.get().getFileList())
                    .extracting(com.google.protobuf.DescriptorProtos.FileDescriptorProto::getName)
                    .contains(expected.getName());
            assertThat(closure.get().getFileList().stream()
                    .filter(file -> file.getName().equals(expected.getName())).findFirst().orElseThrow())
                    .isEqualTo(expected.toProto());
            // Closed: every import of every file is in the set.
            var names = closure.get().getFileList().stream()
                    .map(com.google.protobuf.DescriptorProtos.FileDescriptorProto::getName).toList();
            for (var file : closure.get().getFileList()) {
                assertThat(names).containsAll(file.getDependencyList());
            }
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void closureOfASymbolTheServerDoesNotKnowIsEmptyNotAFailure() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .addService(io.grpc.protobuf.services.ProtoReflectionServiceV1.newInstance())
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThat(ReflectionClient.closure(channel, "grpc.health.v1.HealthCheckResponse", 5_000))
                    .isEmpty();
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void closureFromAServerThatCannotBeAskedIsAFailureNeverAnUnknownSymbol() {
        // No server is registered under this name.
        ManagedChannel channel = InProcessChannelBuilder.forName(InProcessServerBuilder.generateName()).build();
        try {
            assertThatThrownBy(() -> ReflectionClient.closure(channel, "grpc.health.v1.HealthCheckResponse", 2_000))
                    .isInstanceOf(ReflectionException.class);
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void closureRejectsABlankSymbolAndAnUnboundedTimeout() {
        assertThatThrownBy(() -> ReflectionClient.closure(null, " ", 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReflectionClient.closure(null, "a.B", ReflectionClient.MAX_TIMEOUT_MS + 1))
                .isInstanceOf(ReflectionException.class)
                .hasMessageContaining("must be from 1 to");
    }
}
