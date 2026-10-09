package ai.protomolt.proto.repo.service;

import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcServerLifetimeTest {
    @Test void blockedHandlerRetainsSharedResourcesAndCloseCanBeRetried() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var releaseHandler = new java.util.concurrent.CountDownLatch(1);
        var releasedResources = new java.util.concurrent.atomic.AtomicBoolean();
        var method = io.grpc.MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                .setType(io.grpc.MethodDescriptor.MethodType.UNARY).setFullMethodName("shutdown.Test/Block")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance())).build();
        String name = InProcessServerBuilder.generateName();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var lifetime = GrpcServerLifetime.start(InProcessServerBuilder.forName(name).addService(
                io.grpc.ServerServiceDefinition.builder("shutdown.Test").addMethod(method,
                        io.grpc.stub.ServerCalls.asyncUnaryCall((request, observer) -> {
                            entered.countDown();
                            while (releaseHandler.getCount() != 0) {
                                try { releaseHandler.await(); }
                                catch (InterruptedException ignored) { /* Deliberately uncooperative handler. */ }
                            }
                            observer.onError(io.grpc.Status.CANCELLED.asRuntimeException());
                        })).build()), executor);
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(method, io.grpc.CallOptions.DEFAULT), com.google.protobuf.Empty.getDefaultInstance());
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> ShutdownBarrier.releaseAfter(java.util.List.of(
                    () -> lifetime.close(java.time.Duration.ofMillis(50))), () -> releasedResources.set(true)))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(executor.isTerminated()).isFalse();
            assertThat(releasedResources).isFalse();
            // Repeating close while still blocked must not forget the executor.
            assertThatThrownBy(() -> lifetime.close(java.time.Duration.ofMillis(50)))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            releaseHandler.countDown();
            channel.shutdownNow();
            lifetime.close();
        }
        ShutdownBarrier.releaseAfter(java.util.List.of(lifetime), () -> releasedResources.set(true));
        assertThat(releasedResources).isTrue();
        assertThat(executor.isTerminated()).isTrue();
    }

    @Test void hostReceivesLifetimeBeforeFailedBind() throws Exception {
        String name = InProcessServerBuilder.generateName();
        var retained = new java.util.ArrayList<GrpcServerLifetime>();
        try (var existing = GrpcServerLifetime.start(InProcessServerBuilder.forName(name))) {
            assertThatThrownBy(() -> GrpcServerLifetime.start(InProcessServerBuilder.forName(name), retained::add))
                    .isInstanceOf(IOException.class);
            assertThat(retained).hasSize(1);
            retained.getFirst().close();
            assertThat(retained.getFirst().server().isTerminated()).isTrue();
            assertThat(existing.server().isShutdown()).isFalse();
        }
    }

    @Test void closesServerAndBorrowedExecutor() throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var lifetime = GrpcServerLifetime.start(
                InProcessServerBuilder.forName(InProcessServerBuilder.generateName()), executor);
        executor.submit(() -> {}).get();
        lifetime.close();
        assertThat(lifetime.server().isTerminated()).isTrue();
        assertThat(executor.isTerminated()).isTrue();
        lifetime.close();
    }

    @Test void failedTransportStartClosesExecutorWithoutStoppingExistingListener() throws Exception {
        String name = InProcessServerBuilder.generateName();
        try (var existing = GrpcServerLifetime.start(InProcessServerBuilder.forName(name))) {
            var executor = Executors.newVirtualThreadPerTaskExecutor();
            assertThatThrownBy(() -> GrpcServerLifetime.start(InProcessServerBuilder.forName(name), executor))
                    .isInstanceOf(IOException.class);
            assertThat(executor.isTerminated()).isTrue();
            assertThat(existing.server().isShutdown()).isFalse();
        }
    }
}
