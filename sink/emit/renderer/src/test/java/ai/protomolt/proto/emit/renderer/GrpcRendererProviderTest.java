package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.*;
import com.google.protobuf.*;
import io.grpc.*;
import io.grpc.inprocess.*;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class GrpcRendererProviderTest {
    private void withRemote(RendererServiceGrpc.RendererServiceImplBase service, Duration timeout,
                            CheckedRun run) throws Exception {
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            var c = new RendererRegistryTest.Echo().capability();
            var provider = new GrpcRendererProvider(c, StringValue.getDescriptor(), Empty.getDescriptor(), channel, timeout, 65536);
            run.run(new RendererRegistry(List.of(provider), Set.of(c.getProviderId())));
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }
    @FunctionalInterface interface CheckedRun { void run(RendererRegistry registry) throws Exception; }
    @Test void sendsExactBytesAndDescriptorIdentityThroughConfiguredChannel() throws Exception {
        var seen = new AtomicReference<RenderRequest>();
        var service = new RendererServiceGrpc.RendererServiceImplBase() {
            @Override public void render(RenderRequest request, StreamObserver<RenderResponse> reply) {
                seen.set(request);
                reply.onNext(RenderResponse.newBuilder().setCapability(request.getCapability()).setContent(request.getInput()).build());
                reply.onCompleted();
            }
        };
        withRemote(service, Duration.ofSeconds(5), registry -> {
            var message = StringValue.of("hello");
            assertThat(registry.render("test.echo", message, Empty.getDefaultInstance(), RendererRegistryTest.ALLOW, RendererRegistryTest.ACTIVE)).isEqualTo(message.toByteString());
            assertThat(seen.get().getInputDescriptors()).isEqualTo(DescriptorFingerprints.closure(message.getDescriptorForType()).toByteString());
            assertThat(seen.get().getInputFingerprint()).isEqualTo(DescriptorFingerprints.fingerprintOf(message.getDescriptorForType()));
            assertThatThrownBy(() -> registry.render("test.echo", Int32Value.of(1), Empty.getDefaultInstance(), RendererRegistryTest.ALLOW, RendererRegistryTest.ACTIVE)).hasMessageContaining("source descriptor mismatch");
        });
    }
    @Test void remoteRefusalKeepsItsCategoryAndCause() throws Exception {
        withRemote(new RendererServiceGrpc.RendererServiceImplBase() {
            @Override public void render(RenderRequest r, StreamObserver<RenderResponse> reply) {
                reply.onError(Status.PERMISSION_DENIED.asRuntimeException());
            }
        }, Duration.ofSeconds(5), registry -> assertThatThrownBy(() -> registry.render("test.echo", StringValue.of("a"), Empty.getDefaultInstance(), RendererRegistryTest.ALLOW, RendererRegistryTest.ACTIVE))
                .isInstanceOfSatisfying(RenderFailure.class, e -> { assertThat(e.kind()).isEqualTo(RenderFailure.Kind.DENIED); assertThat(e.getCause()).isNotNull(); }));
    }
    @Test void incompleteRpcCannotReturnSuccess() throws Exception {
        withRemote(new RendererServiceGrpc.RendererServiceImplBase() {
            @Override public void render(RenderRequest r, StreamObserver<RenderResponse> reply) {}
        }, Duration.ofMillis(100), registry -> assertThatThrownBy(() -> registry.render("test.echo", StringValue.of("a"), Empty.getDefaultInstance(), RendererRegistryTest.ALLOW, RendererRegistryTest.ACTIVE))
                .isInstanceOfSatisfying(RenderFailure.class, e -> assertThat(e.kind()).isEqualTo(RenderFailure.Kind.UNAVAILABLE)));
    }
    @Test void rejectsCapabilitySubstitution() throws Exception {
        withRemote(new RendererServiceGrpc.RendererServiceImplBase() {
            @Override public void render(RenderRequest r, StreamObserver<RenderResponse> reply) {
                reply.onNext(RenderResponse.newBuilder().setCapability(r.getCapability().toBuilder().setProviderVersion("changed")).build()); reply.onCompleted();
            }
        }, Duration.ofSeconds(5), registry -> assertThatThrownBy(() -> registry.render("test.echo", StringValue.of("a"), Empty.getDefaultInstance(), RendererRegistryTest.ALLOW, RendererRegistryTest.ACTIVE))
                .hasMessageContaining("different capability"));
    }
    @Test void cancellationStopsOutstandingRpc() throws Exception {
        var cancelled = new AtomicBoolean();
        var remoteCancelled = new AtomicBoolean();
        withRemote(new RendererServiceGrpc.RendererServiceImplBase() {
            @Override public void render(RenderRequest r, StreamObserver<RenderResponse> reply) {
                Context.current().addListener(ignored -> remoteCancelled.set(true), Runnable::run);
                cancelled.set(true);
            }
        }, Duration.ofSeconds(5), registry -> {
            assertThatThrownBy(() -> registry.render("test.echo", StringValue.of("a"), Empty.getDefaultInstance(), RendererRegistryTest.ALLOW, cancelled::get))
                    .isInstanceOfSatisfying(RenderFailure.class, e -> assertThat(e.kind()).isEqualTo(RenderFailure.Kind.CANCELLED));
            assertThat(remoteCancelled).isTrue();
        });
    }
}
