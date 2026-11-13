package ai.protomolt.proto.grpc.invoke;

import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicyException;
import io.grpc.CallOptions;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies pooled TLS channel reuse, policy slot leasing, rotation eviction, LRU bounds, and close. */
class PooledChannelFactoryTest {

    private static final MethodDescriptor<String, String> FAKE_METHOD =
            MethodDescriptor.<String, String>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("test.v1.Echo/Ping")
                    .setRequestMarshaller(new StringMarshaller())
                    .setResponseMarshaller(new StringMarshaller())
                    .build();

    @Test
    void reusesHealthyCredentialedChannelAcrossSequentialPolicyLeases() {
        List<FakeManagedChannel> created = new ArrayList<>();
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .maxActiveChannels(1)
                .build();
        ChannelCredentials credentials = TlsChannelCredentials.create();

        try (PooledChannelFactory factory = new PooledChannelFactory(policy, 4,
                (canonical, creds) -> {
                    FakeManagedChannel channel = new FakeManagedChannel(canonical);
                    created.add(channel);
                    return channel;
                })) {
            ManagedChannel first = factory.open("api.example.com:443", credentials);
            assertThat(factory.createdCredentialedChannels()).isEqualTo(1);

            // Policy concurrency slot (maxActiveChannels = 1) is still enforced per active lease.
            assertThatThrownBy(() -> factory.open("api.example.com:443", credentials))
                    .isInstanceOf(OutboundChannelPolicyException.class)
                    .hasMessageContaining("concurrency limit");

            first.shutdownNow();
            assertThat(first.isShutdown()).isTrue();
            assertThat(first.isTerminated()).isTrue();
            assertThat(created.getFirst().shutdownCalled).isFalse();
            assertThat(created.getFirst().shutdownNowCalled).isFalse();

            // Using the closed lease handle fails fast without touching the pooled underlying channel.
            AtomicReference<Status> closedLeaseStatus = new AtomicReference<>();
            ClientCall<String, String> staleCall = first.newCall(FAKE_METHOD, CallOptions.DEFAULT);
            staleCall.start(new ClientCall.Listener<>() {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    closedLeaseStatus.set(status);
                }
            }, new Metadata());
            assertThat(closedLeaseStatus.get().getCode()).isEqualTo(Status.Code.UNAVAILABLE);

            // Second call reuses the warm underlying channel and acquires the released policy slot.
            ManagedChannel second = factory.open("dns:///api.example.com:443", credentials);
            assertThat(factory.createdCredentialedChannels()).isEqualTo(1);
            assertThat(second.isShutdown()).isFalse();
            second.shutdownNow();
            assertThat(created.getFirst().shutdownCalled).isFalse();
        }

        // Closing the factory shuts down all pooled underlying channels.
        assertThat(created.getFirst().shutdownNowCalled).isTrue();
    }

    @Test
    void evictionShutsDownIdleChannelsImmediatelyAndDrainsActiveLeasesOnRelease() {
        List<FakeManagedChannel> created = new ArrayList<>();
        ChannelCredentials credentialsOne = TlsChannelCredentials.create();
        ChannelCredentials credentialsTwo = TlsChannelCredentials.create();

        try (PooledChannelFactory factory = new PooledChannelFactory(
                OutboundChannelPolicy.defaults(), 4,
                (canonical, creds) -> {
                    FakeManagedChannel channel = new FakeManagedChannel(canonical);
                    created.add(channel);
                    return channel;
                })) {
            // Idle channel eviction shuts down immediately.
            ManagedChannel idleLease = factory.open("api.example.com:443", credentialsOne);
            idleLease.shutdownNow();
            assertThat(created.getFirst().shutdownCalled).isFalse();

            factory.evict(credentialsOne);
            assertThat(created.getFirst().shutdownCalled).isTrue();
            assertThat(factory.pooledCount()).isZero();

            // Active lease eviction retires from pool immediately and shuts down once released.
            ManagedChannel inFlight = factory.open("api.example.com:443", credentialsTwo);
            FakeManagedChannel secondUnderlying = created.get(1);
            factory.evict(credentialsTwo);
            assertThat(factory.pooledCount()).isZero();
            assertThat(secondUnderlying.shutdownCalled).isFalse();

            inFlight.shutdownNow();
            assertThat(secondUnderlying.shutdownCalled).isTrue();
        }
    }

    @Test
    void lruCapacityEvictsAndShutsDownLeastRecentlyUsedChannel() {
        List<FakeManagedChannel> created = new ArrayList<>();
        ChannelCredentials credentials = TlsChannelCredentials.create();

        try (PooledChannelFactory factory = new PooledChannelFactory(
                OutboundChannelPolicy.defaults(), 2,
                (canonical, creds) -> {
                    FakeManagedChannel channel = new FakeManagedChannel(canonical);
                    created.add(channel);
                    return channel;
                })) {
            factory.open("one.example.com:443", credentials).shutdownNow();
            factory.open("two.example.com:443", credentials).shutdownNow();
            // Touch one.example.com so two.example.com becomes least-recently-used.
            factory.open("one.example.com:443", credentials).shutdownNow();
            assertThat(created).hasSize(2);

            factory.open("three.example.com:443", credentials).shutdownNow();
            assertThat(created).hasSize(3);
            assertThat(factory.pooledCount()).isEqualTo(2);
            assertThat(created.get(0).shutdownCalled).isFalse();
            assertThat(created.get(1).shutdownCalled).isTrue();
        }
    }

    private static final class FakeManagedChannel extends ManagedChannel {
        private final String authority;
        private boolean shutdownCalled;
        private boolean shutdownNowCalled;

        private FakeManagedChannel(String authority) {
            this.authority = authority;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return new ClientCall<>() {
                @Override
                public void start(Listener<ResponseT> responseListener, Metadata headers) {
                }

                @Override
                public void request(int numMessages) {
                }

                @Override
                public void cancel(String message, Throwable cause) {
                }

                @Override
                public void halfClose() {
                }

                @Override
                public void sendMessage(RequestT message) {
                }
            };
        }

        @Override
        public String authority() {
            return authority;
        }

        @Override
        public ConnectivityState getState(boolean requestConnection) {
            return (shutdownCalled || shutdownNowCalled)
                    ? ConnectivityState.SHUTDOWN
                    : ConnectivityState.READY;
        }

        @Override
        public ManagedChannel shutdown() {
            shutdownCalled = true;
            return this;
        }

        @Override
        public boolean isShutdown() {
            return shutdownCalled || shutdownNowCalled;
        }

        @Override
        public ManagedChannel shutdownNow() {
            shutdownNowCalled = true;
            return this;
        }

        @Override
        public boolean isTerminated() {
            return shutdownCalled || shutdownNowCalled;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }
    }

    private static final class StringMarshaller implements MethodDescriptor.Marshaller<String> {
        @Override
        public InputStream stream(String value) {
            return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String parse(InputStream stream) {
            return "";
        }
    }
}
