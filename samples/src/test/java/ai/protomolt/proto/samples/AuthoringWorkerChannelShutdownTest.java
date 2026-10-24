package ai.protomolt.proto.samples;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that {@link AuthoringWorker#shutdownChannels} never leaks the control channel on interruption. */
class AuthoringWorkerChannelShutdownTest {

    @Test
    void shutsDownControlChannelAndPreservesInterruptWhenExternalAwaitTerminationIsInterrupted() {
        FakeManagedChannel external = new FakeManagedChannel(true);
        FakeManagedChannel control = new FakeManagedChannel(false);

        try {
            AuthoringWorker.shutdownChannels(external, control);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }

        assertThat(external.shutdownNowCalled.get()).isTrue();
        assertThat(control.shutdownNowCalled.get()).isTrue();
        assertThat(control.awaited.get()).isTrue();
    }

    @Test
    void shutsDownControlChannelWhenExternalChannelWasNeverOpened() {
        FakeManagedChannel control = new FakeManagedChannel(false);

        AuthoringWorker.shutdownChannels(null, control);

        assertThat(control.shutdownNowCalled.get()).isTrue();
        assertThat(control.awaited.get()).isTrue();
    }

    private static final class FakeManagedChannel extends ManagedChannel {
        private final boolean interruptOnAwait;
        private final AtomicBoolean shutdownNowCalled = new AtomicBoolean();
        private final AtomicBoolean awaited = new AtomicBoolean();

        private FakeManagedChannel(boolean interruptOnAwait) {
            this.interruptOnAwait = interruptOnAwait;
        }

        @Override
        public ManagedChannel shutdown() {
            return this;
        }

        @Override
        public boolean isShutdown() {
            return shutdownNowCalled.get();
        }

        @Override
        public boolean isTerminated() {
            return awaited.get();
        }

        @Override
        public ManagedChannel shutdownNow() {
            shutdownNowCalled.set(true);
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            awaited.set(true);
            if (interruptOnAwait) {
                throw new InterruptedException("simulated interrupt during channel drain");
            }
            return true;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String authority() {
            return "localhost";
        }
    }
}
