package ai.protomolt.proto.composer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import io.grpc.ManagedChannel;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ComposerCleanupTest {
    enum Failure { TIMEOUT, INTERRUPT, SHUTDOWN }

    @ParameterizedTest
    @EnumSource(Failure.class)
    void channelFailuresAreReportedWithoutSkippingOtherCleanup(Failure mode) {
        var calls = new ArrayList<String>();
        var broken = new TestChannel(mode);
        var healthy = new TestChannel(null);
        var node = Composer.emptyBuilder()
                .environment(Map.of("PROTOMOLT_BROKEN_TARGET", "broken:9090",
                        "PROTOMOLT_HEALTHY_TARGET", "healthy:9090"))
                .remoteOpener(target -> target.contains("broken") ? broken : healthy)
                .module(new ServiceModule() {
                    @Override public String role() { return "test"; }
                    @Override public ServiceMount wire(NodeContext context) {
                        context.channels().to("broken");
                        context.channels().to("healthy");
                        return ServiceMount.inert(() -> calls.add("mount"));
                    }
                }).build().boot(List.of("test"));
        try {
            assertThatThrownBy(node::close).isInstanceOf(ComposerException.class);
            assertThat(broken.shutdown).isTrue();
            assertThat(healthy.shutdown).isTrue();
            assertThat(calls).containsExactly("mount");
            assertThat(Thread.currentThread().isInterrupted()).isEqualTo(mode == Failure.INTERRUPT);
            node.close();
        } finally {
            Thread.interrupted();
        }
    }

    private static final class TestChannel extends ManagedChannel {
        private final Failure failure;
        private boolean shutdown;
        TestChannel(Failure failure) { this.failure = failure; }
        @Override public ManagedChannel shutdown() { return shutdownNow(); }
        @Override public ManagedChannel shutdownNow() {
            shutdown = true;
            if (failure == Failure.SHUTDOWN) throw new IllegalStateException("shutdown failed");
            return this;
        }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown && failure == null; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            if (failure == Failure.INTERRUPT) throw new InterruptedException("interrupted");
            return failure != Failure.TIMEOUT;
        }
        @Override public String authority() { return "test"; }
        @Override public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions options) {
            throw new AssertionError("Cleanup must not make RPCs");
        }
    }

    @Test
    void closeReportsFailuresAfterAttemptingEveryResourceInReverseOrder() {
        var calls = new ArrayList<String>();
        var failure = new IllegalStateException("mount close failed");
        var node = Composer.emptyBuilder().module(module(calls, failure, null))
                .build().boot(List.of("test"));
        assertThatThrownBy(node::close).isInstanceOf(ComposerException.class).hasCause(failure);
        assertThat(calls).containsExactly("mount", "extra");
        node.close();
        assertThat(calls).containsExactly("mount", "extra");
    }

    @Test
    void failedStartupKeepsItsCauseAndAttachesCleanupFailure() {
        var calls = new ArrayList<String>();
        var cleanup = new IllegalStateException("mount close failed");
        var startup = new IllegalArgumentException("start failed");
        assertThatThrownBy(() -> Composer.emptyBuilder().module(module(calls, cleanup, startup))
                .build().boot(List.of("test")))
                .isInstanceOf(ComposerException.class).hasCause(startup);
        assertThat(startup.getSuppressed()).hasSize(1);
        assertThat(startup.getSuppressed()[0]).hasCause(cleanup);
        assertThat(calls).containsExactly("mount", "extra");
    }

    private ServiceModule module(List<String> calls, RuntimeException cleanup, RuntimeException startup) {
        return new ServiceModule() {
            @Override public String role() { return "test"; }
            @Override public ServiceMount wire(NodeContext context) {
                context.onClose(() -> calls.add("extra"));
                return new ServiceMount() {
                    @Override public void start() {
                        if (startup != null) throw startup;
                    }
                    @Override public void close() {
                        calls.add("mount");
                        throw cleanup;
                    }
                };
            }
        };
    }
}
