package ai.protomolt.proto.jobs.service.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import io.grpc.ChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** One channel per target is shared by the calls in flight to it, however they arrive. */
class PipelineChannelCacheTest {

    @Test
    void concurrentFirstCallsToOneTargetOpenOneChannelAndShareIt() throws Exception {
        AtomicInteger opens = new AtomicInteger();
        ChannelFactory slowFactory = (target, tls) -> {
            opens.incrementAndGet();
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return InProcessChannelBuilder.forName("cache-test").build();
        };
        try (PipelineChannelCache cache = new PipelineChannelCache(slowFactory);
                ExecutorService pool = Executors.newFixedThreadPool(2)) {
            CountDownLatch start = new CountDownLatch(1);
            Future<PipelineChannelCache.Lease> first = pool.submit(() -> {
                start.await();
                return cache.acquireShared("127.0.0.1:1", false);
            });
            Future<PipelineChannelCache.Lease> second = pool.submit(() -> {
                start.await();
                return cache.acquireShared("127.0.0.1:1", false);
            });
            start.countDown();
            PipelineChannelCache.Lease a = first.get(5, TimeUnit.SECONDS);
            PipelineChannelCache.Lease b = second.get(5, TimeUnit.SECONDS);

            assertThat(opens.get()).isEqualTo(1);
            assertThat(a.channel()).isSameAs(b.channel());

            a.release();
            assertThat(b.channel().isShutdown()).as("still in use by the other call").isFalse();
            b.release();
            assertThat(b.channel().isShutdown()).isTrue();
        }
    }

    @Test
    void aDedicatedChannelIsClosedWithTheCacheAndNoneOpensAfterClose() {
        AtomicInteger dedicatedOpens = new AtomicInteger();
        ChannelFactory factory = new ChannelFactory() {
            @Override
            public ManagedChannel open(String target, boolean tls) {
                throw new AssertionError("only dedicated channels are opened here");
            }

            @Override
            public ManagedChannel open(String target, ChannelCredentials credentials) {
                dedicatedOpens.incrementAndGet();
                return InProcessChannelBuilder.forName("cache-test-dedicated").build();
            }
        };
        ChannelCredentials customTls = InsecureChannelCredentials.create();
        PipelineChannelCache cache = new PipelineChannelCache(factory);

        // A call that finishes shuts its own dedicated channel down.
        PipelineChannelCache.Lease finished = cache.openDedicated("127.0.0.1:1", customTls);
        finished.release();
        assertThat(finished.channel().isShutdown()).isTrue();

        // A call still in flight when the cache closes loses its channel with the cache.
        PipelineChannelCache.Lease inFlight = cache.openDedicated("127.0.0.1:1", customTls);
        assertThat(inFlight.channel().isShutdown()).isFalse();
        cache.close();
        assertThat(inFlight.channel().isShutdown()).isTrue();
        inFlight.release();

        // A closed cache opens nothing more.
        assertThatThrownBy(() -> cache.openDedicated("127.0.0.1:1", customTls))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThat(dedicatedOpens.get()).isEqualTo(2);
    }
}
