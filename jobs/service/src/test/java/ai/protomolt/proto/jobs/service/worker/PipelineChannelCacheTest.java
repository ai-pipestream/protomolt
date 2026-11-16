package ai.protomolt.proto.jobs.service.worker;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
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
}
