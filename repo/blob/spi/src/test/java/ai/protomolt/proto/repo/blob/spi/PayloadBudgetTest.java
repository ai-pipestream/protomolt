package ai.protomolt.proto.repo.blob.spi;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PayloadBudgetTest {
    @Test void capacityAndArithmeticBoundaries() {
        assertThatThrownBy(() -> new PayloadBudget(0)).isInstanceOf(IllegalArgumentException.class);
        var budget = new PayloadBudget(Long.MAX_VALUE);
        assertThatThrownBy(() -> budget.reserve(-1)).isInstanceOf(IllegalArgumentException.class);
        try (var full = budget.reserve(Long.MAX_VALUE); var empty = budget.reserve(0)) {
            assertThat(budget.reservedBytes()).isEqualTo(Long.MAX_VALUE);
            assertThatThrownBy(() -> budget.reserve(1)).isInstanceOf(PayloadBudget.CapacityExceededException.class);
        }
        assertThat(budget.reservedBytes()).isZero();
        try (var full = budget.reserve(Long.MAX_VALUE)) { assertThat(full.bytes()).isEqualTo(Long.MAX_VALUE); }
    }

    @Test void simultaneousOwnersCannotOverbookAndConcurrentCloseCannotReleaseTwice() throws Exception {
        var budget = new PayloadBudget(5);
        var ready = new CountDownLatch(20);
        var start = new CountDownLatch(1);
        var attempted = new CountDownLatch(20);
        var release = new CountDownLatch(1);
        var accepted = new AtomicInteger();
        var rejected = new AtomicInteger();
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 20; i++) futures.add(executor.submit(() -> {
                ready.countDown();
                if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("start timeout");
                PayloadBudget.Lease lease = null;
                try {
                    lease = budget.reserve(1);
                    accepted.incrementAndGet();
                } catch (PayloadBudget.CapacityExceededException exhausted) { rejected.incrementAndGet(); }
                finally { attempted.countDown(); }
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("release timeout");
                } finally { if (lease != null) lease.close(); }
                return null;
            }));
            try {
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                assertThat(attempted.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(accepted.get()).isEqualTo(5);
                assertThat(rejected.get()).isEqualTo(15);
                assertThat(budget.reservedBytes()).isEqualTo(5);
            } finally { start.countDown(); release.countDown(); }
            for (var future : futures) future.get(5, TimeUnit.SECONDS);
            assertThat(budget.reservedBytes()).isZero();
            var lease = budget.reserve(5);
            futures.clear();
            for (int i = 0; i < 20; i++) futures.add(executor.submit(lease::close));
            for (var future : futures) future.get(5, TimeUnit.SECONDS);
            assertThat(budget.reservedBytes()).isZero();
        }
    }
}
