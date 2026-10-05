package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentPublicationScopeCallsTest {
    @Test void shutdownWaitsThroughResolverCleanupAndRejectsNewCalls() throws Exception {
        var calls = new DocumentPublicationScopeCalls();
        var cleanupEntered = new CountDownLatch(1); var releaseCleanup = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var task = executor.submit(() -> {
                try (var call = calls.enter(); var scope = (AutoCloseable) () -> {
                    cleanupEntered.countDown();
                    if (!releaseCleanup.await(5, TimeUnit.SECONDS)) throw new AssertionError("cleanup barrier timeout");
                }) { /* Inner publication has returned; outer scope cleanup remains live. */ }
                return null;
            });
            try {
                assertThat(cleanupEntered.await(5, TimeUnit.SECONDS)).isTrue();
                calls.close();
                assertThat(calls.awaitIdle(Duration.ZERO)).isFalse();
                assertThatThrownBy(calls::enter).isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
            } finally { releaseCleanup.countDown(); }
            task.get(5, TimeUnit.SECONDS);
            assertThat(calls.awaitIdle(Duration.ofSeconds(1))).isTrue();
        }
    }

    @Test void cleanupFailureStillReleasesCallAndPreservesOriginalFailure() throws Exception {
        var calls = new DocumentPublicationScopeCalls();
        var original = new IllegalArgumentException("publication");
        var cleanup = new IllegalStateException("cleanup");
        assertThatThrownBy(() -> {
            try (var call = calls.enter(); var scope = (AutoCloseable) () -> { throw cleanup; }) { throw original; }
        }).isSameAs(original).satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(cleanup));
        calls.close();
        assertThat(calls.awaitIdle(Duration.ZERO)).isTrue();
    }
}
