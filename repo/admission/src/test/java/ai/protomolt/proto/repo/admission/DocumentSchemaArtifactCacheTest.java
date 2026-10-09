package ai.protomolt.proto.repo.admission;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.StringValue;
import com.google.protobuf.Int32Value;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaArtifactCacheTest {
    private static final Runnable ACTIVE = () -> {};
    private static final ByteString A = FileDescriptorSet.newBuilder()
            .addFile(StringValue.getDescriptor().getFile().toProto()).build().toByteString();
    private static final ByteString B = FileDescriptorSet.newBuilder()
            .addFile(Int32Value.getDescriptor().getFile().toProto().toBuilder().setName("alternate.proto"))
            .build().toByteString();
    private static String hash(ByteString value) { return DocumentSchemaOccurrences.sha256(value, ACTIVE); }
    private static DocumentSchemaArtifactCache cache(int count) {
        return new DocumentSchemaArtifactCache(new DocumentSchemaArtifactCache.Limits(100_000, count, 20_000));
    }

    @Test void leasesRetainBytesAcrossShutdownAndCloseIdempotently() {
        var cache = cache(2);
        var first = cache.put(hash(A), A, ACTIVE);
        var second = cache.acquire(hash(A), ACTIVE).orElseThrow();
        assertThat(first.bytes()).isEqualTo(A).isNotSameAs(A);
        assertThat(cache.ownedBytes()).isEqualTo(A.size());
        cache.close(); cache.close(); first.close(); first.close();
        assertThat(second.bytes()).isEqualTo(A);
        assertThat(cache.ownedBytes()).isEqualTo(A.size());
        assertThatThrownBy(() -> cache.acquire(hash(A), ACTIVE)).isInstanceOf(IllegalStateException.class);
        second.close();
        assertThat(cache.ownedBytes()).isZero();
        assertThatThrownBy(first::bytes).isInstanceOf(IllegalStateException.class);
    }

    @Test void pinnedEntriesRefuseCapacityAndIdleEntriesCanBeEvicted() {
        try (var cache = cache(1)) {
            var first = cache.put(hash(A), A, ACTIVE);
            assertThatThrownBy(() -> cache.put(hash(B), B, ACTIVE))
                    .isInstanceOf(DocumentSchemaArtifactCache.CapacityExceeded.class);
            assertThat(first.bytes()).isEqualTo(A);
            first.close();
            try (var second = cache.put(hash(B), B, ACTIVE)) {
                assertThat(cache.acquire(hash(A), ACTIVE)).isEmpty();
                assertThat(second.bytes()).isEqualTo(B);
                assertThat(cache.ownedBytes()).isEqualTo(B.size());
            }
        }
    }

    @Test void cancellationAfterLookupReleasesItsPin() {
        try (var cache = cache(1)) {
            cache.put(hash(A), A, ACTIVE).close();
            var calls = new AtomicInteger();
            var failure = new CancellationException();
            assertThatThrownBy(() -> cache.acquire(hash(A), () -> {
                if (calls.incrementAndGet() == 2) throw failure;
            })).isSameAs(failure);
            try (var replacement = cache.put(hash(B), B, ACTIVE)) {
                assertThat(replacement.bytes()).isEqualTo(B);
            }
        }
    }

    @Test void byteCapacityIncludesTemporaryCopyAndRejectsOversizedInput() {
        try (var cache = new DocumentSchemaArtifactCache(
                new DocumentSchemaArtifactCache.Limits(A.size(), 10, A.size()))) {
            assertThatThrownBy(() -> cache.put(hash(A), A, ACTIVE))
                    .isInstanceOf(DocumentSchemaArtifactCache.CapacityExceeded.class);
            assertThat(cache.ownedBytes()).isZero();
        }
        try (var cache = new DocumentSchemaArtifactCache(
                new DocumentSchemaArtifactCache.Limits(100_000, 10, 1))) {
            assertThatThrownBy(() -> cache.put(hash(A), A, ACTIVE))
                    .isInstanceOf(DocumentSchemaArtifactCache.CapacityExceeded.class);
        }
    }

    @Test void corruptionAndEveryCancellationCheckpointReleaseReservations() {
        try (var cache = cache(2)) {
            assertThatThrownBy(() -> cache.put(hash(B), A, ACTIVE)).isInstanceOf(IllegalArgumentException.class);
            assertThat(cache.acquire(hash(B), ACTIVE)).isEmpty();
            assertThat(cache.ownedBytes()).isZero();
        }
        var count = new AtomicInteger();
        try (var cache = cache(2); var ignored = cache.put(hash(A), A, count::incrementAndGet)) { }
        for (int point = 1; point <= count.get(); point++) {
            int failAt = point;
            var calls = new AtomicInteger();
            var failure = new CancellationException("test cancellation");
            try (var cache = cache(2)) {
                assertThatThrownBy(() -> cache.put(hash(A), A, () -> {
                    if (calls.incrementAndGet() == failAt) throw failure;
                })).isSameAs(failure);
                assertThat(cache.ownedBytes()).isZero();
                assertThat(cache.acquire(hash(A), ACTIVE)).isEmpty();
                try (var retry = cache.put(hash(A), A, ACTIVE)) { assertThat(retry.bytes()).isEqualTo(A); }
            }
        }
    }

    @Test void shutdownDuringCopyRefusesPublicationAndDrainsPendingCapacity() throws Exception {
        var cache = cache(1);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = executor.submit(() -> cache.put(hash(A), A, () -> {
                if (calls.incrementAndGet() == 2) {
                    entered.countDown();
                    await(release);
                }
            }));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(cache.ownedBytes()).isEqualTo(2L * A.size());
                assertThatThrownBy(() -> cache.put(hash(B), B, ACTIVE))
                        .isInstanceOf(DocumentSchemaArtifactCache.CapacityExceeded.class);
                cache.close();
                assertThat(cache.ownedBytes()).isEqualTo(2L * A.size());
            } finally { release.countDown(); }
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(IllegalStateException.class);
            assertThat(cache.ownedBytes()).isZero();
        } finally { cache.close(); }
    }

    @Test void concurrentEqualArtifactsConvergeWithoutLosingPins() throws Exception {
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        try (var cache = cache(2); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Callable<DocumentSchemaArtifactCache.Lease> task = () -> {
                var calls = new AtomicInteger();
                return cache.put(hash(A), A, () -> {
                    if (calls.incrementAndGet() == 2) { entered.countDown(); await(release); }
                });
            };
            var first = executor.submit(task); var second = executor.submit(task);
            try { assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue(); }
            finally { release.countDown(); }
            try (var a = first.get(5, TimeUnit.SECONDS); var b = second.get(5, TimeUnit.SECONDS)) {
                assertThat(a.bytes()).isSameAs(b.bytes());
                assertThat(cache.ownedBytes()).isEqualTo(A.size());
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("Timed out waiting for test barrier");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
}
