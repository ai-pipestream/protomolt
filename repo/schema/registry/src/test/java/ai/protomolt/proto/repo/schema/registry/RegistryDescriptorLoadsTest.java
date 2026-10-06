package ai.protomolt.proto.repo.schema.registry;

import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.admission.DocumentSchemaArtifactCache;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.StringValue;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class RegistryDescriptorLoadsTest {
    @TempDir Path temp;
    private static final Runnable ACTIVE = () -> {};
    private static final ByteString BYTES = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(StringValue.getDescriptor().getFile().toProto()).build().toByteString();
    private static String digest() throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(BYTES.toByteArray()));
    }
    private static DocumentSchemaArtifactCache cache() {
        return new DocumentSchemaArtifactCache(new DocumentSchemaArtifactCache.Limits(1_000_000, 4, 100_000));
    }
    private GitSchemaRegistryStore store() {
        return GitSchemaRegistryStore.builder().repositoryDir(temp.resolve("registry")).build();
    }

    // Only intercept timing/counting of an actual Git provider. Every operation delegates.
    private static SchemaRegistryStore held(GitSchemaRegistryStore actual, CountDownLatch entered,
            CountDownLatch release, AtomicInteger calls) {
        return (SchemaRegistryStore) Proxy.newProxyInstance(SchemaRegistryStore.class.getClassLoader(),
                new Class<?>[] {SchemaRegistryStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("descriptorSet")) {
                        calls.incrementAndGet(); entered.countDown();
                        if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("fixture release timed out");
                    }
                    try { return method.invoke(actual, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }

    @Test void concurrentCallersShareOneRealReadAndInterruptDoesNotCancelAnother() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var joined = new CountDownLatch(1);
        var calls = new AtomicInteger();
        String digest = digest();
        try (var store = store(); var cache = cache();
             var loads = new RegistryDescriptorLoads(held(store, entered, release, calls), cache, 2);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            store.putDescriptorSet(digest, BYTES);
            var first = executor.submit(() -> { try (var lease = loads.acquire(digest, ACTIVE)) { return lease.bytes(); } });
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var checks = new AtomicInteger();
                var second = executor.submit(() -> {
                    try (var lease = loads.acquire(digest, () -> { if (checks.incrementAndGet() >= 2) joined.countDown(); })) {
                        return lease.bytes();
                    }
                });
                assertThat(joined.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(loads.stats().registryReads()).isEqualTo(1);
                assertThat(loads.stats().joinedLoads()).isEqualTo(1);
                assertThat(loads.stats().activeReads()).isEqualTo(1);
                first.cancel(true);
                assertThat(loads.awaitIdle(Duration.ZERO)).isFalse();
                release.countDown();
                assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo(BYTES);
                assertThat(calls.get()).isEqualTo(1);
                assertThat(loads.awaitIdle(Duration.ofSeconds(5))).isTrue();
                assertThat(loads.stats().retainedLoads()).isZero();
                assertThat(loads.stats().activeReads()).isZero();
                loads.close(); cache.close();
                assertThat(cache.ownedBytes()).isZero();
            } finally { release.countDown(); }
        }
    }

    @Test void abandonedReadRetainsCapacityUntilProviderCompletesAndCloseWakesWaiters() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        String digest = digest();
        try (var store = store(); var cache = cache();
             var loads = new RegistryDescriptorLoads(held(store, entered, release, calls), cache, 1);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            store.putDescriptorSet(digest, BYTES);
            var canceled = new CancellationException("caller canceled");
            var cancel = new java.util.concurrent.atomic.AtomicBoolean();
            var first = executor.submit(() -> {
                try (var lease = loads.acquire(digest, () -> { if (cancel.get()) throw canceled; })) { return lease.bytes(); }
            });
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                cancel.set(true);
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                        .hasCauseReference(canceled);
                assertThat(loads.awaitIdle(Duration.ZERO)).isFalse();
                assertThatThrownBy(() -> loads.acquire("b".repeat(64), ACTIVE))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("capacity");
                var joined = new CountDownLatch(1);
                var checks = new AtomicInteger();
                var waiter = executor.submit(() -> {
                    try (var lease = loads.acquire(digest, () -> { if (checks.incrementAndGet() >= 2) joined.countDown(); })) {
                        return lease.bytes();
                    }
                });
                assertThat(joined.await(5, TimeUnit.SECONDS)).isTrue();
                loads.close(); cache.close();
                assertThatThrownBy(() -> waiter.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class);
                assertThat(loads.awaitIdle(Duration.ZERO)).isFalse();
                assertThatThrownBy(() -> loads.acquire(digest, ACTIVE)).isInstanceOf(IllegalStateException.class);
                release.countDown();
                assertThat(loads.awaitIdle(Duration.ofSeconds(5))).isTrue();
                assertThat(cache.ownedBytes()).isZero();
                assertThat(calls.get()).isEqualTo(1);
            } finally { release.countDown(); }
        }
    }

    @Test void distinctArtifactsLoadConcurrentlyAndRetainIndependentPins() throws Exception {
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        String digest = digest();
        var other = DescriptorProtos.FileDescriptorSet.parseFrom(BYTES).toBuilder()
                .setFile(0, StringValue.getDescriptor().getFile().toProto().toBuilder().setName("alternate.proto"))
                .build().toByteString();
        String otherDigest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(other.toByteArray()));
        try (var store = store(); var cache = cache();
             var loads = new RegistryDescriptorLoads(held(store, entered, release, calls), cache, 2);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            store.putDescriptorSet(digest, BYTES);
            store.putDescriptorSet(otherDigest, other);
            var first = executor.submit(() -> loads.acquire(digest, ACTIVE));
            var second = executor.submit(() -> loads.acquire(otherDigest, ACTIVE));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                release.countDown();
                try (var one = first.get(5, TimeUnit.SECONDS); var two = second.get(5, TimeUnit.SECONDS)) {
                    assertThat(loads.awaitIdle(Duration.ofSeconds(5))).isTrue();
                    loads.close(); cache.close();
                    assertThat(one.bytes()).isEqualTo(BYTES);
                    assertThat(two.bytes()).isEqualTo(other);
                    assertThat(cache.ownedBytes()).isEqualTo(BYTES.size() + other.size());
                }
                assertThat(cache.ownedBytes()).isZero();
                assertThat(calls.get()).isEqualTo(2);
            } finally { release.countDown(); }
        }
    }

    @Test void realMissingArtifactFailureIsSharedButNotCached() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        String digest = digest();
        try (var store = store(); var cache = cache();
             var loads = new RegistryDescriptorLoads(held(store, entered, release, calls), cache, 2);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> loads.acquire(digest, ACTIVE));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var joined = new CountDownLatch(1);
                var checks = new AtomicInteger();
                var second = executor.submit(() -> loads.acquire(digest,
                        () -> { if (checks.incrementAndGet() >= 2) joined.countDown(); }));
                assertThat(joined.await(5, TimeUnit.SECONDS)).isTrue();
                release.countDown();
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(RegistrySchemaResolver.MissingDescriptor.class);
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(RegistrySchemaResolver.MissingDescriptor.class);
                assertThat(calls.get()).isEqualTo(1);
                assertThat(loads.awaitIdle(Duration.ofSeconds(5))).isTrue();
                store.putDescriptorSet(digest, BYTES);
                try (var lease = loads.acquire(digest, ACTIVE)) { assertThat(lease.bytes()).isEqualTo(BYTES); }
                assertThat(calls.get()).isEqualTo(2);
            } finally { release.countDown(); }
        }
    }
}
