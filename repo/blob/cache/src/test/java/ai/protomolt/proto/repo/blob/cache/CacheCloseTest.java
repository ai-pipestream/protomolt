package ai.protomolt.proto.repo.blob.cache;

import ai.protomolt.proto.repo.blob.redis.RedisBlobStore;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreConfig;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheCloseTest {
    // Real adapters allocate pools, but this lifecycle test performs no network I/O.
    // The wrapper delegates everything and injects a failure after actual cleanup.
    private static BlobStore tracked(AtomicInteger closes, RuntimeException failure) {
        RedisBlobStore real = new RedisBlobStore(RedisBlobStoreConfig.LOCAL);
        return (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class, AutoCloseable.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        closes.incrementAndGet();
                        real.close();
                        if (failure != null) throw failure;
                        return null;
                    }
                    try { return method.invoke(real, args); }
                    catch (InvocationTargetException e) { throw e.getCause(); }
                });
    }

    @Test
    void closesCacheEvenWhenBackingCleanupFailsAndPreservesBothCauses() {
        var backingCloses = new AtomicInteger();
        var cacheCloses = new AtomicInteger();
        var first = new IllegalStateException("backing close");
        var second = new IllegalStateException("cache close");
        var store = new CachingBlobStore(tracked(backingCloses, first),
                tracked(cacheCloses, second), 0, 1024);
        assertThatThrownBy(store::close).isSameAs(first).hasSuppressedException(second);
        assertThat(backingCloses.get()).isEqualTo(1);
        assertThat(cacheCloses.get()).isEqualTo(1);
    }

    @Test
    void closesAliasedResourceOnlyOnce() throws Exception {
        var closes = new AtomicInteger();
        BlobStore resource = tracked(closes, null);
        new CachingBlobStore(resource, resource, 0, 1024).close();
        assertThat(closes.get()).isEqualTo(1);
    }
}
