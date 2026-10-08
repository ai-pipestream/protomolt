package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.cache.CachingBlobStore;
import ai.protomolt.proto.repo.blob.spi.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Actual backing bytes and Redis entries; faults decorate actual cache operations. */
@Testcontainers
class CachedReclamationIT {
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static OpenedBlobStore backing;
    static OpenedBlobStore cache;
    static CachingBlobStore composed;
    static final String BUCKET = "cached-reclamation";

    @BeforeAll static void boot() {
        var providers = BlobStores.discover();
        backing = providers.open("s3", Map.ofEntries(
                Map.entry("endpoint", S3.getEndpoint().toString()),
                Map.entry("region", S3.getRegion()),
                Map.entry("access-key", S3.getAccessKey()),
                Map.entry("secret-key", S3.getSecretKey()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")));
        cache = providers.open("redis", Map.of("uri", "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                "ttl-seconds", "0", "max-object-bytes", "0", "key-prefix", "reclamation:", "write-policy", "replace"));
        backing.ensureNamespace(BUCKET);
        cache.ensureNamespace(BUCKET);
        composed = new CachingBlobStore(backing.store(), cache.store(), 0, 1024);
    }
    @AfterAll static void close() throws Exception {
        try { if (cache != null) cache.close(); }
        finally { if (backing != null) backing.close(); }
    }

    @Test void cleanupRemovesBackingAndCacheAndReconcilesLateCacheFill() {
        String key = UUID.randomUUID().toString();
        var spec = new BlobStore.PutSpec(BUCKET, key, "application/octet-stream", null, null);
        composed.put(spec, new byte[] {7});
        assertThat(cache.store().get(BUCKET, key).data()).containsExactly((byte) 7);
        var reclaimer = composed.reclaimer(backing.reclaimer());
        assertThat(reclaimer.reclaim(BUCKET, key)).isTrue();
        missing(backing.store(), key);
        missing(cache.store(), key);
        cache.store().put(spec, new byte[] {7}); // a read-through fill completes late
        assertThat(reclaimer.reclaim(BUCKET, key)).isTrue();
        missing(cache.store(), key);
        missing(composed, key);
    }

    @Test void lostCacheDeleteAcknowledgementPropagatesAndRetryConfirmsAbsence() {
        String key = UUID.randomUUID().toString();
        composed.put(new BlobStore.PutSpec(BUCKET, key, "application/octet-stream", null, null), new byte[] {9});
        var injected = new IllegalStateException("injected cache acknowledgement loss");
        var fired = new AtomicBoolean();
        BlobStore uncertain = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    Object result;
                    try { result = method.invoke(cache.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    if (method.getName().equals("delete") && fired.compareAndSet(false, true)) throw injected;
                    return result;
                });
        var reclaimer = new CachingBlobStore(backing.store(), uncertain, 0, 1024).reclaimer(backing.reclaimer());
        assertThatThrownBy(() -> reclaimer.reclaim(BUCKET, key)).isSameAs(injected);
        assertThat(fired).isTrue();
        missing(backing.store(), key);
        assertThat(reclaimer.reclaim(BUCKET, key)).isTrue();
        missing(cache.store(), key);
    }

    private static void missing(BlobStore store, String key) {
        assertThatThrownBy(() -> store.get(BUCKET, key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test void cacheFillBetweenDeleteAndAbsenceCheckRequiresAnotherPass() {
        String key = UUID.randomUUID().toString();
        var spec = new BlobStore.PutSpec(BUCKET, key, "application/octet-stream", null, null);
        composed.put(spec, new byte[] {8});
        var filled = new AtomicBoolean();
        BlobStore racing = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    Object result;
                    try { result = method.invoke(cache.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    if (method.getName().equals("delete") && filled.compareAndSet(false, true))
                        cache.store().put(spec, new byte[] {8});
                    return result;
                });
        var reclaimer = new CachingBlobStore(backing.store(), racing, 0, 1024).reclaimer(backing.reclaimer());
        assertThat(reclaimer.reclaim(BUCKET, key)).isFalse();
        assertThat(filled).isTrue();
        missing(backing.store(), key);
        assertThat(cache.store().get(BUCKET, key).data()).containsExactly((byte) 8);
        assertThat(reclaimer.reclaim(BUCKET, key)).isTrue();
        missing(cache.store(), key);
    }
}
