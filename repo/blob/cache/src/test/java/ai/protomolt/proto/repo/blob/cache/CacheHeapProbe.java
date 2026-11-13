package ai.protomolt.proto.repo.blob.cache;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Real S3/Valkey qualification fork: the object exceeds the entire Java heap. */
public final class CacheHeapProbe {
    private CacheHeapProbe() {}
    public static void main(String[] args) throws Exception {
        long size = 192L * 1024 * 1024;
        var s3Options = Map.ofEntries(Map.entry("endpoint", args[0]), Map.entry("region", args[1]),
                Map.entry("access-key", args[2]), Map.entry("secret-key", args[3]),
                Map.entry("path-style", "true"), Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"), Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"), Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000"));
        var redisOptions = Map.of("uri", args[4], "ttl-seconds", "0", "max-object-bytes", "0",
                "key-prefix", args[5], "write-policy", "replace");
        var providers = BlobStores.discover();
        try (var backing = providers.open("s3", s3Options); var front = providers.open("valkey", redisOptions)) {
            var calls = new AtomicInteger();
            BlobStore counted = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                    new Class<?>[]{BlobStore.class}, (proxy, method, values) -> {
                        if (method.getName().equals("openSnapshot") || method.getName().equals("openStream")
                                || method.getName().equals("get") || method.getName().equals("getBounded")) calls.incrementAndGet();
                        try { return method.invoke(backing.store(), values); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var store = new CachingBlobStore(counted, front.store(), 60, size);
            var spec = new BlobStore.PutSpec(args[6], args[5], "application/octet-stream", Map.of(), null);
            backing.store().put(spec, new PatternStream(size), size);
            String digest = digest(size);
            for (int attempt = 0; attempt < 2; attempt++) {
                try (var input = store.openVerifiedStream(spec.bucket(), spec.key(), size, digest)) {
                    verify(input, size, digest);
                }
                if (calls.get() != 1) throw new AssertionError("Expected exactly one authoritative body read, got " + calls);
            }
            try (var hit = store.openStream(spec.bucket(), spec.key(), 0, -1)) { verify(hit, size, digest); }
            if (calls.get() != 1) throw new AssertionError("Unverified whole read did not hit cache");
            // A cancelled fill must not create a cache entry or retain a spool.
            front.store().delete(spec.bucket(), spec.key());
            try (var cancelled = store.openVerifiedStream(spec.bucket(), spec.key(), size, digest)) {
                if (cancelled.read() != 29) throw new AssertionError("cancel payload");
            }
            try { front.store().headObject(spec.bucket(), spec.key()); throw new AssertionError("cancelled fill published"); }
            catch (BlobStore.BlobNotFoundException expected) { /* no incomplete entry */ }
            store.delete(spec.bucket(), spec.key());
        }
        try (var files = Files.list(Path.of(System.getProperty("java.io.tmpdir")))) {
            if (files.anyMatch(p -> p.getFileName().toString().startsWith("protomolt-cache-")))
                throw new AssertionError("Leaked cache spool");
        }
        System.out.println("CACHE_BOUNDED_HEAP_OK");
    }

    private static String digest(long size) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        byte[] bytes = new byte[8192];
        Arrays.fill(bytes, (byte) 29);
        for (long left = size; left > 0;) {
            int count = (int) Math.min(left, bytes.length);
            digest.update(bytes, 0, count); left -= count;
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void verify(InputStream input, long size, String expected) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        byte[] bytes = new byte[8192]; long received = 0;
        for (int count; (count = input.read(bytes)) != -1;) {
            digest.update(bytes, 0, count); received += count;
        }
        if (received != size || !HexFormat.of().formatHex(digest.digest()).equals(expected)) throw new AssertionError("body mismatch");
    }

    private static final class PatternStream extends InputStream {
        private long remaining;
        PatternStream(long size) { remaining = size; }
        @Override public int read() { return remaining-- > 0 ? 29 : -1; }
        @Override public int read(byte[] bytes, int offset, int count) {
            if (count == 0) return 0;
            if (remaining <= 0) return -1;
            int n = (int) Math.min(remaining, count);
            Arrays.fill(bytes, offset, offset + n, (byte) 29);
            remaining -= n; return n;
        }
    }
}
