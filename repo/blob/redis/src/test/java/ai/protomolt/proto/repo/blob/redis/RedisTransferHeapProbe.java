package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;

/** Forked by RedisStreamingIT with an object larger than the entire Java heap. */
public final class RedisTransferHeapProbe {
    private RedisTransferHeapProbe() {}

    public static void main(String[] args) throws Exception {
        int size = 128 * 1024 * 1024;
        try (var store = new RedisBlobStore(new RedisBlobStoreConfig(args[0], 0, 0, args[1]))) {
            var source = new BlobStore.PutSpec("heap", "source", "application/octet-stream", Map.of(), null);
            var result = store.put(source, new PatternStream(size), size);
            if (store.objectLength("heap", "source") != size) throw new AssertionError("length");
            var digest = MessageDigest.getInstance("SHA-256");
            byte[] window = new byte[8192];
            long read = 0;
            try (var stream = store.openStream("heap", "source", 0, -1)) {
                for (int count; (count = stream.read(window)) != -1;) {
                    for (int i = 0; i < count; i++) if (window[i] != 37) throw new AssertionError("body");
                    digest.update(window, 0, count);
                    read += count;
                }
            }
            if (read != size || !result.eTag().equals("\"" + HexFormat.of().formatHex(digest.digest()) + "\""))
                throw new AssertionError("full digest");
            try (var stream = store.openStream("heap", "source", size - 19, 7)) {
                if (!Arrays.equals(stream.readAllBytes(), new byte[]{37,37,37,37,37,37,37}))
                    throw new AssertionError("range");
            }
            // The promotion boundary is stream -> put, with both transfers live together.
            try (var stream = store.openStream("heap", "source", 0, -1)) {
                var target = new BlobStore.PutSpec("heap", "target", "application/octet-stream", Map.of(),
                        result.eTag().substring(1, result.eTag().length() - 1));
                if (!store.put(target, stream, size).eTag().equals(result.eTag())) throw new AssertionError("promotion");
            }
            try (var stream = store.openStream("heap", "target", 0, -1)) {
                if (stream.read() != 37) throw new AssertionError("cancel read");
            }
            store.delete("heap", "source");
            store.delete("heap", "target");
        }
        System.out.println("REDIS_BOUNDED_HEAP_TRANSFER_OK");
    }

    private static final class PatternStream extends InputStream {
        private long remaining;
        PatternStream(long length) { remaining = length; }
        @Override public int read() { return remaining-- > 0 ? 37 : -1; }
        @Override public int read(byte[] bytes, int offset, int length) {
            if (length == 0) return 0;
            if (remaining <= 0) return -1;
            int count = (int) Math.min(remaining, length);
            Arrays.fill(bytes, offset, offset + count, (byte) 37);
            remaining -= count;
            return count;
        }
    }
}
