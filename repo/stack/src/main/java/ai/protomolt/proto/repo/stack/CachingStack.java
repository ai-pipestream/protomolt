package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.v2.StackRef;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Keeps small, verified objects in memory in front of another stack. Keys are
 * write-once, so an entry keyed by {@code (stack_id, key, sha256)} stays valid.
 * <ul>
 * <li>Only whole-object reads of at most {@code maxObjectBytes} are cached, and only
 *     after the delegate's stream has verified length and SHA-256 at its end. A read
 *     closed early, or one that fails verification, caches nothing.</li>
 * <li>Larger objects, ranges, heads, deletes and writes go to the delegate. A delete
 *     also drops the entry.</li>
 * <li>Cached bytes are served through the same verifying stream as a stored read.</li>
 * <li>The cache holds at most {@code maxTotalBytes}, evicting least recently used.</li>
 * </ul>
 */
public final class CachingStack implements Stack {
    private record Key(String stackId, String key, String sha256) {
        static Key of(StackRef ref) { return new Key(ref.getStackId(), ref.getKey(), ref.getSha256()); }
    }

    private final Stack delegate;
    private final long maxObjectBytes;
    private final long maxTotalBytes;
    private final LinkedHashMap<Key, byte[]> entries = new LinkedHashMap<>(16, 0.75f, true);
    private long totalBytes;

    public CachingStack(Stack delegate, long maxObjectBytes, long maxTotalBytes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (maxObjectBytes < 0 || maxTotalBytes < maxObjectBytes)
            throw new IllegalArgumentException("Cache limits need 0 <= object limit <= total limit");
        this.maxObjectBytes = maxObjectBytes;
        this.maxTotalBytes = maxTotalBytes;
    }

    @Override public String stackId() { return delegate.stackId(); }

    @Override public StackRef put(InputStream body, long expectedLength) throws IOException {
        return delegate.put(body, expectedLength);
    }

    @Override public StackRef put(byte[] body) throws IOException { return delegate.put(body); }

    @Override public InputStream open(StackRef ref) throws IOException {
        Objects.requireNonNull(ref, "ref");
        var key = Key.of(ref);
        byte[] cached;
        synchronized (this) { cached = entries.get(key); }
        if (cached != null)
            return new VerifyingInputStream(new java.io.ByteArrayInputStream(cached), ref.getLength(), ref.getSha256());
        var stream = delegate.open(ref);
        if (ref.getLength() > maxObjectBytes) return stream;
        return new Filling(stream, key, (int) ref.getLength());
    }

    @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
        Stack.requireRange(ref, offset, length);
        if (offset == 0 && length == ref.getLength()) return open(ref);
        return delegate.openRange(ref, offset, length);
    }

    @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }

    @Override public void delete(StackRef ref) throws IOException {
        synchronized (this) {
            var removed = entries.remove(Key.of(ref));
            if (removed != null) totalBytes -= removed.length;
        }
        delegate.delete(ref);
    }

    @Override public void close() throws IOException {
        synchronized (this) { entries.clear(); totalBytes = 0; }
        delegate.close();
    }

    /** Whether a verified copy of the object is cached; for tests and diagnostics. */
    public synchronized boolean cached(StackRef ref) { return entries.containsKey(Key.of(ref)); }

    private synchronized void remember(Key key, byte[] bytes) {
        if (entries.containsKey(key) || bytes.length > maxObjectBytes) return;
        entries.put(key, bytes);
        totalBytes += bytes.length;
        var oldest = entries.entrySet().iterator();
        while (totalBytes > maxTotalBytes && oldest.hasNext()) {
            Map.Entry<Key, byte[]> evicted = oldest.next();
            totalBytes -= evicted.getValue().length;
            oldest.remove();
        }
    }

    /** Copies what passes; caches it only when the delegate's stream ends cleanly, i.e. verified. */
    private final class Filling extends FilterInputStream {
        private final Key key;
        private final ByteArrayOutputStream copy;
        private boolean done;

        Filling(InputStream verified, Key key, int length) {
            super(verified);
            this.key = key;
            this.copy = new ByteArrayOutputStream(length);
        }

        @Override public int read() throws IOException {
            int value = super.read();
            if (value < 0) finish(); else copy.write(value);
            return value;
        }

        @Override public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = super.read(buffer, offset, length);
            if (read < 0) finish(); else copy.write(buffer, offset, read);
            return read;
        }

        @Override public long skip(long n) throws IOException {
            byte[] buffer = new byte[(int) Math.min(8192, Math.max(n, 1))];
            long skipped = 0;
            while (skipped < n) {
                int read = read(buffer, 0, (int) Math.min(buffer.length, n - skipped));
                if (read < 0) break;
                skipped += read;
            }
            return skipped;
        }

        @Override public boolean markSupported() { return false; }

        private void finish() {
            if (done) return;
            done = true;
            remember(key, copy.toByteArray());
        }
    }
}
