package ai.protomolt.proto.metric.spi;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Thread-safe compute-once cache that evicts its oldest inserted entries once it holds more
 * than {@code maxEntries}.
 *
 * <p>Eviction is first-in first-out by insertion, not by access. Under concurrent inserts
 * the size can briefly exceed the bound before the inserting thread drains the oldest
 * entries, so the bound is a steady-state limit rather than a hard ceiling. A loader that
 * throws caches nothing.
 *
 * @param <K> key type; keys must be immutable
 * @param <V> value type
 */
final class BoundedFifoCache<K, V> {
    private final int maxEntries;
    private final ConcurrentHashMap<K, V> entries = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<K> insertionOrder = new ConcurrentLinkedQueue<>();

    /**
     * @param maxEntries positive steady-state entry bound
     */
    BoundedFifoCache(int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
    }

    /**
     * Returns the cached value for {@code key}, computing it with {@code loader} at most once
     * per resident key.
     *
     * @param key cache key
     * @param loader computes the value of an absent key
     * @return the cached or newly computed value
     */
    V computeIfAbsent(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader, "loader");
        V cached = entries.get(key);
        if (cached != null) {
            return cached;
        }
        AtomicBoolean created = new AtomicBoolean();
        V resolved = entries.computeIfAbsent(key, k -> {
            created.set(true);
            return loader.apply(k);
        });
        if (created.get()) {
            insertionOrder.offer(key);
            while (entries.size() > maxEntries) {
                K oldest = insertionOrder.poll();
                if (oldest == null) {
                    break;
                }
                entries.remove(oldest);
            }
        }
        return resolved;
    }

    /**
     * @param key cache key
     * @return the resident value, or {@code null} when {@code key} is not cached
     */
    V getIfPresent(K key) {
        return entries.get(key);
    }

    /** @return the resident entry count */
    int size() {
        return entries.size();
    }

    /** Drops every entry. */
    void clear() {
        entries.clear();
        insertionOrder.clear();
    }
}
