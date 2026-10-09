package ai.protomolt.proto.repo.schema.registry;

import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.admission.DocumentSchemaArtifactCache;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Bounded host-owned artifact reads. No discovery grants or negative results are cached. */
final class RegistryDescriptorLoads implements AutoCloseable {
    private static final class Flight {
        int callers;
        boolean done;
        DocumentSchemaArtifactCache.Lease pin;
        Throwable failure;
    }

    private final SchemaRegistryStore store;
    private final DocumentSchemaArtifactCache cache;
    private final int limit;
    private final HashMap<String, Flight> flights = new HashMap<>();
    private final ExecutorService workers = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("repository-schema-load-", 0).factory());
    private boolean closed;
    private long registryReads;
    private long joinedLoads;

    record Stats(long registryReads, long joinedLoads, int retainedLoads, int activeReads) {}

    synchronized Stats stats() {
        return new Stats(registryReads, joinedLoads, flights.size(),
                (int) flights.values().stream().filter(flight -> !flight.done).count());
    }

    RegistryDescriptorLoads(SchemaRegistryStore store, DocumentSchemaArtifactCache cache, int limit) {
        this.store = store;
        this.cache = cache;
        this.limit = limit;
    }

    DocumentSchemaArtifactCache.Lease acquire(String digest, Runnable control) {
        control.run();
        Flight flight;
        synchronized (this) {
            requireOpen();
            flight = flights.get(digest);
            if (flight == null) {
                if (flights.size() >= limit) throw new IllegalStateException("Registry load capacity exhausted");
                flight = new Flight();
                flights.put(digest, flight);
                Flight submitted = flight;
                try { workers.execute(() -> load(digest, submitted)); }
                catch (RuntimeException | Error failure) { flights.remove(digest); notifyAll(); throw failure; }
            } else joinedLoads++;
            flight.callers++;
        }
        try {
            while (true) {
                control.run();
                synchronized (this) {
                    requireOpen();
                    if (flight.done) break;
                    try { wait(25); }
                    catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        var canceled = new CancellationException("Interrupted waiting for schema artifact");
                        canceled.initCause(interrupted);
                        throw canceled;
                    }
                }
            }
            control.run();
            if (flight.failure instanceof RuntimeException failure) throw failure;
            if (flight.failure instanceof Error failure) throw failure;
            // The flight pin prevents eviction until this caller has its own pin.
            return cache.acquire(digest, control).orElseThrow(
                    () -> new IllegalStateException("Completed schema load lost its retained artifact"));
        } finally {
            synchronized (this) { flight.callers--; release(digest, flight); }
        }
    }

    private void load(String digest, Flight flight) {
        DocumentSchemaArtifactCache.Lease pin = null;
        Throwable failure = null;
        try {
            // A preceding flight may have completed between the caller's cache miss and join.
            pin = cache.acquire(digest, this::checkOpen).orElse(null);
            if (pin == null) {
                synchronized (this) { registryReads++; }
                var bytes = store.descriptorSet(digest).orElseThrow(RegistrySchemaResolver.MissingDescriptor::new);
                checkOpen();
                if (bytes.isEmpty() || bytes.size() > 16 * 1024 * 1024)
                    throw new IllegalArgumentException("Registry descriptor exceeds admission bounds");
                pin = cache.put(digest, bytes, this::checkOpen);
            }
        } catch (RuntimeException | Error caught) { failure = caught; }
        finally {
            synchronized (this) {
                flight.pin = pin;
                flight.failure = failure;
                flight.done = true;
                release(digest, flight);
                notifyAll();
            }
        }
    }

    private void release(String digest, Flight flight) {
        if (flight.done && flight.callers == 0) {
            flights.remove(digest, flight);
            if (flight.pin != null) flight.pin.close();
            notifyAll();
        }
    }

    private synchronized void checkOpen() { requireOpen(); }
    private void requireOpen() { if (closed) throw new IllegalStateException("Registry loads closed"); }

    /** Reject/wake callers without interrupting a borrowed provider's in-flight operation. */
    @Override public synchronized void close() {
        closed = true;
        workers.shutdown();
        notifyAll();
    }

    /** Includes reads abandoned by every caller; the borrowed store cannot close before they drain. */
    synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative drain timeout");
        long remaining = timeout.toNanos();
        long started = System.nanoTime();
        while (!flights.isEmpty()) {
            if (remaining <= 0) return false;
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - started);
        }
        return true;
    }
}
