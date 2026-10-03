package ai.protomolt.proto.repo.service;

import java.util.ArrayDeque;
import java.util.Deque;

/** Startup acquisitions are released in reverse order, including on partial startup. */
final class OwnedResources implements AutoCloseable {
    private final Deque<AutoCloseable> resources = new ArrayDeque<>();
    private boolean closed;

    synchronized <T extends AutoCloseable> T add(T resource) {
        if (closed) throw new IllegalStateException("Resource scope is closed");
        resources.push(resource);
        return resource;
    }

    @Override public synchronized void close() {
        closed = true;
        Throwable failure = null;
        while (!resources.isEmpty()) {
            try { resources.pop().close(); }
            catch (Throwable next) {
                if (next instanceof InterruptedException) Thread.currentThread().interrupt();
                if (failure == null) failure = next;
                else if (failure != next) failure.addSuppressed(next);
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure != null) throw new IllegalStateException("Repository resource cleanup failed", failure);
    }
}
