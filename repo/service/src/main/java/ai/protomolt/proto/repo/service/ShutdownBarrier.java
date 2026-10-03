package ai.protomolt.proto.repo.service;

import java.util.List;

/** Try every stop, but release shared resources only after all stops succeed. */
final class ShutdownBarrier {
    private ShutdownBarrier() {}

    static void releaseAfter(List<? extends AutoCloseable> users, Runnable release) {
        Throwable failure = null;
        for (var user : users) {
            try { user.close(); }
            catch (Throwable next) {
                if (next instanceof InterruptedException) Thread.currentThread().interrupt();
                if (failure == null) failure = next;
                else if (failure != next) failure.addSuppressed(next);
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure != null) throw new IllegalStateException("Repository shutdown failed; shared resources retained", failure);
        release.run();
    }
}
