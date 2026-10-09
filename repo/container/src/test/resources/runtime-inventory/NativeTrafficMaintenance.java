package ai.protomolt.proto.repo.container.ledger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Explicit host maintenance; errors remain observable to workload and shutdown. */
final class NativeTrafficMaintenance implements AutoCloseable {
    private final AtomicBoolean stop = new AtomicBoolean();
    private final FutureTask<Void> task;
    private final Thread thread;
    NativeTrafficMaintenance(DocumentPublicationRuntime runtime) {
        task = new FutureTask<>(() -> {
            while (!stop.get()) {
                runtime.tick();
                Thread.sleep(25);
            }
            return null;
        });
        thread = Thread.ofVirtual().name("native-traffic-maintenance").start(task);
    }
    void check() throws Exception {
        if (task.isDone()) {
            task.get();
            throw new IllegalStateException("Maintenance stopped during traffic");
        }
    }
    @Override public void close() throws Exception {
        stop.set(true);
        Exception problem = null;
        boolean interrupted = false;
        try { task.get(15, TimeUnit.SECONDS); }
        catch (Exception failure) { problem = failure; interrupted = failure instanceof InterruptedException; }
        if (!task.isDone()) thread.interrupt();
        try { thread.join(5000); }
        catch (InterruptedException failure) {
            interrupted = true;
            if (problem == null) problem = failure; else problem.addSuppressed(failure);
        }
        if (thread.isAlive()) {
            // This is a disposable child process. Fail it before Java resource
            // unwinding can close the database/provider under a live SQL task.
            System.err.println("Native maintenance did not stop; refusing unsafe resource teardown");
            if (problem != null) problem.printStackTrace(System.err);
            Runtime.getRuntime().halt(73);
        }
        if (interrupted) Thread.currentThread().interrupt();
        if (problem != null) throw problem;
    }
}
