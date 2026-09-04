package ai.protomolt.proto.mesh.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Drives directory expiry and resumable watch recovery without blocking placement reads. */
public final class DirectoryMaintenance implements AutoCloseable {

    private final ProcessorDirectoryClient client;
    private final Runnable sweep;
    private final ScheduledExecutorService executor;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public DirectoryMaintenance(
            ProcessorDirectoryClient client, Runnable sweep, Duration interval) {
        this.client = Objects.requireNonNull(client, "client");
        this.sweep = Objects.requireNonNull(sweep, "sweep");
        Objects.requireNonNull(interval, "interval");
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("directory maintenance interval must be positive");
        }
        executor = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon(true)
                        .name("protomolt-directory-maintenance").unstarted(task));
        long millis = Math.max(1, interval.toMillis());
        executor.scheduleWithFixedDelay(this::maintain, millis, millis,
                TimeUnit.MILLISECONDS);
    }

    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    public Optional<Duration> watchLag() {
        return client.watchLag();
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private void maintain() {
        try {
            sweep.run();
            if (client.failure().isPresent()) {
                client.reconnect(false);
            }
            failure.set(null);
        } catch (RuntimeException e) {
            failure.set(e);
        }
    }
}
