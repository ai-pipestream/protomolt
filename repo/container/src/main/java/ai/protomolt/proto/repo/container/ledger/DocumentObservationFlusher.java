package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** One operation's bounded observation queue. No byte payloads or provider calls. */
final class DocumentObservationFlusher {
    private static final int LIMIT = DocumentSelectedAttemptLedger.MAX_OBSERVATIONS;
    private record Completed(DocumentSelectedAttemptLedger.Selected selection,
            DocumentSelectedAttemptLedger.Observation observation, long arrived) {}
    private static final class Pending {
        final long arrived;
        final List<DocumentSelectedAttemptLedger.Observation> rows = new ArrayList<>();
        Pending(long arrived) { this.arrived = arrived; }
    }

    private final DocumentSelectedAttemptLedger ledger;
    private final RepositoryOperationLedger.Owner owner;
    private final java.util.Set<DocumentSelectedAttemptLedger.Selected> allowed;
    private final long maxAge;
    private final Runnable check;
    private final Consumer<Throwable> failed;
    private final ArrayBlockingQueue<Completed> queue = new ArrayBlockingQueue<>(LIMIT);
    private volatile boolean finished;

    DocumentObservationFlusher(DocumentSelectedAttemptLedger ledger, RepositoryOperationLedger.Owner owner,
            List<DocumentSelectedAttemptLedger.Selected> selections, Duration maxAge, Runnable check, Consumer<Throwable> failed) {
        this.ledger = ledger;
        this.owner = owner;
        this.allowed = java.util.Set.copyOf(selections);
        this.maxAge = maxAge.toNanos();
        this.check = check;
        this.failed = failed;
    }

    void add(DocumentSelectedAttemptLedger.Selected selection, DocumentPartTransfer.Verified verified) {
        if (!allowed.contains(selection)) throw new IllegalArgumentException("Observation belongs to an unselected attempt");
        var part = verified.planned();
        var completed = new Completed(selection, new DocumentSelectedAttemptLedger.Observation(part.objectKey(), part.size(),
                part.sha256(), part.contentType(), verified.version(), verified.etag()), System.nanoTime());
        try {
            while (true) {
                check.run();
                if (finished) throw new IllegalStateException("Observation producers already finished");
                if (queue.offer(completed, 50, TimeUnit.MILLISECONDS)) return;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Observation producer interrupted");
        }
    }

    /** Called only once all producers have drained; failures are carried separately. */
    void finish() { finished = true; }

    void run() {
        var pending = new HashMap<DocumentSelectedAttemptLedger.Selected, Pending>();
        int count = 0;
        try {
            while (true) {
                check.run();
                boolean tail = finished && queue.isEmpty();
                if (tail && pending.isEmpty()) return;
                var oldest = oldest(pending);
                long wait = TimeUnit.MILLISECONDS.toNanos(50);
                if (oldest != null) {
                    long remaining = maxAge - (System.nanoTime() - oldest.getValue().arrived);
                    // Aggregate pressure must flush even when no individual attempt has 256 rows.
                    if (tail || count >= LIMIT || remaining <= 0) {
                        check.run();
                        ledger.verifyBatch(owner, oldest.getKey(), oldest.getValue().rows);
                        check.run();
                        count -= oldest.getValue().rows.size();
                        pending.remove(oldest.getKey());
                        continue;
                    }
                    wait = Math.min(wait, remaining);
                }
                var completed = queue.poll(wait, TimeUnit.NANOSECONDS);
                if (completed != null) {
                    pending.computeIfAbsent(completed.selection(), key -> new Pending(completed.arrived()))
                            .rows.add(completed.observation());
                    count++;
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            var cancellation = new CancellationException("Observation flusher interrupted");
            failed.accept(cancellation);
            throw cancellation;
        } catch (RuntimeException | Error cause) {
            failed.accept(cause);
            throw cause;
        } finally {
            queue.clear();
            pending.clear();
        }
    }

    private static Map.Entry<DocumentSelectedAttemptLedger.Selected, Pending> oldest(
            Map<DocumentSelectedAttemptLedger.Selected, Pending> pending) {
        Map.Entry<DocumentSelectedAttemptLedger.Selected, Pending> result = null;
        long now = System.nanoTime();
        for (var entry : pending.entrySet()) {
            if (result == null || now - entry.getValue().arrived > now - result.getValue().arrived) result = entry;
        }
        return result;
    }
}
