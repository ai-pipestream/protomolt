package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.CodedOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Bounds response construction; callers own returned protobufs after the call ends. */
public final class ArchiveGetAdmission implements AutoCloseable {
    public record Limits(int maxObjectBytes, int maxResponseBytes, int maxRenditions) {
        public Limits {
            if (maxObjectBytes < 1 || maxResponseBytes < maxObjectBytes
                    || maxResponseBytes > 256 * 1024 * 1024 || maxRenditions < 1 || maxRenditions > 256)
                throw new IllegalArgumentException("Invalid archive read limits");
        }
    }

    private final Limits limits;
    private final PayloadBudget budget;
    private final int maxActive;
    private int active;
    private boolean closed;

    public ArchiveGetAdmission(Limits limits, PayloadBudget budget, int maxActive) {
        this.limits = Objects.requireNonNull(limits);
        this.budget = Objects.requireNonNull(budget);
        if (maxActive < 1 || maxActive > 1024) throw new IllegalArgumentException("Invalid archive read concurrency");
        this.maxActive = maxActive;
    }

    Scope admit(GetEntryResponse envelope, List<RenditionManifestEntry> selected) {
        if (Thread.currentThread().isInterrupted())
            throw failure(RepositoryException.Code.CANCELLED, "Archive read interrupted before admission");
        if (envelope.getRenditionsCount() != 0) throw new IllegalArgumentException("Expected payload-free envelope");
        if (selected.size() > limits.maxRenditions()) throw exhausted();
        long responseBytes = envelope.getSerializedSize();
        long payload = 0;
        for (var item : selected) {
            long size = item.getSizeBytes();
            if (size < 0) throw failure(RepositoryException.Code.DATA_LOSS, "Negative published archive size");
            if (size > limits.maxObjectBytes()) throw exhausted();
            payload += size;
            long content = CodedOutputStream.computeMessageSize(RenditionContent.RENDITION_FIELD_NUMBER, item.getRendition());
            if (size != 0) content += CodedOutputStream.computeTagSize(RenditionContent.DATA_FIELD_NUMBER)
                    + CodedOutputStream.computeUInt32SizeNoTag((int) size) + size;
            if (content > limits.maxResponseBytes()) throw exhausted();
            responseBytes += CodedOutputStream.computeTagSize(GetEntryResponse.RENDITIONS_FIELD_NUMBER)
                    + CodedOutputStream.computeUInt32SizeNoTag((int) content) + content;
            if (responseBytes > limits.maxResponseBytes()) throw exhausted();
        }
        if (responseBytes > limits.maxResponseBytes()) throw exhausted();
        synchronized (this) {
            if (closed) throw failure(RepositoryException.Code.UNAVAILABLE, "Archive read admission is closed");
            if (active >= maxActive) throw exhausted();
            final PayloadBudget.Lease lease;
            // Complete response plus provider-array and transient-copy allowances.
            // This is admission accounting, not measured SDK/JVM heap.
            try { lease = budget.reserve(responseBytes + 2 * payload); }
            catch (PayloadBudget.CapacityExceededException capacity) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Archive read payload capacity exhausted", capacity);
            }
            active++;
            return new Scope(lease, (int) responseBytes);
        }
    }

    final class Scope implements AutoCloseable {
        private final PayloadBudget.Lease lease;
        private final int expectedBytes;
        private boolean released;
        private Scope(PayloadBudget.Lease lease, int expectedBytes) {
            this.lease = lease; this.expectedBytes = expectedBytes;
        }
        void verify(GetEntryResponse result) {
            if (result.getSerializedSize() != expectedBytes)
                throw failure(RepositoryException.Code.DATA_LOSS, "Archive response disagrees with admitted size");
        }
        @Override public void close() {
            synchronized (ArchiveGetAdmission.this) {
                if (released) return;
                released = true; lease.close(); active--;
                ArchiveGetAdmission.this.notifyAll();
            }
        }
    }

    @Override public synchronized void close() { closed = true; }

    public synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (!closed) throw new IllegalStateException("Close archive read admission before awaiting idle");
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative archive read drain timeout");
        long remaining = timeout.toNanos(), started = System.nanoTime();
        while (active != 0) {
            if (remaining <= 0) return false;
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - started);
        }
        return true;
    }

    private static RepositoryException exhausted() {
        return failure(RepositoryException.Code.RESOURCE_EXHAUSTED, "Archive read capacity exhausted");
    }
    private static RepositoryException failure(RepositoryException.Code code, String message) {
        return new RepositoryException(code, message);
    }
}
