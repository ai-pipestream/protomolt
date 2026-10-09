package ai.protomolt.proto.repo.history.grpc;

import ai.protomolt.proto.repo.admission.DocumentHistoricalResponseMaterialization;
import ai.protomolt.proto.repo.admission.DocumentHistoricalResponseVerifier;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentHistoryMaterializationServiceGrpc;
import ai.protomolt.proto.repo.v1.ReadHistoricalOccurrenceRequest;
import ai.protomolt.proto.repo.v1.ReadHistoricalOccurrenceResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One authorized RPC per read, with verified retained-schema decoding. The host supplies
 * an authenticated stub and owns its channel. This is not the repository SPI's live
 * authorization handle: an already delivered Java result cannot be remotely revoked.
 */
public final class HistoricalOccurrenceClient {
    private final DocumentHistoryMaterializationServiceGrpc.DocumentHistoryMaterializationServiceFutureStub stub;
    private final PayloadBudget budget;
    private final DocumentHistoricalResponseMaterialization.Limits limits;
    private final Semaphore calls;
    private final long timeoutNanos;

    /**
     * Shared budget covers an 8 MiB inbound reservation plus retained response/value and
     * verifier scratch during handoff. It is not total JVM heap. Open results retain call
     * slots to bound concurrent decoded objects; the host must close every result.
     */
    public HistoricalOccurrenceClient(
            DocumentHistoryMaterializationServiceGrpc.DocumentHistoryMaterializationServiceFutureStub stub,
            PayloadBudget budget, DocumentHistoricalResponseMaterialization.Limits limits,
            Duration timeout, int maxOpenCalls) {
        Objects.requireNonNull(stub);
        var suppliedLimit = stub.getCallOptions().getMaxInboundMessageSize();
        this.stub = stub.withMaxInboundMessageSize(suppliedLimit == null ? DocumentHistoricalResponseVerifier.MAX_BYTES
                : Math.min(suppliedLimit, DocumentHistoricalResponseVerifier.MAX_BYTES));
        this.budget = Objects.requireNonNull(budget); this.limits = Objects.requireNonNull(limits);
        Objects.requireNonNull(timeout);
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofDays(1)) > 0 || maxOpenCalls < 1)
            throw new IllegalArgumentException("Positive timeout up to one day and positive call bound required");
        timeoutNanos = timeout.toNanos(); calls = new Semaphore(maxOpenCalls);
    }

    public Result read(ReadHistoricalOccurrenceRequest request, RepositoryReadControl control) {
        Objects.requireNonNull(request); Objects.requireNonNull(control).check();
        if (!calls.tryAcquire()) throw Status.RESOURCE_EXHAUSTED.withDescription("Historical client call capacity exhausted").asRuntimeException();
        boolean transferred = false;
        try (var incoming = reserve(DocumentHistoricalResponseVerifier.MAX_BYTES)) {
            long nanos = Math.min(timeoutNanos, control.remainingNanos());
            var existing = stub.getCallOptions().getDeadline();
            if (existing != null) nanos = Math.min(nanos, existing.timeRemaining(TimeUnit.NANOSECONDS));
            if (nanos <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
            var future = stub.withDeadlineAfter(nanos, TimeUnit.NANOSECONDS).readHistoricalOccurrence(request);
            try {
                while (true) {
                    control.check();
                    try {
                        final ReadHistoricalOccurrenceResponse response;
                        try {
                            response = future.get(Math.min(TimeUnit.MILLISECONDS.toNanos(25), Math.max(1, control.remainingNanos())),
                                    TimeUnit.NANOSECONDS);
                        } catch (java.util.concurrent.CancellationException cancelled) {
                            throw Status.CANCELLED.withDescription("Historical RPC cancelled").withCause(cancelled).asRuntimeException();
                        }
                        control.check();
                        var decoded = decode(request, response, control);
                        var result = new Result(decoded);
                        transferred = true;
                        return result;
                    } catch (TimeoutException waiting) {
                        // A bounded wait lets local cancellation stop an otherwise live RPC.
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw Status.CANCELLED.withDescription("Historical client interrupted").withCause(interrupted).asRuntimeException();
            } catch (ExecutionException failed) {
                throw Status.fromThrowable(failed.getCause()).withCause(failed.getCause()).asRuntimeException();
            } finally { future.cancel(true); }
        } finally { if (!transferred) calls.release(); }
    }

    private DocumentHistoricalResponseMaterialization.Result decode(ReadHistoricalOccurrenceRequest request,
            ReadHistoricalOccurrenceResponse response, RepositoryReadControl control) {
        try {
            return DocumentHistoricalResponseMaterialization.read(request, response, limits,
                    bytes -> { var lease = reserve(bytes); return lease::close; }, () -> {
                        try { control.check(); }
                        catch (RuntimeException failure) { throw new ControlFailure(failure); }
                    });
        } catch (ControlFailure failure) {
            throw failure.original;
        } catch (DocumentHistoricalResponseMaterialization.ResourceLimit full) {
            throw Status.RESOURCE_EXHAUSTED.withDescription("Historical decode limit exceeded").withCause(full).asRuntimeException();
        } catch (InvalidProtocolBufferException | IllegalArgumentException invalid) {
            throw Status.DATA_LOSS.withDescription("Invalid historical response").withCause(invalid).asRuntimeException();
        }
    }

    private PayloadBudget.Lease reserve(long bytes) {
        try { return budget.reserve(bytes); }
        catch (PayloadBudget.CapacityExceededException full) {
            throw Status.RESOURCE_EXHAUSTED.withDescription("Historical client byte capacity exhausted").asRuntimeException();
        }
    }

    private static final class ControlFailure extends RuntimeException {
        private final RuntimeException original;
        private ControlFailure(RuntimeException original) { this.original = original; }
    }

    public final class Result implements AutoCloseable {
        private DocumentHistoricalResponseMaterialization.Result decoded;
        private Result(DocumentHistoricalResponseMaterialization.Result decoded) { this.decoded = decoded; }
        /** Local cancellation check only; call read again for a new server authorization decision. */
        public synchronized DocumentHistoricalResponseMaterialization.View view(RepositoryReadControl control) {
            if (decoded == null) throw new IllegalStateException("Historical client result is closed");
            return decoded.view(Objects.requireNonNull(control)::check);
        }
        @Override public synchronized void close() {
            if (decoded == null) return;
            try { decoded.close(); }
            finally { decoded = null; calls.release(); }
        }
    }
}
