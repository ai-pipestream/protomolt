package ai.protomolt.proto.repo.publication.grpc;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.Status;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One authenticated identity per client. The host supplies its matching credential
 * on the stub and owns the channel. No identity is serialized as a permission grant.
 * The client adds no retries; the host owns channel retry policy. Cancellation or
 * transport errors can follow a durable commit; retry the same operation and inputs
 * to recover its receipt.
 */
public final class RemoteDocumentPublicationRepository implements DocumentPublicationRepository {
    private final RepositoryCaller identity;
    private final DocumentPublicationServiceGrpc.DocumentPublicationServiceFutureStub stub;
    private final PayloadBudget budget;
    private final Semaphore calls;
    private final long timeoutNanos;

    /**
     * Application reservations cover input, canonical command and response until
     * handoff. Returned messages belong to the caller. Channel/parser buffers and
     * caller-owned messages are not a total-heap guarantee.
     */
    public RemoteDocumentPublicationRepository(RepositoryCaller identity,
            DocumentPublicationServiceGrpc.DocumentPublicationServiceFutureStub authenticatedStub,
            PayloadBudget budget, Duration timeout, int maxConcurrentCalls) {
        this.identity=Objects.requireNonNull(identity); Objects.requireNonNull(authenticatedStub);
        this.budget=Objects.requireNonNull(budget); Objects.requireNonNull(timeout);
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofDays(1))>0 || maxConcurrentCalls<1)
            throw new IllegalArgumentException("Positive timeout up to one day and positive call bound required");
        var incoming=authenticatedStub.getCallOptions().getMaxInboundMessageSize();
        var outgoing=authenticatedStub.getCallOptions().getMaxOutboundMessageSize();
        stub=authenticatedStub.withMaxInboundMessageSize(incoming==null ? DocumentPublicationResponseValidator.MAX_BYTES
                        : Math.min(incoming,DocumentPublicationResponseValidator.MAX_BYTES))
                .withMaxOutboundMessageSize(outgoing==null ? DocumentPublicationInput.MAX_ENVELOPE_BYTES
                        : Math.min(outgoing,DocumentPublicationInput.MAX_ENVELOPE_BYTES));
        timeoutNanos=timeout.toNanos(); calls=new Semaphore(maxConcurrentCalls);
    }

    @Override public PublishDocumentResponse publishDocument(RepositoryCaller caller, PublishDocumentRequest request,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(request);
        if (!identity.equals(caller)) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Publication client is bound to a different authenticated identity");
        if (request.getSerializedSize()>DocumentPublicationInput.MAX_ENVELOPE_BYTES)
            throw new IllegalArgumentException("Publication envelope exceeds 10 MiB");
        if (!calls.tryAcquire()) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Publication client is full");
        try (var reservation=reserve((long)request.getSerializedSize()+DocumentPublicationCommand.MAX_COMMAND_BYTES
                +DocumentPublicationResponseValidator.MAX_BYTES)) {
            var input=DocumentPublicationInput.validate(request,control);
            // The authenticated server decides which execution capabilities are enabled.
            long nanos=Math.min(timeoutNanos,control.remainingNanos());
            if (nanos<=0) throw new RepositoryException(RepositoryException.Code.DEADLINE_EXCEEDED,"Publication deadline expired");
            var deadline=io.grpc.Deadline.after(nanos,TimeUnit.NANOSECONDS);
            var existing=stub.getCallOptions().getDeadline();
            if (existing!=null) deadline=deadline.minimum(existing);
            if (deadline.isExpired()) throw new RepositoryException(RepositoryException.Code.DEADLINE_EXCEEDED,"Publication deadline expired");
            control.check();
            var future=stub.withDeadline(deadline).publishDocument(request);
            try {
                while (true) {
                    control.check();
                    try {
                        var response=future.get(Math.min(TimeUnit.MILLISECONDS.toNanos(25),Math.max(1,control.remainingNanos())),TimeUnit.NANOSECONDS);
                        control.check();
                        try { DocumentPublicationResponseValidator.requireValid(input.command(),identity.principalName(),response); }
                        catch (IllegalArgumentException invalid) {
                            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Invalid remote publication receipt",invalid);
                        }
                        control.check();
                        return response;
                    } catch (TimeoutException waiting) {
                        // Recheck local cancellation without extending the RPC deadline.
                    }
                }
            } catch (java.util.concurrent.CancellationException cancelled) {
                throw new RepositoryException(RepositoryException.Code.CANCELLED,"Publication RPC cancelled",cancelled);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RepositoryException(RepositoryException.Code.CANCELLED,"Publication client interrupted",interrupted);
            } catch (ExecutionException failed) {
                var status=Status.fromThrowable(failed.getCause());
                var code=switch (status.getCode()) {
                    case ABORTED -> RepositoryException.Code.CONFLICT;
                    case UNIMPLEMENTED -> RepositoryException.Code.UNSUPPORTED;
                    case OK -> RepositoryException.Code.INTERNAL;
                    default -> RepositoryException.Code.valueOf(status.getCode().name());
                };
                throw new RepositoryException(code,
                        "Publication RPC failed: "+status.getCode(),failed.getCause());
            } finally { future.cancel(true); }
        } finally { calls.release(); }
    }

    private PayloadBudget.Lease reserve(long bytes) {
        try { return budget.reserve(bytes); }
        catch (PayloadBudget.CapacityExceededException full) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Publication client byte capacity exhausted",full);
        }
    }
}
