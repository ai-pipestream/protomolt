package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.HistoricalDocumentRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Opt-in authenticated adapter. Does not own repository resources or infer account grants. */
public final class DocumentHistoryGrpcService extends DocumentHistoryServiceGrpc.DocumentHistoryServiceImplBase {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DocumentHistoryGrpcService.class);
    private final HistoricalDocumentRepository repository;
    private final Function<Caller, RepositoryCaller> bindings;
    private final PayloadBudget responses;
    private final Semaphore calls;

    /** The host must install authentication and provide authoritative membership/ACL bindings. */
    public DocumentHistoryGrpcService(HistoricalDocumentRepository repository,
            Function<Caller, RepositoryCaller> bindings, PayloadBudget responses, int maxConcurrentCalls) {
        this.repository = Objects.requireNonNull(repository);
        this.bindings = Objects.requireNonNull(bindings);
        this.responses = Objects.requireNonNull(responses);
        if (maxConcurrentCalls < 1) throw new IllegalArgumentException("Historical call limit must be positive");
        calls = new Semaphore(maxConcurrentCalls);
    }

    @Override public void readRevision(ReadRevisionRequest request, StreamObserver<ReadRevisionResponse> output) {
        if (!(output instanceof ServerCallStreamObserver<ReadRevisionResponse> observer)) {
            output.onError(Status.INTERNAL.withDescription("Historical reads require a server call lifecycle").asRuntimeException());
            return;
        }
        var context = Context.current();
        var control = new RepositoryReadControl() {
            @Override public boolean isCancelled() { return context.isCancelled() || observer.isCancelled(); }
            @Override public long remainingNanos() {
                return context.getDeadline() == null ? Long.MAX_VALUE : context.getDeadline().timeRemaining(TimeUnit.NANOSECONDS);
            }
        };
        Call call = null;
        boolean handlersInstalled = false;
        try {
            var caller = caller();
            control.check();
            if (!request.getUnknownFields().asMap().isEmpty() || !request.getAddress().getUnknownFields().asMap().isEmpty()
                    || !VALIDATOR.validate(request).valid())
                throw new RepositoryException(RepositoryException.Code.INVALID_ARGUMENT, "Invalid historical read request");
            if (!calls.tryAcquire()) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "Historical call capacity exhausted");
            call = new Call();
            observer.setOnCancelHandler(call::terminated);
            observer.setOnCloseHandler(call::terminated);
            handlersInstalled = true;
            if (control.isCancelled()) call.terminated();
            control.check();
            // These callbacks can race production. Neither may release a snapshot
            // while the producer is still copying, validating or sending it.
            var revision = UUID.fromString(request.getRevisionId());
            try (var read = request.getMode() == HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW
                    ? repository.readRaw(caller, request.getAddress(), revision, control)
                    : repository.readValidated(caller, request.getAddress(), revision, control)) {
                try {
                    call.snapshot = HistoricalDocumentResponses.capture(request, read, responses, control);
                } catch (RuntimeException failure) {
                    control.check();
                    if (failure instanceof RepositoryException repositoryFailure
                            && (repositoryFailure.code() == RepositoryException.Code.CANCELLED
                            || repositoryFailure.code() == RepositoryException.Code.DEADLINE_EXCEEDED))
                        throw new RepositoryException(repositoryFailure.code(), "Historical read cancelled or expired");
                    read.authorizeDelivery(control);
                    throw failure;
                }
                read.authorizeDelivery(control);
            }
            control.check();
            observer.onNext(call.snapshot.response());
            observer.onCompleted();
        } catch (RuntimeException failure) {
            if (call != null && !handlersInstalled) call.terminated();
            if (failure instanceof RepositoryException || failure instanceof io.grpc.StatusRuntimeException)
                observer.onError(GrpcErrors.map(failure));
            else {
                LOG.warn("Historical read failed", failure);
                observer.onError(Status.INTERNAL.withDescription("Historical read failed").asRuntimeException());
            }
        } finally {
            if (call != null) call.producerFinished();
        }
    }

    private RepositoryCaller caller() {
        var authenticated = CallerContexts.CALLER.get();
        if (authenticated == null) throw Status.UNAUTHENTICATED.withDescription("Historical reads require authentication").asRuntimeException();
        var bound = bindings.apply(authenticated);
        if (bound == null || !bound.principalName().equals(authenticated.name())
                || bound.processAuthority() != authenticated.unrestricted())
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                    "Repository binding must preserve the authenticated principal and authority");
        return bound;
    }

    /** Callbacks release application snapshots, not gRPC's internal transport buffers. */
    private final class Call {
        private HistoricalDocumentResponses.Snapshot snapshot;
        private boolean producerFinished;
        private boolean terminated;
        private boolean released;
        synchronized void terminated() { terminated = true; releaseIfDone(); }
        synchronized void producerFinished() { producerFinished = true; releaseIfDone(); }
        private void releaseIfDone() {
            if (!released && producerFinished && terminated) {
                released = true;
                if (snapshot != null) { snapshot.close(); snapshot = null; }
                calls.release();
            }
        }
    }
}
