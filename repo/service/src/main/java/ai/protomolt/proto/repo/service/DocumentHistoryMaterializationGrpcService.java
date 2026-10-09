package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
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

/** Explicitly mounted selected-read capability. The embedding host owns all borrowed resources. */
public final class DocumentHistoryMaterializationGrpcService
        extends DocumentHistoryMaterializationServiceGrpc.DocumentHistoryMaterializationServiceImplBase {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DocumentHistoryMaterializationGrpcService.class);
    private final HistoricalMaterializationRepository repository;
    private final Function<Caller, RepositoryCaller> bindings;
    private final HistoricalMaterializationRepository.Limits limits;
    private final PayloadBudget responses;
    private final Semaphore calls;

    /** Authentication interceptors and authoritative account/ACL binding are host obligations. */
    public DocumentHistoryMaterializationGrpcService(HistoricalMaterializationRepository repository,
            Function<Caller, RepositoryCaller> bindings, HistoricalMaterializationRepository.Limits limits,
            PayloadBudget responses, int maxConcurrentCalls) {
        this.repository = Objects.requireNonNull(repository); this.bindings = Objects.requireNonNull(bindings);
        this.limits = Objects.requireNonNull(limits); this.responses = Objects.requireNonNull(responses);
        if (maxConcurrentCalls < 1) throw new IllegalArgumentException("Selected read call bound must be positive");
        calls = new Semaphore(maxConcurrentCalls);
    }

    @Override public void readHistoricalOccurrence(ReadHistoricalOccurrenceRequest request,
            StreamObserver<ReadHistoricalOccurrenceResponse> output) {
        if (!(output instanceof ServerCallStreamObserver<ReadHistoricalOccurrenceResponse> observer)) {
            output.onError(Status.INTERNAL.withDescription("Selected reads require a server call lifecycle").asRuntimeException());
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
        boolean installed = false;
        try {
            var caller = caller(); control.check();
            if (!request.getUnknownFields().asMap().isEmpty() || !request.getAddress().getUnknownFields().asMap().isEmpty()
                    || !request.getSelection().getUnknownFields().asMap().isEmpty() || !request.getLimits().getUnknownFields().asMap().isEmpty()
                    || !VALIDATOR.validate(request).valid())
                throw new RepositoryException(RepositoryException.Code.INVALID_ARGUMENT, "Invalid selected read request");
            if (!calls.tryAcquire()) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Selected read call capacity exhausted");
            call = new Call();
            observer.setOnCancelHandler(call::terminated); observer.setOnCloseHandler(call::terminated); installed = true;
            if (control.isCancelled()) call.terminated();
            control.check();
            var selection = request.getSelection();
            try (var read = repository.readMaterialized(caller, request.getAddress(), UUID.fromString(request.getRevisionId()),
                    new HistoricalMaterializationRepository.Selection(selection.getRevisionOrdinal(), selection.getRootSha256(), selection.getPathSha256()),
                    capped(request.getLimits()), control)) {
                try { call.snapshot = HistoricalOccurrenceResponses.capture(request, read, responses, control); }
                catch (RuntimeException failure) {
                    control.check();
                    read.view(control); // Current denial suppresses detailed verification/storage failure.
                    throw failure;
                }
                read.view(control); // Reauthorize immediately before exposing the independent snapshot.
                control.check();
                observer.onNext(call.snapshot.response()); observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            if (call != null && !installed) call.terminated();
            if (failure instanceof RepositoryException || failure instanceof io.grpc.StatusRuntimeException)
                observer.onError(GrpcErrors.map(failure));
            else {
                LOG.warn("Selected historical read failed", failure);
                observer.onError(Status.INTERNAL.withDescription("Selected historical read failed").asRuntimeException());
            }
        } finally { if (call != null) call.producerFinished(); }
    }

    private HistoricalMaterializationRepository.Limits capped(HistoricalMaterializationLimits requested) {
        return new HistoricalMaterializationRepository.Limits(Math.min(limits.maxFragmentBytes(), requested.getMaxFragmentBytes()),
                Math.min(limits.maxEvidenceBytes(), requested.getMaxEvidenceBytes()), Math.min(limits.maxRetainedBytes(), requested.getMaxRetainedBytes()),
                Math.min(limits.maxReferences(), requested.getMaxReferences()), Math.min(limits.maxDecodedBytes(), requested.getMaxDecodedBytes()),
                Math.min(limits.maxBoundaries(), requested.getMaxBoundaries()));
    }

    private RepositoryCaller caller() {
        var authenticated = CallerContexts.CALLER.get();
        if (authenticated == null) throw Status.UNAUTHENTICATED.withDescription("Selected reads require authentication").asRuntimeException();
        var bound = bindings.apply(authenticated);
        if (bound == null || !bound.principalName().equals(authenticated.name()) || bound.processAuthority() != authenticated.unrestricted())
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED, "Repository binding must preserve authenticated identity");
        return bound;
    }

    private final class Call {
        private HistoricalOccurrenceResponses.Snapshot snapshot;
        private boolean producerFinished, terminated, released;
        synchronized void terminated() { terminated = true; release(); }
        synchronized void producerFinished() { producerFinished = true; release(); }
        private void release() {
            if (!released && producerFinished && terminated) {
                released = true;
                if (snapshot != null) { snapshot.close(); snapshot = null; }
                calls.release();
            }
        }
    }
}
