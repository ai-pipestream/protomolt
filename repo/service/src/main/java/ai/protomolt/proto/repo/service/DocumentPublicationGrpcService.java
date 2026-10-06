package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AuthenticatedCaller;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Authenticated adapter over the shared publisher; the host owns repository and transport resources. */
public final class DocumentPublicationGrpcService extends DocumentPublicationServiceGrpc.DocumentPublicationServiceImplBase implements AutoCloseable {
    private static final ProtoValidator VALIDATOR=ProtoValidator.create();
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(DocumentPublicationGrpcService.class);
    private static final int MAX_RESPONSE_BYTES=DocumentPublicationResultCodec.MAX_BYTES+16;
    /** Application allowance for one maximum envelope, canonical command and terminal response. */
    public static final long MAX_CALL_RESERVATION_BYTES=(long)DocumentPublicationInput.MAX_ENVELOPE_BYTES
            +MAX_RESPONSE_BYTES+DocumentPublicationCommand.MAX_COMMAND_BYTES;
    private final DocumentPublicationRepository repository;
    private final Function<AuthenticatedCaller,RepositoryCaller> bindings;
    private final PayloadBudget deliveryBudget;
    private final int maxConcurrentCalls;
    private int activeCalls;
    private boolean closed;

    /** Install authentication and a 10 MiB inbound parser limit separately; bindings are host authority. */
    public DocumentPublicationGrpcService(DocumentPublicationRepository repository,
            Function<AuthenticatedCaller,RepositoryCaller> bindings, PayloadBudget deliveryBudget, int maxConcurrentCalls) {
        this.repository=Objects.requireNonNull(repository); this.bindings=Objects.requireNonNull(bindings);
        this.deliveryBudget=Objects.requireNonNull(deliveryBudget);
        if (maxConcurrentCalls<1) throw new IllegalArgumentException("Publication transport call limit must be positive");
        this.maxConcurrentCalls=maxConcurrentCalls;
    }

    @Override public void publishDocument(PublishDocumentRequest request, StreamObserver<PublishDocumentResponse> output) {
        if (!(output instanceof ServerCallStreamObserver<PublishDocumentResponse> observer)) {
            output.onError(Status.INTERNAL.withDescription("Publication requires a server call lifecycle").asRuntimeException());
            return;
        }
        var context=Context.current();
        var control=new RepositoryReadControl() {
            public boolean isCancelled() { return context.isCancelled() || observer.isCancelled(); }
            public long remainingNanos() {
                return context.getDeadline()==null ? Long.MAX_VALUE : context.getDeadline().timeRemaining(TimeUnit.NANOSECONDS);
            }
        };
        Call call=null;
        boolean installed=false;
        try {
            var caller=caller(); control.check();
            call=admit();
            observer.setOnCancelHandler(call::terminated); observer.setOnCloseHandler(call::terminated); installed=true;
            if (control.isCancelled()) call.terminated();
            control.check();
            int size=request.getSerializedSize();
            if (size>DocumentPublicationInput.MAX_ENVELOPE_BYTES) throw new IllegalArgumentException("Publication request exceeds 10 MiB");
            // Application input, response and canonical command allowance. Parser/transport buffers are separate.
            call.bytes=deliveryBudget.reserve((long)size+MAX_RESPONSE_BYTES+DocumentPublicationCommand.MAX_COMMAND_BYTES);
            if (!request.getUnknownFields().asMap().isEmpty() || !VALIDATOR.validate(request).valid())
                throw new IllegalArgumentException("Invalid publication request");
            var command=new DocumentPublicationCommand(request.getIntent());
            control.check();
            var response=repository.publishDocument(caller,request,control);
            requireResponse(command,caller,response);
            call.response=response;
            control.check();
            observer.onNext(response); observer.onCompleted();
        } catch (RuntimeException failure) {
            if (call!=null && !installed) call.terminated();
            observer.onError(error(failure));
        } finally {
            if (call!=null) call.producerFinished();
        }
    }

    private synchronized Call admit() {
        if (closed) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE,"Publication transport is closing");
        if (activeCalls==maxConcurrentCalls)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Publication transport is full");
        var call=new Call();
        activeCalls++;
        return call;
    }

    /** Reject new calls; already admitted producers and deliveries retain their resources. */
    @Override public synchronized void close() { closed=true; }

    /** Wait before closing the listener or shared repository. A timeout does not release either. */
    public synchronized boolean awaitIdle(java.time.Duration timeout) throws InterruptedException {
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative drain timeout");
        long remaining=timeout.toNanos(), started=System.nanoTime();
        while (activeCalls!=0 && remaining>0) {
            TimeUnit.NANOSECONDS.timedWait(this,remaining);
            remaining=timeout.toNanos()-(System.nanoTime()-started);
        }
        return activeCalls==0;
    }

    private synchronized void releaseCall() { activeCalls--; notifyAll(); }

    private RepositoryCaller caller() {
        var authenticated=CallerContexts.CALLER.get();
        if (authenticated==null) throw Status.UNAUTHENTICATED.withDescription("Publication requires authentication").asRuntimeException();
        var authentication=CallerContexts.authentication()
                .orElseGet(() -> AuthenticatedCaller.unbound(authenticated));
        var credential=authentication.binding().map(binding -> new RepositoryCredentialBinding(
                binding.issuer(),binding.credentialId(),binding.generation()));
        var caller=bindings.apply(authentication);
        if (caller==null || !caller.principalName().equals(authenticated.name())
                || caller.processAuthority()!=authenticated.unrestricted()
                || !caller.credentialBinding().equals(credential))
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,"Repository binding changed authenticated identity");
        return caller;
    }

    /** Correspondence is checked here; the repository remains the durable outcome authority. */
    private static void requireResponse(DocumentPublicationCommand command, RepositoryCaller caller, PublishDocumentResponse response) {
        try {
            if (response==null || response.getSerializedSize()>MAX_RESPONSE_BYTES
                    || !response.getUnknownFields().asMap().isEmpty() || !VALIDATOR.validate(response).valid())
                throw new IllegalArgumentException("Invalid publication response");
            if (response.hasCommitted()) command.requireResult(response.getCommitted(),caller.principalName(),response.getCommitted().getOwnerGeneration());
            else if (response.hasRejected()) command.requireRejection(response.getRejected(),caller.principalName(),response.getRejected().getOwnerGeneration());
            else throw new IllegalArgumentException("Publication outcome is absent");
        } catch (IllegalArgumentException malformed) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Repository returned an invalid publication receipt",malformed);
        }
    }

    private static Throwable error(RuntimeException failure) {
        if (failure instanceof RepositoryException || failure instanceof IllegalArgumentException
                || failure instanceof io.grpc.StatusRuntimeException) return GrpcErrors.map(failure);
        if (failure instanceof UnsupportedOperationException) return Status.UNIMPLEMENTED.withDescription("Publication operation is not supported").asRuntimeException();
        if (failure instanceof PayloadBudget.CapacityExceededException) return Status.RESOURCE_EXHAUSTED.withDescription("Publication delivery capacity exhausted").asRuntimeException();
        LOG.warn("Publication transport failed",failure);
        return Status.INTERNAL.withDescription("Publication failed").asRuntimeException();
    }

    /** Cancel/close never releases application state while its producer still uses it. */
    private final class Call {
        private PayloadBudget.Lease bytes;
        private PublishDocumentResponse response;
        private boolean producerFinished,terminated,released;
        synchronized void terminated() { terminated=true; releaseIfDone(); }
        synchronized void producerFinished() { producerFinished=true; releaseIfDone(); }
        private void releaseIfDone() {
            if (!released && producerFinished && terminated) {
                released=true; response=null;
                if (bytes!=null) { bytes.close(); bytes=null; }
                releaseCall();
            }
        }
    }
}
