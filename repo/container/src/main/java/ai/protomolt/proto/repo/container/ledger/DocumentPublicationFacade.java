package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.PublishDocumentRequest;
import ai.protomolt.proto.repo.v1.PublishDocumentResponse;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/** Buffered entry boundary; the runtime owns execution and the admission barrier. */
final class DocumentPublicationFacade implements DocumentPublicationRepository {
    @FunctionalInterface interface Execution {
        PublishDocumentResponse publish(RepositoryCaller caller, DocumentPublicationInput input, RepositoryReadControl control);
    }
    private final DocumentPublicationScopeCalls calls;
    private final PayloadBudget budget;
    private final Semaphore permits;
    private final Execution execution;
    private final int maxObjectBytes;
    private final boolean historical;

    DocumentPublicationFacade(DocumentPublicationScopeCalls calls, PayloadBudget budget, Semaphore permits, Execution execution) {
        this(calls,budget,permits,execution,(int)DocumentPublicationInput.MAX_UPLOAD_BYTES);
    }

    DocumentPublicationFacade(DocumentPublicationScopeCalls calls, PayloadBudget budget, Semaphore permits,
            Execution execution, int maxObjectBytes) {
        this(calls, budget, permits, execution, maxObjectBytes, false);
    }

    DocumentPublicationFacade(DocumentPublicationScopeCalls calls, PayloadBudget budget, Semaphore permits,
            Execution execution, int maxObjectBytes, boolean historical) {
        if (maxObjectBytes < 1 || maxObjectBytes > DocumentPublicationInput.MAX_UPLOAD_BYTES)
            throw new IllegalArgumentException("Upload object limit must be positive and at most 8 MiB");
        this.maxObjectBytes=maxObjectBytes;
        this.historical=historical;
        this.calls=Objects.requireNonNull(calls); this.budget=Objects.requireNonNull(budget);
        this.permits=Objects.requireNonNull(permits); this.execution=Objects.requireNonNull(execution);
    }

    @Override public PublishDocumentResponse publishDocument(RepositoryCaller caller, PublishDocumentRequest request,
            RepositoryReadControl control) {
        Objects.requireNonNull(request); Objects.requireNonNull(control).check();
        if (caller==null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,"Authenticated repository caller is required");
        try (var call=calls.enter()) {
            if (!permits.tryAcquire()) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Publication call capacity exhausted");
            try {
                int size=request.getSerializedSize();
                if (size>DocumentPublicationInput.MAX_ENVELOPE_BYTES)
                    throw new IllegalArgumentException("Publication envelope exceeds 10 MiB");
                // Account for retained input and bounded canonical command copies. Parser memory is host-owned.
                try (var inputLease=budget.reserve((long)size+2L*DocumentPublicationCommand.MAX_COMMAND_BYTES)) {
                    var input=DocumentPublicationInput.validate(request,control,maxObjectBytes);
                    if (!historical) input.command().requireExecutionSupported();
                    // Reserve before materializing ByteString uploads as the runtime's private byte arrays.
                    try (var copyLease=budget.reserve(input.uploadBytes())) {
                        return execution.publish(caller,input,control);
                    }
                }
            } finally { permits.release(); }
        } catch (RepositoryOperationLedger.CommandConflictException conflict) {
            throw new RepositoryException(RepositoryException.Code.CONFLICT, conflict.getMessage(), conflict);
        } catch (PayloadBudget.CapacityExceededException exhausted) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Publication byte capacity exhausted",exhausted);
        }
    }
}
