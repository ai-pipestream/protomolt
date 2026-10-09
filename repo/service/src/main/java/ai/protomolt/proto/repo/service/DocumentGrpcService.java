package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.engine.DocumentOperations;
import ai.protomolt.proto.repo.engine.BlobOperations;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.container.lifecycle.*;
import io.grpc.stub.StreamObserver;

/** gRPC adapter for the shared repository operations. */
public final class DocumentGrpcService extends DocumentServiceGrpc.DocumentServiceImplBase {
    private final DocumentRepository documents;
    private final BlobRepository blobs;
    private final java.util.function.Function<ai.protomolt.proto.authz.AuthenticatedCaller, RepositoryCaller> callerBindings;

    public DocumentGrpcService(DocumentRepository documents, BlobRepository blobs) {
        this(documents, blobs, new AuthenticatedBindings(authentication -> new RepositoryCaller(
                authentication.caller().name(), authentication.caller().unrestricted(), java.util.Set.of(),
                java.util.Set.of(), credential(authentication))));
    }

    /** Principal-only host binding; identified credentials require withAuthenticatedBindings. */
    public DocumentGrpcService(DocumentRepository documents, BlobRepository blobs,
            java.util.function.Function<ai.protomolt.proto.actions.Caller, RepositoryCaller> callerBindings) {
        this(documents, blobs, new AuthenticatedBindings(authentication -> callerBindings.apply(authentication.caller())));
        java.util.Objects.requireNonNull(callerBindings);
    }

    /** The host can bind accounts using the complete trusted authentication result. */
    public static DocumentGrpcService withAuthenticatedBindings(DocumentRepository documents, BlobRepository blobs,
            java.util.function.Function<ai.protomolt.proto.authz.AuthenticatedCaller, RepositoryCaller> bindings) {
        return new DocumentGrpcService(documents, blobs, new AuthenticatedBindings(java.util.Objects.requireNonNull(bindings)));
    }

    private record AuthenticatedBindings(
            java.util.function.Function<ai.protomolt.proto.authz.AuthenticatedCaller, RepositoryCaller> resolve) {}

    private DocumentGrpcService(DocumentRepository documents, BlobRepository blobs, AuthenticatedBindings bindings) {
        this.documents = java.util.Objects.requireNonNull(documents);
        this.blobs = java.util.Objects.requireNonNull(blobs);
        this.callerBindings = bindings.resolve();
    }

    public DocumentGrpcService(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue) {
        this(documents, drives, tx, blobStore, partStorage, purgeQueue, null);
    }

    public DocumentGrpcService(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue, JdbcEventOutbox events) {
        this(new DocumentOperations(documents, drives, tx, blobStore, partStorage, purgeQueue, events),
                new BlobOperations(blobStore, drives));
    }

    DocumentRepository repository() { return documents; }

    private RepositoryCaller caller() {
        var caller = ai.protomolt.proto.authz.grpc.CallerContexts.current();
        var authentication = ai.protomolt.proto.authz.grpc.CallerContexts.authentication()
                .orElseGet(() -> ai.protomolt.proto.authz.AuthenticatedCaller.unbound(caller));
        var resolved = callerBindings.apply(authentication);
        if (resolved == null || !resolved.principalName().equals(caller.name())
                || resolved.processAuthority() != caller.unrestricted()
                || !resolved.credentialBinding().equals(credential(authentication))) {
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                    "Repository binding must preserve the authenticated principal, authority and credential identity");
        }
        return resolved;
    }

    private static java.util.Optional<RepositoryCredentialBinding> credential(
            ai.protomolt.proto.authz.AuthenticatedCaller authentication) {
        return authentication.binding().map(binding -> new RepositoryCredentialBinding(
                binding.issuer(), binding.credentialId(), binding.generation()));
    }

    @Override public void saveDocument(SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> observer) {
        var read = readControl();
        var control = new ai.protomolt.proto.repo.spi.RepositoryOperationControl() {
            @Override public boolean isCancelled() { return read.isCancelled(); }
            @Override public long remainingNanos() { return read.remainingNanos(); }
        };
        GrpcErrors.run(observer, () -> documents.saveDocument(caller(), request, control));
    }

    @Override public void getDocument(GetDocumentRequest request, StreamObserver<GetDocumentResponse> observer) {
        var control = readControl();
        GrpcErrors.run(observer, () -> documents.getDocument(caller(), request, control));
    }

    @Override public void getDocumentByReference(GetDocumentByReferenceRequest request, StreamObserver<GetDocumentResponse> observer) {
        var control = readControl();
        GrpcErrors.run(observer, () -> documents.getDocumentByReference(caller(), request, control));
    }

    private static RepositoryReadControl readControl() {
        var context = io.grpc.Context.current();
        var deadline = context.getDeadline();
        return new RepositoryReadControl() {
            @Override public boolean isCancelled() { return context.isCancelled(); }
            @Override public long remainingNanos() {
                return deadline == null ? Long.MAX_VALUE : deadline.timeRemaining(java.util.concurrent.TimeUnit.NANOSECONDS);
            }
        };
    }

    @Override public void getDocumentManifest(GetDocumentManifestRequest request, StreamObserver<GetDocumentManifestResponse> observer) {
        GrpcErrors.run(observer, () -> documents.getDocumentManifest(caller(), request));
    }

    @Override public void deleteDocument(DeleteDocumentRequest request, StreamObserver<DeleteDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> documents.deleteDocument(caller(), request));
    }

    @Override public void listDocuments(ListDocumentsRequest request, StreamObserver<ListDocumentsResponse> observer) {
        GrpcErrors.run(observer, () -> documents.listDocuments(caller(), request));
    }

    @Override public void getBlob(GetBlobRequest request, StreamObserver<GetBlobResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.get(caller(), request));
    }

    @Override public void putBlob(PutBlobRequest request, StreamObserver<PutBlobResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.put(caller(), request));
    }

    @Override public void deleteBlob(DeleteBlobRequest request, StreamObserver<DeleteBlobResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.delete(caller(), request));
    }

    @Override public void getBlobForUpdate(GetBlobForUpdateRequest request, StreamObserver<GetBlobForUpdateResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.getForUpdate(caller(), request));
    }

    @Override public void compareAndPutBlob(CompareAndPutBlobRequest request, StreamObserver<CompareAndPutBlobResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.compareAndPut(caller(), request));
    }

}
