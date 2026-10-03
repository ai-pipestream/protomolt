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

    public DocumentGrpcService(DocumentRepository documents, BlobRepository blobs) {
        this.documents = java.util.Objects.requireNonNull(documents);
        this.blobs = java.util.Objects.requireNonNull(blobs);
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

    private static RepositoryCaller caller() {
        var caller = ai.protomolt.proto.authz.grpc.CallerContexts.current();
        return new RepositoryCaller(caller.name(), caller.unrestricted());
    }

    @Override public void saveDocument(SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> documents.saveDocument(caller(), request));
    }

    @Override public void getDocument(GetDocumentRequest request, StreamObserver<GetDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> documents.getDocument(caller(), request));
    }

    @Override public void getDocumentByReference(GetDocumentByReferenceRequest request, StreamObserver<GetDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> documents.getDocumentByReference(caller(), request));
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
