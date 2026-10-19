package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records every {@code SaveDocument} call and answers with a plausible
 * receipt. The contract under test is WHICH save requests intake issues —
 * storage and durability are qualified separately by the real-provider integration test.
 */
final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {

    final List<SaveDocumentRequest> saves = new CopyOnWriteArrayList<>();
    final List<ai.protomolt.proto.repo.v2.DeleteDocumentRequest> deletes = new CopyOnWriteArrayList<>();
    volatile boolean deduplicated;
    /** When set, every save fails with this status instead of answering (repo-failure tests). */
    volatile StatusRuntimeException failure;

    @Override
    public void deleteDocument(ai.protomolt.proto.repo.v2.DeleteDocumentRequest request,
            StreamObserver<ai.protomolt.proto.repo.v2.DeleteDocumentResponse> observer) {
        deletes.add(request);
        if (failure != null) { observer.onError(failure); return; }
        observer.onNext(ai.protomolt.proto.repo.v2.DeleteDocumentResponse.newBuilder()
                .setOperationId(request.getOperationId())
                .setAccession(ai.protomolt.proto.repo.v2.Accession.newBuilder().setAddress(request.getAddress())
                        .setRevision(request.getExpected().getRevision() + 1).setTombstone(true)).build());
        observer.onCompleted();
    }

    @Override
    public void saveDocument(
            SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> observer) {
        StatusRuntimeException failWith = failure;
        if (failWith != null) {
            observer.onError(failWith);
            return;
        }
        saves.add(request);
        String docId = request.getDocument().getDocId();
        if (docId.isBlank()) {
            docId = UUID.randomUUID().toString();
        }
        NodeAddress address =
                NodeAddress.newBuilder()
                        .setDocId(docId)
                        .setGraphAddressId(request.getDocument().getOwnership().getDatasourceId())
                        .setAccountId(request.getDocument().getOwnership().getAccountId())
                        .setGraphId("intake:" + request.getDocument().getOwnership().getAccountId())
                        .build();
        observer.onNext(
                SaveDocumentResponse.newBuilder()
                        .setAccession(ai.protomolt.proto.repo.v2.Accession.newBuilder()
                                .setAddress(address).setRevision(1))
                        .setReplayed(deduplicated)
                        .build());
        observer.onCompleted();
    }
}
