package ai.pipestream.proto.intake.service;

import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records every {@code SaveDocument} call and answers with a plausible
 * receipt. The contract under test is WHICH save requests intake issues —
 * the whole repo-service stack would prove nothing extra here.
 */
final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {

    final List<SaveDocumentRequest> saves = new CopyOnWriteArrayList<>();
    volatile boolean deduplicated;
    /** When set, every save fails with this status instead of answering (repo-failure tests). */
    volatile StatusRuntimeException failure;

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
                        .setGraphId(request.getGraphId())
                        .build();
        observer.onNext(
                SaveDocumentResponse.newBuilder()
                        .setNodeId(UUID.nameUUIDFromBytes(address.toByteArray()).toString())
                        .setDrive(request.getDrive())
                        .setSizeBytes(request.getDocument().getSerializedSize())
                        .setDeduplicated(deduplicated)
                        .setAddress(address)
                        .build());
        observer.onCompleted();
    }
}
