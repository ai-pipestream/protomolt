package ai.pipestream.proto.parse.service;

import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Serves seeded documents by reference and records every {@code SaveDocument}
 * call. The contract under test is WHICH reads and saves the coordinator
 * issues — the whole repo-service stack would prove nothing extra here.
 *
 * <p>{@link #saveGate} (when set) blocks each save until released, so the
 * serialized-write contract is observable: a second save of the same document
 * must not arrive while the first is still in flight.
 */
final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {

    final Map<String, Document> seeded = new ConcurrentHashMap<>();
    final List<SaveDocumentRequest> saves = new CopyOnWriteArrayList<>();
    volatile CountDownLatch saveGate;
    volatile String drive = "";

    /** Seeds the document served for {@code address}. */
    void seed(NodeAddress address, Document document) {
        seeded.put(key(address), document);
    }

    static String key(NodeAddress address) {
        return address.getDocId() + '|' + address.getGraphAddressId() + '|'
                + address.getAccountId() + '|' + address.getGraphId();
    }

    @Override
    public void getDocumentByReference(
            GetDocumentByReferenceRequest request, StreamObserver<GetDocumentResponse> observer) {
        Document document = seeded.get(key(request.getAddress()));
        if (document == null) {
            observer.onError(Status.NOT_FOUND
                    .withDescription("no seeded document at " + key(request.getAddress()))
                    .asRuntimeException());
            return;
        }
        observer.onNext(GetDocumentResponse.newBuilder()
                .setDocument(document)
                .setNodeId(UUID.nameUUIDFromBytes(key(request.getAddress()).getBytes()).toString())
                .setDrive(drive)
                .build());
        observer.onCompleted();
    }

    @Override
    public void saveDocument(
            SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> observer) {
        saves.add(request);
        CountDownLatch gate = saveGate;
        if (gate != null) {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        NodeAddress address = NodeAddress.newBuilder()
                .setDocId(request.getDocument().getDocId())
                .setGraphAddressId(request.getDocument().getOwnership().getDatasourceId())
                .setAccountId(request.getDocument().getOwnership().getAccountId())
                .setGraphId(request.getGraphId())
                .build();
        observer.onNext(SaveDocumentResponse.newBuilder()
                .setNodeId(UUID.nameUUIDFromBytes(address.toByteArray()).toString())
                .setDrive(request.getDrive())
                .setSizeBytes(request.getDocument().getSerializedSize())
                .setAddress(address)
                .build());
        observer.onCompleted();
    }
}
