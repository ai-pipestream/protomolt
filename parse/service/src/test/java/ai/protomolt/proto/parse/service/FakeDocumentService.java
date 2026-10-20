package ai.protomolt.proto.parse.service;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Serves seeded documents by reference and records every {@code SaveDocument}
 * call. The contract under test is WHICH reads and saves the coordinator
 * issues — storage and durability are qualified separately by the real-provider integration test.
 */
final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {

    final Map<String, Document> seeded = new ConcurrentHashMap<>();
    final List<SaveDocumentRequest> saves = new CopyOnWriteArrayList<>();
    volatile io.grpc.StatusRuntimeException saveFailure;
    /** Reads to fail UNAVAILABLE before serving (the durable-retry lever). */
    final java.util.concurrent.atomic.AtomicInteger failReadsRemaining =
            new java.util.concurrent.atomic.AtomicInteger();

    /** Seeds the document served for {@code address}. */
    void seed(NodeAddress address, Document document) {
        seeded.put(key(address), document);
    }

    static String key(NodeAddress address) {
        return address.getDocId() + '|' + address.getGraphAddressId() + '|'
                + address.getAccountId() + '|' + address.getGraphId();
    }

    @Override
    public void getDocument(
            GetDocumentRequest request, StreamObserver<GetDocumentResponse> observer) {
        if (failReadsRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            observer.onError(Status.UNAVAILABLE
                    .withDescription("injected transient read failure")
                    .asRuntimeException());
            return;
        }
        Document document = seeded.get(key(request.getAddress()));
        if (document == null) {
            observer.onError(Status.NOT_FOUND
                    .withDescription("no seeded document at " + key(request.getAddress()))
                    .asRuntimeException());
            return;
        }
        observer.onNext(GetDocumentResponse.newBuilder()
                .setDocument(document)
                .setManifest(ai.protomolt.proto.repo.v2.DocumentManifest.newBuilder()
                        .setAddress(request.getAddress()).setRevision(1))
                .build());
        observer.onCompleted();
    }

    @Override
    public void saveDocument(
            SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> observer) {
        saves.add(request);
        if (saveFailure != null) { observer.onError(saveFailure); return; }
        observer.onNext(SaveDocumentResponse.newBuilder()
                .setAccession(ai.protomolt.proto.repo.v2.Accession.newBuilder()
                        .setAddress(request.getTarget().getAddress()).setRevision(2))
                .build());
        observer.onCompleted();
    }
}
