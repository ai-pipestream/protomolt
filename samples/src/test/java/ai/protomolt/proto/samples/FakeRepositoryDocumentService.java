package ai.protomolt.proto.samples;

import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ProcessAuthorityEvidence;
import ai.protomolt.proto.repo.v2.RevisionConflict;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The v2 DocumentService contract over in-memory documents for the authoring process
 * tests: current-revision reads and compare-and-set saves answering ABORTED with a
 * RevisionConflict detail on a stale expectation, with the contract-valid responses the
 * coordinator's client-side protocol validation accepts. Subclasses hook committed
 * saves through {@link #afterSave}; the committed transcripts stay inspectable to tests
 * through the same client-side encryption the coordinator applies.
 */
class FakeRepositoryDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {
    private static final Timestamp COMMITTED_AT =
            Timestamp.newBuilder().setSeconds(1786449600L).build();
    private static final SaveOwner OWNER = SaveOwner.newBuilder()
            .setIntake(IntakeOrigin.newBuilder().setDatasourceId("samples-test")).build();
    private static final RevisionValidation VALIDATION = RevisionValidation.newBuilder()
            .setStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
            .setCandidateSha256("0".repeat(64))
            .setValidatorProfile("samples-test")
            .setProcessAuthority(ProcessAuthorityEvidence.newBuilder().setPrincipal("samples-test"))
            .build();

    protected final Map<String, ai.protomolt.proto.repo.v1.Document> documents =
            new ConcurrentHashMap<>();
    private final Map<String, Long> revisions = new ConcurrentHashMap<>();

    @Override
    public void getDocument(GetDocumentRequest request,
                            StreamObserver<GetDocumentResponse> observer) {
        String key = key(request.getAddress());
        var document = documents.get(key);
        if (document == null) {
            observer.onError(Status.NOT_FOUND.asRuntimeException());
            return;
        }
        observer.onNext(GetDocumentResponse.newBuilder().setDocument(document)
                .setManifest(manifest(request.getAddress(), revisions.get(key)))
                .setValidation(VALIDATION)
                .build());
        observer.onCompleted();
    }

    @Override
    public final synchronized void saveDocument(SaveDocumentRequest request,
                                                StreamObserver<SaveDocumentResponse> observer) {
        String key = key(request.getTarget().getAddress());
        long current = revisions.getOrDefault(key, 0L);
        long expected = request.getExpected().hasNone() ? 0 : request.getExpected().getRevision();
        if (current != expected) {
            var detail = RevisionConflict.newBuilder().setAddress(request.getTarget().getAddress())
                    .setExpected(request.getExpected()).setCurrentRevision(current).build();
            observer.onError(io.grpc.protobuf.StatusProto.toStatusRuntimeException(
                    com.google.rpc.Status.newBuilder().setCode(Status.Code.ABORTED.value())
                            .setMessage("conditional write conflict")
                            .addDetails(com.google.protobuf.Any.pack(detail)).build()));
            return;
        }
        documents.put(key, request.getDocument());
        revisions.put(key, current + 1);
        if (!afterSave(request, observer)) {
            return;
        }
        observer.onNext(SaveDocumentResponse.newBuilder().setOperationId(request.getOperationId())
                .setAccession(Accession.newBuilder().setAddress(request.getTarget().getAddress())
                        .setRevision(current + 1).setPreviousRevision(current)
                        .setCommittedAt(COMMITTED_AT)
                        .setManifest(manifest(request.getTarget().getAddress(), current + 1))
                        .setValidation(VALIDATION)
                        .setOwner(OWNER))
                .build());
        observer.onCompleted();
    }

    /**
     * Runs after a save commits, before its acknowledgement. Returning false means the
     * hook already answered the call.
     */
    protected boolean afterSave(SaveDocumentRequest request,
                                StreamObserver<SaveDocumentResponse> observer) {
        return true;
    }

    private static DocumentManifest manifest(ai.protomolt.proto.repo.v1.NodeAddress address,
                                             long revision) {
        return DocumentManifest.newBuilder().setAddress(address).setRevision(revision)
                .setCommittedAt(COMMITTED_AT)
                .addParts(PartManifestEntry.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setState(PartState.PART_STATE_PRESENT).setWrittenInRevision(revision)
                        .setOwner(OWNER))
                .build();
    }

    private static String key(ai.protomolt.proto.repo.v1.NodeAddress address) {
        return address.getAccountId() + "/" + address.getDocId();
    }
}
