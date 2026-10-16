package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.GetManifestRequest;
import ai.protomolt.proto.repo.v2.GetManifestResponse;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeResponse;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsResponse;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * gRPC adapter for the implemented v2 save, read, delete and listing RPCs. Refusals carry the
 * errors.proto detail message in the status details. Remaining RPCs
 * answer UNIMPLEMENTED. Not registered in any production server.
 */
public final class AccessionGrpcService extends DocumentServiceGrpc.DocumentServiceImplBase {
    private final DocumentSaves saves;
    private final DocumentReads reads;
    private final DocumentDeletes deletes;
    private final DocumentLists lists;
    private final Function<AuthenticatedCaller, RepositoryCaller> bindings;

    /**
     * Constructs a new gRPC accession service.
     *
     * @param saves the document saves coordinator
     * @param reads the document reads coordinator
     * @param deletes the tombstone coordinator
     * @param lists the document listing coordinator
     * @param bindings resolves the trusted authentication result into repository
     *        identity; it must preserve principal, authority and credential binding
     */
    public AccessionGrpcService(DocumentSaves saves, DocumentReads reads,
            DocumentDeletes deletes, DocumentLists lists, Function<AuthenticatedCaller, RepositoryCaller> bindings) {
        this.saves = Objects.requireNonNull(saves);
        this.reads = Objects.requireNonNull(reads);
        this.deletes = Objects.requireNonNull(deletes);
        this.lists = Objects.requireNonNull(lists);
        this.bindings = Objects.requireNonNull(bindings);
    }

    @Override public void saveDocument(SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> observer) {
        respond(observer, () -> saves.save(caller(), request));
    }

    @Override public void getDocument(GetDocumentRequest request, StreamObserver<GetDocumentResponse> observer) {
        respond(observer, () -> reads.getDocument(caller(), request));
    }

    @Override public void getManifest(GetManifestRequest request, StreamObserver<GetManifestResponse> observer) {
        respond(observer, () -> reads.getManifest(caller(), request));
    }

    @Override public void listRevisions(ListRevisionsRequest request, StreamObserver<ListRevisionsResponse> observer) {
        respond(observer, () -> reads.listRevisions(caller(), request));
    }

    @Override public void getOperationOutcome(GetOperationOutcomeRequest request,
            StreamObserver<GetOperationOutcomeResponse> observer) {
        respond(observer, () -> reads.outcome(caller(), request));
    }

    @Override public void deleteDocument(DeleteDocumentRequest request, StreamObserver<DeleteDocumentResponse> observer) {
        respond(observer, () -> deletes.delete(caller(), request));
    }

    @Override public void listDocuments(ListDocumentsRequest request, StreamObserver<ListDocumentsResponse> observer) {
        respond(observer, () -> lists.list(caller(), request));
    }

    private RepositoryCaller caller() {
        var caller = CallerContexts.current();
        var authentication = CallerContexts.authentication().orElseGet(() -> AuthenticatedCaller.unbound(caller));
        var resolved = bindings.apply(authentication);
        var credential = authentication.binding().map(binding ->
                new RepositoryCredentialBinding(binding.issuer(), binding.credentialId(), binding.generation()));
        if (resolved == null || !resolved.principalName().equals(caller.name())
                || resolved.processAuthority() != caller.unrestricted() || !resolved.credentialBinding().equals(credential))
            throw new AccessionException(Status.Code.PERMISSION_DENIED,
                    "Repository binding must preserve the authenticated principal, authority and credential identity", null);
        return resolved;
    }

    private static <T> void respond(StreamObserver<T> observer, Supplier<T> call) {
        final T response;
        try {
            response = call.get();
        } catch (AccessionException failure) {
            observer.onError(toStatus(failure));
            return;
        } catch (RuntimeException failure) {
            observer.onError(Status.INTERNAL.withDescription("Repository failure").withCause(failure).asRuntimeException());
            return;
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Converts an accession exception to a gRPC {@link StatusRuntimeException} carrying any error details.
     *
     * @param failure the accession exception to convert
     * @return the corresponding gRPC status runtime exception
     */
    static StatusRuntimeException toStatus(AccessionException failure) {
        var status = com.google.rpc.Status.newBuilder().setCode(failure.code().value())
                .setMessage(Optional.ofNullable(failure.getMessage()).orElse(""));
        failure.detail().ifPresent(detail -> status.addDetails(Any.pack(detail)));
        return StatusProto.toStatusRuntimeException(status.build());
    }
}
