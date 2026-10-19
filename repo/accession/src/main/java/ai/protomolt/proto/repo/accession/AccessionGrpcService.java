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
import ai.protomolt.proto.repo.v2.ItemError;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsResponse;
import ai.protomolt.proto.repo.v2.OperationReuse;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.ReadDocumentsRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentsResponse;
import ai.protomolt.proto.repo.v2.RevisionConflict;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.SaveDocumentsRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentsResponse;
import ai.protomolt.proto.repo.v2.SaveRejection;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * gRPC adapter for the implemented v2 save, read, delete, purge and listing RPCs
 * and the slice 6 bulk streams. Refusals carry the errors.proto detail message
 * in the status details; on a bulk stream the same refusal is one item's
 * {@link ItemError}, so a failed item never aborts the stream for the others.
 * Items are answered in request order, one at a time, under a synchronized
 * observer: in-flight concurrency is bounded at one per stream and responses
 * correlate by request_id. Remaining RPCs answer UNIMPLEMENTED. Not registered
 * in any production server.
 */
public final class AccessionGrpcService extends DocumentServiceGrpc.DocumentServiceImplBase {
    private final DocumentSaves saves;
    private final DocumentReads reads;
    private final DocumentDeletes deletes;
    private final DocumentLists lists;
    private final DocumentPurges purges;
    private final Function<AuthenticatedCaller, RepositoryCaller> bindings;

    /**
     * Constructs a new gRPC accession service.
     *
     * @param saves the document saves coordinator
     * @param reads the document reads coordinator
     * @param deletes the tombstone coordinator
     * @param lists the document listing coordinator
     * @param purges the administrator purge coordinator
     * @param bindings resolves the trusted authentication result into repository
     *        identity; it must preserve principal, authority and credential binding
     */
    public AccessionGrpcService(DocumentSaves saves, DocumentReads reads,
            DocumentDeletes deletes, DocumentLists lists, DocumentPurges purges, Function<AuthenticatedCaller, RepositoryCaller> bindings) {
        this.saves = Objects.requireNonNull(saves);
        this.reads = Objects.requireNonNull(reads);
        this.deletes = deletes;
        this.lists = lists;
        this.purges = purges;
        this.bindings = Objects.requireNonNull(bindings);
    }

    /**
     * A saves/reads service (bulk streams included); delete, list and purge
     * answer UNIMPLEMENTED until their coordinators are wired.
     *
     * @param saves the document saves coordinator
     * @param reads the document reads coordinator
     * @param bindings resolves the trusted authentication result into repository identity
     */
    public AccessionGrpcService(DocumentSaves saves, DocumentReads reads,
            Function<AuthenticatedCaller, RepositoryCaller> bindings) {
        this(saves, reads, null, null, null, bindings);
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
        if (deletes == null) { observer.onError(Status.UNIMPLEMENTED.asRuntimeException()); return; }
        respond(observer, () -> deletes.delete(caller(), request));
    }

    @Override public void listDocuments(ListDocumentsRequest request, StreamObserver<ListDocumentsResponse> observer) {
        if (lists == null) { observer.onError(Status.UNIMPLEMENTED.asRuntimeException()); return; }
        respond(observer, () -> lists.list(caller(), request));
    }

    @Override public void purgeDocument(ai.protomolt.proto.repo.v2.PurgeDocumentRequest request,
            StreamObserver<ai.protomolt.proto.repo.v2.PurgeDocumentResponse> observer) {
        if (purges == null) { observer.onError(Status.UNIMPLEMENTED.asRuntimeException()); return; }
        respond(observer, () -> purges.purge(caller(), request));
    }

    @Override public void readDocument(ReadDocumentRequest request, StreamObserver<ReadDocumentResponse> observer) {
        var caller = streamCaller(observer);
        if (caller == null) return;
        try {
            reads.readDocument(caller, request, frame -> {
                synchronized (observer) {
                    observer.onNext(frame);
                }
            });
        } catch (AccessionException failure) {
            observer.onError(toStatus(failure));
            return;
        } catch (RuntimeException failure) {
            observer.onError(internal(failure));
            return;
        }
        observer.onCompleted();
    }

    @Override public StreamObserver<SaveDocumentsRequest> saveDocuments(StreamObserver<SaveDocumentsResponse> observer) {
        var caller = streamCaller(observer);
        if (caller == null) return noOp();
        return new StreamObserver<>() {
            private final Set<String> seen = new HashSet<>();
            private boolean terminated;

            @Override public void onNext(SaveDocumentsRequest item) {
                synchronized (observer) {
                    if (terminated || cancelled(observer)) return;
                    if (item.getRequestId().isBlank()) {
                        terminate("A bulk save item needs a non-blank request_id");
                        return;
                    }
                    if (!item.hasSave()) {
                        terminate("Bulk save item " + item.getRequestId() + " carries no save");
                        return;
                    }
                    if (!seen.add(item.getRequestId())) {
                        terminate("Duplicate request_id " + item.getRequestId() + " on one bulk save stream");
                        return;
                    }
                    try {
                        var saved = saves.save(caller, item.getSave());
                        observer.onNext(SaveDocumentsResponse.newBuilder().setRequestId(item.getRequestId())
                                .setOperationId(saved.getOperationId()).setSaved(saved).build());
                    } catch (AccessionException failure) {
                        observer.onNext(SaveDocumentsResponse.newBuilder().setRequestId(item.getRequestId())
                                .setOperationId(item.getSave().getOperationId()).setError(itemError(failure)).build());
                    } catch (RuntimeException failure) {
                        terminated = true;
                        observer.onError(internal(failure));
                    }
                }
            }

            private void terminate(String message) {
                terminated = true;
                observer.onError(Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
            }

            @Override public void onError(Throwable failure) { terminated = true; }

            @Override public void onCompleted() {
                synchronized (observer) {
                    if (terminated) return;
                    terminated = true;
                    observer.onCompleted();
                }
            }
        };
    }

    @Override public StreamObserver<ReadDocumentsRequest> readDocuments(StreamObserver<ReadDocumentsResponse> observer) {
        var caller = streamCaller(observer);
        if (caller == null) return noOp();
        return new StreamObserver<>() {
            private final Set<String> seen = new HashSet<>();
            private boolean terminated;

            @Override public void onNext(ReadDocumentsRequest item) {
                synchronized (observer) {
                    if (terminated || cancelled(observer)) return;
                    if (item.getRequestId().isBlank()) {
                        terminate("A bulk read item needs a non-blank request_id");
                        return;
                    }
                    if (!item.hasRead()) {
                        terminate("Bulk read item " + item.getRequestId() + " carries no read");
                        return;
                    }
                    if (!seen.add(item.getRequestId())) {
                        terminate("Duplicate request_id " + item.getRequestId() + " on one bulk read stream");
                        return;
                    }
                    try {
                        reads.readDocument(caller, item.getRead(), frame -> emit(ReadDocumentsResponse.newBuilder()
                                .setRequestId(item.getRequestId()).setFrame(frame).build()));
                    } catch (AccessionException failure) {
                        emit(ReadDocumentsResponse.newBuilder().setRequestId(item.getRequestId())
                                .setError(itemError(failure)).build());
                    } catch (RuntimeException failure) {
                        terminated = true;
                        observer.onError(internal(failure));
                    }
                }
            }

            /** Observer emissions are serialized; the read loop re-enters this lock on the same thread. */
            private void emit(ReadDocumentsResponse response) {
                synchronized (observer) {
                    if (!terminated) observer.onNext(response);
                }
            }

            private void terminate(String message) {
                terminated = true;
                observer.onError(Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
            }

            @Override public void onError(Throwable failure) { terminated = true; }

            @Override public void onCompleted() {
                synchronized (observer) {
                    if (terminated) return;
                    terminated = true;
                    observer.onCompleted();
                }
            }
        };
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

    /**
     * The caller for a stream, captured once when the stream opens. A binding
     * failure fails the stream before any item is read; the returned null caller
     * means the response observer is already terminated.
     */
    private RepositoryCaller streamCaller(StreamObserver<?> observer) {
        try {
            return caller();
        } catch (AccessionException failure) {
            observer.onError(toStatus(failure));
            return null;
        } catch (RuntimeException failure) {
            observer.onError(internal(failure));
            return null;
        }
    }

    /** The refusal of one bulk item: the status code and detail the unary call would carry. */
    static ItemError itemError(AccessionException failure) {
        var error = ItemError.newBuilder().setCode(failure.code().value())
                .setMessage(Optional.ofNullable(failure.getMessage()).orElse(""));
        failure.detail().ifPresent(detail -> {
            if (detail instanceof RevisionConflict conflict) error.setConflict(conflict);
            else if (detail instanceof OperationReuse reuse) error.setReuse(reuse);
            else if (detail instanceof SaveRejection rejection) error.setRejection(rejection);
        });
        return error.build();
    }

    private static boolean cancelled(StreamObserver<?> observer) {
        return observer instanceof ServerCallStreamObserver<?> server && server.isCancelled();
    }

    private static StatusRuntimeException internal(RuntimeException failure) {
        return Status.INTERNAL.withDescription("Repository failure").withCause(failure).asRuntimeException();
    }

    /** The request observer of a stream that already failed: nothing more may reach it. */
    private static <T> StreamObserver<T> noOp() {
        return new StreamObserver<>() {
            @Override public void onNext(T value) {}
            @Override public void onError(Throwable failure) {}
            @Override public void onCompleted() {}
        };
    }

    private static <T> void respond(StreamObserver<T> observer, Supplier<T> call) {
        final T response;
        try {
            response = call.get();
        } catch (AccessionException failure) {
            observer.onError(toStatus(failure));
            return;
        } catch (RuntimeException failure) {
            observer.onError(internal(failure));
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
