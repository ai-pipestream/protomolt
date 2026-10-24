package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationResponse;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationResponse;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * {@code ArchiveMutationService} over v2 documents (docs/design/archive.md,
 * "The archive over v2 documents"): {@code DeleteEntry} maps to
 * {@code DeleteDocument} (a tombstone revision) and {@code GetArchiveMutation}
 * maps to {@code GetOperationOutcome}. {@code DeleteRendition} and
 * {@code PruneVersions} have no v2 counterpart under keep-forever retention and
 * are refused by name with {@code FAILED_PRECONDITION}.
 */
public final class DocumentArchiveMutationGrpcService
        extends ArchiveMutationServiceGrpc.ArchiveMutationServiceImplBase {

    private final DocumentArchive archive;

    /**
     * Constructs a {@link DocumentArchiveMutationGrpcService} backed by a new
     * {@link DocumentArchive} over {@code documents}.
     *
     * @param documents the channel to the v2 document service the archive is stored through
     * @param bindings the archive definitions
     * @param clock stamps {@code observed_at} fallback timestamps when needed
     */
    public DocumentArchiveMutationGrpcService(
            Channel documents, ArchiveBindings bindings, Clock clock) {
        this(new DocumentArchive(documents, bindings, clock));
    }

    /**
     * Constructs a {@link DocumentArchiveMutationGrpcService} sharing an existing
     * {@link DocumentArchive}.
     *
     * @param archive the {@link DocumentArchive} engine
     */
    public DocumentArchiveMutationGrpcService(DocumentArchive archive) {
        this.archive = Objects.requireNonNull(archive, "archive");
    }

    @Override
    public void archiveMutation(
            ArchiveMutationRequest request, StreamObserver<ArchiveMutationResponse> responses) {
        unary(() -> archive.archiveMutation(request), responses);
    }

    @Override
    public void getArchiveMutation(
            GetArchiveMutationRequest request, StreamObserver<GetArchiveMutationResponse> responses) {
        unary(() -> archive.getArchiveMutation(request), responses);
    }

    private static <T> void unary(Supplier<T> call, StreamObserver<T> responses) {
        T response;
        try {
            response = call.get();
        } catch (StatusRuntimeException failure) {
            responses.onError(failure);
            return;
        }
        responses.onNext(response);
        responses.onCompleted();
    }
}
