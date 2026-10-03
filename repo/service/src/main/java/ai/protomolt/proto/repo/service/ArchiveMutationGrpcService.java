package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationResponse;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationResponse;
import ai.protomolt.proto.repo.spi.ArchiveMutationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import io.grpc.stub.StreamObserver;

/** Optional transport; the host owns authentication and recovery scheduling. */
public final class ArchiveMutationGrpcService extends ArchiveMutationServiceGrpc.ArchiveMutationServiceImplBase {
    private final ArchiveMutationRepository operations;

    public ArchiveMutationGrpcService(ArchiveMutationRepository operations) { this.operations = java.util.Objects.requireNonNull(operations); }

    private static RepositoryCaller caller() {
        if (io.grpc.Context.current().isCancelled()) throw io.grpc.Status.CANCELLED.asRuntimeException();
        var caller = ai.protomolt.proto.authz.grpc.CallerContexts.CALLER.get();
        if (caller == null) throw io.grpc.Status.UNAUTHENTICATED.withDescription("A trusted archive caller is required").asRuntimeException();
        return new RepositoryCaller(caller.name(), caller.unrestricted());
    }

    @Override public void archiveMutation(ArchiveMutationRequest request, StreamObserver<ArchiveMutationResponse> observer) {
        GrpcErrors.run(observer, () -> ArchiveMutationResponse.newBuilder().setReceipt(operations.mutateArchive(caller(), request)).build());
    }

    @Override public void getArchiveMutation(GetArchiveMutationRequest request, StreamObserver<GetArchiveMutationResponse> observer) {
        GrpcErrors.run(observer, () -> GetArchiveMutationResponse.newBuilder().setReceipt(operations.getArchiveMutation(caller(), request)).build());
    }
}
