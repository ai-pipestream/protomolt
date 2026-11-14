package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceResponse;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesResponse;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * gRPC {@code RepositorySinkService} adapter delegating capability discovery, publication,
 * durable job control, and provenance listing to {@link RepositorySink}.
 */
public final class RepositorySinkGrpcService
        extends RepositorySinkServiceGrpc.RepositorySinkServiceImplBase {

    private final RepositorySink sink;

    /**
     * Constructs a {@link RepositorySinkGrpcService} backed by {@code sink}.
     *
     * @param sink the repository sink coordinator
     */
    public RepositorySinkGrpcService(RepositorySink sink) {
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /** Returns the underlying {@link RepositorySink} coordinator. */
    public RepositorySink sink() {
        return sink;
    }

    @Override
    public void listSaveAsCapabilities(
            ListSaveAsCapabilitiesRequest request,
            StreamObserver<ListSaveAsCapabilitiesResponse> responses) {
        unary(() -> sink.listSaveAsCapabilities(request), responses);
    }

    @Override
    public void publishRepositorySink(
            PublishRepositorySinkRequest request,
            StreamObserver<PublishRepositorySinkResponse> responses) {
        unary(() -> sink.publish(request), responses);
    }

    @Override
    public void getRepositorySinkJob(
            GetRepositorySinkJobRequest request,
            StreamObserver<GetRepositorySinkJobResponse> responses) {
        unary(() -> sink.getJob(request), responses);
    }

    @Override
    public void cancelRepositorySinkJob(
            CancelRepositorySinkJobRequest request,
            StreamObserver<CancelRepositorySinkJobResponse> responses) {
        unary(() -> sink.cancelJob(request), responses);
    }

    @Override
    public void listRenditionProvenance(
            ListRenditionProvenanceRequest request,
            StreamObserver<ListRenditionProvenanceResponse> responses) {
        unary(() -> sink.listRenditionProvenance(request), responses);
    }

    private static <T> void unary(Supplier<T> call, StreamObserver<T> responses) {
        T response;
        try {
            response = call.get();
        } catch (StatusRuntimeException failure) {
            responses.onError(failure);
            return;
        } catch (RuntimeException unexpected) {
            responses.onError(
                    Status.INTERNAL
                            .withDescription("Unexpected repository sink failure")
                            .withCause(unexpected)
                            .asRuntimeException());
            return;
        }
        responses.onNext(response);
        responses.onCompleted();
    }
}
