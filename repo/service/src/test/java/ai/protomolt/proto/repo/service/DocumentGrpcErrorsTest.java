package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.v1.DocumentPart;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The v1 document services keep the part-storage mapping that the shared
 * {@link GrpcErrors} no longer names, and map everything else the same way.
 */
class DocumentGrpcErrorsTest {

    @Test
    void partObjectMissingMapsToFailedPrecondition() {
        Throwable mapped = DocumentGrpcErrors.map(missing());
        Status status = Status.fromThrowable(mapped);
        assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(status.getDescription()).isEqualTo("manifest PRESENT but object gone");
    }

    @Test
    void sharedMapperLeavesPartObjectMissingToTheDocumentMapper() {
        Status status = Status.fromThrowable(GrpcErrors.map(missing()));
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
    }

    @Test
    void otherFailuresMapAsTheSharedMapperDoes() {
        StatusRuntimeException explicit = Status.ABORTED.withDescription("raced").asRuntimeException();
        assertThat(DocumentGrpcErrors.map(explicit)).isSameAs(explicit);
        assertSameStatus(new IllegalArgumentException("node_id must be a UUID"));
        assertSameStatus(new BlobStore.BlobNotFoundException("no object at bucket/k", null));
        assertSameStatus(new IllegalStateException("boom"));
    }

    @Test
    void runMapsAPartObjectMissingFailureOntoTheWire() {
        Throwable[] error = new Throwable[1];
        boolean[] completed = new boolean[1];
        DocumentGrpcErrors.run(new StreamObserver<String>() {
            @Override public void onNext(String value) { throw new AssertionError("no value expected"); }
            @Override public void onError(Throwable t) { error[0] = t; }
            @Override public void onCompleted() { completed[0] = true; }
        }, () -> { throw missing(); });
        assertThat(completed[0]).isFalse();
        assertThat(Status.fromThrowable(error[0]).getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    private static PartStorage.PartObjectMissingException missing() {
        return new PartStorage.PartObjectMissingException(
                "manifest PRESENT but object gone", Set.of(DocumentPart.DOCUMENT_PART_CORE));
    }

    private static void assertSameStatus(Throwable failure) {
        Status document = Status.fromThrowable(DocumentGrpcErrors.map(failure));
        Status shared = Status.fromThrowable(GrpcErrors.map(failure));
        assertThat(document.getCode()).isEqualTo(shared.getCode());
        assertThat(document.getDescription()).isEqualTo(shared.getDescription());
    }
}
