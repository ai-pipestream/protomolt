package ai.protomolt.proto.repo.service.client;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.BindableService;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteBlobStoreConditionalTest {
    private static final String BUCKET = "ignored";
    private static final String KEY = "state/current";

    @Test void oldServerCannotSilentlyFallBackToUnconditionalOperations() throws Exception {
        withService(new DocumentServiceGrpc.DocumentServiceImplBase() {}, store -> {
            assertThatThrownBy(() -> store.getForUpdate(BUCKET, KEY))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> store.conditionalPut(spec(), new byte[0],
                    BlobStore.WriteCondition.absent()))
                    .isInstanceOf(UnsupportedOperationException.class);
        });
    }

    @Test void mismatchedSuccessfulResponsesCannotBecomeBackingTokens() throws Exception {
        ConditionalBlobKey address = ConditionalBlobKey.newBuilder()
                .setDriveName("drive").setObjectKey(KEY).build();
        ConditionalBlobVersion wrongDigest = ConditionalBlobVersion.newBuilder()
                .setKey(address).setEtag("\"tag\"").setSizeBytes(6)
                .setSha256("0".repeat(64)).build();
        withService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override public void getBlobForUpdate(GetBlobForUpdateRequest request,
                    StreamObserver<GetBlobForUpdateResponse> observer) {
                observer.onNext(GetBlobForUpdateResponse.newBuilder().setVersion(wrongDigest)
                        .setData(ByteString.copyFromUtf8("actual")).build());
                observer.onCompleted();
            }
            @Override public void compareAndPutBlob(CompareAndPutBlobRequest request,
                    StreamObserver<CompareAndPutBlobResponse> observer) {
                observer.onNext(CompareAndPutBlobResponse.newBuilder().setVersion(wrongDigest)
                        .build());
                observer.onCompleted();
            }
        }, store -> {
            assertThatThrownBy(() -> store.getForUpdate(BUCKET, KEY))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("differs");
            assertThatThrownBy(() -> store.conditionalPut(spec(), "actual".getBytes(),
                    BlobStore.WriteCondition.absent()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("differs");
        });
    }

    @Test void invalidOutgoingMimeTypeIsRejectedBeforeRpc() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        withService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override public void compareAndPutBlob(CompareAndPutBlobRequest request,
                    StreamObserver<CompareAndPutBlobResponse> observer) {
                called.set(true);
                observer.onError(Status.INTERNAL.asRuntimeException());
            }
        }, store -> assertThatThrownBy(() -> store.conditionalPut(
                new BlobStore.PutSpec(BUCKET, KEY, "x".repeat(1025), null, null),
                new byte[0], BlobStore.WriteCondition.absent()))
                .isInstanceOf(IllegalArgumentException.class));
        org.assertj.core.api.Assertions.assertThat(called).isFalse();
    }

    private static BlobStore.PutSpec spec() {
        return new BlobStore.PutSpec(BUCKET, KEY, "application/octet-stream", null, null);
    }

    private static void withService(BindableService service, Consumer<RemoteBlobStore> check)
            throws Exception {
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(service).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            check.accept(new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel), "drive"));
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
