package ai.protomolt.proto.repo.blob.grpc;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteBlobStoreDeadlineTest {
    @Test void renewsTimeoutForEachCallAndHonorsShorterCallerDeadline() throws Exception {
        var remaining = new ArrayList<Long>();
        var service = new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override public void getBlob(GetBlobRequest request, StreamObserver<GetBlobResponse> observer) {
                remaining.add(Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
                // Inject a transport failure, not a successful storage response.
                observer.onError(Status.UNAVAILABLE.asRuntimeException());
            }
        };
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            var stub = DocumentServiceGrpc.newBlockingStub(channel);
            var store = new RemoteBlobStore(stub, "drive", Duration.ofSeconds(1));
            failsUnavailable(store);
            // Expire the first call's timeout; the next call must acquire a new one.
            Thread.sleep(1100);
            failsUnavailable(store);
            assertThat(remaining).hasSize(2).allSatisfy(ms -> assertThat(ms).isBetween(1L, 1000L));
            var shorter = new RemoteBlobStore(stub.withDeadlineAfter(2, TimeUnit.SECONDS),
                    "drive", Duration.ofSeconds(10));
            failsUnavailable(shorter);
            assertThat(remaining.get(2)).isBetween(1L, 2000L);
            assertThatThrownBy(() -> new RemoteBlobStore(stub, "drive", Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new RemoteBlobStore(stub, "drive", Duration.ofSeconds(Long.MAX_VALUE)))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test void stalledTransportReturnsDeadlineExceeded() throws Exception {
        String name = InProcessServerBuilder.generateName();
        // Deliberately stall a real RPC to test cancellation; no storage success is synthesized.
        var service = new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override public void getBlob(GetBlobRequest request, StreamObserver<GetBlobResponse> observer) {}
        };
        var server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            var store = new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel),
                    "drive", Duration.ofMillis(100));
            assertThatThrownBy(() -> store.get("drive", "key"))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class,
                            error -> assertThat(error.code()).isEqualTo(ai.protomolt.proto.repo.blob.spi.BlobStoreException.Code.DEADLINE_EXCEEDED));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void failsUnavailable(BlobStore store) {
        assertThatThrownBy(() -> store.get("drive", "key"))
                .isInstanceOfSatisfying(ai.protomolt.proto.repo.blob.spi.BlobStoreException.class,
                        error -> assertThat(error.code()).isEqualTo(ai.protomolt.proto.repo.blob.spi.BlobStoreException.Code.UNAVAILABLE));
    }
}
