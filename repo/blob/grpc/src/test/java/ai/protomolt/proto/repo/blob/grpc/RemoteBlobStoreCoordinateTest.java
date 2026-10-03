package ai.protomolt.proto.repo.blob.grpc;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.time.Duration;
import java.io.InputStream;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteBlobStoreCoordinateTest {
    @Test void rejectsMismatchedCoordinatesBeforeRpcOrStreamRead() throws Exception {
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).directExecutor().build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            var store = new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel), java.util.Map.of("local", "remote"), Duration.ofSeconds(5));
            var wrong = new BlobStore.PutSpec("other", "key", null, null, null);
            assertThatThrownBy(() -> store.put(wrong, new byte[0])).isInstanceOf(IllegalArgumentException.class);
            InputStream unreadable = new InputStream() {
                @Override public int read() { throw new AssertionError("wrong coordinate must not consume stream"); }
            };
            assertThatThrownBy(() -> store.put(wrong, unreadable, 0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.get("other", "key")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.getForUpdate("other", "key")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.conditionalPut(wrong, new byte[0], BlobStore.WriteCondition.absent()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.delete("other", "key")).isInstanceOf(IllegalArgumentException.class);
            // Check destination before reading the source.
            assertThatThrownBy(() -> store.copy("local", "key", "other", "copy"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.get("local", "key"))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
        } finally {
            channel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
}
