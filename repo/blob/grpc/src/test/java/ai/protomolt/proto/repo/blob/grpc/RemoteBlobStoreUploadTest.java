package ai.protomolt.proto.repo.blob.grpc;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteBlobStoreUploadTest {
    private static final BlobStore.PutSpec SPEC = new BlobStore.PutSpec("bucket", "key", null, null, null);

    @Test void refusesInvalidDeclaredLengthBeforeReading() throws Exception {
        withStore(store -> {
            InputStream unreadable = new InputStream() {
                @Override public int read() { throw new AssertionError("must not read invalid length"); }
            };
            assertThatThrownBy(() -> store.put(SPEC, unreadable, -1)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.put(SPEC, unreadable, 9L * 1024 * 1024 + 1))
                    .isInstanceOf(IllegalArgumentException.class);
        });
    }

    @Test void refusesShortAndLongBodiesWithoutCallingServer() throws Exception {
        withStore(store -> {
            assertThatThrownBy(() -> store.put(SPEC, new ByteArrayInputStream(new byte[1]), 2))
                    .isInstanceOf(IllegalArgumentException.class);
            var longBody = new ByteArrayInputStream(new byte[100]);
            assertThatThrownBy(() -> store.put(SPEC, longBody, 2)).isInstanceOf(IllegalArgumentException.class);
            assertThat(longBody.available()).isEqualTo(97);
        });
    }

    @Test void refusesOversizedArrayAndWrongDigestBeforeRpc() throws Exception {
        withStore(store -> {
            assertThatThrownBy(() -> store.put(SPEC, new byte[9 * 1024 * 1024 + 1]))
                    .isInstanceOf(IllegalArgumentException.class);
            var verified = new BlobStore.PutSpec("bucket", "key", null, null, "0".repeat(64));
            assertThatThrownBy(() -> store.put(verified, new byte[1])).isInstanceOf(IllegalArgumentException.class);
        });
    }

    @Test void boundsTheCompleteSerializedRequestAsWellAsThePayload() throws Exception {
        withStore(store -> {
            var oversizedHeader = new BlobStore.PutSpec("bucket", "k".repeat(10 * 1024 * 1024), null, null, null);
            assertThatThrownBy(() -> store.put(oversizedHeader, new byte[0]))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RPC limit");
        });
    }

    @Test void retainsReadFailureAndDoesNotCloseBorrowedStream() throws Exception {
        withStore(store -> {
            var failure = new IOException("injected read failure");
            InputStream input = new InputStream() {
                @Override public int read() throws IOException { throw failure; }
                @Override public void close() { throw new AssertionError("borrowed stream"); }
            };
            assertThatThrownBy(() -> store.put(SPEC, input, 1))
                    .isInstanceOf(UncheckedIOException.class).hasCause(failure);
        });
    }

    private static void withStore(java.util.function.Consumer<RemoteBlobStore> test) throws Exception {
        String name = InProcessServerBuilder.generateName();
        // Real transport with no operation installed: unexpected RPCs fail UNIMPLEMENTED.
        var server = InProcessServerBuilder.forName(name).directExecutor().build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try { test.accept(new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel), "drive")); }
        finally {
            channel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
}
