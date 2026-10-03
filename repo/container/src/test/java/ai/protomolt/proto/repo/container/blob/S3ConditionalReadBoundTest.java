package ai.protomolt.proto.repo.container.blob;

import ai.protomolt.proto.repo.blob.spi.BlobStore;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ConditionalReadBoundTest {

    @Test
    void conditionalOperationsAreDisabledByDefaultBeforeAnyS3Request() {
        AtomicInteger requests = new AtomicInteger();
        S3Client client = (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(),
                new Class<?>[] { S3Client.class }, (proxy, method, args) -> {
                    requests.incrementAndGet();
                    throw new AssertionError("unexpected S3 request");
                });
        S3BlobStore store = new S3BlobStore(client);

        assertThatThrownBy(() -> store.getForUpdate("b", "k"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.conditionalPut(
                new BlobStore.PutSpec("b", "k", "application/octet-stream", null, null),
                new byte[] { 1 }, BlobStore.WriteCondition.absent()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(requests.get()).isZero();
    }

    @Test
    void knownOversizedObjectIsRejectedBeforeReadingItsBody() {
        CountingBody body = new CountingBody();
        GetObjectResponse response = response((long) BlobStore.MAX_CONDITIONAL_BYTES + 1);

        assertThatThrownBy(() -> new S3BlobStore(client(response, body), true).getForUpdate("b", "k"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds 9 MiB");
        assertThat(body.bytesRead.get()).isZero();
        assertThat(body.closed.get()).isTrue();
    }

    @Test
    void absentLengthCanReadAtMostTheLimitPlusOneByte() {
        CountingBody body = new CountingBody();

        assertThatThrownBy(() -> new S3BlobStore(client(response(null), body), true)
                .getForUpdate("b", "k"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds 9 MiB");
        assertThat(body.bytesRead.get()).isEqualTo(BlobStore.MAX_CONDITIONAL_BYTES + 1);
        assertThat(body.closed.get()).isTrue();
    }

    @Test
    void rejectsShortBodyAgainstDeclaredLength() {
        AtomicBoolean closed = new AtomicBoolean();
        InputStream body = new ByteArrayInputStream(new byte[] { 1, 2 }) {
            @Override public void close() {
                closed.set(true);
            }
        };

        assertThatThrownBy(() -> new S3BlobStore(client(response(3L), body), true)
                .getForUpdate("b", "k"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("length does not match");
        assertThat(closed.get()).isTrue();
    }

    @Test
    void invalidEtagAbortsBeforeReadingBody() {
        CountingBody body = new CountingBody();
        GetObjectResponse response = GetObjectResponse.builder().eTag("bare-tag")
                .contentLength((long) BlobStore.MAX_CONDITIONAL_BYTES + 10).build();

        assertThatThrownBy(() -> new S3BlobStore(client(response, body), true)
                .getForUpdate("b", "k"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("strong ETag");
        assertThat(body.bytesRead.get()).isZero();
        assertThat(body.closed.get()).isTrue();
    }

    private static GetObjectResponse response(Long length) {
        return GetObjectResponse.builder().eTag("\"etag\"").contentLength(length).build();
    }

    private static S3Client client(GetObjectResponse response, InputStream body) {
        return (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(),
                new Class<?>[] { S3Client.class }, (proxy, method, args) -> {
                    if (method.getName().equals("getObject") && args.length == 1) {
                        return new ResponseInputStream<>(response, body);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static final class CountingBody extends InputStream {
        private final AtomicInteger bytesRead = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public int read() {
            bytesRead.incrementAndGet();
            return 0;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            bytesRead.addAndGet(length);
            return length;
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
