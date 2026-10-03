package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import static org.assertj.core.api.Assertions.*;

/** Fault injection at the SDK stream boundary, not a successful storage substitute. */
class S3BoundedReadStreamTest {
    @Test void unknownLengthConsumesOnlyLimitAndSentinelThenCloses() {
        var consumed = new AtomicInteger();
        var closed = new AtomicInteger();
        var aborted = new AtomicInteger();
        var body = new InputStream() {
            @Override public int read() { consumed.incrementAndGet(); return 0; }
            @Override public void close() { closed.incrementAndGet(); }
        };
        assertThatThrownBy(() -> read(null, body, 17, aborted)).isInstanceOf(BlobStore.BlobReadLimitException.class);
        assertThat(consumed.get()).isEqualTo(18);
        assertThat(closed.get()).isPositive();
        assertThat(aborted.get()).isEqualTo(1);
    }

    @Test void negativeProviderLengthFailsBeforeBodyRead() {
        var body = new InputStream() {
            @Override public int read() { throw new AssertionError("Malformed length must fail before reading"); }
        };
        assertThatThrownBy(() -> read(-1L, body, 10)).isInstanceOfSatisfying(BlobStoreException.class,
                e -> assertThat(e.code()).isEqualTo(BlobStoreException.Code.DATA_LOSS));
    }

    @Test void prematureEofDoesNotReturnPartialSuccess() {
        assertThatThrownBy(() -> read(3L, new ByteArrayInputStream(new byte[2]), 3))
                .isInstanceOfSatisfying(BlobStoreException.class,
                        e -> assertThat(e.code()).isEqualTo(BlobStoreException.Code.DATA_LOSS));
    }

    @Test void falseSmallLengthDoesNotBypassConsumptionLimit() {
        assertThatThrownBy(() -> read(1L, new ByteArrayInputStream(new byte[20]), 10))
                .isInstanceOf(BlobStore.BlobReadLimitException.class);
    }

    private static BlobStore.GetResult read(Long length, InputStream body, int limit) {
        return read(length, body, limit, new AtomicInteger());
    }

    private static BlobStore.GetResult read(Long length, InputStream body, int limit, AtomicInteger aborted) {
        var response = GetObjectResponse.builder().contentLength(length).build();
        var sdk = (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getObject") && args.length == 1)
                        return new ResponseInputStream<>(response,
                                software.amazon.awssdk.http.AbortableInputStream.create(body, aborted::incrementAndGet));
                    throw new AssertionError("Unexpected SDK method: " + method.getName());
                });
        return new S3BlobStore(sdk).getBounded("bucket", "key", null, limit);
    }
}
