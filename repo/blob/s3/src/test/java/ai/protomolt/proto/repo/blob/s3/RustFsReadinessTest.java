package ai.protomolt.proto.repo.blob.s3;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RustFsReadinessTest {
    @Test void waitsThroughStartup503UntilAuthenticatedS3Responds() throws Exception {
        var attempts = new AtomicInteger();
        try (var client = client(() -> {
            if (attempts.incrementAndGet() < 3) throw unavailable();
            return ListBucketsResponse.builder().build();
        })) {
            RustFsReadiness.awaitServing(client, Duration.ofSeconds(5));
        }
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test void refusesAuthenticationErrorsWithoutRetrying() {
        var attempts = new AtomicInteger();
        var denied = S3Exception.builder().statusCode(403).message("wrong fixture credentials").build();
        try (var client = client(() -> { attempts.incrementAndGet(); throw denied; })) {
            assertThatThrownBy(() -> RustFsReadiness.awaitServing(client, Duration.ofSeconds(5)))
                    .isSameAs(denied);
        }
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test void persistentUnavailabilityFailsWithTheProviderCause() {
        var pending = unavailable();
        try (var client = client(() -> { throw pending; })) {
            assertThatThrownBy(() -> RustFsReadiness.awaitServing(client, Duration.ZERO))
                    .isSameAs(pending);
        }
    }

    private static S3Exception unavailable() {
        var builder = S3Exception.builder();
        builder.statusCode(503).message("S3 subsystems starting");
        return (S3Exception) builder.build();
    }

    private static S3Client client(Supplier<ListBucketsResponse> response) {
        return (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[]{S3Client.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "listBuckets" -> response.get();
                    case "close" -> null;
                    default -> throw new AssertionError("Unexpected fixture call: " + method.getName());
                });
    }
}
