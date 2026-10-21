package ai.protomolt.proto.repo.service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class UploadHttpShutdownTest {
    @Test void blockedHttpHandlerPreventsResourceReleaseAndRestartUntilJoined() throws Exception {
        var entered = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var released = new AtomicBoolean();
        // Fault-only archive boundary: never fabricates a successful storage result.
        var archive = (ai.protomolt.proto.repo.spi.ArchiveRepository) java.lang.reflect.Proxy.newProxyInstance(
                UploadHttpShutdownTest.class.getClassLoader(),
                new Class<?>[] { ai.protomolt.proto.repo.spi.ArchiveRepository.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("uploadStream")) {
                        entered.countDown();
                        while (finish.getCount() != 0) {
                            try { finish.await(); }
                            catch (InterruptedException ignored) { /* Deliberately uncooperative handler. */ }
                        }
                        throw new IllegalStateException("Injected archive failure");
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        var server = new UploadHttpServer(null, null, "synthetic-http-operator-key", archive,
                RepositoryConsole.notConfigured());
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.start(0)
                + UploadHttpServer.ARCHIVE_UPLOAD_PATH + "?account_id=a&archive=ar&entry_id=e"))
                .header("api_token", "synthetic-http-operator-key")
                .POST(HttpRequest.BodyPublishers.ofString("x")).build();
        var response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> ShutdownBarrier.releaseAfter(List.of(
                    () -> server.close(Duration.ofMillis(50))), () -> released.set(true)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("executor did not terminate");
            assertThat(released).isFalse();
            assertThatThrownBy(() -> server.start(0)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> server.close(Duration.ofMillis(50)))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            finish.countDown();
            response.cancel(true);
            server.close();
            client.close();
        }
        ShutdownBarrier.releaseAfter(List.of(server), () -> released.set(true));
        assertThat(released).isTrue();
    }
}
