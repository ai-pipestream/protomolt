package ai.pipestream.proto.serve;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code --registry-git} pointed at a directory that does not exist yet: the store creates
 * and initializes the repository on first use, so first startup needs no {@code git init}.
 */
class RegistryAutoInitTest {

    @Test
    void aFreshPathBecomesAWorkingRegistry(@TempDir Path tmp) throws Exception {
        Path registryDir = tmp.resolve("brand-new/schemas");
        assertThat(Files.exists(registryDir)).isFalse();

        try (ProtoMoltServe serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, registryDir, 0))) {
            assertThat(serve.registryPort()).isPositive();
            assertThat(Files.isDirectory(registryDir.resolve(".git"))).isTrue();

            HttpResponse<String> subjects = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(
                            "http://127.0.0.1:" + serve.registryPort() + "/subjects")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(subjects.statusCode()).isEqualTo(200);
            assertThat(subjects.body().trim()).isEqualTo("[]");
        }
    }
}
