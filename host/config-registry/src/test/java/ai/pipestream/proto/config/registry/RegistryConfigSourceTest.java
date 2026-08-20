package ai.pipestream.proto.config.registry;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.config.ConfigSource;
import ai.pipestream.proto.config.DistributedConfig;
import ai.pipestream.proto.registry.ConfigSupport;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.SchemaReference;
import ai.pipestream.proto.registry.server.SchemaRegistryServer;
import ai.pipestream.proto.registry.server.SchemaRegistryServerConfig;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The registry plug end to end: a schema with declared rules registers,
 * the registry's config gate refuses a document violating those rules
 * and versions a valid one by commit, the source reads the typed bytes
 * back, and the consumer applies them with the commit as evidence. The
 * whole lane is the house stack — git store, native HTTP surface,
 * runtime-compiled schema, validate.v1 rules — eating our own lunch.
 */
class RegistryConfigSourceTest {

    private static final String THROTTLE_SUBJECT = "configit/v1/throttle.proto";
    private static final String THROTTLE = """
            syntax = "proto3";
            package configit.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Throttle {
              int32 limit = 1 [
                (ai.pipestream.proto.validate.v1.field) = {
                  required: true
                  int32: {gt: 0, lte: 1000}
                }
              ];
              string label = 2;
            }
            """;

    @TempDir
    static Path work;

    static GitSchemaRegistryStore store;
    static SchemaRegistryServer server;
    static String base;
    static HttpClient http;

    @BeforeAll
    static void boot() throws Exception {
        store = GitSchemaRegistryStore.builder()
                .repositoryDir(work.resolve("registry"))
                .build();
        String validateSubject = "ai/pipestream/proto/validate/v1/validate.proto";
        String validateSource;
        try (var in = RegistryConfigSourceTest.class.getClassLoader()
                .getResourceAsStream(validateSubject)) {
            validateSource = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        store.register(validateSubject, validateSource, List.of());
        store.register(THROTTLE_SUBJECT, THROTTLE, List.of(
                new SchemaReference(validateSubject, validateSubject, 1)));

        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withPort(0), store);
        int port = server.start();
        base = "http://127.0.0.1:" + port + "/protomolt";
        http = HttpClient.newHttpClient();
    }

    @AfterAll
    static void shutdown() throws Exception {
        if (server != null) {
            server.close();
        }
        if (store != null) {
            store.close();
        }
    }

    static HttpResponse<String> put(String name, String envelope) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base + "/configs/" + name))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(envelope))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void theGateChecksTheSourceReadsAndTheConsumerAppliesWithTheCommitAsEvidence()
            throws Exception {
        // A document violating the type's own declared rules never
        // reaches Git.
        HttpResponse<String> refused = put("parse-throttle", """
                {"messageType": "configit.v1.Throttle", "config": {"limit": 0}}""");
        assertThat(refused.statusCode()).isEqualTo(422);
        assertThat(refused.body()).contains("limit");
        // The refusal happened at the service: nothing reached Git.
        assertThat(store.config("parse-throttle")).isEmpty();

        // An unregistered type refuses naming it.
        HttpResponse<String> unknown = put("parse-throttle", """
                {"messageType": "configit.v1.Nope", "config": {"limit": 5}}""");
        assertThat(unknown.statusCode()).isEqualTo(422);
        assertThat(unknown.body()).contains("configit.v1.Nope");

        HttpResponse<String> accepted = put("parse-throttle", """
                {"messageType": "configit.v1.Throttle",
                 "config": {"limit": 25, "label": "steady"}}""");
        assertThat(accepted.statusCode()).isEqualTo(200);
        assertThat(accepted.body()).contains("version");

        RegistryConfigSource source = new RegistryConfigSource(base, null);
        Optional<ConfigSource.Fetched> fetched = source.fetch("parse-throttle");
        assertThat(fetched).isPresent();
        assertThat(fetched.orElseThrow().version()).isNotBlank();

        Descriptor throttle = ConfigSupport
                .resolveType(store, "configit.v1.Throttle").orElseThrow();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            DistributedConfig.Subscription<DynamicMessage> subscription =
                    config.subscribe("parse-throttle",
                            DynamicMessage.getDefaultInstance(throttle));
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(outcome.applied()).containsExactly("parse-throttle");
            DynamicMessage applied = subscription.current().orElseThrow().config();
            assertThat(applied.getField(throttle.findFieldByName("limit"))).isEqualTo(25);
            assertThat(applied.getField(throttle.findFieldByName("label")))
                    .isEqualTo("steady");
            assertThat(subscription.current().orElseThrow().version())
                    .isEqualTo(fetched.orElseThrow().version());

            // A second put is a new commit: the consumer follows.
            put("parse-throttle", """
                    {"messageType": "configit.v1.Throttle",
                     "config": {"limit": 50, "label": "faster"}}""");
            DistributedConfig.RefreshOutcome followed = config.refresh();
            assertThat(followed.applied()).containsExactly("parse-throttle");
            assertThat(subscription.current().orElseThrow().version())
                    .isNotEqualTo(fetched.orElseThrow().version());
            assertThat(subscription.current().orElseThrow().config()
                    .getField(throttle.findFieldByName("limit"))).isEqualTo(50);
        }
    }

    @Test
    void aMissingDocumentIsEmptinessNeverAnError() throws Exception {
        RegistryConfigSource source = new RegistryConfigSource(base, null);
        assertThat(source.fetch("no-such-config")).isEmpty();
    }

    @Test
    void aHandEditedRepositoryServesARefusalNeverAnInvalidDocument() throws Exception {
        // Straight into the store, past the service: the way a hand edit or a
        // bad federation merge would land. The read gate still refuses.
        store.putConfig("hand-edited",
                "{\"messageType\": \"configit.v1.Throttle\", \"config\": {\"limit\": 0}}");
        HttpResponse<String> served = http.send(
                HttpRequest.newBuilder(URI.create(base + "/configs/hand-edited")).GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(served.statusCode()).isEqualTo(422);
        assertThat(served.body()).contains("no longer gates");
        RegistryConfigSource source = new RegistryConfigSource(base, null);
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> source.fetch("hand-edited"))
                .hasMessageContaining("422");
    }

    @Test
    void versionsAreVersionsOfTheDocumentNotOfTheRepository() throws Exception {
        String versionA = store.putConfig("doc-a",
                "{\"messageType\": \"configit.v1.Throttle\", \"config\": {\"limit\": 1}}");
        store.putConfig("doc-b",
                "{\"messageType\": \"configit.v1.Throttle\", \"config\": {\"limit\": 2}}");
        assertThat(store.configVersion("doc-a")).contains(versionA);
        assertThat(store.configVersion("doc-b")).isPresent();
        assertThat(store.configVersion("doc-b").orElseThrow()).isNotEqualTo(versionA);
        assertThat(store.configs()).contains("doc-a", "doc-b");
    }
}
