package ai.pipestream.proto.schema.confluent;

import ai.pipestream.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the two Confluent-compatible loaders against a live registry. Run once
 * per endpoint via the concrete subclasses (Apicurio ccompat and real Confluent Schema
 * Registry).
 *
 * <p>Start the registries with {@code docker compose -f docker-compose.integration.yml up -d}
 * (repo root). When the registry is not reachable these tests skip via JUnit assumptions, so
 * {@code ./gradlew build} stays green without containers.</p>
 *
 * <p>Two loaders, two protocols: {@link ConfluentSchemaRegistryLoader} speaks the Schema
 * Registry subjects REST API (schema text + references, compiled to descriptors at load time),
 * exercised here with a subject graph that includes a schema reference and a well-known import.
 * {@link ConfluentDescriptorSource} remains the binary path: it performs a plain HTTP GET and
 * parses the response body as a compiled {@code FileDescriptorSet}, so it cannot consume the
 * subjects API responses (pinned below).</p>
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractConfluentCompatIntegrationTest {

    private static final String PERSON_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            message Person {
              string name = 1;
              int32 id = 2;
            }
            """;

    /**
     * References {@link #PERSON_PROTO} via the Schema Registry reference mechanism: the import
     * path {@code person.proto} is supplied as the reference {@code name}, resolving to the
     * person subject registered in setup.
     */
    private static final String TEAM_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            import "person.proto";
            import "google/protobuf/timestamp.proto";
            message Team {
              string name = 1;
              repeated Person members = 2;
              google.protobuf.Timestamp created = 3;
            }
            """;

    private final String subject = "pipestream-it-" + UUID.randomUUID().toString().substring(0, 8);
    private final String teamSubject = subject + "-team";
    private HttpClient http;

    /** Base URL of the Confluent-compatible API (no trailing slash). */
    abstract String registryBaseUrl();

    static String configuredUrl(String systemProperty, String envVariable, String defaultUrl) {
        String url = System.getProperty(systemProperty);
        if (url == null || url.isBlank()) {
            url = System.getenv(envVariable);
        }
        if (url == null || url.isBlank()) {
            url = defaultUrl;
        }
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    @BeforeAll
    void setUp() throws Exception {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        assumeTrue(registryReachable(),
                "Schema registry not reachable at " + registryBaseUrl() + " - skipping integration tests");
        registerProtobufSchema(subject, PERSON_PROTO, null);
        registerProtobufSchema(teamSubject, TEAM_PROTO,
                "[{\"name\":\"person.proto\",\"subject\":\"" + subject + "\",\"version\":1}]");
    }

    @AfterAll
    void cleanUp() {
        if (http == null) {
            return;
        }
        // Best-effort cleanup: soft-delete then permanently delete the per-run subjects.
        // The referencing subject goes first so the referenced one is free to delete.
        deleteSubject(teamSubject, false);
        deleteSubject(teamSubject, true);
        deleteSubject(subject, false);
        deleteSubject(subject, true);
    }

    private boolean registryReachable() {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(registryBaseUrl() + "/subjects"))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void registerProtobufSchema(String subjectName, String protoText, String referencesJson)
            throws IOException, InterruptedException {
        String references = referencesJson == null ? "" : ",\"references\":" + referencesJson;
        String body = "{\"schemaType\":\"PROTOBUF\",\"schema\":\"" + jsonEscape(protoText) + "\""
                + references + "}";
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(registryBaseUrl() + "/subjects/" + subjectName + "/versions"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("Failed to register subject " + subjectName
                    + ": HTTP " + response.statusCode() + " " + response.body());
        }
        assertThat(response.body()).contains("\"id\"");
    }

    private void deleteSubject(String subjectName, boolean permanent) {
        try {
            String suffix = permanent ? "?permanent=true" : "";
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create(registryBaseUrl() + "/subjects/" + subjectName + suffix))
                    .timeout(Duration.ofSeconds(5))
                    .DELETE()
                    .build();
            http.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException ignored) {
            // best effort
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String jsonEscape(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * Sanity check that the live endpoint really speaks the Confluent subjects protocol: the
     * schema registered in setup round-trips as .proto text. This is the API surface that
     * {@code ConfluentDescriptorSource} would need to consume to integrate directly.
     */
    @Test
    void registryServesRegisteredProtobufSchemaAsText() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(registryBaseUrl() + "/subjects/" + subject + "/versions/1/schema"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body())
                .contains("message Person")
                .contains("string name = 1");
    }

    /**
     * Pins that {@link ConfluentDescriptorSource} stays the binary-only path: pointing it at
     * the registry's own schema endpoint fails, because the registry serves .proto text while
     * the source expects a binary {@code FileDescriptorSet}. The subjects REST protocol is
     * covered by {@link ConfluentSchemaRegistryLoader} (see
     * {@link #loadsRegisteredSchemasDirectlyFromSubjectsApi()}).
     */
    @Test
    void descriptorSourceCannotConsumeSubjectsApiResponses() {
        URI schemaEndpoint = URI.create(
                registryBaseUrl() + "/subjects/" + subject + "/versions/1/schema");
        ConfluentDescriptorSource source = new ConfluentDescriptorSource(schemaEndpoint);
        assertThat(source.isAvailable()).isTrue();
        assertThatThrownBy(source::loadDescriptors)
                .isInstanceOf(DescriptorLoadException.class);
    }

    /**
     * Closest working path today: a compiled {@code FileDescriptorSet} equivalent to the schema
     * registered in the live registry, served over plain HTTP (what
     * {@code ConfluentDescriptorSource} actually consumes). A descriptor-set-exporting bridge in
     * front of the registry would serve exactly this.
     */
    @Test
    void loadsDescriptorSetServedOverPlainHttp() throws Exception {
        byte[] payload = personDescriptorSet().toByteArray();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/descriptor-set", exchange -> {
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.start();
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/descriptor-set");
            ConfluentDescriptorSource source = new ConfluentDescriptorSource(uri);

            List<FileDescriptor> descriptors = source.loadDescriptors();
            assertThat(descriptors).hasSize(1);
            Descriptor person = descriptors.getFirst().findMessageTypeByName("Person");
            assertThat(person).isNotNull();
            assertThat(person.findFieldByName("name").getType()).isEqualTo(FieldDescriptor.Type.STRING);
            assertThat(person.findFieldByName("id").getNumber()).isEqualTo(2);

            assertThat(source.loadDescriptor("person.proto")).isNotNull();
            assertThat(source.loadDescriptor("missing.proto")).isNull();
        } finally {
            server.stop(0);
        }
    }

    /**
     * End-to-end subjects-API integration:
     * {@link ConfluentSchemaRegistryLoader} lists the subjects, fetches PROTOBUF schema text
     * plus references directly from the registry base URL, and compiles them to usable runtime
     * descriptors — including the cross-subject reference ({@code Team.members} resolves to the
     * {@code Person} registered under another subject) and the well-known
     * {@code google/protobuf/timestamp.proto} import.
     */
    @Test
    void loadsRegisteredSchemasDirectlyFromSubjectsApi() throws Exception {
        ConfluentSchemaRegistryLoader loader =
                new ConfluentSchemaRegistryLoader(URI.create(registryBaseUrl()));
        assertThat(loader.isAvailable()).isTrue();

        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors)
                .anySatisfy(fd -> assertThat(fd.findMessageTypeByName("Person")).isNotNull());

        // The registry may hold unrelated subjects (shared containers); find our Team schema.
        Descriptor team = descriptors.stream()
                .map(fd -> fd.findMessageTypeByName("Team"))
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Team schema not loaded from " + teamSubject));
        assertThat(team.findFieldByName("name").getType()).isEqualTo(FieldDescriptor.Type.STRING);

        FieldDescriptor members = team.findFieldByName("members");
        assertThat(members.isRepeated()).isTrue();
        assertThat(members.getMessageType().getFullName()).isEqualTo("pipestream.it.v1.Person");
        assertThat(members.getMessageType().findFieldByName("id").getNumber()).isEqualTo(2);

        assertThat(team.findFieldByName("created").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Timestamp");

        // On-demand lookup scans the loaded subjects by file or message name.
        FileDescriptor person = loader.loadDescriptor("pipestream.it.v1.Person");
        assertThat(person).isNotNull();
        assertThat(person.findMessageTypeByName("Person").findFieldByName("name")).isNotNull();
    }

    /** In-test equivalent of protoc output for {@link #PERSON_PROTO}. */
    private static FileDescriptorSet personDescriptorSet() {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("person.proto")
                .setSyntax("proto3")
                .setPackage("pipestream.it.v1")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Person")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("name")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("id")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptorSet.newBuilder().addFile(file).build();
    }
}
