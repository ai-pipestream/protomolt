package ai.pipestream.proto.schema.confluent;

import ai.pipestream.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ConfluentSchemaRegistryLoader} against an in-process HTTP stub that
 * mimics the Confluent Schema Registry subjects REST API (same idiom as
 * {@link ConfluentDescriptorSourceTest}).
 */
class ConfluentSchemaRegistryLoaderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String PERSON_PROTO = """
            syntax = "proto3";
            package pipestream.unit;
            message Person {
              string name = 1;
              int32 id = 2;
              map<string, string> attributes = 3;
              oneof contact {
                string email = 4;
                string phone = 5;
              }
              Status status = 6;
              enum Status {
                STATUS_UNKNOWN = 0;
                STATUS_ACTIVE = 1;
              }
            }
            """;

    private static final String TEAM_PROTO = """
            syntax = "proto3";
            package pipestream.unit;
            import "person.proto";
            import "google/protobuf/timestamp.proto";
            message Team {
              string name = 1;
              repeated Person members = 2;
              google.protobuf.Timestamp created = 3;
            }
            """;

    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void reportsLoaderType() {
        assertThat(new ConfluentSchemaRegistryLoader(baseUri()).getLoaderType())
                .contains("Confluent");
    }

    @Test
    void loadsSubjectsWithReferencesAndWellKnownImports() throws Exception {
        stubPersonAndTeam();
        ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(baseUri());

        assertThat(loader.isAvailable()).isTrue();
        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).hasSize(2);
        assertThat(descriptors).extracting(FileDescriptor::getName)
                .containsExactlyInAnyOrder("person.proto", "team.proto");

        Descriptor person = byName(descriptors, "person.proto").findMessageTypeByName("Person");
        assertThat(person.findFieldByName("name").getType()).isEqualTo(FieldDescriptor.Type.STRING);
        assertThat(person.findFieldByName("attributes").isMapField()).isTrue();
        assertThat(person.getOneofs()).extracting(o -> o.getName()).contains("contact");
        assertThat(person.findFieldByName("status").getEnumType().findValueByName("STATUS_ACTIVE")
                .getNumber()).isEqualTo(1);

        Descriptor team = byName(descriptors, "team.proto").findMessageTypeByName("Team");
        FieldDescriptor members = team.findFieldByName("members");
        assertThat(members.isRepeated()).isTrue();
        assertThat(members.getMessageType().getFullName()).isEqualTo("pipestream.unit.Person");
        assertThat(members.getMessageType().findFieldByName("id").getNumber()).isEqualTo(2);
        assertThat(team.findFieldByName("created").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Timestamp");
    }

    @Test
    void skipsNonProtobufSubjects() throws Exception {
        serveJson("/subjects", subjects("person", "avro-thing", "json-thing"));
        serveJson("/subjects/person/versions/latest", schemaBody("person", 1, "PROTOBUF", PERSON_PROTO));
        // No schemaType field means AVRO in the Confluent protocol.
        ObjectNode avro = JSON.createObjectNode()
                .put("subject", "avro-thing").put("version", 1).put("id", 7)
                .put("schema", "{\"type\":\"record\",\"name\":\"R\",\"fields\":[]}");
        serveJson("/subjects/avro-thing/versions/latest", avro.toString());
        serveJson("/subjects/json-thing/versions/latest",
                schemaBody("json-thing", 1, "JSON", "{\"type\":\"object\"}"));

        List<FileDescriptor> descriptors = new ConfluentSchemaRegistryLoader(baseUri()).loadDescriptors();
        assertThat(descriptors).extracting(FileDescriptor::getName).containsExactly("person.proto");
    }

    @Test
    void skipsSubjectWithDanglingReference() throws Exception {
        serveJson("/subjects", subjects("person", "team"));
        serveJson("/subjects/person/versions/latest", schemaBody("person", 1, "PROTOBUF", PERSON_PROTO));
        // team references a subject the registry does not have -> 404 -> team skipped.
        serveJson("/subjects/team/versions/latest", schemaBody("team", 1, "PROTOBUF", TEAM_PROTO,
                reference("person.proto", "missing-subject", 1)));

        ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(baseUri());
        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).extracting(FileDescriptor::getName).containsExactly("person.proto");
        assertThat(loader.lastSkippedSubjectCount()).isEqualTo(1);
    }

    @Test
    void authFailureOnSubjectAbortsWholeLoad() throws Exception {
        serveJson("/subjects", subjects("person"));
        server.createContext("/subjects/person/versions/latest", exchange -> {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
        });
        assertThatThrownBy(() -> new ConfluentSchemaRegistryLoader(baseUri()).loadDescriptors())
                .isInstanceOf(DescriptorLoadException.class)
                .hasMessageContaining("401");
    }

    @Test
    void serverErrorOnSubjectAbortsWholeLoad() throws Exception {
        serveJson("/subjects", subjects("person"));
        server.createContext("/subjects/person/versions/latest", exchange -> {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        assertThatThrownBy(() -> new ConfluentSchemaRegistryLoader(baseUri()).loadDescriptors())
                .isInstanceOf(DescriptorLoadException.class)
                .hasMessageContaining("503");
    }

    @Test
    void requestTimeoutAbortsInsteadOfHangingForever() throws Exception {
        server.createContext("/subjects", exchange -> {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        try (ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(
                baseUri(), java.time.Duration.ofSeconds(1), java.time.Duration.ofMillis(250))) {
            assertThatThrownBy(loader::loadDescriptors)
                    .isInstanceOf(DescriptorLoadException.class);
        }
    }

    @Test
    void fullyQualifiedNameMatchBeatsEarlierSimpleNameMatch() throws Exception {
        // Subject "aaa" (listed first) has a message whose SIMPLE name is "Conflict";
        // subject "zzz" has a package-less message whose FULL name is "Conflict".
        String packaged = "syntax = \"proto3\";\npackage pkg;\nmessage Conflict { string a = 1; }\n";
        String packageless = "syntax = \"proto3\";\nmessage Conflict { string b = 1; }\n";
        serveJson("/subjects", subjects("aaa", "zzz"));
        serveJson("/subjects/aaa/versions/latest", schemaBody("aaa", 1, "PROTOBUF", packaged));
        serveJson("/subjects/zzz/versions/latest", schemaBody("zzz", 1, "PROTOBUF", packageless));

        ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(baseUri());
        assertThat(loader.loadDescriptor("Conflict").getName()).isEqualTo("zzz.proto");
        assertThat(loader.loadDescriptor("pkg.Conflict").getName()).isEqualTo("aaa.proto");
    }

    @Test
    void lookupUsesCachedRegistryUntilCleared() throws Exception {
        java.util.concurrent.atomic.AtomicInteger subjectsCalls = new java.util.concurrent.atomic.AtomicInteger();
        byte[] payload = subjects("person").getBytes(StandardCharsets.UTF_8);
        server.createContext("/subjects", exchange -> {
            subjectsCalls.incrementAndGet();
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        serveJson("/subjects/person/versions/latest", schemaBody("person", 1, "PROTOBUF", PERSON_PROTO));

        ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(baseUri());
        assertThat(loader.loadDescriptor("person.proto")).isNotNull();
        assertThat(loader.loadDescriptor("Person")).isNotNull();
        assertThat(subjectsCalls.get()).isEqualTo(1);

        loader.clearCache();
        assertThat(loader.loadDescriptor("person.proto")).isNotNull();
        assertThat(subjectsCalls.get()).isEqualTo(2);
    }

    @Test
    void guardsAgainstReferenceCycles() throws Exception {
        String protoA = "syntax = \"proto3\";\nimport \"b.proto\";\nmessage A { B b = 1; }\n";
        String protoB = "syntax = \"proto3\";\nimport \"a.proto\";\nmessage B { A a = 1; }\n";
        serveJson("/subjects", subjects("a", "b"));
        serveJson("/subjects/a/versions/latest",
                schemaBody("a", 1, "PROTOBUF", protoA, reference("b.proto", "b", 1)));
        serveJson("/subjects/a/versions/1",
                schemaBody("a", 1, "PROTOBUF", protoA, reference("b.proto", "b", 1)));
        serveJson("/subjects/b/versions/latest",
                schemaBody("b", 1, "PROTOBUF", protoB, reference("a.proto", "a", 1)));
        serveJson("/subjects/b/versions/1",
                schemaBody("b", 1, "PROTOBUF", protoB, reference("a.proto", "a", 1)));

        // Must terminate and skip the cyclic subjects rather than recurse forever.
        assertThat(new ConfluentSchemaRegistryLoader(baseUri()).loadDescriptors()).isEmpty();
    }

    @Test
    void loadsDescriptorByFileNameOrMessageName() throws Exception {
        stubPersonAndTeam();
        ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(baseUri());

        assertThat(loader.loadDescriptor("person.proto").getName()).isEqualTo("person.proto");
        assertThat(loader.loadDescriptor("pipestream.unit.Team").getName()).isEqualTo("team.proto");
        assertThat(loader.loadDescriptor("Person").getName()).isEqualTo("person.proto");
        assertThat(loader.loadDescriptor("missing.proto")).isNull();
    }

    @Test
    void subjectsEndpointErrorFails() {
        server.createContext("/subjects", exchange -> {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(baseUri());
        assertThat(loader.isAvailable()).isFalse();
        assertThatThrownBy(loader::loadDescriptors)
                .isInstanceOf(DescriptorLoadException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void unreachableRegistryIsNotAvailable() {
        server.stop(0);
        assertThat(new ConfluentSchemaRegistryLoader(baseUri()).isAvailable()).isFalse();
    }

    @Test
    void truncatedResponseIsRetriedOnceAndTheLoadSucceeds() throws Exception {
        // The registry's Jetty side closes idle connections under the JDK client's pool
        // timeout, and the next request dies mid-read. Simulate that class of failure: the
        // first /subjects response declares more bytes than it sends, so the client sees EOF
        // mid-body; the loader must retry once on a fresh connection and carry on.
        stubPersonAndTeam();
        server.removeContext("/subjects");
        byte[] payload = subjects("person", "team").getBytes(StandardCharsets.UTF_8);
        AtomicBoolean truncateNext = new AtomicBoolean(true);
        server.createContext("/subjects", exchange -> {
            if (truncateNext.compareAndSet(true, false)) {
                exchange.sendResponseHeaders(200, payload.length + 1000);
                exchange.getResponseBody().write(payload, 0, 1);
                exchange.close();
                return;
            }
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });

        List<FileDescriptor> descriptors = new ConfluentSchemaRegistryLoader(baseUri()).loadDescriptors();

        assertThat(truncateNext.get()).isFalse();
        assertThat(descriptors).extracting(FileDescriptor::getName)
                .containsExactlyInAnyOrder("person.proto", "team.proto");
    }

    // ---------------------------------------------------------------- stub helpers

    private URI baseUri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private void stubPersonAndTeam() {
        serveJson("/subjects", subjects("person", "team"));
        serveJson("/subjects/person/versions/latest", schemaBody("person", 1, "PROTOBUF", PERSON_PROTO));
        serveJson("/subjects/person/versions/1", schemaBody("person", 1, "PROTOBUF", PERSON_PROTO));
        serveJson("/subjects/team/versions/latest", schemaBody("team", 1, "PROTOBUF", TEAM_PROTO,
                reference("person.proto", "person", 1)));
    }

    private void serveJson(String path, String body) {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        server.createContext(path, exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
    }

    private static String subjects(String... names) {
        ArrayNode array = JSON.createArrayNode();
        for (String name : names) {
            array.add(name);
        }
        return array.toString();
    }

    private static String schemaBody(String subject, int version, String schemaType, String schema,
                                     ObjectNode... references) {
        ObjectNode node = JSON.createObjectNode()
                .put("subject", subject)
                .put("version", version)
                .put("id", 1)
                .put("schemaType", schemaType)
                .put("schema", schema);
        if (references.length > 0) {
            node.putArray("references").addAll(List.of(references));
        }
        return node.toString();
    }

    private static ObjectNode reference(String name, String subject, int version) {
        return JSON.createObjectNode().put("name", name).put("subject", subject).put("version", version);
    }

    private static FileDescriptor byName(List<FileDescriptor> descriptors, String name) {
        return descriptors.stream().filter(fd -> fd.getName().equals(name)).findFirst().orElseThrow();
    }
}
