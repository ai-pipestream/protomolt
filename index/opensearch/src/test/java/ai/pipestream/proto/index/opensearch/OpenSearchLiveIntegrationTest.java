package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The generated OpenSearch mappings against a live engine: the index must accept them,
 * documents mapped from dynamic messages must land, and the analyzed / keyword / sorted /
 * kNN behaviors the hints promise must answer real queries. Start the engine with
 * {@code docker compose -f docker-compose.integration.yml up -d} (repo root); without it
 * these tests skip.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OpenSearchLiveIntegrationTest {

    private static final String BASE = System.getProperty(
            "protomolt.it.opensearch", "http://localhost:19200");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private static String index;
    private static Descriptor descriptor;
    private static IndexingPlan plan;

    @BeforeAll
    static void setUp() {
        assumeTrue(reachable(), "OpenSearch not reachable at " + BASE + "; skipping");
        index = "it-" + UUID.randomUUID().toString().substring(0, 12);
        descriptor = bookDescriptor();
        plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                                .analyzer("english")
                                .subFields(List.of(new ResolvedFieldHint.SubField(
                                        IndexFieldKind.KEYWORD, "raw", "")))
                                .build()),
                new IndexingPlan.IndexedField("genre", "genre",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .sortable(true).facetable(true)
                                .build()),
                new IndexingPlan.IndexedField("rank", "rank",
                        ResolvedFieldHint.builder(IndexFieldKind.INT64)
                                .sortable(true)
                                .build()),
                new IndexingPlan.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(4)
                                .build())));
    }

    @AfterAll
    static void deleteIndex() throws Exception {
        if (index != null) {
            send("DELETE", "/" + index, null);
        }
    }

    private static boolean reachable() {
        try {
            return HTTP.send(HttpRequest.newBuilder(URI.create(BASE + "/_cluster/health"))
                            .timeout(Duration.ofSeconds(3)).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static JsonNode send(String method, String path, Object body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(BASE + path))
                .header("content-type", "application/json");
        request.method(method, body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        HttpResponse<String> response = HTTP.send(request.build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as("%s %s: %s", method, path, response.body())
                .isBetween(200, 299);
        return JSON.readTree(response.body());
    }

    @Test
    @Order(1)
    void generatedMappingsCreateTheIndex() throws Exception {
        Map<String, Object> mappings = new OpenSearchMappingGenerator().generate(plan);
        JsonNode created = send("PUT", "/" + index, Map.of(
                "settings", Map.of("index.knn", true),
                "mappings", mappings));
        assertThat(created.path("acknowledged").asBoolean()).isTrue();

        JsonNode mapping = send("GET", "/" + index + "/_mapping", null);
        JsonNode properties = mapping.path(index).path("mappings").path("properties");
        assertThat(properties.path("title").path("type").asText()).isEqualTo("text");
        assertThat(properties.path("title").path("analyzer").asText()).isEqualTo("english");
        assertThat(properties.path("genre").path("type").asText()).isEqualTo("keyword");
        assertThat(properties.path("embedding").path("type").asText()).isEqualTo("knn_vector");
    }

    @Test
    @Order(2)
    void mappedDocumentsIndexAndAnswerQueries() throws Exception {
        OpenSearchDocumentMapper mapper = new OpenSearchDocumentMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        int id = 0;
        for (DynamicMessage message : List.of(
                book("Running with Scissors", "memoir", 3, 1f, 0f, 0f, 0f),
                book("The Silent Library", "mystery", 1, 0f, 1f, 0f, 0f),
                book("Runs in the Family", "mystery", 2, 0.9f, 0.1f, 0f, 0f))) {
            send("PUT", "/" + index + "/_doc/" + id++ + "?refresh=true",
                    mapper.map(message, plan));
        }

        // Analyzed text: the english analyzer stems 'runs' and 'running' together.
        JsonNode stemmed = send("POST", "/" + index + "/_search", Map.of(
                "query", Map.of("match", Map.of("title", "runs"))));
        assertThat(stemmed.path("hits").path("total").path("value").asLong()).isEqualTo(2);

        // The keyword sub-field is exact.
        JsonNode raw = send("POST", "/" + index + "/_search", Map.of(
                "query", Map.of("term", Map.of("title.raw", "Runs in the Family"))));
        assertThat(raw.path("hits").path("total").path("value").asLong()).isEqualTo(1);

        // Keyword aggregation (the facet story) and numeric sort.
        JsonNode sorted = send("POST", "/" + index + "/_search", Map.of(
                "query", Map.of("term", Map.of("genre", "mystery")),
                "sort", List.of(Map.of("rank", "desc")),
                "aggs", Map.of("genres", Map.of("terms", Map.of("field", "genre")))));
        assertThat(sorted.path("hits").path("hits").get(0)
                .path("_source").path("title").asText()).contains("Runs in the Family");
        assertThat(sorted.path("aggregations").path("genres").path("buckets").size())
                .isEqualTo(1);

        // kNN over the generated knn_vector field.
        JsonNode knn = send("POST", "/" + index + "/_search", Map.of(
                "size", 2,
                "query", Map.of("knn", Map.of("embedding", Map.of(
                        "vector", List.of(1.0, 0.0, 0.0, 0.0), "k", 2)))));
        assertThat(knn.path("hits").path("hits").get(0)
                .path("_source").path("title").asText()).contains("Running with Scissors");
    }

    // ---- fixture ----

    private static DynamicMessage book(String title, String genre, long rank, float... vector) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        builder.setField(descriptor.findFieldByName("title"), title);
        builder.setField(descriptor.findFieldByName("genre"), genre);
        builder.setField(descriptor.findFieldByName("rank"), rank);
        FieldDescriptor embedding = descriptor.findFieldByName("embedding");
        for (float component : vector) {
            builder.addRepeatedField(embedding, component);
        }
        return builder.build();
    }

    private static Descriptor bookDescriptor() {
        try {
            FileDescriptorProto file = FileDescriptorProto.newBuilder()
                    .setName("library/book.proto")
                    .setPackage("library")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Book")
                            .addField(field("title", 1, FieldDescriptorProto.Type.TYPE_STRING, false))
                            .addField(field("genre", 2, FieldDescriptorProto.Type.TYPE_STRING, false))
                            .addField(field("rank", 3, FieldDescriptorProto.Type.TYPE_INT64, false))
                            .addField(field("embedding", 4, FieldDescriptorProto.Type.TYPE_FLOAT, true)))
                    .build();
            return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                    .findMessageTypeByName("Book");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static FieldDescriptorProto.Builder field(String name, int number,
                                                      FieldDescriptorProto.Type type,
                                                      boolean repeated) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(repeated
                        ? FieldDescriptorProto.Label.LABEL_REPEATED
                        : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }
}
