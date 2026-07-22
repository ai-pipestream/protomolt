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
import org.opensearch.testcontainers.OpenSearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The generated OpenSearch mappings and the HTTP sink against a live engine: the index
 * created by {@link OpenSearchSink#ensureIndex} must carry the generated mappings, documents
 * mapped from dynamic messages must land through {@link OpenSearchSink#bulkWrite}, and the
 * analyzed / keyword / sorted / kNN behaviors the hints promise must answer real queries.
 * The engine is a Testcontainers OpenSearch instance; the suite skips when Docker is
 * unavailable.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OpenSearchLiveIntegrationTest {

    // Latest OpenSearch line. The container defaults to DISABLE_SECURITY_PLUGIN=true,
    // so the suite talks plain HTTP with no auth.
    @Container
    static final OpenSearchContainer<?> OPENSEARCH = new OpenSearchContainer<>(
            DockerImageName.parse("opensearchproject/opensearch:3.7.0"));

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private static String base;
    private static String index;
    private static OpenSearchSink sink;
    private static Descriptor descriptor;
    private static IndexingPlan plan;

    @BeforeAll
    static void setUp() {
        base = OPENSEARCH.getHttpHostAddress();
        index = "it-" + UUID.randomUUID().toString().substring(0, 12);
        sink = new OpenSearchSink(base, HTTP);
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

    private static JsonNode send(String method, String path, Object body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
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
    void ensureIndexCreatesFromThePlan() throws Exception {
        assertThat(sink.ensureIndex(index, plan)).isTrue();

        JsonNode mapping = send("GET", "/" + index + "/_mapping", null);
        JsonNode properties = mapping.path(index).path("mappings").path("properties");
        assertThat(properties.path("title").path("type").asText()).isEqualTo("text");
        assertThat(properties.path("title").path("analyzer").asText()).isEqualTo("english");
        assertThat(properties.path("genre").path("type").asText()).isEqualTo("keyword");
        assertThat(properties.path("embedding").path("type").asText()).isEqualTo("knn_vector");

        // The plan has a VECTOR field, so the sink enabled knn on the index.
        JsonNode settings = send("GET", "/" + index + "/_settings", null);
        assertThat(settings.path(index).path("settings").path("index").path("knn").asText())
                .isEqualTo("true");
    }

    @Test
    @Order(2)
    void mappedDocumentsIndexAndAnswerQueries() throws Exception {
        OpenSearchDocumentMapper mapper = new OpenSearchDocumentMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        int id = 0;
        for (DynamicMessage message : List.of(
                book("Running with Scissors", "memoir", 3, 1f, 0f, 0f, 0f),
                book("The Silent Library", "mystery", 1, 0f, 1f, 0f, 0f),
                book("Runs in the Family", "mystery", 2, 0.9f, 0.1f, 0f, 0f))) {
            documents.put(String.valueOf(id++), mapper.map(message, plan));
        }
        sink.bulkWrite(index, documents, true);

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

    @Test
    @Order(3)
    void ensureIndexLeavesAnExistingIndexUntouched() throws Exception {
        JsonNode before = send("GET", "/" + index + "/_mapping", null);
        assertThat(sink.ensureIndex(index, plan)).isFalse();
        JsonNode after = send("GET", "/" + index + "/_mapping", null);
        assertThat(after).isEqualTo(before);
    }

    @Test
    @Order(4)
    void bulkWriteWithoutIdsAutoAssignsThem() throws Exception {
        sink.bulkWrite(index, List.of(Map.<String, Object>of(
                "title", "Autoid Chronicle", "genre", "reference", "rank", 9L)), true);

        JsonNode hits = send("POST", "/" + index + "/_search", Map.of(
                "query", Map.of("term", Map.of("title.raw", "Autoid Chronicle"))))
                .path("hits").path("hits");
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).path("_id").asText()).isNotBlank();
    }

    @Test
    @Order(5)
    void bulkItemFailureSurfacesTheItemReason() throws Exception {
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        documents.put("bulk-good", Map.of("title", "Bulk Survivor", "genre", "reference", "rank", 7L));
        // rank is mapped long; a non-numeric string fails that item alone.
        documents.put("bulk-bad", Map.of("title", "Bulk Casualty", "rank", "not-a-number"));

        assertThatThrownBy(() -> sink.bulkWrite(index, documents, true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("1 of 2")
                .hasMessageContaining("bulk-bad")
                .hasMessageContaining("mapper_parsing_exception")
                .hasMessageContaining("[rank]");

        // Bulk is not atomic: the valid document in the same request landed.
        JsonNode good = send("GET", "/" + index + "/_doc/bulk-good", null);
        assertThat(good.path("found").asBoolean()).isTrue();
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

    /**
     * The secure-search pattern: content is AES-GCM ciphertext in a store-only container
     * ({@code index: false} — exactly what render-index-mappings emits for an encrypted
     * class), while the embedding computed from the plaintext is the only searchable
     * representation. Semantic search works; the engine cannot read, and refuses to
     * search, the text; only the key holder decrypts what comes back.
     */
    @Test
    @Order(9)
    void encryptedContentIsVectorSearchableAndKeyRecoverable() throws Exception {
        String secure = "it-secure-" + UUID.randomUUID().toString().substring(0, 8);
        send("PUT", "/" + secure, Map.of(
                "settings", Map.of("index.knn", true),
                "mappings", Map.of("properties", Map.of(
                        "content", Map.of("type", "keyword", "index", false,
                                "doc_values", false),
                        "embedding", Map.of("type", "knn_vector", "dimension", 3)))));
        try {
            Descriptor note = sensitiveNoteDescriptor();
            String plaintext = "patient responded well to the treatment";
            DynamicMessage message = DynamicMessage.newBuilder(note)
                    .setField(note.findFieldByName("content"), plaintext)
                    .build();
            byte[] key = new byte[32];
            ai.pipestream.proto.meta.SensitivityMasker.MaskResult sealed =
                    ai.pipestream.proto.meta.SensitivityMasker.mask(message,
                            java.util.Set.of("pii"),
                            ai.pipestream.proto.meta.SensitivityMasker.Strategy.ENCRYPT, key);
            String ciphertext = (String) sealed.message()
                    .getField(note.findFieldByName("content"));
            assertThat(ciphertext).isNotEqualTo(plaintext);

            send("POST", "/" + secure + "/_doc/n-1?refresh=true", Map.of(
                    "content", ciphertext,
                    "embedding", List.of(0.9, 0.1, 0.0)));

            // Semantic search over the embedding finds the note...
            JsonNode hits = send("POST", "/" + secure + "/_search", Map.of(
                    "query", Map.of("knn", Map.of("embedding", Map.of(
                            "vector", List.of(0.88, 0.12, 0.0), "k", 1)))))
                    .path("hits").path("hits");
            assertThat(hits).hasSize(1);
            String stored = hits.get(0).path("_source").path("content").asText();
            assertThat(stored).isEqualTo(ciphertext).isNotEqualTo(plaintext);

            // ...while the engine refuses to search the ciphertext container at all.
            HttpResponse<String> refused = HTTP.send(HttpRequest.newBuilder(
                            URI.create(base + "/" + secure + "/_search"))
                            .header("content-type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "{\"query\": {\"term\": {\"content\": \"patient\"}}}"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(refused.statusCode()).isEqualTo(400);
            assertThat(refused.body()).contains("not indexed");

            // Only the key recovers the text.
            DynamicMessage recovered = (DynamicMessage)
                    ai.pipestream.proto.meta.SensitivityMasker.mask(sealed.message(),
                            java.util.Set.of("pii"),
                            ai.pipestream.proto.meta.SensitivityMasker.Strategy.DECRYPT, key)
                            .message();
            assertThat(recovered.getField(note.findFieldByName("content")))
                    .isEqualTo(plaintext);
        } finally {
            HTTP.send(HttpRequest.newBuilder(URI.create(base + "/" + secure))
                    .DELETE().build(), HttpResponse.BodyHandlers.discarding());
        }
    }

    /** content [sensitivity=pii] — options built directly, as runtime schemas carry them. */
    private static Descriptor sensitiveNoteDescriptor() throws Exception {
        var contentOptions = com.google.protobuf.DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ai.pipestream.proto.meta.MetadataProto.field,
                        ai.pipestream.proto.meta.FieldMeta.newBuilder()
                                .setSensitivity("pii").build())
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("it/secure_note.proto")
                .setPackage("it.secure")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Note")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("content").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(contentOptions)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{
                ai.pipestream.proto.meta.MetadataProto.getDescriptor(),
        }).findMessageTypeByName("Note");
    }
}
