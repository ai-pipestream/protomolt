package ai.pipestream.proto.index.solr;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The generated Solr schema against a live Solr: every add-field-type / add-field /
 * add-copy-field the generator emits must be accepted by the Schema API, and documents
 * mapped from dynamic messages must index and answer analyzed, sorted, faceted, and kNN
 * queries. Start the engine with {@code docker compose -f docker-compose.integration.yml up -d}
 * (repo root); without it these tests skip.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SolrLiveIntegrationTest {

    private static final String BASE = System.getProperty(
            "protomolt.it.solr", "http://localhost:18983/solr");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private static String core;
    private static Descriptor descriptor;
    private static IndexingPlan plan;

    @BeforeAll
    static void createCore() throws Exception {
        assumeTrue(reachable(), "Solr not reachable at " + BASE + "; skipping");
        // Standalone Solr cannot copy a configset over the core-admin API, so the compose
        // file precreates this core; schema application below is idempotent across runs.
        core = "protomolt";

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

    private static boolean reachable() {
        try {
            return HTTP.send(HttpRequest.newBuilder(URI.create(BASE + "/admin/info/system"))
                            .timeout(Duration.ofSeconds(3)).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static JsonNode post(String path, Object body) throws Exception {
        return post(path, body, false);
    }

    private static JsonNode post(String path, Object body, boolean allowAlreadyExists)
            throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(BASE + path))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode node = JSON.readTree(response.body());
        boolean clean = response.statusCode() == 200
                && (node.path("errors").isMissingNode() || node.path("errors").isEmpty());
        if (!clean && allowAlreadyExists && response.body().contains("already exists")) {
            return node;
        }
        assertThat(clean).as("%s: %s", path, response.body()).isTrue();
        return node;
    }

    private static JsonNode get(String path) throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(BASE + path))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s: %s", path, response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    @Test
    @Order(1)
    void generatedSchemaIsAcceptedByTheSchemaApi() throws Exception {
        SolrSchemaGenerator.SolrSchema schema = new SolrSchemaGenerator().generate(plan);
        // The vector hint requires a custom DenseVectorField type.
        assertThat(schema.fieldTypes()).isNotEmpty();
        for (Map<String, Object> type : schema.fieldTypes()) {
            post("/" + core + "/schema", Map.of("add-field-type", type), true);
        }
        for (Map<String, Object> field : schema.fields()) {
            post("/" + core + "/schema", Map.of("add-field", field), true);
        }
        for (Map<String, Object> copy : schema.copyFields()) {
            post("/" + core + "/schema", Map.of("add-copy-field", copy), true);
        }
        JsonNode fields = get("/" + core + "/schema/fields");
        List<String> names = fields.path("fields").findValuesAsText("name");
        assertThat(names).contains("title", "title_raw", "genre", "rank", "embedding");
    }

    @Test
    @Order(2)
    void mappedDocumentsIndexAndAnswerQueries() throws Exception {
        SolrDocumentMapper mapper = new SolrDocumentMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        List<Map<String, Object>> docs = new ArrayList<>();
        int id = 0;
        for (DynamicMessage message : List.of(
                book("Running with Scissors", "memoir", 3, 1f, 0f, 0f, 0f),
                book("The Silent Library", "mystery", 1, 0f, 1f, 0f, 0f),
                book("Runs in the Family", "mystery", 2, 0.9f, 0.1f, 0f, 0f))) {
            Map<String, Object> doc = new java.util.LinkedHashMap<>(mapper.map(message, plan));
            doc.put("id", "doc-" + id++);
            docs.add(doc);
        }
        post("/" + core + "/update?commit=true", Map.of("delete", Map.of("query", "*:*")));
        post("/" + core + "/update?commit=true", docs);

        // Analyzed text: text_en stems 'runs' and 'running' to the same term.
        JsonNode stemmed = get("/" + core + "/select?q=title:runs&wt=json");
        assertThat(stemmed.path("response").path("numFound").asLong()).isEqualTo(2);

        // The keyword sub-field holds the exact, unanalyzed title.
        JsonNode raw = get("/" + core + "/select?q=title_raw:%22Runs%20in%20the%20Family%22&wt=json");
        assertThat(raw.path("response").path("numFound").asLong()).isEqualTo(1);

        // Facet on the keyword field.
        JsonNode facets = get("/" + core
                + "/select?q=*:*&rows=0&facet=true&facet.field=genre&wt=json");
        JsonNode counts = facets.path("facet_counts").path("facet_fields").path("genre");
        assertThat(counts.toString()).contains("mystery").contains("memoir");

        // Sort on the numeric docValues field.
        JsonNode sorted = get("/" + core + "/select?q=genre:mystery&sort=rank%20desc&wt=json");
        assertThat(sorted.path("response").path("docs").get(0).path("title").asText())
                .contains("Runs in the Family");

        // kNN over the DenseVectorField the generator registered.
        JsonNode knn = post("/" + core + "/select", Map.of(
                "query", "{!knn f=embedding topK=2}[1.0, 0.0, 0.0, 0.0]",
                "fields", "id,title"));
        assertThat(knn.path("response").path("docs").get(0).path("title").asText())
                .contains("Running with Scissors");
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

    private static Descriptor bookDescriptor() throws Exception {
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
