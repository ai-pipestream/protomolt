package ai.pipestream.proto.embeddings.model2vec;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.embeddings.PlanEmbedder;
import ai.pipestream.proto.index.opensearch.OpenSearchDocumentMapper;
import ai.pipestream.proto.index.opensearch.OpenSearchSink;
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
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.testcontainers.OpenSearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end semantic search with a real model: sentences flow through
 * {@link OpenSearchDocumentMapper}, get their vector from a {@link Model2VecEmbeddingProvider}
 * via {@link PlanEmbedder}, land through {@link OpenSearchSink}, and a query embedded with the
 * same provider must rank the semantically nearest sentence first under kNN — no synthetic
 * vectors anywhere. The engine is a Testcontainers OpenSearch instance; the suite skips when
 * Docker is unavailable.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SemanticSearchLiveIntegrationTest {

    // Latest OpenSearch line. The container defaults to DISABLE_SECURITY_PLUGIN=true,
    // so the suite talks plain HTTP with no auth.
    @Container
    static final OpenSearchContainer<?> OPENSEARCH = new OpenSearchContainer<>(
            DockerImageName.parse("opensearchproject/opensearch:3.7.0"));

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    @TempDir
    static Path modelDirectory;

    private static String base;
    private static String index;
    private static OpenSearchSink sink;
    private static Model2VecEmbeddingProvider provider;
    private static Descriptor descriptor;
    private static IndexingPlan plan;

    @BeforeAll
    static void setUp() throws Exception {
        Model2VecTestModel.write(modelDirectory);
        provider = new Model2VecEmbeddingProvider(modelDirectory);
        base = OPENSEARCH.getHttpHostAddress();
        index = "semantic-" + UUID.randomUUID().toString().substring(0, 12);
        sink = new OpenSearchSink(base, HTTP);
        descriptor = sentenceDescriptor();
        plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("sentence", "sentence",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                new IndexingPlan.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(Model2VecTestModel.DIMENSION)
                                .build())));
    }

    @AfterAll
    static void deleteIndex() throws Exception {
        if (index != null) {
            HTTP.send(HttpRequest.newBuilder(URI.create(base + "/" + index))
                    .DELETE().build(), HttpResponse.BodyHandlers.discarding());
        }
    }

    @Test
    void modelEmbeddedSentencesAnswerASemanticKnnQuery() throws Exception {
        assertThat(sink.ensureIndex(index, plan)).isTrue();

        // Messages carry only text; the vector field is filled by the model, not by hand.
        OpenSearchDocumentMapper mapper = new OpenSearchDocumentMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        PlanEmbedder embedder = new PlanEmbedder(provider, plan);
        Map<String, String> sentences = new LinkedHashMap<>();
        sentences.put("dog", "the dog sat on the mat");
        sentences.put("cat", "the cat sat on the mat");
        sentences.put("car", "the car drove on");
        sentences.put("engine", "the engine drove the car");
        sentences.put("filler", "the mat sat on the mat");
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : sentences.entrySet()) {
            DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("sentence"), entry.getValue())
                    .build();
            documents.put(entry.getKey(), embedder.embed(mapper.map(message, plan)));
        }
        sink.bulkWrite(index, documents, true);

        // "puppy" appears in no indexed sentence; only the vector space connects it to "dog".
        assertThat(topHitId("puppy")).isEqualTo("dog");
        assertThat(topHitId("kitten")).isEqualTo("cat");
    }

    /** Embeds the query with the same provider and returns the kNN top-1 document id. */
    private static String topHitId(String query) throws Exception {
        List<Float> vector = new ArrayList<>(Model2VecTestModel.DIMENSION);
        for (float component : provider.embed(query)) {
            vector.add(component);
        }
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(
                        URI.create(base + "/" + index + "/_search"))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of(
                                "size", 1,
                                "query", Map.of("knn", Map.of("embedding", Map.of(
                                        "vector", vector, "k", 3)))))))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        JsonNode hits = JSON.readTree(response.body()).path("hits").path("hits");
        assertThat(hits).as("query '%s' returned no hits", query).isNotEmpty();
        return hits.get(0).path("_id").asText();
    }

    private static Descriptor sentenceDescriptor() {
        try {
            FileDescriptorProto file = FileDescriptorProto.newBuilder()
                    .setName("semantic/sentence.proto")
                    .setPackage("semantic")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Sentence")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("sentence").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("embedding").setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .build();
            return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                    .findMessageTypeByName("Sentence");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
