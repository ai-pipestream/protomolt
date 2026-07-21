package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.embeddings.PlanEmbedder;
import ai.pipestream.proto.embeddings.tei.TeiEmbeddingProvider;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.rerank.tei.TeiRerankProvider;
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
import org.opensearch.testcontainers.OpenSearchContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link RerankedSemanticSearch} verified live against real models: a self-provisioned
 * Testcontainers stack of OpenSearch plus two TEI CPU containers (one serving
 * sentence-transformers/all-MiniLM-L6-v2 embeddings, one serving BAAI/bge-reranker-base), with
 * nothing running but Docker. Sentences are embedded through {@link PlanEmbedder}, land through
 * {@link OpenSearchSink}, and a semantic query is answered by the rerank head: the cross-encoder
 * must put the puppy sentence first for "a young dog", with sigmoid-normalized relevance scores
 * and positive kNN scores.
 *
 * <p>TEI serves gRPC on container port 80 and logs {@code Starting gRPC server} only after the
 * model has loaded, so readiness waits on that line; the first run downloads the models from the
 * HF hub (about 1.2 GB combined) and can take several minutes. Setting the
 * {@value #CACHE_ENVIRONMENT_VARIABLE} environment variable to an existing directory bind-mounts
 * it into both TEI containers at {@code /data}, so models persist across runs; unset, each run
 * downloads them again.
 *
 * <p>The suite is tagged {@code tei} and excluded from the default test task; it runs via
 * {@code ./gradlew :protomolt-index-opensearch:teiIntegrationTest}, in CI on the Forgejo lane
 * ({@code .forgejo/workflows/tei-integration.yml}). It skips when Docker is unavailable.
 */
@Tag("integration")
@Tag("tei")
@Testcontainers(disabledWithoutDocker = true)
class TeiSemanticSearchLiveIntegrationTest {

    /** Environment variable naming an existing directory to reuse as the TEI model cache. */
    private static final String CACHE_ENVIRONMENT_VARIABLE = "PROTOMOLT_TEI_CACHE";

    private static final DockerImageName TEI_IMAGE =
            DockerImageName.parse("ghcr.io/huggingface/text-embeddings-inference:cpu-1.9-grpc");

    private static final Map<String, String> SENTENCES = new LinkedHashMap<>();

    static {
        SENTENCES.put("puppy", "the puppy played in the garden");
        SENTENCES.put("cat", "the old cat napped on the windowsill");
        SENTENCES.put("car", "a red sports car sped down the highway");
        SENTENCES.put("chef", "the chef prepared a delicious meal");
        SENTENCES.put("rain", "rain fell softly on the roof");
        SENTENCES.put("engine", "the engineer fixed the broken engine");
        SENTENCES.put("children", "children laughed at the playground");
        SENTENCES.put("market", "the stock market closed higher on friday");
    }

    private static OpenSearchContainer<?> opensearch;
    private static GenericContainer<?> embeddings;
    private static GenericContainer<?> reranker;

    @BeforeAll
    static void startContainers() {
        opensearch = new OpenSearchContainer<>(
                DockerImageName.parse("opensearchproject/opensearch:3.7.0"));
        embeddings = tei("sentence-transformers/all-MiniLM-L6-v2");
        reranker = tei("BAAI/bge-reranker-base");
        // Parallel start: each container's log-message wait strategy handles its own readiness.
        Startables.deepStart(List.of(opensearch, embeddings, reranker)).join();
    }

    @AfterAll
    static void stopContainers() {
        if (reranker != null) {
            reranker.stop();
        }
        if (embeddings != null) {
            embeddings.stop();
        }
        if (opensearch != null) {
            opensearch.stop();
        }
    }

    /** A TEI CPU container serving {@code modelId} over gRPC on port 80, ready when it logs so. */
    private static GenericContainer<?> tei(String modelId) {
        GenericContainer<?> container = new GenericContainer<>(TEI_IMAGE)
                .withCommand("--model-id", modelId)
                .withExposedPorts(80)
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(10)));
        String cache = System.getenv(CACHE_ENVIRONMENT_VARIABLE);
        if (cache != null && !cache.isBlank() && Files.isDirectory(Path.of(cache))) {
            container.withFileSystemBind(cache, "/data", BindMode.READ_WRITE);
        }
        return container;
    }

    @Test
    void teiProvidersDriveTheRerankedSearch() throws Exception {
        String base = opensearch.getHttpHostAddress();
        String index = "tei-reranked-" + UUID.randomUUID().toString().substring(0, 12);

        Descriptor descriptor = sentenceDescriptor();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("sentence", "sentence",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                new IndexingPlan.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(384)
                                .build())));
        try (TeiEmbeddingProvider embedder = new TeiEmbeddingProvider(
                embeddings.getHost() + ":" + embeddings.getMappedPort(80));
             TeiRerankProvider rerankProvider = new TeiRerankProvider(
                reranker.getHost() + ":" + reranker.getMappedPort(80));
             OpenSearchSink sink = new OpenSearchSink(base)) {
            sink.ensureIndex(index, plan);
            OpenSearchDocumentMapper mapper = new OpenSearchDocumentMapper(
                    new ProtoFieldMapperImpl(new DescriptorRegistry()));
            PlanEmbedder planEmbedder = new PlanEmbedder(embedder, plan);
            Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : SENTENCES.entrySet()) {
                DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("sentence"), entry.getValue())
                        .build();
                documents.put(entry.getKey(), planEmbedder.embed(mapper.map(message, plan)));
            }
            sink.bulkWrite(index, documents, true);

            RerankedSemanticSearch search = new RerankedSemanticSearch(
                    new OpenSearchSearch(base), embedder, rerankProvider);
            List<RankedHit> hits = search.search(index, "embedding", "sentence",
                    "a young dog", 2, 6);

            assertThat(hits).hasSize(2);
            assertThat(hits.get(0).id()).isEqualTo("puppy");
            assertThat(hits.get(0).text()).isEqualTo("the puppy played in the garden");
            for (RankedHit hit : hits) {
                assertThat(hit.relevanceScore())
                        .as("TEI answers sigmoid-normalized scores").isBetween(0.0, 1.0);
                assertThat(hit.knnScore()).isPositive();
            }
        }
    }

    private static Descriptor sentenceDescriptor() {
        try {
            FileDescriptorProto file = FileDescriptorProto.newBuilder()
                    .setName("tei/reranked_sentence.proto")
                    .setPackage("tei.reranked")
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
