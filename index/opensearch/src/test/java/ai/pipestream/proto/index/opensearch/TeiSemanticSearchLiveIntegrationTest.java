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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.opensearch.testcontainers.OpenSearchContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link RerankedSemanticSearch} against real providers: a TEI embeddings server (384-dim
 * all-MiniLM-L6-v2 vectors) and a TEI rerank server, with a Testcontainers OpenSearch as the
 * engine. The suite runs only when
 * {@value TeiEmbeddingProvider#TARGET_ENVIRONMENT_VARIABLE} and
 * {@value TeiRerankProvider#TARGET_ENVIRONMENT_VARIABLE} point at live servers; it skips
 * cleanly otherwise, before the engine container is even started.
 */
@Tag("integration")
class TeiSemanticSearchLiveIntegrationTest {

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

    @Test
    void teiProvidersDriveTheRerankedSearch() throws Exception {
        String embeddingsTarget = System.getenv(TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE);
        String rerankTarget = System.getenv(TeiRerankProvider.TARGET_ENVIRONMENT_VARIABLE);
        assumeTrue(embeddingsTarget != null && !embeddingsTarget.isBlank(),
                "Set " + TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE
                        + " to the TEI embeddings server's host:port to run this test");
        assumeTrue(rerankTarget != null && !rerankTarget.isBlank(),
                "Set " + TeiRerankProvider.TARGET_ENVIRONMENT_VARIABLE
                        + " to the TEI rerank server's host:port to run this test");
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Docker is required for the OpenSearch container");

        System.setProperty(TeiEmbeddingProvider.TARGET_PROPERTY, embeddingsTarget);
        System.setProperty(TeiRerankProvider.TARGET_PROPERTY, rerankTarget);
        try (OpenSearchContainer<?> opensearch = new OpenSearchContainer<>(
                DockerImageName.parse("opensearchproject/opensearch:3.7.0"));
             TeiEmbeddingProvider embedder = new TeiEmbeddingProvider();
             TeiRerankProvider reranker = new TeiRerankProvider()) {
            opensearch.start();
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
            try (OpenSearchSink sink = new OpenSearchSink(base)) {
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
            }

            RerankedSemanticSearch search;
            try (OpenSearchSearch openSearch = new OpenSearchSearch(base)) {
                search = new RerankedSemanticSearch(openSearch, embedder, reranker);
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
        } finally {
            System.clearProperty(TeiEmbeddingProvider.TARGET_PROPERTY);
            System.clearProperty(TeiRerankProvider.TARGET_PROPERTY);
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
