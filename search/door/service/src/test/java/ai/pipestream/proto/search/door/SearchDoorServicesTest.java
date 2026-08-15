package ai.pipestream.proto.search.door;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.chunk.SentencePackedChunker;
import ai.pipestream.proto.index.spi.ChunkingPolicy;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.IndexDocumentResponse;
import ai.pipestream.proto.search.v1.ListSubjectsRequest;
import ai.pipestream.proto.search.v1.SubjectInfo;
import ai.pipestream.proto.search.v1.SearchHit;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The door's behavior over an in-process server and a map-backed fetcher:
 * indexing runs the chunk-and-embed lane, every lane answers, and requests
 * outside the served surface are refused by name.
 */
class SearchDoorServicesTest {

    static final String BODY = "The alpha protocol begins at dawn. Falcons circle the"
            + " ancient tower slowly. Gears and pistons drive the great engine. Rivers"
            + " of quicksilver cross the valley floor. The archive keeps every treaty"
            + " ever signed.";

    @TempDir
    static Path work;

    static final Map<String, Document> DOCUMENTS = new HashMap<>();
    static SearchDoorServices door;
    static ManagedChannel channel;
    static SearchIndexServiceGrpc.SearchIndexServiceBlockingStub indexStub;
    static SearchServiceGrpc.SearchServiceBlockingStub searchStub;

    static ChunkingPolicy policy() {
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        12, 0, 2, 30, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        DoorTestProvider.PROVIDER_ID, DoorTestProvider.DIMENSION,
                        VectorSimilarity.COSINE, true),
                "", true);
    }

    @BeforeAll
    static void bootTheDoor() throws Exception {
        DOCUMENTS.put("doc-1", Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Alpha Treaty Archive")
                        .setBody(BODY))
                .build());
        door = SearchDoorServices.build(
                new SearchDoorConfig(0, work.resolve("index"), Map.of(
                        RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(policy()),
                        "repo-document-lexical", RepoDocumentMapping.served())),
                address -> {
                    Document document = DOCUMENTS.get(address.getDocId());
                    if (document == null) {
                        throw Status.NOT_FOUND.withDescription(address.getDocId())
                                .asRuntimeException();
                    }
                    return document;
                });
        String name = InProcessServerBuilder.generateName();
        door.startInProcess(name);
        channel = InProcessChannelBuilder.forName(name).build();
        indexStub = SearchIndexServiceGrpc.newBlockingStub(channel);
        searchStub = SearchServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void shutdown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (door != null) {
            door.close();
        }
    }

    static IndexDocumentResponse index(String docId) {
        return indexStub.indexDocument(IndexDocumentRequest.newBuilder()
                .setAddress(NodeAddress.newBuilder().setDocId(docId)
                        .setGraphAddressId("ds").setAccountId("acct").setGraphId("intake:acct"))
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .build());
    }

    static SearchRequest.Builder query(SearchLane lane, String text) {
        return SearchRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .setQuery(text)
                .setK(3)
                .setLane(lane);
    }

    @Test
    void indexingRunsTheChunkLaneAndReportsIt() {
        IndexDocumentResponse landed = index("doc-1");
        assertThat(landed.getDocId()).isEqualTo("doc-1");
        assertThat(landed.getChunksIndexed()).isGreaterThan(1);
        assertThat(landed.getPolicyDigest())
                .isEqualTo(policy().digest().substring(0, 12));
    }

    @Test
    void lexicalSearchFindsTheDocumentThroughItsMappedFields() {
        index("doc-1");
        SearchResponse byBody = searchStub.search(
                query(SearchLane.SEARCH_LANE_LEXICAL, "quicksilver valley").build());
        assertThat(byBody.getHitsList()).isNotEmpty();
        assertThat(byBody.getHits(0).getDocId()).isEqualTo("doc-1");
        assertThat(byBody.getHits(0).getChunkId()).isEmpty();
        assertThat(byBody.getHits(0).getStoredMap())
                .containsEntry("search_metadata_title", "Alpha Treaty Archive");
    }

    @Test
    void vectorSearchRanksTheQuerysSourceChunkFirst() {
        index("doc-1");
        // A whole sentence embeds to its own chunk's vector.
        SearchResponse byVector = searchStub.search(
                query(SearchLane.SEARCH_LANE_VECTOR,
                        "Gears and pistons drive the great engine.").build());
        assertThat(byVector.getHitsList()).isNotEmpty();
        SearchHit nearest = byVector.getHits(0);
        assertThat(nearest.getDocId()).isEqualTo("doc-1");
        assertThat(nearest.getChunkId())
                .startsWith("doc-1#" + policy().digest().substring(0, 12) + "#");
        assertThat(nearest.getStoredMap().get(LuceneSearchStore.CHUNK_TEXT_FIELD))
                .contains("Gears and pistons");
    }

    @Test
    void hybridSearchFusesBothLanes() {
        index("doc-1");
        SearchResponse fused = searchStub.search(
                query(SearchLane.SEARCH_LANE_HYBRID, "ancient tower falcons").build());
        assertThat(fused.getHitsList()).isNotEmpty();
        assertThat(fused.getHitsList())
                .allSatisfy(hit -> assertThat(hit.getDocId()).isEqualTo("doc-1"));
    }

    @Test
    void reindexingReplacesTheBlockInsteadOfDuplicating() {
        index("doc-1");
        index("doc-1");
        SearchResponse byTitle = searchStub.search(
                query(SearchLane.SEARCH_LANE_LEXICAL, "alpha")
                        .addFields("search_metadata_title").setK(10).build());
        assertThat(byTitle.getHitsList()).hasSize(1);
    }

    @Test
    void unknownSubjectsAreRefusedWithTheServedList() {
        assertThatThrownBy(() -> searchStub.search(
                query(SearchLane.SEARCH_LANE_LEXICAL, "alpha")
                        .setMappingSubject("no-such-subject").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription())
                            .contains("no-such-subject")
                            .contains(RepoDocumentMapping.SUBJECT);
                });
    }

    @Test
    void fieldsOutsideTheMappingAreRefusedByName() {
        assertThatThrownBy(() -> searchStub.search(
                query(SearchLane.SEARCH_LANE_LEXICAL, "alpha")
                        .addFields("ownership_account").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("ownership_account");
                });
    }

    @Test
    void theUnspecifiedLaneAndNonPositiveKAreRefused() {
        assertThatThrownBy(() -> searchStub.search(
                query(SearchLane.SEARCH_LANE_UNSPECIFIED, "alpha").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> searchStub.search(
                query(SearchLane.SEARCH_LANE_LEXICAL, "alpha").setK(0).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void vectorQueriesAgainstAPolicyFreeSubjectAreRefusedAsPreconditions() {
        assertThatThrownBy(() -> searchStub.search(
                query(SearchLane.SEARCH_LANE_VECTOR, "alpha")
                        .setMappingSubject("repo-document-lexical").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription()).contains("chunking policy");
                });
    }

    @Test
    void theRepositorysRefusalPassesThroughOnIndexing() {
        assertThatThrownBy(() -> index("doc-missing"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void listSubjectsDescribesTheServedSurface() {
        List<SubjectInfo> subjects = searchStub
                .listSubjects(ListSubjectsRequest.getDefaultInstance())
                .getSubjectsList();
        assertThat(subjects).hasSize(2);

        SubjectInfo withLane = subjects.stream()
                .filter(s -> s.getSubject().equals(RepoDocumentMapping.SUBJECT))
                .findFirst().orElseThrow();
        assertThat(withLane.getDocIdField()).isEqualTo("doc_id");
        assertThat(withLane.getTextFieldsList())
                .contains("search_metadata_title", RepoDocumentMapping.BODY_FIELD);
        assertThat(withLane.getHasVectorLane()).isTrue();
        assertThat(withLane.getPolicyDigest()).isEqualTo(policy().digest());

        SubjectInfo lexicalOnly = subjects.stream()
                .filter(s -> s.getSubject().equals("repo-document-lexical"))
                .findFirst().orElseThrow();
        assertThat(lexicalOnly.getHasVectorLane()).isFalse();
        assertThat(lexicalOnly.getPolicyDigest()).isEmpty();
    }

    @Test
    void indexingUnderAnUnknownSubjectIsRefusedWithTheServedList() {
        assertThatThrownBy(() -> indexStub.indexDocument(IndexDocumentRequest.newBuilder()
                .setAddress(NodeAddress.newBuilder().setDocId("doc-1")
                        .setGraphAddressId("ds").setAccountId("acct").setGraphId("intake:acct"))
                .setMappingSubject("no-such-subject")
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription())
                            .contains("no-such-subject")
                            .contains(RepoDocumentMapping.SUBJECT);
                });
    }

    @Test
    void indexingWithoutAnAddressOrSubjectIsRefused() {
        assertThatThrownBy(() -> indexStub.indexDocument(IndexDocumentRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> indexStub.indexDocument(IndexDocumentRequest.newBuilder()
                .setAddress(NodeAddress.newBuilder().setDocId("doc-1"))
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void aBlankQueryIsRefused() {
        assertThatThrownBy(() -> searchStub.search(
                query(SearchLane.SEARCH_LANE_LEXICAL, "   ").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }
}
