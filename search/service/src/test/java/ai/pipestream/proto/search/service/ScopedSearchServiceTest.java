package ai.pipestream.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.CallerResolver;
import ai.pipestream.proto.chunk.SentencePackedChunker;
import ai.pipestream.proto.index.spi.ChunkingPolicy;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The search service under identity: querying requires search-query, the indexing service
 * requires search-index, the credential check runs before everything else, and the operator
 * keeps both sides.
 */
class ScopedSearchServiceTest {

    private static final String OPERATOR = "test-operator";
    private static final String QUERIER = "querier-cred";
    private static final String INDEXER = "indexer-cred";

    @TempDir
    static Path work;

    private static SearchServices service;
    private static ManagedChannel channel;

    private static ChunkingPolicy policy() {
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        12, 0, 2, 30, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        SearchTestProvider.PROVIDER_ID, SearchTestProvider.DIMENSION,
                        VectorSimilarity.COSINE, true),
                "", true);
    }

    @BeforeAll
    static void boot() throws Exception {
        Document document = Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Alpha Treaty Archive")
                        .setBody("The alpha treaty archive holds the northern accords."))
                .build();
        service = SearchServices.build(
                new SearchServiceConfig(0, work.resolve("index"), Map.of(
                        RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(policy()))),
                address -> document);

        CallerResolver resolver = credential -> switch (credential) {
            case QUERIER -> Optional.of(Caller.scoped("querier",
                    Set.of(Scopes.SEARCH_QUERY)));
            case INDEXER -> Optional.of(Caller.scoped("indexer",
                    Set.of(Scopes.SEARCH_INDEX)));
            default -> Optional.empty();
        };
        String name = InProcessServerBuilder.generateName();
        service.startInProcess(name, OPERATOR, resolver);
        channel = InProcessChannelBuilder.forName(name).build();

        index(OPERATOR);
    }

    @AfterAll
    static void shutdown() {
        channel.shutdownNow();
        service.close();
    }

    private static Metadata credentials(String credential) {
        Metadata headers = new Metadata();
        if (credential != null) {
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    credential);
        }
        return headers;
    }

    private static void index(String credential) {
        SearchIndexServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                        credentials(credential)))
                .indexDocument(IndexDocumentRequest.newBuilder()
                        .setAddress(NodeAddress.newBuilder().setDocId("doc-1")
                                .setGraphAddressId("ds").setAccountId("acct")
                                .setGraphId("intake:acct"))
                        .setMappingSubject(RepoDocumentMapping.SUBJECT)
                        .build());
    }

    private static SearchServiceGrpc.SearchServiceBlockingStub search(String credential) {
        return SearchServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                        credentials(credential)));
    }

    private static SearchRequest query() {
        return SearchRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .setQuery("alpha treaty")
                .setK(3)
                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                .build();
    }

    @Test
    void aQuerierSearchesButCannotIndex() {
        assertThat(search(QUERIER).search(query()).getHitsCount()).isGreaterThan(0);

        assertThatThrownBy(() -> index(QUERIER))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("querier").contains(Scopes.SEARCH_INDEX);
                });
    }

    @Test
    void anIndexerIndexesButCannotSearch() {
        index(INDEXER);

        assertThatThrownBy(() -> search(INDEXER).search(query()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("indexer").contains(Scopes.SEARCH_QUERY);
                });
    }

    @Test
    void theCredentialCheckRunsFirst() {
        assertThatThrownBy(() -> search(null).search(query()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.UNAUTHENTICATED));
        assertThatThrownBy(() -> search("guessed").search(query()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void theOperatorKeepsBothSides() {
        assertThat(search(OPERATOR).search(query()).getHitsCount()).isGreaterThan(0);
        index(OPERATOR);
    }

    @Test
    void aTokenlessServiceStaysOpen() throws Exception {
        try (SearchServices open = SearchServices.build(
                new SearchServiceConfig(0, work.resolve("open-index"), Map.of(
                        RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(policy()))),
                address -> Document.newBuilder().setDocId("doc-1").build())) {
            String name = InProcessServerBuilder.generateName();
            open.startInProcess(name);
            ManagedChannel openChannel = InProcessChannelBuilder.forName(name).build();
            try {
                assertThat(SearchServiceGrpc.newBlockingStub(openChannel)
                        .search(query()).getHitsCount()).isEqualTo(0);
            } finally {
                openChannel.shutdownNow();
            }
        }
    }
}
