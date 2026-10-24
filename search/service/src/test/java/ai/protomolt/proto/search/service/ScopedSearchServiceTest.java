package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.search.chunk.SentencePackedChunker;
import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.v1.IndexDocumentRequest;
import ai.protomolt.proto.search.v1.SearchIndexServiceGrpc;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SearchResponse;
import ai.protomolt.proto.search.v1.SearchServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
                        RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(policy())))
                        .vectorizing(RepoDocumentMapping.laneVectorization()),
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
                        RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(policy())))
                        .vectorizing(RepoDocumentMapping.laneVectorization()),
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

    @Test
    void nettyEnforcesPayloadBudgetAndUnaryAdmissionBeforeHalfClose() throws Exception {
        CallerResolver resolver = credential -> "capped-querier".equals(credential)
                ? Optional.of(Caller.scoped("capped",
                        Set.of(Scopes.SEARCH_QUERY),
                        Map.of(Scopes.SEARCH_QUERY, new Caller.Budget(0, 128))))
                : Optional.empty();
        try (SearchServices bounded = SearchServices.build(
                new SearchServiceConfig(0, work.resolve("bounded-index"), Map.of(
                        RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(policy())))
                        .vectorizing(RepoDocumentMapping.laneVectorization()),
                address -> Document.newBuilder().setDocId("doc-1").build())
                .withAdmission(1024, 1)) {
            Server netty = bounded.startNetty(0, OPERATOR, resolver);
            ManagedChannel nettyChannel = NettyChannelBuilder
                    .forAddress("localhost", netty.getPort())
                    .usePlaintext()
                    .build();
            try {
                // 1. An over-cap frame for a budgeted caller is refused before halfClose()
                // and releases its UnaryRequestAdmission reservation.
                CountDownLatch budgetClosed = new CountDownLatch(1);
                AtomicReference<Status> budgetStatus = new AtomicReference<>();
                ClientCall<SearchRequest, SearchResponse> overCap =
                        nettyChannel.newCall(SearchServiceGrpc.getSearchMethod(), CallOptions.DEFAULT);
                overCap.start(new ClientCall.Listener<>() {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        budgetStatus.set(status);
                        budgetClosed.countDown();
                    }
                }, credentials("capped-querier"));
                overCap.request(1);
                overCap.sendMessage(query().toBuilder()
                        .setQuery("q".repeat(256))
                        .build());
                assertThat(budgetClosed.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(budgetStatus.get().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                assertThat(budgetStatus.get().getDescription()).contains("128-byte");
                awaitReservedBytes(bounded, 0);

                // 2. A call held open before halfClose() reserves 2 * maxRequestBytes (2048)
                // and blocks a second concurrent call before halfClose().
                ClientCall<SearchRequest, SearchResponse> held =
                        nettyChannel.newCall(SearchServiceGrpc.getSearchMethod(), CallOptions.DEFAULT);
                held.start(new ClientCall.Listener<>() { }, credentials(OPERATOR));
                held.request(1);
                held.sendMessage(query());
                awaitReservedBytes(bounded, 2048);

                CountDownLatch secondClosed = new CountDownLatch(1);
                AtomicReference<Status> secondStatus = new AtomicReference<>();
                ClientCall<SearchRequest, SearchResponse> second =
                        nettyChannel.newCall(SearchServiceGrpc.getSearchMethod(), CallOptions.DEFAULT);
                second.start(new ClientCall.Listener<>() {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        secondStatus.set(status);
                        secondClosed.countDown();
                    }
                }, credentials(OPERATOR));
                second.request(1);
                second.sendMessage(query());
                assertThat(secondClosed.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(secondStatus.get().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);

                held.cancel("cleanup", null);
                awaitReservedBytes(bounded, 0);
            } finally {
                nettyChannel.shutdownNow();
            }
        }
    }

    private static void awaitReservedBytes(SearchServices target, long expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (target.admission().reservedBytes() != expected
                && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(target.admission().reservedBytes()).isEqualTo(expected);
    }
}
