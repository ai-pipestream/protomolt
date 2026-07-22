package ai.pipestream.proto.rerank.tei;

import ai.pipestream.proto.rerank.RerankProvider;
import ai.pipestream.proto.rerank.RerankProviders;
import ai.pipestream.proto.rerank.ScoredText;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tei.v1.RerankGrpc;
import tei.v1.Tei.Rank;
import tei.v1.Tei.RerankRequest;
import tei.v1.Tei.RerankResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class TeiRerankProviderTest {

    private String serverName;
    private Server server;
    private ManagedChannel channel;
    private FakeRerank fake;
    private String savedTargetProperty;

    @BeforeEach
    void startInProcessServer() throws IOException {
        savedTargetProperty = System.clearProperty(TeiRerankProvider.TARGET_PROPERTY);
        serverName = "tei-rerank-" + UUID.randomUUID();
        fake = new FakeRerank();
        server = InProcessServerBuilder.forName(serverName)
                .addService(fake)
                .directExecutor()
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    @AfterEach
    void stopServer() {
        channel.shutdownNow();
        server.shutdownNow();
        if (savedTargetProperty == null) {
            System.clearProperty(TeiRerankProvider.TARGET_PROPERTY);
        } else {
            System.setProperty(TeiRerankProvider.TARGET_PROPERTY, savedTargetProperty);
        }
    }

    @Test
    void scoreSendsOneRequestWithQueryTextsAndNormalizedScores() {
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        provider.score("bamboo", List.of("bb", "ddddd", "a", "ccc"));

        assertThat(fake.requests).singleElement().satisfies(request -> {
            assertThat(request.getQuery()).isEqualTo("bamboo");
            assertThat(request.getTextsList()).containsExactly("bb", "ddddd", "a", "ccc");
            assertThat(request.getRawScores()).isFalse();
            assertThat(request.getReturnText()).isFalse();
        });
    }

    @Test
    void shuffledRanksScatterBackIntoInputOrder() {
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        List<Double> scores = provider.score("bamboo", List.of("bb", "ddddd", "a", "ccc"));

        // The fake scores by text length and answers sorted by descending score, as the real
        // server does, so the ranks arrive out of index order and prove the scatter realigns.
        assertThat(fake.lastRankOrder).containsExactly(1, 3, 0, 2);
        assertThat(scores).containsExactly(2.0, 5.0, 1.0, 3.0);
    }

    @Test
    void emptyTextsReturnEmptyWithoutACall() {
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        assertThat(provider.score("bamboo", List.of())).isEmpty();
        assertThat(fake.requests).isEmpty();
    }

    @Test
    void rankDefaultSortsAndTruncates() {
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        List<ScoredText> ranked = provider.rank("bamboo", List.of("bb", "ddddd", "a", "ccc"), 2);

        assertThat(ranked).containsExactly(
                new ScoredText(1, "ddddd", 5.0),
                new ScoredText(3, "ccc", 3.0));
    }

    @Test
    void outOfRangeRankIndexThrows() {
        fake.extraRankIndex = 7;
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        assertThatThrownBy(() -> provider.score("bamboo", List.of("a", "b")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rank index 7");
    }

    @Test
    void missingRankThrows() {
        fake.dropLastRank = true;
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        assertThatThrownBy(() -> provider.score("bamboo", List.of("a", "b")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no rank for text");
    }

    @Test
    void serverFailureIsWrappedNamingTheTarget() {
        fake.failWith = Status.UNAVAILABLE.asRuntimeException();
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        assertThatThrownBy(() -> provider.score("bamboo", List.of("boom")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(channel.authority())
                .hasCauseInstanceOf(io.grpc.StatusRuntimeException.class);
    }

    @Test
    void closeLeavesACallerOwnedChannelOpen() {
        TeiRerankProvider provider = new TeiRerankProvider(channel);

        provider.close();

        assertThat(channel.isShutdown()).isFalse();
        assertThat(provider.score("bamboo", List.of("still open"))).hasSize(1);
    }

    @Test
    void serviceLoaderFindsTheProviderById() {
        // The no-arg constructor must not touch the knobs, or an unconfigured provider would
        // break discovery of every other provider on the classpath.
        assertThat(RerankProviders.all()).containsKey("tei");
    }

    @Test
    void firstUseWithoutConfigurationNamesBothKnobs() {
        assumeThat(System.getenv(TeiRerankProvider.TARGET_ENVIRONMENT_VARIABLE)).isNull();

        TeiRerankProvider provider = new TeiRerankProvider();

        assertThatThrownBy(() -> provider.score("bamboo", List.of("a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(TeiRerankProvider.TARGET_PROPERTY)
                .hasMessageContaining(TeiRerankProvider.TARGET_ENVIRONMENT_VARIABLE);
    }

    @Test
    void noArgConstructorResolvesTheTargetProperty() throws Exception {
        Server loopback = NettyServerBuilder.forPort(0).addService(new FakeRerank())
                .build().start();
        try {
            System.setProperty(TeiRerankProvider.TARGET_PROPERTY,
                    "localhost:" + loopback.getPort());

            RerankProvider provider = RerankProviders.byId("tei");

            assertThat(provider).isInstanceOf(TeiRerankProvider.class);
            assertThat(provider.score("bamboo", List.of("wired"))).containsExactly(5.0);
            ((TeiRerankProvider) provider).close();
        } finally {
            loopback.shutdownNow();
        }
    }

    /**
     * Fake Rerank service: scores each text by its length and answers with ranks sorted by
     * descending score (as the real server does, out of index order for the fixture texts),
     * records requests, and can be switched to fail or to answer malformed ranks.
     */
    private static final class FakeRerank extends RerankGrpc.RerankImplBase {

        private final List<RerankRequest> requests = new CopyOnWriteArrayList<>();
        private final List<Integer> lastRankOrder = new CopyOnWriteArrayList<>();
        private volatile io.grpc.StatusRuntimeException failWith;
        private volatile int extraRankIndex = -1;
        private volatile boolean dropLastRank;

        @Override
        public void rerank(RerankRequest request, StreamObserver<RerankResponse> observer) {
            if (failWith != null) {
                observer.onError(failWith);
                return;
            }
            requests.add(request);
            List<Rank> ranks = new ArrayList<>();
            for (int i = 0; i < request.getTextsCount(); i++) {
                ranks.add(Rank.newBuilder()
                        .setIndex(i)
                        .setScore(request.getTexts(i).length())
                        .build());
            }
            ranks.sort(Comparator.comparingDouble(Rank::getScore).reversed());
            if (dropLastRank && !ranks.isEmpty()) {
                ranks.remove(ranks.size() - 1);
            }
            if (extraRankIndex >= 0) {
                ranks.add(Rank.newBuilder().setIndex(extraRankIndex).setScore(0).build());
            }
            lastRankOrder.clear();
            RerankResponse.Builder response = RerankResponse.newBuilder();
            for (Rank rank : ranks) {
                lastRankOrder.add(rank.getIndex());
                response.addRanks(rank);
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }
    }
}
