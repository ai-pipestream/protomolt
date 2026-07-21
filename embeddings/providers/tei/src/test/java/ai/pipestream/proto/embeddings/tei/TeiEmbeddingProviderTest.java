package ai.pipestream.proto.embeddings.tei;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.embeddings.EmbeddingProviders;
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
import tei.v1.EmbedGrpc;
import tei.v1.Tei.EmbedRequest;
import tei.v1.Tei.EmbedResponse;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class TeiEmbeddingProviderTest {

    private String serverName;
    private Server server;
    private ManagedChannel channel;
    private FakeEmbed fake;
    private String savedTargetProperty;
    private String savedTruncateProperty;

    @BeforeEach
    void startInProcessServer() throws IOException {
        savedTargetProperty = System.clearProperty(TeiEmbeddingProvider.TARGET_PROPERTY);
        savedTruncateProperty = System.clearProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY);
        serverName = "tei-" + UUID.randomUUID();
        fake = new FakeEmbed();
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
            System.clearProperty(TeiEmbeddingProvider.TARGET_PROPERTY);
        } else {
            System.setProperty(TeiEmbeddingProvider.TARGET_PROPERTY, savedTargetProperty);
        }
        if (savedTruncateProperty == null) {
            System.clearProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY);
        } else {
            System.setProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY, savedTruncateProperty);
        }
    }

    @Test
    void embedSendsTheTextAndReturnsTheVector() {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        float[] vector = provider.embed("hello world");

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.getInputs()).isEqualTo("hello world"));
        assertThat(vector).containsExactly(FakeEmbed.vectorFor("hello world"));
    }

    @Test
    void embedAllPreservesInputOrderAcrossConcurrentResponses() {
        List<String> texts = List.of("alpha", "beta", "gamma", "delta", "epsilon");
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        List<float[]> vectors = provider.embedAll(texts);

        assertThat(vectors).hasSize(texts.size());
        for (int i = 0; i < texts.size(); i++) {
            assertThat(vectors.get(i)).as("vector for '%s'", texts.get(i))
                    .containsExactly(FakeEmbed.vectorFor(texts.get(i)));
        }
        assertThat(fake.requests).hasSize(texts.size());
    }

    @Test
    void dimensionProbesExactlyOnceAcrossRepeatedCalls() {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        int first = provider.dimension();
        int second = provider.dimension();

        assertThat(first).isEqualTo(FakeEmbed.DIMENSION);
        assertThat(second).isEqualTo(FakeEmbed.DIMENSION);
        assertThat(fake.requests).hasSize(1);
    }

    @Test
    void embedRequestsServerSideTruncationByDefault() {
        assumeThat(System.getenv(TeiEmbeddingProvider.TRUNCATE_ENVIRONMENT_VARIABLE)).isNull();
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        provider.embed("possibly overlength");

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.getTruncate()).isTrue());
    }

    @Test
    void truncatePropertySetToFalseTurnsServerSideTruncationOff() {
        System.setProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY, "false");
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        provider.embed("keep whole or fail");

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.getTruncate()).isFalse());
    }

    @Test
    void embedAllCancelsUncollectedCallsWhenACollectedCallFails() {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        long start = System.nanoTime();
        assertThatThrownBy(() -> provider.embedAll(List.of("boom", "hang")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(channel.authority());
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // The 'hang' call never gets an answer; only cancellation keeps embedAll from
        // sitting on it until the 30 second per-call deadline.
        assertThat(elapsedMillis).isLessThan(10_000L);
    }

    @Test
    void serverFailureIsWrappedNamingTheTarget() {
        fake.failWith = Status.UNAVAILABLE.asRuntimeException();
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        assertThatThrownBy(() -> provider.embed("boom"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(channel.authority())
                .hasCauseInstanceOf(io.grpc.StatusRuntimeException.class);
    }

    @Test
    void closeLeavesACallerOwnedChannelOpen() {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel);

        provider.close();

        assertThat(channel.isShutdown()).isFalse();
        assertThat(provider.embed("still open")).hasSize(FakeEmbed.DIMENSION);
    }

    @Test
    void serviceLoaderFindsTheProviderById() {
        // The no-arg constructor must not touch the knobs, or an unconfigured provider would
        // break discovery of every other provider on the classpath.
        assertThat(EmbeddingProviders.all()).containsKey("tei");
    }

    @Test
    void firstUseWithoutConfigurationNamesBothKnobs() {
        assumeThat(System.getenv(TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE)).isNull();

        TeiEmbeddingProvider provider = new TeiEmbeddingProvider();

        assertThatThrownBy(provider::dimension)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(TeiEmbeddingProvider.TARGET_PROPERTY)
                .hasMessageContaining(TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE);
    }

    @Test
    void noArgConstructorResolvesTheTargetProperty() throws Exception {
        Server loopback = NettyServerBuilder.forPort(0).addService(new FakeEmbed())
                .build().start();
        try {
            System.setProperty(TeiEmbeddingProvider.TARGET_PROPERTY,
                    "localhost:" + loopback.getPort());

            EmbeddingProvider provider = EmbeddingProviders.byId("tei");

            assertThat(provider).isInstanceOf(TeiEmbeddingProvider.class);
            assertThat(provider.embed("wired")).containsExactly(FakeEmbed.vectorFor("wired"));
            assertThat(provider.dimension()).isEqualTo(FakeEmbed.DIMENSION);
            ((TeiEmbeddingProvider) provider).close();
        } finally {
            loopback.shutdownNow();
        }
    }

    /**
     * Fake Embed service: answers every request with a deterministic three-component vector
     * derived from the input text, records requests, and can be switched to fail. Two inputs
     * are special: {@code boom} fails immediately with INTERNAL, {@code hang} is never
     * answered, so its call stays open until the client cancels it or the deadline expires.
     */
    private static final class FakeEmbed extends EmbedGrpc.EmbedImplBase {
        private static final int DIMENSION = 3;

        private final List<EmbedRequest> requests = new CopyOnWriteArrayList<>();
        private volatile io.grpc.StatusRuntimeException failWith;

        /** The vector {@code text} embeds to: derived from the text, so tests see ordering. */
        private static float[] vectorFor(String text) {
            return new float[]{text.length(), text.hashCode() % 1000, text.isEmpty() ? 0 : 1};
        }

        @Override
        public void embed(EmbedRequest request, StreamObserver<EmbedResponse> observer) {
            if (failWith != null) {
                observer.onError(failWith);
                return;
            }
            requests.add(request);
            if (request.getInputs().equals("boom")) {
                observer.onError(Status.INTERNAL.withDescription("boom").asRuntimeException());
                return;
            }
            if (request.getInputs().equals("hang")) {
                return;
            }
            EmbedResponse.Builder response = EmbedResponse.newBuilder();
            for (float component : vectorFor(request.getInputs())) {
                response.addEmbeddings(component);
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }
    }
}
