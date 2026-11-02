package ai.protomolt.proto.search.embedding.tei;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import ai.protomolt.proto.search.embedding.EmbeddingProviders;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TeiEmbeddingProviderTest {

    private String serverName;
    private Server server;
    private ManagedChannel channel;
    private FakeEmbed fake;
    private String savedTargetProperty;
    private String savedTruncateProperty;
    private String savedMaxInFlightProperty;

    @BeforeEach
    void startInProcessServer() throws IOException {
        savedTargetProperty = System.clearProperty(TeiEmbeddingProvider.TARGET_PROPERTY);
        savedTruncateProperty = System.clearProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY);
        savedMaxInFlightProperty = System.clearProperty(TeiEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY);
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
        if (savedMaxInFlightProperty == null) {
            System.clearProperty(TeiEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY);
        } else {
            System.setProperty(TeiEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, savedMaxInFlightProperty);
        }
    }

    @Test
    void embedAllKeepsAtMostTheConfiguredCallsInFlight() throws Exception {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel,
                Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "2"));
        List<String> texts = List.of("c0", "c1", "c2", "c3", "c4", "c5", "c6");
        fake.hold = true;

        CompletableFuture<List<float[]>> batch = CompletableFuture.supplyAsync(() -> provider.embedAll(texts));

        int answered = 0;
        while (answered < texts.size()) {
            int expected = Math.min(2, texts.size() - answered);
            awaitHeld(expected);
            // Give any task that ignored the cap time to open its call before checking.
            Thread.sleep(150);
            assertThat(fake.held.size()).as("calls in flight").isEqualTo(expected);
            assertThat(fake.peakInFlight()).isLessThanOrEqualTo(2);
            fake.answerOneHeld();
            answered++;
        }
        List<float[]> vectors = batch.get(10, TimeUnit.SECONDS);

        assertThat(fake.peakInFlight()).isEqualTo(2);
        assertThat(vectors).hasSize(texts.size());
        for (int i = 0; i < texts.size(); i++) {
            assertThat(vectors.get(i)).containsExactly(FakeEmbed.vectorFor(texts.get(i)));
        }
    }

    @Test
    void embedAllDefaultsToEightCallsInFlight() throws Exception {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel, Map.of());
        List<String> texts = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            texts.add("chunk-" + i);
        }
        fake.hold = true;

        CompletableFuture<List<float[]>> batch = CompletableFuture.supplyAsync(() -> provider.embedAll(texts));
        awaitHeld(TeiEmbeddingProvider.DEFAULT_MAX_IN_FLIGHT);
        Thread.sleep(150);
        assertThat(fake.held.size()).isEqualTo(TeiEmbeddingProvider.DEFAULT_MAX_IN_FLIGHT);
        fake.hold = false;
        while (!fake.held.isEmpty()) {
            fake.answerOneHeld();
        }

        assertThat(batch.get(10, TimeUnit.SECONDS)).hasSize(texts.size());
        assertThat(fake.peakInFlight()).isEqualTo(TeiEmbeddingProvider.DEFAULT_MAX_IN_FLIGHT);
    }

    @Test
    void embedAllFailureStillCancelsTheRestUnderTheCap() {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel,
                Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "1"));

        long start = System.nanoTime();
        assertThatThrownBy(() -> provider.embedAll(List.of("boom", "hang", "after")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(channel.authority());
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(elapsedMillis).isLessThan(10_000L);
    }

    @Test
    void nonPositiveInFlightCapIsRefusedNamingBothKnobs() {
        for (String bad : List.of("0", "-3", "many")) {
            TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel,
                    Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, bad));
            assertThatThrownBy(() -> provider.embedAll(List.of("x")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(TeiEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY)
                    .hasMessageContaining(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE)
                    .hasMessageContaining(bad);
        }
    }

    @Test
    void inFlightCapPropertyBeatsTheEnvironmentVariable() throws Exception {
        System.setProperty(TeiEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, "3");
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel,
                Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "1"));
        fake.hold = true;

        CompletableFuture<List<float[]>> batch = CompletableFuture.supplyAsync(
                () -> provider.embedAll(List.of("a", "b", "c", "d", "e")));
        awaitHeld(3);
        fake.hold = false;
        while (!fake.held.isEmpty()) {
            fake.answerOneHeld();
        }

        assertThat(batch.get(10, TimeUnit.SECONDS)).hasSize(5);
        assertThat(fake.peakInFlight()).isEqualTo(3);
    }

    private void awaitHeld(int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (fake.held.size() < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("only " + fake.held.size() + " of " + count + " calls arrived");
            }
            Thread.sleep(5);
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
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel, Map.of());

        provider.embed("possibly overlength");

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.getTruncate()).isTrue());
    }

    @Test
    void truncatePropertySetToFalseTurnsServerSideTruncationOff() {
        System.setProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY, "false");
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel, Map.of());

        provider.embed("keep whole or fail");

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.getTruncate()).isFalse());
    }

    @Test
    void truncateEnvironmentVariableSetToFalseTurnsServerSideTruncationOff() {
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel,
                Map.of(TeiEmbeddingProvider.TRUNCATE_ENVIRONMENT_VARIABLE, "false"));

        provider.embed("keep whole or fail");

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.getTruncate()).isFalse());
    }

    @Test
    void truncatePropertyBeatsTheEnvironmentVariable() {
        System.setProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY, "true");
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel,
                Map.of(TeiEmbeddingProvider.TRUNCATE_ENVIRONMENT_VARIABLE, "false"));

        provider.embed("possibly overlength");

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.getTruncate()).isTrue());
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
        TeiEmbeddingProvider provider = new TeiEmbeddingProvider(Map.of());

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
        private final LinkedBlockingDeque<Runnable> held = new LinkedBlockingDeque<>();
        private final java.util.concurrent.atomic.AtomicInteger inFlight =
                new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger peak =
                new java.util.concurrent.atomic.AtomicInteger();
        private volatile io.grpc.StatusRuntimeException failWith;
        /** When set, answers are parked in {@link #held} until {@link #answerOneHeld()}. */
        private volatile boolean hold;

        int peakInFlight() {
            return peak.get();
        }

        /** Answers the oldest parked call. */
        void answerOneHeld() throws InterruptedException {
            Runnable answer = held.pollFirst(10, TimeUnit.SECONDS);
            if (answer == null) {
                throw new AssertionError("no parked call to answer");
            }
            answer.run();
        }

        private void track() {
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
        }

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
            Runnable answer = () -> {
                observer.onNext(response.build());
                observer.onCompleted();
                inFlight.decrementAndGet();
            };
            track();
            if (hold) {
                held.addLast(answer);
                return;
            }
            answer.run();
        }
    }
}
