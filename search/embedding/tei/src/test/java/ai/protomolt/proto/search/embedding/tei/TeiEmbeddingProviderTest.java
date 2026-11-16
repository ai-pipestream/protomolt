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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

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
    private String savedAdmissionTimeoutProperty;

    @BeforeEach
    void startInProcessServer() throws IOException {
        savedTargetProperty = System.clearProperty(TeiEmbeddingProvider.TARGET_PROPERTY);
        savedTruncateProperty = System.clearProperty(TeiEmbeddingProvider.TRUNCATE_PROPERTY);
        savedMaxInFlightProperty = System.clearProperty(TeiEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY);
        savedAdmissionTimeoutProperty = System.clearProperty(TeiEmbeddingProvider.ADMISSION_TIMEOUT_PROPERTY);
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
        if (savedAdmissionTimeoutProperty == null) {
            System.clearProperty(TeiEmbeddingProvider.ADMISSION_TIMEOUT_PROPERTY);
        } else {
            System.setProperty(TeiEmbeddingProvider.ADMISSION_TIMEOUT_PROPERTY, savedAdmissionTimeoutProperty);
        }
    }

    @Test
    void completedFakeCallIsReleasedBeforeItsReplacementArrives() {
        var responses = new java.util.ArrayList<EmbedResponse>();
        StreamObserver<EmbedResponse> replacement = new StreamObserver<>() {
            @Override public void onNext(EmbedResponse value) { responses.add(value); }
            @Override public void onError(Throwable failure) { throw new AssertionError(failure); }
            @Override public void onCompleted() { }
        };
        fake.embed(EmbedRequest.newBuilder().setInputs("first").build(), new StreamObserver<>() {
            @Override public void onNext(EmbedResponse value) { responses.add(value); }
            @Override public void onError(Throwable failure) { throw new AssertionError(failure); }
            @Override public void onCompleted() {
                // The client can start another RPC as soon as completion is delivered,
                // before the previous server handler returns from onCompleted().
                fake.embed(EmbedRequest.newBuilder().setInputs("next").build(), replacement);
            }
        });
        assertThat(responses).hasSize(2);
        assertThat(fake.requests).extracting(EmbedRequest::getInputs).containsExactly("first", "next");
        assertThat(fake.inFlight.get()).isZero();
        assertThat(fake.peakInFlight()).isEqualTo(1);
    }

    @Test
    void overlappingInstancesAndAllEntryPointsShareProcessCap() throws Exception {
        var config = Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "2");
        try (var first = new TeiEmbeddingProvider(channel, config);
             var second = new TeiEmbeddingProvider(channel, config);
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            fake.hold = true;
            var calls = List.of(
                    executor.submit(() -> first.embedAll(List.of("a", "b", "c"))),
                    executor.submit(() -> second.embedAll(List.of("d", "e", "f"))),
                    executor.submit(() -> first.embedAllDocuments(List.of("g", "h"))),
                    executor.submit(() -> second.embedAllQueries(List.of("i", "j"))),
                    executor.submit(() -> first.embed("k")),
                    executor.submit(second::dimension));
            for (int answered = 0; answered < 12; answered++) {
                awaitHeld(1);
                Thread.sleep(30);
                assertThat(fake.peakInFlight()).isLessThanOrEqualTo(2);
                fake.answerOneHeld();
            }
            for (var call : calls) {
                assertThat(call.get(10, TimeUnit.SECONDS)).isNotNull();
            }
            assertThat(fake.peakInFlight()).isEqualTo(2);
        }
    }

    @Test
    void interruptedBatchCancelsActiveRpcAndReleasesAdmission() throws Exception {
        try (var provider = new TeiEmbeddingProvider(channel,
                Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "1"))) {
            var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var interrupted = new java.util.concurrent.atomic.AtomicBoolean();
            Thread caller = Thread.ofVirtual().start(() -> {
                try {
                    provider.embedAll(List.of("hang", "never"));
                } catch (Throwable problem) {
                    failure.set(problem);
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (fake.requests.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(fake.requests).hasSize(1);
            caller.interrupt();
            caller.join(5000);
            assertThat(caller.isAlive()).isFalse();
            assertThat(failure.get()).hasCauseInstanceOf(InterruptedException.class);
            assertThat(interrupted).isTrue();
            assertThat(provider.embed("recovered")).containsExactly(FakeEmbed.vectorFor("recovered"));
            assertThat(fake.requests).extracting(EmbedRequest::getInputs).doesNotContain("never");
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

    @Test
    void configuredAdmissionTimeoutBoundsTheWaitForASlot() throws Exception {
        var config = Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "1",
                TeiEmbeddingProvider.ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE, "1");
        try (var holder = new TeiEmbeddingProvider(channel, config);
             var waiter = new TeiEmbeddingProvider(channel, config)) {
            fake.hold = true;
            CompletableFuture<float[]> held = CompletableFuture.supplyAsync(() -> holder.embed("held"));
            awaitHeld(1);
            long started = System.nanoTime();
            assertThatThrownBy(() -> waiter.embed("waits"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
            long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertThat(waitedMillis).isBetween(900L, 10_000L);
            fake.hold = false;
            fake.answerOneHeld();
            assertThat(held.get(10, TimeUnit.SECONDS)).containsExactly(FakeEmbed.vectorFor("held"));
            assertThat(fake.requests).extracting(EmbedRequest::getInputs).doesNotContain("waits");
        }
    }

    @Test
    void nonPositiveAdmissionTimeoutIsRefusedNamingBothKnobs() {
        for (String bad : List.of("0", "-1", "soon")) {
            TeiEmbeddingProvider provider = new TeiEmbeddingProvider(channel,
                    Map.of(TeiEmbeddingProvider.ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE, bad));
            assertThatThrownBy(() -> provider.embed("x"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(TeiEmbeddingProvider.ADMISSION_TIMEOUT_PROPERTY)
                    .hasMessageContaining(TeiEmbeddingProvider.ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE)
                    .hasMessageContaining(bad);
        }
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

    @Test
    void embedAllQueuesOnlyCallerWaiterAndSpawnsWorkersOnlyAsPermitsAreAcquired() throws Exception {
        var config = Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "3");
        List<String> firstTexts = List.of("a0", "a1", "a2", "a3", "a4", "a5", "a6");
        List<String> secondTexts = List.of("b0", "b1", "b2", "b3", "b4", "b5", "b6");
        fake.hold = true;
        try (var holder = new TeiEmbeddingProvider(channel, config);
             var batchProvider = new TeiEmbeddingProvider(channel, config);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<float[]>> heldCalls = List.of(
                    executor.submit(() -> holder.embed("held-0")),
                    executor.submit(() -> holder.embed("held-1")),
                    executor.submit(() -> holder.embed("held-2")));
            try {
                for (int i = 0; i < 3; i++) {
                    assertThat(fake.arrivedHeld.poll(5, TimeUnit.SECONDS)).isNotNull();
                }
                assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isEqualTo(3);
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isZero();

                var batchOneResult = new CompletableFuture<List<float[]>>();
                var batchTwoResult = new CompletableFuture<List<float[]>>();
                Thread batchOneCaller = Thread.ofVirtual().start(() -> completeWith(
                        batchOneResult, () -> batchProvider.embedAll(firstTexts)));
                awaitParkedAdmissionWaiters(1, batchOneCaller);
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isEqualTo(1);
                assertThat(batchProvider.activeBatchWorkers()).isZero();

                Thread batchTwoCaller = Thread.ofVirtual().start(() -> completeWith(
                        batchTwoResult, () -> batchProvider.embedAll(secondTexts)));
                awaitParkedAdmissionWaiters(2, batchOneCaller, batchTwoCaller);
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isEqualTo(2);
                assertThat(batchProvider.activeBatchWorkers()).isZero();

                // Free exactly one permit: only one batch worker can run, and waiters stay at 2.
                fake.answerOneHeld();
                assertThat(fake.arrivedHeld.poll(5, TimeUnit.SECONDS)).isEqualTo("a0");
                awaitParkedAdmissionWaiters(2, batchOneCaller, batchTwoCaller);
                assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isEqualTo(3);
                assertThat(batchProvider.activeBatchWorkers()).isEqualTo(1);
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isEqualTo(2);

                fake.hold = false;
                while (!fake.held.isEmpty()) {
                    fake.answerOneHeld();
                }

                for (Future<float[]> heldCall : heldCalls) {
                    assertThat(heldCall.get(5, TimeUnit.SECONDS)).hasSize(FakeEmbed.DIMENSION);
                }
                List<float[]> firstVectors = batchOneResult.get(10, TimeUnit.SECONDS);
                List<float[]> secondVectors = batchTwoResult.get(10, TimeUnit.SECONDS);
                for (int i = 0; i < firstTexts.size(); i++) {
                    assertThat(firstVectors.get(i)).containsExactly(FakeEmbed.vectorFor(firstTexts.get(i)));
                    assertThat(secondVectors.get(i)).containsExactly(FakeEmbed.vectorFor(secondTexts.get(i)));
                }
                assertThat(batchProvider.activeBatchWorkers()).isZero();
                assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isZero();
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isZero();
            } finally {
                fake.hold = false;
                while (!fake.held.isEmpty()) {
                    fake.answerOneHeld();
                }
            }
        }
    }

    @Test
    void holdingProcessCapBlocksNeitherChannelCloseNorAddsWorkerWaitersInEmbedAllAndDimension()
            throws Exception {
        FakeEmbed loopbackFake = new FakeEmbed();
        Server loopback = NettyServerBuilder.forPort(0).addService(loopbackFake).build().start();
        var capConfig = Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "2");
        var ownedConfig = Map.of(
                TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE, "localhost:" + loopback.getPort(),
                TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "2");
        fake.hold = true;
        try (var holder = new TeiEmbeddingProvider(channel, capConfig);
             var owned = new TeiEmbeddingProvider(ownedConfig);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<float[]> firstHeld = executor.submit(() -> holder.embed("cap-0"));
            Future<float[]> secondHeld = executor.submit(() -> holder.embed("cap-1"));
            try {
                assertThat(fake.arrivedHeld.poll(5, TimeUnit.SECONDS)).isNotNull();
                assertThat(fake.arrivedHeld.poll(5, TimeUnit.SECONDS)).isNotNull();
                assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isEqualTo(2);

                var batchFuture = new CompletableFuture<List<float[]>>();
                var dimensionFuture = new CompletableFuture<Integer>();
                Thread batchCaller = Thread.ofVirtual().start(() -> completeWith(
                        batchFuture, () -> owned.embedAll(List.of("x0", "x1", "x2", "x3", "x4"))));
                Thread dimensionCaller = Thread.ofVirtual().start(() -> completeWith(
                        dimensionFuture, owned::dimension));

                awaitParkedAdmissionWaiters(2, batchCaller, dimensionCaller);
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isEqualTo(2);
                assertThat(owned.activeBatchWorkers()).isZero();
                assertThat(owned.activeChannel()).isNotNull();
                assertThat(owned.activeChannel().isShutdown()).isFalse();

                // Close must not wait on dimension()'s admission wait or hold-up.
                CompletableFuture.runAsync(owned::close, executor).get(2, TimeUnit.SECONDS);
                assertThat(owned.activeChannel().isShutdown()).isTrue();

                fake.hold = false;
                fake.answerOneHeld();
                fake.answerOneHeld();

                assertThat(firstHeld.get(5, TimeUnit.SECONDS)).containsExactly(FakeEmbed.vectorFor("cap-0"));
                assertThat(secondHeld.get(5, TimeUnit.SECONDS)).containsExactly(FakeEmbed.vectorFor("cap-1"));
                assertThatThrownBy(() -> batchFuture.get(5, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("closed");
                assertThatThrownBy(() -> dimensionFuture.get(5, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("closed");
                assertThat(loopbackFake.requests).isEmpty();
                assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isZero();
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isZero();
            } finally {
                fake.hold = false;
                while (!fake.held.isEmpty()) {
                    fake.answerOneHeld();
                }
            }
        } finally {
            loopback.shutdownNow();
        }
    }

    @Test
    void dimensionProbeRunsOutsideProviderLockAndDeduplicatesConcurrentColdCallers()
            throws Exception {
        FakeEmbed loopbackFake = new FakeEmbed();
        loopbackFake.holdWhen = input -> input.equals("dimension probe");
        Server loopback = NettyServerBuilder.forPort(0).addService(loopbackFake).build().start();
        var ownedConfig = Map.of(
                TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE, "localhost:" + loopback.getPort(),
                TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "4");
        try (var owned = new TeiEmbeddingProvider(ownedConfig);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var d1 = new CompletableFuture<Integer>();
            var d2 = new CompletableFuture<Integer>();
            var d3 = new CompletableFuture<Integer>();
            Thread t1 = Thread.ofVirtual().start(() -> completeWith(d1, owned::dimension));
            Thread t2 = Thread.ofVirtual().start(() -> completeWith(d2, owned::dimension));
            Thread t3 = Thread.ofVirtual().start(() -> completeWith(d3, owned::dimension));
            try {
                assertThat(loopbackFake.arrivedHeld.poll(5, TimeUnit.SECONDS))
                        .isEqualTo("dimension probe");
                awaitAllThreadsWaiting(t1, t2, t3);

                // While the probe RPC is parked open on the server and two cold callers
                // await the shared probe, another embed call and close() both proceed
                // without waiting on the provider lock, and only one probe RPC was sent.
                assertThat(loopbackFake.requests)
                        .extracting(EmbedRequest::getInputs)
                        .containsExactly("dimension probe");
                assertThat(executor.submit(() -> owned.embed("concurrent"))
                        .get(2, TimeUnit.SECONDS))
                        .containsExactly(FakeEmbed.vectorFor("concurrent"));
                assertThat(loopbackFake.requests)
                        .extracting(EmbedRequest::getInputs)
                        .containsExactly("dimension probe", "concurrent");
                assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isEqualTo(1);
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isZero();

                executor.submit(owned::close).get(2, TimeUnit.SECONDS);
                assertThat(owned.activeChannel().isShutdown()).isTrue();

                loopbackFake.answerOneHeld();

                assertThat(d1.get(5, TimeUnit.SECONDS)).isEqualTo(FakeEmbed.DIMENSION);
                assertThat(d2.get(5, TimeUnit.SECONDS)).isEqualTo(FakeEmbed.DIMENSION);
                assertThat(d3.get(5, TimeUnit.SECONDS)).isEqualTo(FakeEmbed.DIMENSION);
                assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isZero();
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isZero();
            } finally {
                loopbackFake.holdWhen = null;
                while (!loopbackFake.held.isEmpty()) {
                    loopbackFake.answerOneHeld();
                }
            }
        } finally {
            loopback.shutdownNow();
        }
    }

    @Test
    void interruptedDimensionProbeOwnerHandsOffToConcurrentCallerAndFailedProbeCanRetry()
            throws Exception {
        var config = Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "1");
        fake.failWith = Status.UNAVAILABLE.asRuntimeException();
        try (var provider = new TeiEmbeddingProvider(channel, config)) {
            assertThatThrownBy(provider::dimension)
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(io.grpc.StatusRuntimeException.class);
            fake.failWith = null;

            fake.hold = true;
            try (var holder = new TeiEmbeddingProvider(channel, config);
                 var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<float[]> heldCall = executor.submit(() -> holder.embed("occupy"));
                assertThat(fake.arrivedHeld.poll(5, TimeUnit.SECONDS)).isEqualTo("occupy");

                var ownerFailure = new AtomicReference<Throwable>();
                var ownerInterrupted = new AtomicBoolean();
                var ownerEntered = new CountDownLatch(1);
                Thread ownerThread = Thread.ofVirtual().start(() -> {
                    ownerEntered.countDown();
                    try {
                        provider.dimension();
                    } catch (Throwable failure) {
                        ownerFailure.set(failure);
                        ownerInterrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                assertThat(ownerEntered.await(5, TimeUnit.SECONDS)).isTrue();
                awaitParkedAdmissionWaiters(1, ownerThread);

                var waiterDimension = new CompletableFuture<Integer>();
                Thread waiterThread = Thread.ofVirtual().start(
                        () -> completeWith(waiterDimension, provider::dimension));
                awaitAllThreadsWaiting(ownerThread, waiterThread);
                assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isEqualTo(1);

                ownerThread.interrupt();
                ownerThread.join(5000);
                assertThat(ownerThread.isAlive()).isFalse();
                assertThat(ownerInterrupted).isTrue();
                assertThat(ownerFailure.get()).hasCauseInstanceOf(InterruptedException.class);

                fake.hold = false;
                fake.answerOneHeld();
                assertThat(heldCall.get(5, TimeUnit.SECONDS)).containsExactly(FakeEmbed.vectorFor("occupy"));
                assertThat(waiterDimension.get(5, TimeUnit.SECONDS)).isEqualTo(FakeEmbed.DIMENSION);
                assertThat(provider.dimension()).isEqualTo(FakeEmbed.DIMENSION);
            } finally {
                fake.hold = false;
                while (!fake.held.isEmpty()) {
                    fake.answerOneHeld();
                }
            }
        }
    }

    @Test
    void workerFailureWhileCallerWaitsForNextPermitAbortsWithoutLeavingInterruptOrLeakingSlots() {
        var config = Map.of(TeiEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE, "2");
        try (var provider = new TeiEmbeddingProvider(channel, config)) {
            long started = System.nanoTime();
            assertThatThrownBy(() -> provider.embedAll(List.of("hang", "boom", "never")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(channel.authority());
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertThat(elapsedMillis).isLessThan(10_000L);
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            assertThat(fake.requests).extracting(EmbedRequest::getInputs).doesNotContain("never");
            assertThat(provider.activeBatchWorkers()).isZero();
            assertThat(TeiEmbeddingProvider.inFlightLimit().inFlight()).isZero();
            assertThat(TeiEmbeddingProvider.inFlightLimit().waiting()).isZero();
        }
    }

    @Test
    void emptySingleAndNullBatchElementsRespectContractsWithoutStartingBatchWorkers() {
        try (var provider = new TeiEmbeddingProvider(channel)) {
            List<float[]> empty = provider.embedAll(List.of());
            assertThat(empty).isEmpty();
            assertThat(fake.requests).isEmpty();

            List<float[]> single = provider.embedAll(List.of("solo"));
            assertThat(single).singleElement()
                    .satisfies(v -> assertThat(v).containsExactly(FakeEmbed.vectorFor("solo")));
            assertThat(provider.activeBatchWorkers()).isZero();

            fake.requests.clear();
            assertThatThrownBy(() -> provider.embedAll(Arrays.asList("valid", null)))
                    .isInstanceOf(NullPointerException.class);
            assertThat(fake.requests).isEmpty();
        }
    }

    private static <T> void completeWith(
            CompletableFuture<T> target,
            Callable<T> action) {
        try {
            target.complete(action.call());
        } catch (Throwable failure) {
            target.completeExceptionally(failure);
        }
    }

    private static void awaitAllThreadsWaiting(Thread... threads) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!allWaiting(threads)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for threads to park");
            }
            LockSupport.parkNanos(100_000L);
        }
    }

    private static boolean allWaiting(Thread... threads) {
        for (Thread thread : threads) {
            Thread.State state = thread.getState();
            if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
                return false;
            }
        }
        return true;
    }

    private static void awaitParkedAdmissionWaiters(int expectedWaiters, Thread... callers) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!allParkedInAdmission(expectedWaiters, callers)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Expected " + expectedWaiters + " parked waiters, found "
                        + TeiEmbeddingProvider.inFlightLimit().waiting());
            }
            LockSupport.parkNanos(100_000L);
        }
    }

    private static boolean allParkedInAdmission(int expectedWaiters, Thread... callers) {
        if (TeiEmbeddingProvider.inFlightLimit().waiting() != expectedWaiters) {
            return false;
        }
        for (Thread caller : callers) {
            Thread.State state = caller.getState();
            if (state != Thread.State.TIMED_WAITING && state != Thread.State.WAITING) {
                return false;
            }
        }
        return true;
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
        private final LinkedBlockingQueue<String> arrivedHeld = new LinkedBlockingQueue<>();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private volatile io.grpc.StatusRuntimeException failWith;
        /** When set, answers are parked in {@link #held} until {@link #answerOneHeld()}. */
        private volatile boolean hold;
        /** Optional selective hold filter; parks matching inputs when non-null. */
        private volatile Predicate<String> holdWhen;

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
                // Completion can release the client and admit its next request before
                // onCompleted returns. Stop counting this response before publishing it.
                inFlight.decrementAndGet();
                observer.onCompleted();
            };
            track();
            Predicate<String> filter = holdWhen;
            if (hold || (filter != null && filter.test(request.getInputs()))) {
                held.addLast(answer);
                arrivedHeld.add(request.getInputs());
                return;
            }
            answer.run();
        }
    }
}
