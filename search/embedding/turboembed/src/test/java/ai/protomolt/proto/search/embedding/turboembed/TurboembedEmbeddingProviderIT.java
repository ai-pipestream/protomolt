package ai.protomolt.proto.search.embedding.turboembed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import com.google.protobuf.ByteString;
import inference.GRPCInferenceServiceGrpc;
import inference.GrpcPredictV2.InferParameter;
import inference.GrpcPredictV2.ModelInferRequest;
import inference.GrpcPredictV2.ModelInferResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Protocol integration fixtures, not a certification of a live Turboembed deployment. */
class TurboembedEmbeddingProviderIT {
    private static final String DIGEST = "a".repeat(64);
    private final List<ModelInferRequest> requests = new java.util.concurrent.CopyOnWriteArrayList<>();
    private UnaryOperator<ModelInferResponse> transform = UnaryOperator.identity();
    private Server server;
    private ManagedChannel channel;
    private TurboembedEmbeddingProvider provider;
    private Status failure;
    private String savedCap;
    private volatile boolean hold;
    private final java.util.concurrent.LinkedBlockingQueue<Runnable> held = new java.util.concurrent.LinkedBlockingQueue<>();
    private final java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicInteger peak = new java.util.concurrent.atomic.AtomicInteger();

    @BeforeEach
    void start() throws Exception {
        savedCap = System.clearProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new GRPCInferenceServiceGrpc.GRPCInferenceServiceImplBase() {
                    @Override
                    public void modelInfer(ModelInferRequest request, StreamObserver<ModelInferResponse> observer) {
                        requests.add(request);
                        if (failure != null) {
                            observer.onError(failure.asRuntimeException());
                            return;
                        }
                        int count = request.getInputs(0).getContents().getBytesContentsCount();
                        ByteBuffer raw = ByteBuffer.allocate(count * 2 * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                        for (int i = 0; i < count; i++) {
                            raw.putFloat(request.getInputs(0).getContents().getBytesContents(i).size()).putFloat(1);
                        }
                        Runnable answer = () -> {
                            observer.onNext(transform.apply(ModelInferResponse.newBuilder()
                                    .setModelName("bundle").setModelVersion("revision-1")
                                    .putParameters("manifest_sha256", InferParameter.newBuilder().setStringParam(DIGEST).build())
                                    .addOutputs(ModelInferResponse.InferOutputTensor.newBuilder()
                                            .setName("vectors").setDatatype("FP32").addShape(count).addShape(2))
                                    .addRawOutputContents(ByteString.copyFrom(raw.array())).build()));
                            inFlight.decrementAndGet();
                            observer.onCompleted();
                        };
                        peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                        if (hold) {
                            held.add(answer);
                        } else {
                            answer.run();
                        }
                    }
                }).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        provider = new TurboembedEmbeddingProvider(channel,
                new TurboembedEmbeddingProvider.Model("bundle", "revision-1", DIGEST, 2));
    }

    @AfterEach
    void close() {
        if (savedCap == null) {
            System.clearProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY);
        } else {
            System.setProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, savedCap);
        }
        provider.close();
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void overlappingInstancesAndEveryEntryPointShareCap() throws Exception {
        System.setProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, "2");
        hold = true;
        try (var second = new TurboembedEmbeddingProvider(channel,
                new TurboembedEmbeddingProvider.Model("bundle", "revision-1", DIGEST, 2));
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var calls = List.of(
                    executor.submit(() -> provider.embedAll(java.util.Collections.nCopies(33, "a"))),
                    executor.submit(() -> second.embedAll(java.util.Collections.nCopies(33, "b"))),
                    executor.submit(() -> provider.embedAllDocuments(List.of("c"))),
                    executor.submit(() -> second.embedAllQueries(List.of("d"))),
                    executor.submit(() -> provider.embed("e")),
                    executor.submit(() -> second.embedDocument("f")),
                    executor.submit(() -> second.embedQuery("g")));
            for (int answered = 0; answered < 9; answered++) {
                Runnable answer = held.poll(10, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(answer).isNotNull();
                Thread.sleep(30);
                assertThat(peak.get()).isLessThanOrEqualTo(2);
                answer.run();
            }
            for (var call : calls) {
                assertThat(call.get(10, java.util.concurrent.TimeUnit.SECONDS)).isNotNull();
            }
            assertThat(peak.get()).isEqualTo(2);
        }
    }

    @Test
    void closeRefusesQueuedCallsWithoutClosingCallerChannelOrOtherProvider() throws Exception {
        System.setProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, "1");
        hold = true;
        try (var other = new TurboembedEmbeddingProvider(channel,
                new TurboembedEmbeddingProvider.Model("bundle", "revision-1", DIGEST, 2));
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var active = executor.submit(() -> other.embed("active"));
            Runnable answer = held.poll(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(answer).isNotNull();
            var queued = executor.submit(() -> provider.embed("queued"));
            Thread.sleep(100);
            provider.close();
            assertThat(channel.isShutdown()).isFalse();
            hold = false;
            answer.run();
            active.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThatThrownBy(() -> queued.get(5, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasStackTraceContaining("provider is closed");
            assertThat(requests).hasSize(1);
            assertThat(other.embed("survives")).containsExactly(8, 1);
        }
    }

    @Test
    void invalidCapRefusesBeforeSendingAnyRpc() {
        for (String value : List.of("0", "-1", "invalid")) {
            System.setProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, value);
            assertThatThrownBy(() -> provider.embed("x"))
                    .hasMessageContaining(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY)
                    .hasMessageContaining(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE);
        }
        assertThat(requests).isEmpty();
        System.setProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, "1");
        assertThat(provider.embed("valid")).containsExactly(5, 1);
    }

    @Test
    void waitingCallCanBeInterruptedWithoutConsumingAPermit() throws Exception {
        System.setProperty(TurboembedEmbeddingProvider.MAX_IN_FLIGHT_PROPERTY, "1");
        hold = true;
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var active = executor.submit(() -> provider.embed("active"));
            Runnable answer = held.poll(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(answer).isNotNull();
            var problem = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var interrupted = new java.util.concurrent.atomic.AtomicBoolean();
            Thread waiting = Thread.ofVirtual().start(() -> {
                try {
                    provider.embedAllQueries(List.of("cancelled"));
                } catch (Throwable failure) {
                    problem.set(failure);
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            });
            Thread.sleep(100);
            waiting.interrupt();
            waiting.join(5000);
            assertThat(waiting.isAlive()).isFalse();
            assertThat(problem.get()).hasCauseInstanceOf(InterruptedException.class);
            assertThat(interrupted).isTrue();
            assertThat(requests).hasSize(1);
            hold = false;
            answer.run();
            active.get(10, java.util.concurrent.TimeUnit.SECONDS);
            failure = Status.UNAVAILABLE;
            assertThatThrownBy(() -> provider.embed("fail")).hasMessageContaining("UNAVAILABLE");
            failure = null;
            assertThat(provider.embed("recovered")).containsExactly(9, 1);
        }
    }

    @Test
    void documentAndQuerySelectDifferentPurposesAndPinIdentity() {
        assertThat(provider.embedDocument("héllo")).containsExactly(6, 1);
        assertThat(provider.embedQuery("question")).containsExactly(8, 1);
        assertThat(requests).hasSize(2);
        assertThat(requests.get(0).getParametersOrThrow("prompt_role").getStringParam()).isEqualTo("PROMPT_DOCUMENT");
        assertThat(requests.get(1).getParametersOrThrow("prompt_role").getStringParam()).isEqualTo("PROMPT_QUERY");
        for (ModelInferRequest request : requests) {
            assertThat(request.getModelName()).isEqualTo("bundle");
            assertThat(request.getModelVersion()).isEqualTo("revision-1");
            assertThat(request.getInputs(0).getName()).isEqualTo("texts");
            assertThat(request.getParametersOrThrow("truncate").getStringParam()).isEqualTo("TRUNCATE_NONE");
            assertThat(request.getParametersOrThrow("quantization").getStringParam()).isEqualTo("QUANTIZE_NONE");
        }
    }

    @Test
    void batchingPreservesOrderAndNeutralPurpose() {
        List<String> texts = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            texts.add("x".repeat(i));
        }
        List<float[]> vectors = provider.embedAll(texts);
        assertThat(requests).hasSize(2);
        for (int i = 0; i < texts.size(); i++) {
            assertThat(vectors.get(i)).containsExactly(i + 1, 1);
        }
        assertThat(requests.getFirst().getParametersOrThrow("prompt_role").getStringParam()).isEqualTo("PROMPT_NONE");
    }

    @Test
    void batchedPurposesKeepTheirPromptRole() {
        List<String> texts = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            texts.add("x".repeat(i));
        }
        List<float[]> documents = provider.embedAllDocuments(texts);
        List<float[]> queries = provider.embedAllQueries(List.of("q"));
        assertThat(requests).hasSize(3);
        for (int i = 0; i < texts.size(); i++) {
            assertThat(documents.get(i)).containsExactly(i + 1, 1);
        }
        assertThat(queries).singleElement().satisfies(vector -> assertThat(vector).containsExactly(1, 1));
        assertThat(requests.subList(0, 2)).allSatisfy(request ->
                assertThat(request.getParametersOrThrow("prompt_role").getStringParam()).isEqualTo("PROMPT_DOCUMENT"));
        assertThat(requests.get(2).getParametersOrThrow("prompt_role").getStringParam()).isEqualTo("PROMPT_QUERY");
    }

    @Test
    void wrongRevisionAndMissingOrDifferentManifestRefuse() {
        transform = r -> r.toBuilder().setModelVersion("revision-2").build();
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("model revision or manifest");
        transform = r -> r.toBuilder().clearParameters().build();
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("model revision or manifest");
        transform = r -> r.toBuilder().putParameters("manifest_sha256",
                InferParameter.newBuilder().setStringParam("b".repeat(64)).build()).build();
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("model revision or manifest");
    }

    @Test
    void malformedRawBytesAndNonFiniteVectorsRefuse() {
        transform = r -> r.toBuilder().setRawOutputContents(0, ByteString.copyFrom(new byte[3])).build();
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("raw bytes");
        transform = r -> r.toBuilder().setRawOutputContents(0, ByteString.copyFrom(
                ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putFloat(Float.NaN).putFloat(1).array())).build();
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("non-finite");
    }

    @Test
    void shapeOrPinnedDimensionMismatchRefuses() {
        transform = r -> r.toBuilder().setOutputs(0, r.getOutputs(0).toBuilder().setShape(0, 2)).build();
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("shape");
        transform = UnaryOperator.identity();
        try (var different = new TurboembedEmbeddingProvider(channel,
                new TurboembedEmbeddingProvider.Model("bundle", "revision-1", DIGEST, 3))) {
            assertThatThrownBy(() -> different.embed("x")).hasMessageContaining("dimension");
        }
    }

    @Test
    void remoteFailureRemainsFailureAndCloseDoesNotOwnTheChannel() {
        failure = Status.UNAVAILABLE.withDescription("fixture unavailable");
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("UNAVAILABLE");
        provider.close();
        assertThat(channel.isShutdown()).isFalse();
        assertThatThrownBy(() -> provider.embed("x")).hasMessageContaining("closed");
    }

    @Test
    void modelBundleChangesProviderIdentityForPolicyDigests() {
        assertThat(provider.providerId()).isEqualTo("turboembed-v1:" + DIGEST);
        try (var another = new TurboembedEmbeddingProvider(channel,
                new TurboembedEmbeddingProvider.Model("bundle", "revision-1", "b".repeat(64), 2))) {
            assertThat(another.providerId()).isNotEqualTo(provider.providerId());
        }
    }

    @Test
    void discoveryIsInertAndMissingSelectedConfigurationRefuses() {
        assertThat(ServiceLoader.load(EmbeddingProvider.class).stream().map(ServiceLoader.Provider::type))
                .contains(TurboembedEmbeddingProvider.class);
        try (var unconfigured = new TurboembedEmbeddingProvider(Map.of())) {
            assertThat(unconfigured.providerId()).isEqualTo("turboembed:unconfigured");
            assertThatThrownBy(unconfigured::dimension).hasMessageContaining("PROTOMOLT_TURBOEMBED_TARGET");
        }
    }
}
