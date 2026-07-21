package ai.pipestream.proto.embeddings.ovms;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.embeddings.EmbeddingProviders;
import com.google.protobuf.ByteString;
import inference.GRPCInferenceServiceGrpc;
import inference.GrpcPredictV2.ModelInferRequest;
import inference.GrpcPredictV2.ModelInferResponse;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class OvmsEmbeddingProviderTest {

    private static final String MODEL = "test-embedder";

    private String serverName;
    private Server server;
    private ManagedChannel channel;
    private FakeInferenceService fake;
    private String savedTargetProperty;
    private String savedModelProperty;
    private String savedInputProperty;
    private String savedOutputProperty;

    @BeforeEach
    void startInProcessServer() throws IOException {
        savedTargetProperty = System.clearProperty(OvmsEmbeddingProvider.TARGET_PROPERTY);
        savedModelProperty = System.clearProperty(OvmsEmbeddingProvider.MODEL_PROPERTY);
        savedInputProperty = System.clearProperty(OvmsEmbeddingProvider.INPUT_NAME_PROPERTY);
        savedOutputProperty = System.clearProperty(OvmsEmbeddingProvider.OUTPUT_NAME_PROPERTY);
        serverName = "ovms-" + UUID.randomUUID();
        fake = new FakeInferenceService();
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
        restore(OvmsEmbeddingProvider.TARGET_PROPERTY, savedTargetProperty);
        restore(OvmsEmbeddingProvider.MODEL_PROPERTY, savedModelProperty);
        restore(OvmsEmbeddingProvider.INPUT_NAME_PROPERTY, savedInputProperty);
        restore(OvmsEmbeddingProvider.OUTPUT_NAME_PROPERTY, savedOutputProperty);
    }

    private static void restore(String property, String saved) {
        if (saved == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, saved);
        }
    }

    @Test
    void requestCarriesModelDatatypeShapeAndUtf8Bytes() {
        fake.outputName = "dense";
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input_ids", "dense");

        provider.embed("héllo");

        assertThat(fake.requests).singleElement().satisfies(request -> {
            assertThat(request.getModelName()).isEqualTo(MODEL);
            assertThat(request.getInputsCount()).isEqualTo(1);
            ModelInferRequest.InferInputTensor input = request.getInputs(0);
            assertThat(input.getName()).isEqualTo("input_ids");
            assertThat(input.getDatatype()).isEqualTo("BYTES");
            assertThat(input.getShapeList()).containsExactly(1L);
            assertThat(input.getContents().getBytesContentsList())
                    .containsExactly(ByteString.copyFrom("héllo", StandardCharsets.UTF_8));
        });
    }

    @Test
    void fp32ContentsResponseParses() {
        fake.form = FakeInferenceService.ResponseForm.FP32_CONTENTS;
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        assertThat(provider.embed("alpha")).containsExactly(FakeInferenceService.vectorFor("alpha"));
    }

    @Test
    void rawOutputContentsResponseParses() {
        fake.form = FakeInferenceService.ResponseForm.RAW_OUTPUT_CONTENTS;
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        assertThat(provider.embed("alpha")).containsExactly(FakeInferenceService.vectorFor("alpha"));
    }

    @Test
    void batchOfThreeReturnsThreeVectorsInOrder() {
        List<String> texts = List.of("alpha", "beta", "gamma");
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        List<float[]> vectors = provider.embedAll(texts);

        assertThat(vectors).hasSize(3);
        for (int i = 0; i < texts.size(); i++) {
            assertThat(vectors.get(i)).as("vector for '%s'", texts.get(i))
                    .containsExactly(FakeInferenceService.vectorFor(texts.get(i)));
        }
        assertThat(fake.requests).singleElement().satisfies(request -> {
            assertThat(request.getInputs(0).getShapeList()).containsExactly(3L);
            assertThat(request.getInputs(0).getContents().getBytesContentsCount()).isEqualTo(3);
        });
    }

    @Test
    void batchOfSixHundredChunksIntoThreeRequestsPreservingGlobalOrder() {
        List<String> texts = new ArrayList<>(600);
        for (int i = 0; i < 600; i++) {
            texts.add("batch-" + i);
        }
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        List<float[]> vectors = provider.embedAll(texts);

        assertThat(vectors).hasSize(600);
        for (int i = 0; i < 600; i++) {
            // vectorFor puts the global index first, so equality proves end-to-end order.
            assertThat(vectors.get(i)).as("vector for global index %d", i)
                    .containsExactly(FakeInferenceService.vectorFor("batch-" + i));
        }
        assertThat(fake.requests).hasSize(3);
        assertThat(fake.requests.get(0).getInputs(0).getShapeList()).containsExactly(256L);
        assertThat(fake.requests.get(1).getInputs(0).getShapeList()).containsExactly(256L);
        assertThat(fake.requests.get(2).getInputs(0).getShapeList()).containsExactly(88L);
    }

    @Test
    void shapeMismatchThrowsNamingTheTensor() {
        fake.claimedShapeRows = 7;
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        assertThatThrownBy(() -> provider.embed("alpha"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("embedding");
    }

    @Test
    void missingOutputTensorThrowsNamingIt() {
        fake.outputName = "something_else";
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        assertThatThrownBy(() -> provider.embed("alpha"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("embedding");
    }

    @Test
    void serverFailureIsWrappedNamingTargetAndModel() {
        fake.failWith = Status.UNAVAILABLE.asRuntimeException();
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        assertThatThrownBy(() -> provider.embed("boom"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(MODEL)
                .hasMessageContaining(channel.authority())
                .hasCauseInstanceOf(io.grpc.StatusRuntimeException.class);
    }

    @Test
    void dimensionProbesExactlyOnceAcrossRepeatedCalls() {
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        int first = provider.dimension();
        int second = provider.dimension();

        assertThat(first).isEqualTo(FakeInferenceService.DIMENSION);
        assertThat(second).isEqualTo(FakeInferenceService.DIMENSION);
        assertThat(fake.requests).hasSize(1);
    }

    @Test
    void closeLeavesACallerOwnedChannelOpen() {
        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(
                channel, MODEL, "input", "embedding");

        provider.close();

        assertThat(channel.isShutdown()).isFalse();
        assertThat(provider.embed("still open")).hasSize(FakeInferenceService.DIMENSION);
    }

    @Test
    void serviceLoaderFindsTheProviderById() {
        // The no-arg constructor must not touch the knobs, or an unconfigured provider would
        // break discovery of every other provider on the classpath.
        assertThat(EmbeddingProviders.all()).containsKey("ovms");
    }

    @Test
    void firstUseWithoutConfigurationNamesAllKnobs() {
        assumeThat(System.getenv(OvmsEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE)).isNull();
        assumeThat(System.getenv(OvmsEmbeddingProvider.MODEL_ENVIRONMENT_VARIABLE)).isNull();

        OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider();

        assertThatThrownBy(provider::dimension)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OvmsEmbeddingProvider.TARGET_PROPERTY)
                .hasMessageContaining(OvmsEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE)
                .hasMessageContaining(OvmsEmbeddingProvider.MODEL_PROPERTY)
                .hasMessageContaining(OvmsEmbeddingProvider.MODEL_ENVIRONMENT_VARIABLE);
    }

    @Test
    void noArgConstructorResolvesTheTargetAndModelProperties() throws Exception {
        Server loopback = NettyServerBuilder.forPort(0).addService(new FakeInferenceService())
                .build().start();
        try {
            System.setProperty(OvmsEmbeddingProvider.TARGET_PROPERTY,
                    "localhost:" + loopback.getPort());
            System.setProperty(OvmsEmbeddingProvider.MODEL_PROPERTY, MODEL);
            // Pin the tensor names too: the environment may carry PROTOMOLT_OVMS_INPUT /
            // PROTOMOLT_OVMS_OUTPUT for a live run, and system properties win over it.
            System.setProperty(OvmsEmbeddingProvider.INPUT_NAME_PROPERTY,
                    OvmsEmbeddingProvider.DEFAULT_INPUT_NAME);
            System.setProperty(OvmsEmbeddingProvider.OUTPUT_NAME_PROPERTY,
                    OvmsEmbeddingProvider.DEFAULT_OUTPUT_NAME);

            EmbeddingProvider provider = EmbeddingProviders.byId("ovms");

            assertThat(provider).isInstanceOf(OvmsEmbeddingProvider.class);
            assertThat(provider.embed("wired"))
                    .containsExactly(FakeInferenceService.vectorFor("wired"));
            assertThat(provider.dimension()).isEqualTo(FakeInferenceService.DIMENSION);
            ((OvmsEmbeddingProvider) provider).close();
        } finally {
            loopback.shutdownNow();
        }
    }

    /**
     * Fake KServe v2 inference service: answers ModelInfer with deterministic per-text vectors,
     * records requests, and can answer with either contents form, a wrong shape, or an error.
     */
    private static final class FakeInferenceService
            extends GRPCInferenceServiceGrpc.GRPCInferenceServiceImplBase {

        private enum ResponseForm { FP32_CONTENTS, RAW_OUTPUT_CONTENTS }

        private static final int DIMENSION = 3;

        private final List<ModelInferRequest> requests = new CopyOnWriteArrayList<>();
        private volatile ResponseForm form = ResponseForm.FP32_CONTENTS;
        private volatile io.grpc.StatusRuntimeException failWith;
        private volatile int claimedShapeRows = -1;
        private volatile String outputName = "embedding";

        /**
         * The vector {@code text} embeds to: derived from the text, so tests see ordering.
         * A {@code batch-<n>} text carries the literal n as the first component, so chunked
         * batches prove global input order collision-free.
         */
        private static float[] vectorFor(String text) {
            if (text.startsWith("batch-")) {
                int index = Integer.parseInt(text.substring("batch-".length()));
                return new float[]{index, text.length(), 1};
            }
            return new float[]{text.length(), text.hashCode() % 1000, text.isEmpty() ? 0 : 1};
        }

        @Override
        public void modelInfer(ModelInferRequest request,
                StreamObserver<ModelInferResponse> observer) {
            if (failWith != null) {
                observer.onError(failWith);
                return;
            }
            requests.add(request);
            int texts = request.getInputs(0).getContents().getBytesContentsCount();
            float[] flat = new float[texts * DIMENSION];
            for (int i = 0; i < texts; i++) {
                String text = request.getInputs(0).getContents()
                        .getBytesContents(i).toString(StandardCharsets.UTF_8);
                System.arraycopy(vectorFor(text), 0, flat, i * DIMENSION, DIMENSION);
            }
            int rows = claimedShapeRows >= 0 ? claimedShapeRows : texts;
            ModelInferResponse.InferOutputTensor.Builder output =
                    ModelInferResponse.InferOutputTensor.newBuilder()
                            .setName(outputName)
                            .setDatatype("FP32")
                            .addShape(rows)
                            .addShape(DIMENSION);
            if (form == ResponseForm.FP32_CONTENTS) {
                for (float value : flat) {
                    output.getContentsBuilder().addFp32Contents(value);
                }
            }
            ModelInferResponse.Builder response = ModelInferResponse.newBuilder()
                    .setModelName(request.getModelName())
                    .addOutputs(output);
            if (form == ResponseForm.RAW_OUTPUT_CONTENTS) {
                ByteBuffer buffer = ByteBuffer.allocate(flat.length * Float.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN);
                for (float value : flat) {
                    buffer.putFloat(value);
                }
                response.addRawOutputContents(ByteString.copyFrom(buffer.array()));
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }
    }
}
