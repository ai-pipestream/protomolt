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
    private final List<ModelInferRequest> requests = new ArrayList<>();
    private UnaryOperator<ModelInferResponse> transform = UnaryOperator.identity();
    private Server server;
    private ManagedChannel channel;
    private TurboembedEmbeddingProvider provider;
    private Status failure;

    @BeforeEach
    void start() throws Exception {
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
                        observer.onNext(transform.apply(ModelInferResponse.newBuilder()
                                .setModelName("bundle").setModelVersion("revision-1")
                                .putParameters("manifest_sha256", InferParameter.newBuilder().setStringParam(DIGEST).build())
                                .addOutputs(ModelInferResponse.InferOutputTensor.newBuilder()
                                        .setName("vectors").setDatatype("FP32").addShape(count).addShape(2))
                                .addRawOutputContents(ByteString.copyFrom(raw.array())).build()));
                        observer.onCompleted();
                    }
                }).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        provider = new TurboembedEmbeddingProvider(channel,
                new TurboembedEmbeddingProvider.Model("bundle", "revision-1", DIGEST, 2));
    }

    @AfterEach
    void close() {
        provider.close();
        channel.shutdownNow();
        server.shutdownNow();
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
