package ai.protomolt.proto.inference.service;

import ai.protomolt.proto.inference.spi.ChunkObserver;
import ai.protomolt.proto.inference.spi.InferenceCatalog;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.spi.InferenceProvider;
import ai.protomolt.proto.inference.v1.ChatTurn;
import ai.protomolt.proto.inference.v1.DescribeModelRequest;
import ai.protomolt.proto.inference.v1.FinishReason;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.GenerateStreamResponse;
import ai.protomolt.proto.inference.v1.InferenceServiceGrpc;
import ai.protomolt.proto.inference.v1.ListModelsRequest;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.inference.v1.Role;
import ai.protomolt.proto.inference.v1.StructuredOutputConstraint;
import ai.protomolt.proto.inference.v1.Usage;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InferenceServiceImplTest {

    /** A scripted provider: returns a canned unary answer and a two-delta stream. */
    static final class StubProvider implements InferenceProvider {
        @Override
        public String id() {
            return "stub";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            return GenerateResponse.newBuilder()
                    .setText("stub says: " + request.getMessages(request.getMessagesCount() - 1)
                            .getContent())
                    .setModel(request.getModel())
                    .setProvider(id())
                    .setModelVersion("stub-1.0")
                    .setFinishReason(FinishReason.FINISH_REASON_STOP)
                    .setUsage(Usage.newBuilder().setPromptTokens(5).setCompletionTokens(3).build())
                    .build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request,
                                   ChunkObserver observer) {
            observer.onNext(GenerateStreamResponse.newBuilder().setTextDelta("hel").build());
            observer.onNext(GenerateStreamResponse.newBuilder().setTextDelta("lo").build());
            observer.onNext(GenerateStreamResponse.newBuilder()
                    .setLast(true)
                    .setFinishReason(FinishReason.FINISH_REASON_STOP)
                    .setUsage(Usage.newBuilder().setPromptTokens(5).setCompletionTokens(2).build())
                    .build());
            observer.onComplete();
        }
    }

    private Server server;
    private ManagedChannel channel;
    private InferenceServiceGrpc.InferenceServiceBlockingStub blocking;
    private InferenceServiceGrpc.InferenceServiceStub async;

    @BeforeEach
    void start() throws IOException {
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog, List.of(new StubProvider()));
        engines.register(ModelEntry.newBuilder()
                .setId("judge").setProvider("stub").setEndpoint("stub://local").build());
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new InferenceServiceImpl(engines))
                .directExecutor()
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        blocking = InferenceServiceGrpc.newBlockingStub(channel);
        async = InferenceServiceGrpc.newStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private static GenerateRequest request(String model, String text) {
        return GenerateRequest.newBuilder()
                .setModel(model)
                .addMessages(ChatTurn.newBuilder().setRole(Role.ROLE_USER).setContent(text))
                .build();
    }

    @Test
    void generateRoundTripsWithProvenance() {
        GenerateResponse response = blocking.generate(request("judge", "verdict?"));
        assertThat(response.getText()).isEqualTo("stub says: verdict?");
        assertThat(response.getProvider()).isEqualTo("stub");
        assertThat(response.getModelVersion()).isEqualTo("stub-1.0");
        assertThat(response.getUsage().getPromptTokens()).isEqualTo(5);
    }

    @Test
    void validationViolationIsInvalidArgument() {
        GenerateRequest empty = GenerateRequest.newBuilder().setModel("judge").build();
        assertThatThrownBy(() -> blocking.generate(empty))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void malformedStructuredOutputConstraintIsInvalidArgument() {
        GenerateRequest malformed = GenerateRequest.newBuilder(request("judge", "verdict?"))
                .setStructuredOutput(StructuredOutputConstraint.newBuilder()
                        .setName("not.a.provider.safe.name")
                        .setJsonSchema("{}"))
                .build();

        assertThatThrownBy(() -> blocking.generate(malformed))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("structured_output.name");
                });
    }

    @Test
    void unknownModelIsNotFound() {
        assertThatThrownBy(() -> blocking.generate(request("ghost", "hi")))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void providerFailureIsInternal() {
        GenerateRequest bad = request("judge", "hi").toBuilder()
                .setModel("judge").build();
        InferenceEngines engines = new InferenceEngines(new InferenceCatalog(),
                List.of(new InferenceProvider() {
                    @Override
                    public String id() {
                        return "stub";
                    }

                    @Override
                    public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
                        throw new InferenceException("backend exploded");
                    }

                    @Override
                    public void generateStream(ModelEntry model, GenerateStreamRequest request,
                                               ChunkObserver observer) {
                    }
                }));
        engines.register(ModelEntry.newBuilder()
                .setId("judge").setProvider("stub").setEndpoint("stub://local").build());
        InferenceServiceImpl service = new InferenceServiceImpl(engines);
        String name = InProcessServerBuilder.generateName();
        Server failing;
        try {
            failing = InProcessServerBuilder.forName(name)
                    .addService(service).directExecutor().build().start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        try (InferenceServiceImpl ignored = service) {
            ManagedChannel c = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                assertThatThrownBy(() -> InferenceServiceGrpc.newBlockingStub(c).generate(bad))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
                            assertThat(e.getStatus().getDescription()).contains("backend exploded");
                        });
            } finally {
                c.shutdownNow();
            }
        } finally {
            failing.shutdownNow();
        }
    }

    @Test
    void streamDeliversDeltasAndFinal() throws InterruptedException {
        List<GenerateStreamResponse> chunks = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        async.generateStream(GenerateStreamRequest.newBuilder()
                        .setModel("judge")
                        .addMessages(ChatTurn.newBuilder().setRole(Role.ROLE_USER).setContent("go"))
                        .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(GenerateStreamResponse chunk) {
                        chunks.add(chunk);
                    }

                    @Override
                    public void onError(Throwable t) {
                        error.set(t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();
        assertThat(chunks).hasSize(3);
        assertThat(chunks.get(0).getTextDelta()).isEqualTo("hel");
        assertThat(chunks.get(1).getTextDelta()).isEqualTo("lo");
        assertThat(chunks.get(2).getLast()).isTrue();
        assertThat(chunks.get(2).getFinishReason()).isEqualTo(FinishReason.FINISH_REASON_STOP);
        assertThat(chunks.get(2).getUsage().getCompletionTokens()).isEqualTo(2);
    }

    @Test
    void listAndDescribeWork() {
        assertThat(blocking.listModels(ListModelsRequest.getDefaultInstance()).getModelsList())
                .extracting(ModelEntry::getId).containsExactly("judge");
        assertThat(blocking.describeModel(
                        DescribeModelRequest.newBuilder().setModel("judge").build())
                .getEntry().getProvider()).isEqualTo("stub");
    }
}
