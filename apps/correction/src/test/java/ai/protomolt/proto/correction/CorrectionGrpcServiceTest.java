package ai.protomolt.proto.correction;

import ai.protomolt.proto.correction.v1.*;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.AssessmentDisposition;
import ai.protomolt.proto.inference.spi.ChunkObserver;
import ai.protomolt.proto.inference.spi.InferenceCatalog;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.spi.InferenceProvider;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.*;
import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.workflow.WorkflowRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class CorrectionGrpcServiceTest {
    private static final String VALID_CONTACT = "{\"recordId\":\"contact-1\","
            + "\"displayName\":\"Ada Lovelace\",\"email\":\"ada@example.org\","
            + "\"needsReview\":false}";

    @TempDir Path temp;
    private ScriptedProvider provider;
    private ScriptedEvaluator evaluator;
    private CorrectionGrpcService service;

    @BeforeEach
    void setUp() throws Exception {
        provider = new ScriptedProvider();
        var engines = new InferenceEngines(new InferenceCatalog(), List.of(provider));
        engines.register(ModelEntry.newBuilder().setId("starter-correction").setProvider("scripted")
                .setEndpoint("in-process://scripted")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true)).build());
        var generator = new StructuredGenerator(engines, new DescriptorRegistry());
        evaluator = new ScriptedEvaluator();
        SigningIdentity identity = SigningIdentity.open(temp);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNodeFixture fixture = workflow(mapper);
        WorkflowRepository repository = ignored -> Optional.of(fixture.workflow().deepCopy());
        var correction = new ContactCorrection(temp, repository, generator, evaluator,
                identity.signing(),
                Clock.fixed(Instant.parse("2026-09-24T12:00:00Z"), ZoneOffset.UTC));
        service = new CorrectionGrpcService(correction, identity.trust(), temp);
    }

    private record ObjectNodeFixture(com.fasterxml.jackson.databind.node.ObjectNode workflow) {}

    private static ObjectNodeFixture workflow(ObjectMapper mapper) throws Exception {
        try (InputStream in = CorrectionGrpcServiceTest.class.getResourceAsStream(
                "/starter/correct-contact.workflow.json")) {
            if (in == null) throw new IllegalStateException("missing fixed workflow fixture");
            return new ObjectNodeFixture((com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(in));
        }
    }

    @AfterEach
    void close() {
        service.close();
    }

    private static RunCorrectionRequest request(String id) {
        return RunCorrectionRequest.newBuilder().setWorkflowName(ContactCorrection.NAME)
                .setRunId(id).setSource(RawContact.newBuilder().setRecordId("contact-1")
                        .setContactText("Ada Lovelace, ada [at] example.org")
                        .setInternalNotes("private note")).build();
    }

    private TrustSnapshot trust() throws Exception {
        return SigningIdentity.open(temp).trust();
    }

    private static Status.Code code(Throwable failure) {
        return Status.fromThrowable(failure).getCode();
    }

    private static <T> Capture<T> await(Capture<T> capture) throws InterruptedException {
        assertThat(capture.done.await(10, TimeUnit.SECONDS)).isTrue();
        return capture;
    }

    private Capture<RunCorrectionResponse> run(RunCorrectionRequest request) throws InterruptedException {
        Capture<RunCorrectionResponse> capture = new Capture<>();
        service.runCorrection(request, capture);
        return await(capture);
    }

    private Capture<GetCorrectionResponse> get(GetCorrectionRequest request) throws InterruptedException {
        Capture<GetCorrectionResponse> capture = new Capture<>();
        service.getCorrection(request, capture);
        return await(capture);
    }

    @Test
    void invalidRequestsStopBeforeEitherProviderAndMissingLookupIsNotFound() throws Exception {
        var invalid = run(RunCorrectionRequest.getDefaultInstance());
        assertThat(code(invalid.failure)).isEqualTo(Status.Code.INVALID_ARGUMENT);
        var missing = get(GetCorrectionRequest.newBuilder().setRunId("missing").build());
        assertThat(code(missing.failure)).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(provider.invocations()).isZero();
        assertThat(evaluator.invocations()).isZero();

        for (String malformed : List.of("Run-1", "run-", "run--1", "../bad")) {
            assertThat(code(run(request(malformed)).failure)).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(code(get(GetCorrectionRequest.newBuilder().setRunId(malformed).build()).failure))
                    .isEqualTo(Status.Code.INVALID_ARGUMENT);
        }
        assertThat(provider.invocations()).isZero();
        assertThat(evaluator.invocations()).isZero();
    }

    @Test
    void freshAndRetrievedOutcomesCarryVerifiedReceiptsAndRunIdentity() throws Exception {
        provider.script(VALID_CONTACT);
        var fresh = run(request("fresh"));
        assertThat(fresh.failure).isNull();
        assertThat(fresh.value.getOutcome().getRunId()).isEqualTo("fresh");
        assertThat(fresh.value.getOutcome().getAssessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_ACCEPTED);
        assertThat(RecordVerifier.verify(fresh.value.getOutcome().getExecutionReceipt().toByteArray(), trust())
                .verified()).isTrue();
        assertThat(RecordVerifier.verify(fresh.value.getOutcome().getAssessmentReceipt().toByteArray(), trust())
                .verified()).isTrue();

        var reopened = get(GetCorrectionRequest.newBuilder().setRunId("fresh").build());
        assertThat(reopened.failure).isNull();
        assertThat(reopened.value.getOutcome()).isEqualTo(fresh.value.getOutcome());
        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isEqualTo(1);
    }

    @Test
    void duplicateRunIdReturnsAlreadyExistsWithoutCallingProvidersAgain() throws Exception {
        provider.script(VALID_CONTACT);
        assertThat(run(request("once")).failure).isNull();
        var duplicate = run(request("once"));
        assertThat(code(duplicate.failure)).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isEqualTo(1);
    }

    @Test
    void duplicateRunIdWhileTheFirstRunIsActiveReturnsAlreadyExists() throws Exception {
        provider.blockNext();
        Context.CancellableContext context = Context.current().withCancellation();
        Capture<RunCorrectionResponse> first = new Capture<>();
        context.run(() -> service.runCorrection(request("same-active"), first));
        assertThat(provider.started.await(5, TimeUnit.SECONDS)).isTrue();

        var duplicate = run(request("same-active"));
        assertThat(code(duplicate.failure)).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isZero();

        context.cancel(null);
        assertThat(provider.cancelled.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(first.callbacks.get()).isZero();
    }

    @Test
    void durableDuplicateWinsOverBusyCapacityButANewRunIsStillRejected() throws Exception {
        provider.script(VALID_CONTACT);
        assertThat(run(request("already-used")).failure).isNull();
        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isEqualTo(1);

        provider.blockNext();
        Context.CancellableContext activeContext = Context.current().withCancellation();
        Capture<RunCorrectionResponse> active = new Capture<>();
        activeContext.run(() -> service.runCorrection(request("holds-capacity"), active));
        assertThat(provider.started.await(5, TimeUnit.SECONDS)).isTrue();

        var duplicate = run(request("already-used"));
        assertThat(code(duplicate.failure)).isEqualTo(Status.Code.ALREADY_EXISTS);
        var distinct = run(request("new-while-busy"));
        assertThat(code(distinct.failure)).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(provider.invocations()).isEqualTo(2);
        assertThat(evaluator.invocations()).isEqualTo(1);

        activeContext.cancel(null);
        assertThat(provider.cancelled.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(active.callbacks.get()).isZero();
    }

    @Test
    void tamperedStoredReceiptIsReportedAsDataLoss() throws Exception {
        provider.script(VALID_CONTACT);
        assertThat(run(request("tampered")).failure).isNull();
        Path receipt = temp.resolve("outcomes/tampered/receipt.pb");
        Files.writeString(receipt, "corrupt", StandardCharsets.UTF_8);

        var reopened = get(GetCorrectionRequest.newBuilder().setRunId("tampered").build());
        assertThat(code(reopened.failure)).isEqualTo(Status.Code.DATA_LOSS);
        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isEqualTo(1);
    }

    @Test
    void cancellationStopsTheActiveAttemptDoesNotReplyAndReleasesCapacity() throws Exception {
        provider.blockNext();
        Capture<RunCorrectionResponse> cancelled = new Capture<>();
        Context.CancellableContext context = Context.current().withCancellation();
        context.run(() -> service.runCorrection(request("cancelled"), cancelled));
        assertThat(provider.started.await(5, TimeUnit.SECONDS)).isTrue();
        context.cancel(null);
        assertThat(provider.cancelled.await(5, TimeUnit.SECONDS)).isTrue();

        provider.script(VALID_CONTACT);
        Capture<RunCorrectionResponse> next = null;
        for (int i = 0; i < 100; i++) {
            next = new Capture<>();
            service.runCorrection(request("after-cancel"), next);
            if (!next.done.await(25, TimeUnit.MILLISECONDS)) {
                // No immediate rejection means this attempt acquired the permit and is
                // doing useful work. Wait for that one instead of submitting duplicates.
                assertThat(next.done.await(10, TimeUnit.SECONDS)).isTrue();
                break;
            }
            if (next.failure == null || code(next.failure) != Status.Code.RESOURCE_EXHAUSTED) break;
            Thread.sleep(20);
        }
        assertThat(next).isNotNull();
        assertThat(next.done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(next.failure).isNull();
        assertThat(next.value.getOutcome().getRunId()).isEqualTo("after-cancel");
        assertThat(cancelled.callbacks.get()).isZero();
        assertThat(provider.invocations()).isEqualTo(2);
    }

    private static final class Capture<T> implements StreamObserver<T> {
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicInteger callbacks = new AtomicInteger();
        volatile T value;
        volatile Throwable failure;
        @Override public void onNext(T result) { callbacks.incrementAndGet(); value = result; }
        @Override public void onError(Throwable error) { callbacks.incrementAndGet(); failure = error; done.countDown(); }
        @Override public void onCompleted() { callbacks.incrementAndGet(); done.countDown(); }
    }

    private static final class ScriptedEvaluator implements ContactCorrection.Evaluator {
        private final AtomicInteger calls = new AtomicInteger();
        @Override public String profile() { return "evaluator.local"; }
        @Override public byte[] configuration() { return "evaluator-v1".getBytes(StandardCharsets.UTF_8); }
        int invocations() { return calls.get(); }
        @Override public EvaluateResponse evaluate(EvaluateRequest request) {
            calls.incrementAndGet();
            var distribution = ChoiceJudgment.newBuilder().setOption("supported")
                    .addDistribution(EvaluationProbability.newBuilder().setLabel("supported").setProbability(1))
                    .addDistribution(EvaluationProbability.newBuilder().setLabel("unsupported").setProbability(0))
                    .addDistribution(EvaluationProbability.newBuilder().setLabel("unresolved").setProbability(0));
            return EvaluateResponse.newBuilder().setRequestId(request.getRequestId())
                    .setBinding(request.getBinding())
                    .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("source-support")
                            .setChoice(distribution))
                    .setProvider("scripted-judge").setModel("contact-judge-v1").build();
        }
    }

    private static final class ScriptedProvider implements InferenceProvider {
        private final Queue<String> responses = new ArrayDeque<>();
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicBoolean block = new AtomicBoolean();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch cancelled = new CountDownLatch(1);
        synchronized void script(String value) { responses.add(value); }
        void blockNext() { block.set(true); }
        int invocations() { return calls.get(); }
        @Override public String id() { return "scripted"; }
        @Override public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            calls.incrementAndGet();
            if (block.compareAndSet(true, false)) {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException interrupted) {
                    cancelled.countDown();
                    Thread.currentThread().interrupt();
                    throw new InferenceException("cancelled", interrupted);
                }
            }
            String response;
            synchronized (this) { response = responses.poll(); }
            if (response == null) throw new InferenceException("no scripted response");
            return GenerateResponse.newBuilder().setText(response).setModel(model.getId())
                    .setProvider(id()).setModelVersion("scripted-v1")
                    .setFinishReason(FinishReason.FINISH_REASON_STOP).build();
        }
        @Override public void generateStream(ModelEntry model, GenerateStreamRequest request,
                ChunkObserver observer) { throw new InferenceException("stream unsupported"); }
    }
}
