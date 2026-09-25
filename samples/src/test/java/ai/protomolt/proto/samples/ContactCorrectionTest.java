package ai.protomolt.proto.samples;

import ai.protomolt.proto.correction.ContactCorrection;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.AssessmentDisposition;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowAssessment;
import ai.protomolt.proto.inference.spi.ChunkObserver;
import ai.protomolt.proto.inference.spi.InferenceCatalog;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.spi.InferenceProvider;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.ChoiceJudgment;
import ai.protomolt.proto.inference.v1.EvaluateRequest;
import ai.protomolt.proto.inference.v1.EvaluateResponse;
import ai.protomolt.proto.inference.v1.EvaluationAnswer;
import ai.protomolt.proto.inference.v1.EvaluationProbability;
import ai.protomolt.proto.inference.v1.FinishReason;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.inference.v1.Usage;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.correction.v1.ContactEvidence;
import ai.protomolt.proto.correction.v1.ContactGrounding;
import ai.protomolt.proto.correction.v1.CorrectedContact;
import ai.protomolt.proto.correction.v1.RawContact;
import ai.protomolt.proto.workflow.RecordSigning;
import ai.protomolt.proto.workflow.WorkRecordProjector;
import ai.protomolt.proto.workflow.WorkflowAssessmentRecords;
import ai.protomolt.proto.workflow.WorkflowRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Any;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContactCorrectionTest {
    private static final String ISSUER = "records.protomolt.dev";
    private static final String KEY_ID = "key-2026";
    private static final byte[] SEED = HexFormat.of().parseHex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    private static final byte[] PUBLIC_KEY = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String VALID_CONTACT = "{\"recordId\":\"contact-1\","
            + "\"displayName\":\"Ada Lovelace\",\"email\":\"ada@example.org\","
            + "\"needsReview\":false}";

    @TempDir Path temp;
    private ScriptedProvider provider;
    private StructuredGenerator generator;
    private ScriptedEvaluator evaluator;
    private ObjectNode storedWorkflow;
    private WorkflowRepository workflows;

    @BeforeEach
    void setUp() throws Exception {
        provider = new ScriptedProvider();
        var engines = new InferenceEngines(new InferenceCatalog(), List.of(provider));
        engines.register(ModelEntry.newBuilder().setId("starter-correction").setProvider("scripted")
                .setEndpoint("in-process://scripted")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true)).build());
        generator = new StructuredGenerator(engines, new DescriptorRegistry());
        evaluator = new ScriptedEvaluator();
        storedWorkflow = JSON.readTree(resource()).deepCopy();
        workflows = ignored -> Optional.of(storedWorkflow.deepCopy());
    }

    private ContactCorrection correction() throws Exception {
        return correction(Duration.ofSeconds(30));
    }

    private ContactCorrection correction(Duration timeout) throws Exception {
        return new ContactCorrection(temp, workflows, generator, evaluator,
                new RecordSigning(ISSUER, new RecordSigner(KEY_ID, RecordKeys.privateKey(SEED))),
                Clock.fixed(Instant.parse("2026-09-24T12:00:00Z"), ZoneOffset.UTC), timeout);
    }

    private static String resource() throws IOException {
        try (InputStream input = ContactCorrectionTest.class.getResourceAsStream(
                "/starter/correct-contact.workflow.json")) {
            if (input == null) throw new IOException("missing correction workflow fixture");
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static RawContact raw() {
        return RawContact.newBuilder().setRecordId("contact-1")
                .setContactText("Ada Lovelace, ada [at] example.org")
                .setInternalNotes("source-account-private").build();
    }

    private ContactCorrection.Outcome run(String id) throws Exception {
        return correction().run(id, raw());
    }

    private static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder().setIssuer(ISSUER)
                .addKeys(TrustedKey.newBuilder().setKeyId(KEY_ID)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setPublicKey(ByteString.copyFrom(PUBLIC_KEY)).setState(KeyState.KEY_STATE_ACTIVE))
                .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)).build();
    }

    private void validOutput() { provider.script(VALID_CONTACT); }

    @Test
    void validFirstTryAcceptsAndIssuesDistinctVerifiableReceipts() throws Exception {
        validOutput();
        var correction = correction();
        var outcome = correction.run("valid-first", raw());

        assertThat(outcome.run().getStatus()).isEqualTo(RunStatus.RUN_STATUS_SUCCEEDED);
        assertThat(outcome.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_ACCEPTED);
        assertThat(outcome.assessment().getReason()).isEqualTo("source-supported");
        assertThat(outcome.replayed()).isTrue();
        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isEqualTo(1);
        assertThat(WorkRecord.parseFrom(outcome.executionReceipt().getManifest()).getRecordId())
                .isNotEqualTo(WorkRecord.parseFrom(outcome.assessmentReceipt().getManifest()).getRecordId());
        assertThat(WorkflowAssessmentRecords.verify(outcome.assessmentReceipt().toByteArray(),
                trust(), outcome.run(), outcome.assessment(), correction.artifacts()).ok()).isTrue();
        assertThat(correction.verify(outcome, trust()).ok()).isTrue();
        int generatedBeforeReopen = provider.invocations();
        int judgedBeforeReopen = evaluator.invocations();
        assertThat(ContactCorrection.verifyStored(temp, "valid-first", trust()).ok()).isTrue();
        assertThat(provider.invocations()).isEqualTo(generatedBeforeReopen);
        assertThat(evaluator.invocations()).isEqualTo(judgedBeforeReopen);
        ContactEvidence evidence = outcome.assessment().getRequest().getEvidence()
                .unpack(ContactEvidence.class);
        assertThat(evidence.getSource().getContactText()).contains("ada [at] example.org");
        assertThat(evidence.toString()).doesNotContain("internal_notes", "source-account-private");
        assertThat(outcome.assessment().getResponse().getBinding())
                .isEqualTo(outcome.assessment().getRequest().getBinding());
    }

    @Test
    void retriesBadEmailThenAcceptsTheRepairedCandidate() throws Exception {
        provider.script("{\"recordId\":\"contact-1\",\"displayName\":\"Ada Lovelace\","
                + "\"email\":\"not-an-email\",\"needsReview\":false}", VALID_CONTACT);
        var outcome = correction().run("repair-email", raw());

        assertThat(outcome.run().getStatus()).isEqualTo(RunStatus.RUN_STATUS_SUCCEEDED);
        assertThat(outcome.run().getSteps(0).getStructured().getAttemptsCount()).isEqualTo(2);
        assertThat(outcome.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_ACCEPTED);
        assertThat(provider.invocations()).isEqualTo(2);
        assertThat(evaluator.invocations()).isEqualTo(1);
    }

    @Test
    void exhaustsThreeInvalidGenerationsWithoutCallingEvaluator() throws Exception {
        String missingName = "{\"recordId\":\"contact-1\",\"email\":\"ada@example.org\"}";
        provider.script(missingName, missingName, missingName);
        var outcome = correction().run("three-invalid", raw());

        assertThat(provider.invocations()).isEqualTo(3);
        assertThat(evaluator.invocations()).isZero();
        assertThat(outcome.run().getStatus()).isEqualTo(RunStatus.RUN_STATUS_FAILED);
        assertThat(outcome.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED);
        assertThat(outcome.assessment().hasRequest()).isFalse();
    }

    @Test
    void providerFailureFailsTheRunWithoutCallingEvaluator() throws Exception {
        provider.fail("offline provider");
        var outcome = correction().run("provider-failure", raw());

        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isZero();
        assertThat(outcome.run().getStatus()).isEqualTo(RunStatus.RUN_STATUS_FAILED);
        assertThat(outcome.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED);
        assertThat(outcome.assessment().getReason()).isEqualTo("execution-failed");
    }

    @Test
    void invalidRawContactIsRecordedWithoutGenerationOrEvaluation() throws Exception {
        var correction = correction();
        var outcome = correction.run("invalid-raw", raw().toBuilder().clearRecordId().build());

        assertThat(provider.invocations()).isZero();
        assertThat(evaluator.invocations()).isZero();
        assertThat(outcome.run().getStatus()).isEqualTo(RunStatus.RUN_STATUS_FAILED);
        assertThat(outcome.run().getInputArtifact().getSha256()).isNotEmpty();
        var stored = RawContact.parseFrom(correction.artifacts().find(
                outcome.run().getInputArtifact().getSha256()).orElseThrow().content());
        assertThat(stored.getInternalNotes()).isEmpty();
    }

    @Test
    void invalidProjectedGroundingIsRejectedBeforeGenerationOrEvaluation() throws Exception {
        var outcome = correction().run("empty-grounding", raw().toBuilder().setContactText("").build());

        assertThat(provider.invocations()).isZero();
        assertThat(evaluator.invocations()).isZero();
        assertThat(outcome.run().getStatus()).isEqualTo(RunStatus.RUN_STATUS_FAILED);
        assertThat(outcome.assessment().hasRequest()).isFalse();
        assertThat(outcome.assessment().hasResponse()).isFalse();
    }

    @Test
    void candidateWithDifferentRecordIdRequiresReviewWithoutJudgeInvocation() throws Exception {
        provider.script("{\"recordId\":\"another-record\",\"displayName\":\"Ada Lovelace\","
                + "\"email\":\"ada@example.org\",\"needsReview\":false}");
        var correction = correction();
        var outcome = correction.run("wrong-record-id", raw());

        assertThat(provider.invocations()).isEqualTo(1);
        assertThat(evaluator.invocations()).isZero();
        assertThat(outcome.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_REVIEW);
        assertThat(outcome.assessment().getReason()).isEqualTo("source-identity-mismatch");
        assertThat(correction.verify(outcome, trust()).ok()).isTrue();
    }

    @Test
    void reviewFlagAndUnsupportedInventedAddressNeverBecomeAccepted() throws Exception {
        provider.script("{\"recordId\":\"contact-1\",\"displayName\":\"Ada Lovelace\","
                + "\"email\":\"ada@example.org\",\"needsReview\":true}");
        var flagged = correction().run("needs-review", raw());
        assertThat(flagged.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_REVIEW);
        assertThat(flagged.assessment().getReason()).isEqualTo("candidate-needs-review");

        provider.script("{\"recordId\":\"contact-1\",\"displayName\":\"Ada Lovelace\","
                + "\"email\":\"invented@example.net\",\"needsReview\":false}");
        evaluator.answer("unsupported");
        var invented = correction().run("invented-address", raw());
        assertThat(invented.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_REVIEW);
        assertThat(invented.assessment().getReason()).isEqualTo("source-not-supported");
    }

    @Test
    void malformedEvaluatorResponseAndEvaluatorFailureAreFailedAssessments() throws Exception {
        validOutput();
        evaluator.malformedResponse();
        var malformed = correction().run("bad-judgment", raw());
        assertThat(malformed.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED);
        assertThat(malformed.assessment().getReason()).isEqualTo("evaluator-response-invalid");
        assertThat(malformed.assessment().hasResponse()).isFalse();

        validOutput();
        evaluator.fail("judge unavailable");
        var unavailable = correction().run("judge-failure", raw());
        assertThat(unavailable.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED);
        assertThat(unavailable.assessment().getReason()).isEqualTo("evaluator-unavailable");
        assertThat(unavailable.assessment().hasResponse()).isFalse();
    }

    @Test
    void evaluatorTimeoutCancelsTheCallAndRecordsAFailedAssessment() throws Exception {
        validOutput();
        evaluator.blockUntilCancelled();
        var outcome = correction(Duration.ofMillis(40)).run("judge-timeout", raw());

        assertThat(evaluator.started().await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(evaluator.cancelled().await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(outcome.assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED);
        assertThat(outcome.assessment().getReason()).isEqualTo("evaluator-timeout");
        assertThat(outcome.assessment().hasResponse()).isFalse();
    }

    @Test
    void interruptingRunCancelsEvaluationAndRecordsCancellation() throws Exception {
        validOutput();
        evaluator.blockUntilCancelled();
        var correction = correction(Duration.ofSeconds(5));
        var outcome = new AtomicReference<ContactCorrection.Outcome>();
        var failure = new AtomicReference<Throwable>();
        Thread run = Thread.ofVirtual().start(() -> {
            try {
                outcome.set(correction.run("judge-cancelled", raw()));
            } catch (Throwable t) {
                failure.set(t);
            }
        });

        assertThat(evaluator.started().await(2, TimeUnit.SECONDS)).isTrue();
        run.interrupt();
        run.join(2_000);

        assertThat(run.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(evaluator.cancelled().await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(outcome.get().assessment().getDisposition())
                .isEqualTo(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED);
        assertThat(outcome.get().assessment().getReason()).isEqualTo("evaluation-cancelled");
        assertThat(outcome.get().assessment().hasResponse()).isFalse();
    }

    @Test
    void assessmentReceiptRejectsTamperedRunAndTamperedAssessmentOffline() throws Exception {
        validOutput();
        var correction = correction();
        var outcome = correction.run("tamper-check", raw());
        byte[] signed = outcome.assessmentReceipt().toByteArray();

        assertThat(WorkflowAssessmentRecords.verify(signed, trust(),
                outcome.run().toBuilder().setFailureSummary("forged").build(), outcome.assessment(),
                correction.artifacts()).ok()).isFalse();
        assertThat(WorkflowAssessmentRecords.verify(signed, trust(), outcome.run(),
                outcome.assessment().toBuilder().setReason("forged").build(),
                correction.artifacts()).ok()).isFalse();
    }

    @Test
    void offlinePolicyVerificationRejectsFreshlySignedWrongDisposition() throws Exception {
        validOutput();
        var correction = correction();
        var outcome = correction.run("wrong-disposition", raw());
        var wrongAssessment = outcome.assessment().toBuilder()
                .setDisposition(AssessmentDisposition.ASSESSMENT_DISPOSITION_REVIEW).build();
        var issuance = new WorkRecordProjector.Issuance("assessment-forged-policy", ISSUER,
                KEY_ID, com.google.protobuf.Timestamp.newBuilder().setSeconds(1790251200).build(), "");
        SignedWorkRecord freshlySigned = new RecordSigner(KEY_ID, RecordKeys.privateKey(SEED))
                .sign(WorkflowAssessmentRecords.project(outcome.run(), wrongAssessment,
                        correction.artifacts(), issuance));
        var forged = new ContactCorrection.Outcome(outcome.run(), wrongAssessment,
                outcome.executionReceipt(), freshlySigned, outcome.replayed());

        assertThat(correction.verify(forged, trust()).ok()).isFalse();
    }

    @Test
    void offlinePolicyVerifierRejectsFreshlySignedChangedGroundingEvidence() throws Exception {
        validOutput();
        var correction = correction();
        var outcome = correction.run("changed-grounding", raw());
        EvaluateRequest originalRequest = outcome.assessment().getRequest();
        ContactEvidence originalEvidence = originalRequest.getEvidence().unpack(ContactEvidence.class);
        ContactGrounding alteredSource = originalEvidence.getSource().toBuilder()
                .setContactText("A different source says Ada uses another address.").build();
        ContactEvidence alteredEvidenceMessage = originalEvidence.toBuilder().setSource(alteredSource).build();
        Any alteredEvidence = Any.pack(alteredEvidenceMessage);
        var evidenceReference = correction.artifacts().save(alteredEvidence.toByteArray(),
                "application/x-protobuf", false);
        var alteredBinding = originalRequest.getBinding().toBuilder()
                .setEvidenceSha256(evidenceReference.getSha256()).build();
        EvaluateRequest alteredRequest = originalRequest.toBuilder().setEvidence(alteredEvidence)
                .setBinding(alteredBinding).build();
        var alteredResponse = outcome.assessment().getResponse().toBuilder()
                .setBinding(alteredBinding).build();
        WorkflowAssessment alteredAssessment = outcome.assessment().toBuilder()
                .setRequest(alteredRequest).setResponse(alteredResponse).build();
        var issuance = new WorkRecordProjector.Issuance("assessment-changed-grounding", ISSUER,
                KEY_ID, com.google.protobuf.Timestamp.newBuilder().setSeconds(1790251201).build(), "");
        SignedWorkRecord freshlySigned = new RecordSigner(KEY_ID, RecordKeys.privateKey(SEED))
                .sign(WorkflowAssessmentRecords.project(outcome.run(), alteredAssessment,
                        correction.artifacts(), issuance));
        var forged = new ContactCorrection.Outcome(outcome.run(), alteredAssessment,
                outcome.executionReceipt(), freshlySigned, outcome.replayed());

        assertThat(WorkflowAssessmentRecords.verify(freshlySigned.toByteArray(), trust(),
                outcome.run(), alteredAssessment, correction.artifacts()).ok()).isTrue();
        assertThat(correction.verify(forged, trust()).ok()).isFalse();
    }

    @Test
    void workflowIsSnapshottedAtConstructionAndRunIdsAreSingleUse() throws Exception {
        validOutput();
        var correction = correction();
        String originalVersion = correction.version();
        var originalDefinition = correction.definition();
        storedWorkflow.put("deadlineMs", 1);
        assertThat(correction.definition()).isEqualTo(originalDefinition);
        assertThat(correction.version()).isEqualTo(originalVersion);

        correction.run("one-shot", raw());
        int generated = provider.invocations();
        int judged = evaluator.invocations();
        assertThatThrownBy(() -> correction.run("one-shot", raw())).isInstanceOf(IOException.class);
        assertThat(provider.invocations()).isEqualTo(generated);
        assertThat(evaluator.invocations()).isEqualTo(judged);
    }

    @Test
    void assessmentAnnotationRequiresRequestAndJudgmentForAcceptance() {
        var assessment = WorkflowAssessment.newBuilder().setRunId("assessment-test")
                .setRunEvidenceSha256("a".repeat(64)).setExecutionRecordSha256("b".repeat(64))
                .setInputSha256("c".repeat(64)).setPolicySha256("d".repeat(64))
                .setDisposition(AssessmentDisposition.ASSESSMENT_DISPOSITION_ACCEPTED)
                .setReason("accepted").build();
        var validation = ai.protomolt.proto.validate.ProtoValidator
                .forMessageType(WorkflowAssessment.getDescriptor()).validate(assessment);
        assertThat(validation.violations()).anyMatch(v ->
                v.ruleId().equals("assessment-acceptance-needs-judgment"));
    }

    private static final class ScriptedEvaluator implements ContactCorrection.Evaluator {
        private final AtomicInteger invocations = new AtomicInteger();
        private String selected = "supported";
        private boolean malformed;
        private String failure;
        private CountDownLatch started = new CountDownLatch(0);
        private CountDownLatch cancelled = new CountDownLatch(0);
        private boolean block;

        @Override public String profile() { return "evaluator.local"; }
        @Override public byte[] configuration() { return "scripted-evaluator-v1".getBytes(StandardCharsets.UTF_8); }

        int invocations() { return invocations.get(); }
        void answer(String option) { selected = option; }
        void malformedResponse() { malformed = true; }
        void fail(String message) { failure = message; }
        void blockUntilCancelled() {
            block = true;
            started = new CountDownLatch(1);
            cancelled = new CountDownLatch(1);
        }
        CountDownLatch started() { return started; }
        CountDownLatch cancelled() { return cancelled; }

        @Override
        public EvaluateResponse evaluate(EvaluateRequest request) throws Exception {
            invocations.incrementAndGet();
            if (failure != null) throw new IllegalStateException(failure);
            if (block) {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException interrupted) {
                    cancelled.countDown();
                    throw interrupted;
                }
            }
            String questionId = malformed ? "wrong-question" : request.getRubric().getQuestions(0).getId();
            return EvaluateResponse.newBuilder().setRequestId(request.getRequestId())
                    .setBinding(request.getBinding())
                    .addAnswers(EvaluationAnswer.newBuilder().setQuestionId(questionId)
                            .setChoice(ChoiceJudgment.newBuilder().setOption(selected)
                                    .addDistribution(EvaluationProbability.newBuilder()
                                            .setLabel("supported").setProbability(selected.equals("supported") ? 1 : 0))
                                    .addDistribution(EvaluationProbability.newBuilder()
                                            .setLabel("unsupported").setProbability(selected.equals("unsupported") ? 1 : 0))
                                    .addDistribution(EvaluationProbability.newBuilder()
                                            .setLabel("unresolved").setProbability(selected.equals("unresolved") ? 1 : 0))))
                    .setProvider("scripted-judge").setModel("contact-judge-v1").build();
        }
    }

    private static final class ScriptedProvider implements InferenceProvider {
        private final Queue<String> responses = new ArrayDeque<>();
        private final AtomicInteger invocations = new AtomicInteger();
        private String failure;

        void script(String... values) { responses.addAll(List.of(values)); }
        void fail(String message) { failure = message; }
        int invocations() { return invocations.get(); }

        @Override public String id() { return "scripted"; }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            invocations.incrementAndGet();
            if (failure != null) throw new InferenceException(failure);
            String text = responses.poll();
            if (text == null) throw new InferenceException("script ran out of responses");
            return GenerateResponse.newBuilder().setText(text).setModel(model.getId()).setProvider(id())
                    .setModelVersion("scripted-v1").setFinishReason(FinishReason.FINISH_REASON_STOP)
                    .setUsage(Usage.newBuilder().setPromptTokens(5).setCompletionTokens(3)).build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request, ChunkObserver observer) {
            throw new InferenceException("scripted provider does not stream");
        }
    }
}
