package ai.protomolt.proto.correction;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.inference.openai.OpenAiCompatProvider;
import ai.protomolt.proto.inference.spi.*;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.*;
import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.correction.v1.*;
import ai.protomolt.proto.workflow.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/** Small executable starter; fixture mode needs no provider, account, container, or network. */
public final class CorrectionSample {
    private CorrectionSample() {}

    public static void main(String[] args) throws Exception {
        boolean live = false;
        String verifyRun = null;
        Path trustFile = null;
        Path workspace = Path.of("build", "correction-example");
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--live" -> live = true;
                case "--verify" -> verifyRun = args[++i];
                case "--trust" -> trustFile = Path.of(args[++i]);
                case "--workspace" -> workspace = Path.of(args[++i]);
                default -> throw new IllegalArgumentException(
                        "Use [--live] [--workspace directory] [--verify run-id --trust trust.pb]");
            }
        }
        if (verifyRun != null) {
            if (live || trustFile == null) throw new IllegalArgumentException("Offline verification requires --trust, without --live");
            var check = ContactCorrection.verifyStored(workspace, verifyRun,
                    TrustSnapshot.parseFrom(Files.readAllBytes(trustFile)));
            System.out.println("receipt-verified=" + check.ok());
            System.out.println("verification=" + check.reason());
            if (!check.ok()) throw new IllegalStateException("offline verification refused");
            return;
        }
        if (trustFile != null) throw new IllegalArgumentException("--trust requires --verify");
        Files.createDirectories(workspace);
        String runId = "contact-" + UUID.randomUUID();
        var keys = RecordKeys.generate();
        String keyId = "demo-" + UUID.randomUUID();
        var signing = new RecordSigning("protomolt-correction-example",
                new RecordSigner(keyId, keys.getPrivate()));
        var trust = TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer(signing.issuer()).addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId(keyId).setState(KeyState.KEY_STATE_ACTIVE)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(keys.getPublic()))))).build();
        InferenceProvider provider = live ? new OpenAiCompatProvider(Duration.ofSeconds(25))
                : new FixtureProvider();
        var model = ModelEntry.newBuilder().setId("starter-correction").setProvider(provider.id())
                .setEndpoint(live ? requiredEnvironment("PROTOMOLT_CORRECTION_ENDPOINT") : "in-process://fixture")
                .setBackendModel(live ? requiredEnvironment("PROTOMOLT_CORRECTION_MODEL") : "contact-fixture-v1")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true));
        String credential = System.getenv("PROTOMOLT_CORRECTION_CREDENTIAL_REF");
        if (live && credential != null && !credential.isBlank()) model.setCredentialRef(credential);
        var entry = model.build();
        var engines = new InferenceEngines(new InferenceCatalog(), List.of(provider));
        engines.register(entry);
        var generator = new StructuredGenerator(engines, DescriptorRegistry.create(false));
        ContactCorrection.Evaluator evaluator = live ? liveEvaluator(provider, entry) : fixtureEvaluator();
        var mapper = new ObjectMapper();
        var registry = GitSchemaRegistryStore.builder().repositoryDir(workspace.resolve("registry")).build();
        try {
            if (registry.workflow(ContactCorrection.NAME).isEmpty()) {
                try (var stream = CorrectionSample.class.getResourceAsStream("/starter/correct-contact.workflow.json")) {
                    if (stream == null) throw new IllegalStateException("missing bundled workflow");
                    registry.putWorkflow(ContactCorrection.NAME, new String(stream.readAllBytes(), StandardCharsets.UTF_8));
                }
            }
            WorkflowRepository workflows = name -> registry.workflow(name).map(json -> {
                try { return (ObjectNode) mapper.readTree(json); }
                catch (Exception error) { throw new IllegalArgumentException("invalid stored workflow", error); }
            });
            var correction = new ContactCorrection(workspace, workflows, generator, evaluator, signing, Clock.systemUTC());
            var input = RawContact.newBuilder().setRecordId("contact-1")
                    .setContactText("Ada Lovelace; ada [at] example.org")
                    .setInternalNotes("source-account-private").build();
            var outcome = correction.run(runId, input);
            var check = correction.verify(outcome, trust);
            Files.write(workspace.resolve("outcomes").resolve(runId).resolve("trust.pb"), trust.toByteArray());
            System.out.println("mode=" + (live ? "live-provider" : "deterministic-fixture"));
            System.out.println("run=" + runId);
            System.out.println("disposition=" + outcome.assessment().getDisposition());
            System.out.println("reason=" + outcome.assessment().getReason());
            System.out.println("offline-replay=" + outcome.replayed());
            System.out.println("receipt-verified=" + check.ok());
            System.out.println("output=" + workspace.resolve("outcomes").resolve(runId).toAbsolutePath());
            System.out.println("Signing uses a fresh demonstration key; supplied trust is for this example only.");
            if (!check.ok()) throw new IllegalStateException("receipt verification failed: " + check.reason());
            if (outcome.assessment().getDisposition()
                    == ai.protomolt.proto.grpc.workflow.v1.AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED) {
                throw new IllegalStateException("correction failed: " + outcome.assessment().getReason()
                        + "; failure evidence and receipts were retained");
            }
        } finally {
            registry.close();
        }
    }

    /** Deliberately small deterministic test policy; never advertised as an LLM or general judge. */
    public static ContactCorrection.Evaluator fixtureEvaluator() {
        return new ContactCorrection.Evaluator() {
            public String profile() { return "contact-fixture-evaluator"; }
            public byte[] configuration() { return "contact-fixture-support-v1".getBytes(StandardCharsets.UTF_8); }
            public EvaluateResponse evaluate(EvaluateRequest request) throws Exception {
                var evidence = request.getEvidence().unpack(ContactEvidence.class);
                String text = evidence.getSource().getContactText().toLowerCase(Locale.ROOT).replace(" [at] ", "@");
                var result = evidence.getCandidate();
                boolean supported = !result.getEmail().isBlank()
                        && text.contains(result.getEmail().toLowerCase(Locale.ROOT))
                        && text.contains(result.getDisplayName().toLowerCase(Locale.ROOT));
                String option = supported ? "supported" : "unsupported";
                var choice = ChoiceJudgment.newBuilder().setOption(option);
                for (String label : List.of("supported", "unsupported", "unresolved")) {
                    choice.addDistribution(EvaluationProbability.newBuilder().setLabel(label)
                            .setProbability(label.equals(option) ? 1 : 0));
                }
                return response(request, choice.build(), "fixture", "contact-fixture-support-v1", "");
            }
        };
    }

    /** One bounded model call; malformed judgments fail rather than being accepted or silently repaired. */
    static ContactCorrection.Evaluator liveEvaluator(InferenceProvider provider, ModelEntry entry) {
        return liveEvaluator(provider, entry, false);
    }

    static ContactCorrection.Evaluator liveEvaluator(InferenceProvider provider, ModelEntry entry,
                                                     boolean promptGuided) {
        return new ContactCorrection.Evaluator() {
            public String profile() { return "contact-live-evaluator"; }
            public byte[] configuration() { return entry.toByteArray(); }
            public EvaluateResponse evaluate(EvaluateRequest request) throws Exception {
                var evidence = request.getEvidence().unpack(ContactEvidence.class);
                var generated = provider.generate(entry, GenerateRequest.newBuilder().setModel(entry.getId())
                        .setMaxOutputTokens(promptGuided ? 0 : 1024)
                        .addMessages(ChatTurn.newBuilder().setRole(Role.ROLE_SYSTEM).setContent(
                                ContactCorrection.RUBRIC.getQuestions(0).getInstruction()
                                + " Return only a JSON object with option (supported, unsupported, unresolved) "
                                + "and distribution (all three objects with label and probability, sum 1)."
                                + (promptGuided ? " No Markdown, no code fence, no explanation." : "")))
                        .addMessages(ChatTurn.newBuilder().setRole(Role.ROLE_USER)
                                .setContent(JsonFormat.printer().print(evidence))).build());
                if (generated.getFinishReason() != FinishReason.FINISH_REASON_STOP) {
                    throw new InferenceException("judgment did not complete");
                }
                var judgment = ChoiceJudgment.newBuilder();
                JsonFormat.parser().merge(generated.getText(), judgment);
                return response(request, judgment.build(), generated.getProvider(),
                        entry.getBackendModel(), generated.getModelVersion());
            }
        };
    }

    private static EvaluateResponse response(EvaluateRequest request, ChoiceJudgment judgment,
                                             String provider, String model, String version) {
        var result = EvaluateResponse.newBuilder().setRequestId(request.getRequestId())
                .setBinding(request.getBinding()).setProvider(provider).setModel(model)
                .addAnswers(EvaluationAnswer.newBuilder().setQuestionId("source-support").setChoice(judgment));
        if (!version.isBlank()) result.setModelVersion(version);
        return result.build();
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("missing " + name);
        return value;
    }

    static final class FixtureProvider implements InferenceProvider {
        private int calls;
        public String id() { return "fixture"; }
        public GenerateResponse generate(ModelEntry entry, GenerateRequest request) {
            String email = ++calls == 1 ? "invalid-address" : "ada@example.org";
            return GenerateResponse.newBuilder().setProvider(id()).setModel(entry.getId())
                    .setModelVersion("fixture-v1").setFinishReason(FinishReason.FINISH_REASON_STOP)
                    .setText("{\"recordId\":\"contact-1\",\"displayName\":\"Ada Lovelace\",\"email\":\""
                            + email + "\"}").build();
        }
        public void generateStream(ModelEntry entry, GenerateStreamRequest request, ChunkObserver observer) {
            throw new UnsupportedOperationException("fixture is unary");
        }
    }
}
