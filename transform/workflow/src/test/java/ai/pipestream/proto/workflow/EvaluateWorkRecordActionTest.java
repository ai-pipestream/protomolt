package ai.pipestream.proto.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.RunStatus;
import ai.pipestream.proto.grpc.workflow.v1.ServiceDependency;
import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
import ai.pipestream.proto.grpc.workflow.v1.StepEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StepStatus;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.WorkflowStep;
import ai.pipestream.proto.receipt.Disclosure;
import ai.pipestream.proto.receipt.KeyState;
import ai.pipestream.proto.receipt.RecordKeys;
import ai.pipestream.proto.receipt.RecordSigner;
import ai.pipestream.proto.receipt.SignatureAlgorithm;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustedIssuer;
import ai.pipestream.proto.receipt.TrustedKey;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The evaluation sidecar's decision procedure: verification, the
 * byte-identical evidence reprojection, and offline replay, each named in
 * the evaluation record, with the signed record never modified.
 */
class EvaluateWorkRecordActionTest {

    private static final String PROTO = """
            syntax = "proto3";
            package eval.test;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Summarizer { rpc Summarize(Tokens) returns (Text); }
            """;
    private static final String MEDIA = "application/x-protobuf";
    private static final byte[] SEED = HexFormat.of().parseHex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    private static final byte[] PUBLIC_KEY = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ActionContext CONTEXT = ActionContext.create();

    private static CompiledProtos compiled;
    private static Descriptor text;
    private static Descriptor tokens;
    private static String schemaFingerprint;

    private final RecordSigner signer =
            new RecordSigner("key-2026", RecordKeys.privateKey(SEED));
    private final Clock clock =
            Clock.fixed(Instant.ofEpochSecond(1750000100), ZoneOffset.UTC);

    @BeforeAll
    static void compile() throws Exception {
        compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("eval/test/eval.proto", PROTO, "test").build());
        var file = compiled.descriptorFor("eval/test/eval.proto").orElseThrow();
        text = file.findMessageTypeByName("Text");
        tokens = file.findMessageTypeByName("Tokens");
        schemaFingerprint = WorkflowCompiler.descriptorFingerprint(List.of(file));
    }

    private static DynamicMessage text(String value) {
        return DynamicMessage.newBuilder(text)
                .setField(text.findFieldByName("text"), value).build();
    }

    private static DynamicMessage tokens(long... ids) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(tokens);
        for (long id : ids) {
            builder.addRepeatedField(tokens.findFieldByName("ids"), id);
        }
        return builder.build();
    }

    private static Workflow workflow() {
        return Workflow.newBuilder()
                .setName("analyze")
                .setInputType("eval.test.Text")
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("nlp")
                        .setServiceProfile("tokenizer")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(schemaFingerprint))
                .addSteps(WorkflowStep.newBuilder()
                        .setName("tokenize")
                        .setDependency("nlp")
                        .setMethod("eval.test.Tokenizer/Tokenize")
                        .addRules("text=input.text")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE))
                .addSteps(WorkflowStep.newBuilder()
                        .setName("summarize")
                        .setDependency("nlp")
                        .setMethod("eval.test.Summarizer/Summarize")
                        .addRules("ids=tokenize.ids")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE))
                .setDeadline(Duration.newBuilder().setSeconds(30))
                .build();
    }

    private static ArtifactReference save(ArtifactRepository artifacts, Message message)
            throws IOException {
        return artifacts.save(message.toByteArray(), MEDIA, true);
    }

    private static StepEvidence step(String name, String method,
                                     ArtifactReference request, ArtifactReference response) {
        return StepEvidence.newBuilder()
                .setStepName(name)
                .setMethod(method)
                .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(20).build())
                .setCompletedAt(Timestamp.newBuilder().setSeconds(21).build())
                .setRequestArtifact(request)
                .setResponseArtifact(response)
                .build();
    }

    private static RunEvidence golden(Workflow workflow, ArtifactRepository artifacts)
            throws IOException {
        return RunEvidence.newBuilder()
                .setRunId("run-1")
                .setWorkflowName(workflow.getName())
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(20).build())
                .setCompletedAt(Timestamp.newBuilder().setSeconds(25).build())
                .addDependencies(workflow.getDependencies(0))
                .setInputArtifact(save(artifacts, text("go")))
                .setOutputArtifact(save(artifacts, text("done")))
                .addSteps(step("tokenize", "eval.test.Tokenizer/Tokenize",
                        save(artifacts, text("go")), save(artifacts, tokens(1, 2))))
                .addSteps(step("summarize", "eval.test.Summarizer/Summarize",
                        save(artifacts, tokens(1, 2)), save(artifacts, text("done"))))
                .build();
    }

    private static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer("records.protomolt.dev")
                        .addKeys(TrustedKey.newBuilder()
                                .setKeyId("key-2026")
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(PUBLIC_KEY))
                                .setState(KeyState.KEY_STATE_ACTIVE))
                        .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN))
                .build();
    }

    private WorkRecord manifest(RunEvidence evidence) {
        return WorkRecordProjector.project(evidence, new WorkRecordProjector.Issuance(
                "record-run-1", "records.protomolt.dev", "key-2026",
                Timestamp.newBuilder().setSeconds(1750000000).build(), ""));
    }

    private ObjectNode input(WorkRecord manifest, Workflow workflow) throws Exception {
        ObjectNode input = MAPPER.createObjectNode();
        input.put("recordBase64", Base64.getEncoder()
                .encodeToString(signer.sign(manifest).toByteArray()));
        input.set("trust", (ObjectNode) MAPPER.readTree(
                JsonFormat.printer().print(trust())));
        input.set("workflow", (ObjectNode) MAPPER.readTree(
                JsonFormat.printer().print(workflow)));
        input.putObject("schema").put("descriptorSetBase64", Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray()));
        return input;
    }

    @Test
    void aCleanRecordEvaluatesAccepted(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow();
        RunEvidence evidence = golden(workflow, artifacts);

        ObjectNode output = new EvaluateWorkRecordAction(artifacts,
                new StubRuns(evidence), clock)
                .execute(input(manifest(evidence), workflow), CONTEXT);

        assertThat(output.path("accepted").asBoolean())
                .as(output.toString())
                .isTrue();
        assertThat(output.path("policyId").asText())
                .isEqualTo(EvaluateWorkRecordAction.POLICY_ID);
        assertThat(output.path("policySha256").asText())
                .isEqualTo(EvaluateWorkRecordAction.policySha256());
        assertThat(output.path("evaluatedAt").asText())
                .isEqualTo(Instant.ofEpochSecond(1750000100).toString());
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_RECORD_MATCHES_EVIDENCE)
                .path("status").asText()).isEqualTo("PASSED");
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_REPLAY)
                .path("status").asText()).isEqualTo("PASSED");
        assertThat(output.path("replaySteps")).hasSize(2);
    }

    @Test
    void aRecordOfDifferentEvidenceIsRejectedByTheMatch(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow();
        RunEvidence evidence = golden(workflow, artifacts);
        RunEvidence altered = evidence.toBuilder()
                .setSteps(0, evidence.getSteps(0).toBuilder().setSummary("altered"))
                .build();

        ObjectNode output = new EvaluateWorkRecordAction(artifacts,
                new StubRuns(evidence), clock)
                .execute(input(manifest(altered), workflow), CONTEXT);

        assertThat(output.path("accepted").asBoolean()).isFalse();
        JsonNode match = check(output, EvaluateWorkRecordAction.CHECK_RECORD_MATCHES_EVIDENCE);
        assertThat(match.path("status").asText()).isEqualTo("FAILED");
        assertThat(match.path("detail").asText()).contains("not the projection");
    }

    @Test
    void replayDriftRejectsARecordThatStillMatchesItsEvidence(@TempDir Path dir)
            throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow();
        RunEvidence golden = golden(workflow, artifacts);
        RunEvidence forged = golden.toBuilder()
                .setSteps(0, golden.getSteps(0).toBuilder()
                        .setRequestArtifact(save(artifacts, text("forged"))))
                .build();

        ObjectNode output = new EvaluateWorkRecordAction(artifacts,
                new StubRuns(forged), clock)
                .execute(input(manifest(forged), workflow), CONTEXT);

        assertThat(output.path("accepted").asBoolean()).isFalse();
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_RECORD_MATCHES_EVIDENCE)
                .path("status").asText()).isEqualTo("PASSED");
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_REPLAY)
                .path("status").asText()).isEqualTo("FAILED");
    }

    @Test
    void aFailedVerificationSkipsTheEvaluationChecks(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow();
        RunEvidence evidence = golden(workflow, artifacts);
        ObjectNode input = input(manifest(evidence).toBuilder()
                .setIssuer("nobody.example").build(), workflow);

        ObjectNode output = new EvaluateWorkRecordAction(artifacts,
                new StubRuns(evidence), clock).execute(input, CONTEXT);

        assertThat(output.path("accepted").asBoolean()).isFalse();
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_RECORD_MATCHES_EVIDENCE)
                .path("status").asText()).isEqualTo("SKIPPED");
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_REPLAY)
                .path("status").asText()).isEqualTo("SKIPPED");
        assertThat(output.path("replaySteps")).isEmpty();
    }

    @Test
    void aRecordNamingAnUnknownRunFailsTheMatchAndSkipsReplay(@TempDir Path dir)
            throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow();
        RunEvidence evidence = golden(workflow, artifacts);

        ObjectNode output = new EvaluateWorkRecordAction(artifacts,
                new StubRuns(), clock)
                .execute(input(manifest(evidence), workflow), CONTEXT);

        assertThat(output.path("accepted").asBoolean()).isFalse();
        JsonNode match = check(output, EvaluateWorkRecordAction.CHECK_RECORD_MATCHES_EVIDENCE);
        assertThat(match.path("status").asText()).isEqualTo("FAILED");
        assertThat(match.path("detail").asText()).contains("run-1");
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_REPLAY)
                .path("status").asText()).isEqualTo("SKIPPED");
    }

    @Test
    void aDisclosureSkipsTheMatchAndStillReplays(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow();
        RunEvidence evidence = golden(workflow, artifacts);
        WorkRecord disclosure = manifest(evidence).toBuilder()
                .setDisclosure(Disclosure.newBuilder()
                        .setSourceManifestSha256(WorkRecords.sha256Hex("source".getBytes()))
                        .setPolicy("remove internal"))
                .build();

        ObjectNode output = new EvaluateWorkRecordAction(artifacts,
                new StubRuns(evidence), clock)
                .execute(input(disclosure, workflow), CONTEXT);

        assertThat(output.path("accepted").asBoolean())
                .as(output.toString())
                .isTrue();
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_RECORD_MATCHES_EVIDENCE)
                .path("status").asText()).isEqualTo("SKIPPED");
        assertThat(check(output, EvaluateWorkRecordAction.CHECK_REPLAY)
                .path("status").asText()).isEqualTo("PASSED");
    }

    @Test
    void unavailableWithoutAWorkspace() {
        assertThatThrownBy(() -> new EvaluateWorkRecordAction(null, null)
                .execute(MAPPER.createObjectNode(), CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("--workflow-workspace");
    }

    private static JsonNode check(ObjectNode output, String id) {
        for (JsonNode check : output.path("checks")) {
            if (check.path("id").asText().equals(id)) {
                return check;
            }
        }
        throw new AssertionError("no check '" + id + "' in " + output.path("checks"));
    }

    /** In-memory evidence store keyed by run id. */
    private static final class StubRuns implements RunEvidenceRepository {
        private final Map<String, RunEvidence> store = new HashMap<>();

        private StubRuns(RunEvidence... evidence) {
            for (RunEvidence run : evidence) {
                store.put(run.getRunId(), run);
            }
        }

        @Override
        public Optional<RunEvidence> find(String runId) {
            return Optional.ofNullable(store.get(runId));
        }

        @Override
        public List<RunEvidence> list(String workflowName, int limit) {
            return List.copyOf(store.values());
        }

        @Override
        public void save(RunEvidence evidence) throws IOException {
            throw new IOException("read-only");
        }
    }
}
