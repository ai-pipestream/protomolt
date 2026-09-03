package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.StepEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StepStatus;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Offline replay: a recorded run verifies against its workflow with no server, and every
 * alteration — request fixture, gate verdict, descriptor fingerprint, workflow content, or a
 * missing artifact — fails with the step and the mismatch named.
 */
class WorkflowReplayTest {

    private static final String VALIDATE = "ai/pipestream/proto/validate/v1/validate.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package replay.test;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Text { string text = 1 [(ai.pipestream.proto.validate.v1.field) = {
              string: { min_len: 3 }
            }]; }
            message Tokens { repeated int64 ids = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Summarizer { rpc Summarize(Tokens) returns (Text); }
            """;

    private static final String MEDIA = "application/x-protobuf";

    private static FileDescriptor file;
    private static Descriptor text;
    private static Descriptor tokens;
    private static String schemaFingerprint;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("replay/test/replay.proto", PROTO, "test").build());
        file = compiled.descriptorFor("replay/test/replay.proto").orElseThrow();
        text = file.findMessageTypeByName("Text");
        tokens = file.findMessageTypeByName("Tokens");
        schemaFingerprint = WorkflowCompiler.descriptorFingerprint(List.of(file));
    }

    private static String resource(String name) {
        try (InputStream in = WorkflowReplayTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    private static Workflow.Builder workflow() {
        return Workflow.newBuilder()
                .setName("analyze")
                .setInputType("replay.test.Text")
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("nlp")
                        .setServiceProfile("tokenizer")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(schemaFingerprint)
                        .build())
                .addSteps(WorkflowStep.newBuilder()
                        .setName("tokenize")
                        .setDependency("nlp")
                        .setMethod("replay.test.Tokenizer/Tokenize")
                        .addRules("text=input.text")
                        .setValidateResponse(false)
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build())
                .addSteps(WorkflowStep.newBuilder()
                        .setName("summarize")
                        .setDependency("nlp")
                        .setMethod("replay.test.Summarizer/Summarize")
                        .setWhen("input.text == 'go'")
                        .addRules("ids=tokenize.ids")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build())
                .setDeadline(Duration.newBuilder().setSeconds(30).build());
    }

    private static ArtifactReference save(ArtifactRepository artifacts, Message message)
            throws IOException {
        return artifacts.save(message.toByteArray(), MEDIA, true);
    }

    private static StepEvidence succeeded(String name, String method,
                                          ArtifactReference request, ArtifactReference response) {
        StepEvidence.Builder builder = StepEvidence.newBuilder()
                .setStepName(name)
                .setMethod(method)
                .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(20).build())
                .setCompletedAt(Timestamp.newBuilder().setSeconds(21).build())
                .setGrpcStatusCode(0)
                .setResponseArtifact(response);
        if (request != null) {
            builder.setRequestArtifact(request);
        }
        return builder.build();
    }

    /** An honest golden recording: both steps ran, output is the last response. */
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
                .addSteps(succeeded("tokenize", "replay.test.Tokenizer/Tokenize",
                        save(artifacts, text("go")), save(artifacts, tokens(1, 2))))
                .addSteps(succeeded("summarize", "replay.test.Summarizer/Summarize",
                        save(artifacts, tokens(1, 2)), save(artifacts, text("done"))))
                .build();
    }

    @Test
    void goldenRecordingReplaysClean(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow().build();

        WorkflowReplay.ReplayResult result = WorkflowReplay.replay(
                workflow, golden(workflow, artifacts), List.of(file), artifacts);

        assertThat(result.ok()).as(result.failure()).isTrue();
        assertThat(result.steps()).hasSize(2);
    }

    @Test
    void alteredRequestFixtureFailsWithTheStepNamed(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow().build();
        RunEvidence tampered = RunEvidence.newBuilder(golden(workflow, artifacts))
                .setSteps(0, succeeded("tokenize", "replay.test.Tokenizer/Tokenize",
                        save(artifacts, text("forged")), save(artifacts, tokens(1, 2))))
                .build();

        WorkflowReplay.ReplayResult result = WorkflowReplay.replay(
                workflow, tampered, List.of(file), artifacts);

        assertThat(result.ok()).isFalse();
        assertThat(result.failure()).contains("tokenize").contains("request");
    }

    @Test
    void descriptorDriftFailsBeforeAnyStepRuns(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow().build();
        RunEvidence drifted = RunEvidence.newBuilder(golden(workflow, artifacts))
                .setDependencies(0, ServiceDependency.newBuilder(workflow.getDependencies(0))
                        .setDescriptorFingerprint("f".repeat(64)).build())
                .build();

        WorkflowReplay.ReplayResult result = WorkflowReplay.replay(
                workflow, drifted, List.of(file), artifacts);

        assertThat(result.ok()).isFalse();
        assertThat(result.failure()).contains("descriptor set");
    }

    @Test
    void evidenceFromOtherWorkflowContentIsRefused(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow().build();
        RunEvidence evidence = golden(workflow, artifacts);
        Workflow edited = workflow().setDeadline(Duration.newBuilder().setSeconds(60)).build();

        WorkflowReplay.ReplayResult result = WorkflowReplay.replay(
                edited, evidence, List.of(file), artifacts);

        assertThat(result.ok()).isFalse();
        assertThat(result.failure()).contains("different workflow content");
    }

    @Test
    void missingArtifactFailsLoudly(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow().build();
        RunEvidence missing = RunEvidence.newBuilder(golden(workflow, artifacts))
                .setInputArtifact(ArtifactReference.newBuilder()
                        .setSha256("0".repeat(64))
                        .setMediaType(MEDIA)
                        .setSizeBytes(5)
                        .setRedacted(true)
                        .build())
                .build();

        assertThatThrownBy(() -> WorkflowReplay.replay(workflow, missing, List.of(file), artifacts))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing artifact");
    }

    @Test
    void skippedStepReEvaluatesItsGate(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        Workflow workflow = workflow().build();
        // Input "stop": the summarize gate is false, so a SKIPPED record is honest.
        RunEvidence skipped = RunEvidence.newBuilder()
                .setRunId("run-2")
                .setWorkflowName(workflow.getName())
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(20).build())
                .setCompletedAt(Timestamp.newBuilder().setSeconds(25).build())
                .addDependencies(workflow.getDependencies(0))
                .setInputArtifact(save(artifacts, text("stop")))
                .setOutputArtifact(save(artifacts, tokens(1, 2)))
                .addSteps(succeeded("tokenize", "replay.test.Tokenizer/Tokenize",
                        save(artifacts, text("stop")), save(artifacts, tokens(1, 2))))
                .addSteps(StepEvidence.newBuilder()
                        .setStepName("summarize")
                        .setMethod("replay.test.Summarizer/Summarize")
                        .setStatus(StepStatus.STEP_STATUS_SKIPPED)
                        .setStartedAt(Timestamp.newBuilder().setSeconds(21).build())
                        .setCompletedAt(Timestamp.newBuilder().setSeconds(21).build())
                        .build())
                .build();

        WorkflowReplay.ReplayResult honest = WorkflowReplay.replay(
                workflow, skipped, List.of(file), artifacts);
        assertThat(honest.ok()).as(honest.failure()).isTrue();

        // The same skip claim against an input whose gate is true is a forgery. The workflow
        // changes to make the gate fire on "stop", so the evidence is re-fingerprinted.
        Workflow flipped = workflow().setSteps(1,
                WorkflowStep.newBuilder(workflow.getSteps(1))
                        .setWhen("input.text == 'stop'").build()).build();
        RunEvidence forged = RunEvidence.newBuilder(skipped)
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(flipped))
                .build();

        WorkflowReplay.ReplayResult caught = WorkflowReplay.replay(
                flipped, forged, List.of(file), artifacts);
        assertThat(caught.ok()).isFalse();
        assertThat(caught.failure()).contains("summarize").contains("gate");
    }

    @Test
    void recordedResponseMustPassDeclaredValidation(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir);
        // The output is a Text, whose rules demand a non-empty text; the run validates it.
        Workflow validating = workflow()
                .setSteps(1, WorkflowStep.newBuilder(workflow().getSteps(1))
                        .setValidateResponse(true).build())
                .build();
        RunEvidence invalid = RunEvidence.newBuilder()
                .setRunId("run-3")
                .setWorkflowName(validating.getName())
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(validating))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(20).build())
                .setCompletedAt(Timestamp.newBuilder().setSeconds(25).build())
                .addDependencies(validating.getDependencies(0))
                .setInputArtifact(save(artifacts, text("go")))
                .addSteps(succeeded("tokenize", "replay.test.Tokenizer/Tokenize",
                        save(artifacts, text("go")), save(artifacts, tokens(1, 2))))
                .addSteps(succeeded("summarize", "replay.test.Summarizer/Summarize",
                        save(artifacts, tokens(1, 2)), save(artifacts, text("ab"))))
                .build();

        WorkflowReplay.ReplayResult result = WorkflowReplay.replay(
                validating, invalid, List.of(file), artifacts);

        assertThat(result.ok()).isFalse();
        assertThat(result.failure()).contains("summarize").contains("validation");
    }
}
