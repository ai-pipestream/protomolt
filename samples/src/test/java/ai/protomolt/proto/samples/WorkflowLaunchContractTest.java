package ai.protomolt.proto.samples;

import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchAuthorization;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowCompiler;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Runtime validation boundaries for the unmounted accepted-workflow launch contract. */
class WorkflowLaunchContractTest {
    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000001";
    private static final String LAUNCH_ID = "00000000-0000-4000-8000-000000000002";
    private static final String SHA_A = "a".repeat(64);
    private static final String SHA_B = "b".repeat(64);
    private static final String SHA_C = "c".repeat(64);
    private static final String SHA_D = "d".repeat(64);
    private static final String PROTO = """
            syntax = "proto3";
            package launch.fixture;
            message Input { string name = 1; }
            message Result { string text = 1; }
            service Echo { rpc Run(Input) returns (Result); }
            """;

    private static Workflow workflow;
    private static Workflow differentWorkflow;
    private static WorkflowAuthoringDeliverable authored;
    private static WorkflowAcceptedCandidate acceptance;

    @BeforeAll static void fixtures() throws Exception {
        CompiledProtos protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("launch/fixture.proto", PROTO, "test").build());
        FileDescriptor file = protos.descriptorFor("launch/fixture.proto").orElseThrow();
        workflow = compile(file, "launch-workflow");
        differentWorkflow = compile(file, "other-workflow");
        authored = authored(workflow);
        acceptance = accepted();
    }

    private static Workflow compile(FileDescriptor file, String name) {
        var files = List.of(file);
        return WorkflowCompiler.compile(new CompiledWorkflow(name, files,
                file.findMessageTypeByName("Input"), 30_000,
                List.of(CompiledWorkflow.Step.grpc("echo", "fixture:9090", false,
                        CompiledWorkflow.resolveMethod(files, "launch.fixture.Echo/Run"),
                        null, List.of("name = input.name"), List.of(), false, 0, "")),
                null, true));
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = WorkflowLaunchContractTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) throw new IllegalStateException(path + " not on the test classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static ArtifactReference artifact(String hash, String mediaType,
                                              long size, boolean redacted) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType)
                .setSizeBytes(size).setRedacted(redacted).build();
    }

    private static ArtifactReference protobuf(String hash) {
        return artifact(hash, "application/x-protobuf", 32, false);
    }

    private static WorkflowAcceptedCandidate accepted() {
        return WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK_ID).setAttempt(1)
                .setRevision(1).setTaskSpecSha256(SHA_A).setCandidateSha256(SHA_B)
                .setAcceptedEntrySha256(SHA_C).build();
    }

    private static WorkflowAuthoringLaunchRequest request() {
        return WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(LAUNCH_ID)
                .setAcceptance(acceptance).setInput(protobuf(SHA_D)).build();
    }

    private static WorkflowAuthoringDeliverable authored(Workflow durable) {
        ArtifactReference artifact = protobuf(SHA_D);
        WorkflowDeliverable deliverable = WorkflowDeliverable.newBuilder()
                .setWorkflow(durable).setWorkflowArtifact(artifact).setDescriptors(artifact)
                .addFixtures(artifact)
                .addChecks(CheckEvidence.newBuilder().setCheckName("contract-tests")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)).addArtifacts(artifact))
                .setRunId("accepted-fixture-run").setReceipt(artifact).build();
        return WorkflowAuthoringDeliverable.newBuilder().setDeliverable(deliverable)
                .setExecutableSource(artifact.toBuilder().setMediaType("application/json"))
                .addAcceptanceFixtures(WorkflowAcceptanceFixture.newBuilder()
                        .setName("hello").setInput(artifact).setExpectedOutput(artifact))
                .build();
    }

    private static WorkflowAuthoringLaunchAuthorization authorization() {
        return authorization(request(), authored, workflow);
    }

    private static WorkflowAuthoringLaunchAuthorization authorization(
            WorkflowAuthoringLaunchRequest launch, WorkflowAuthoringDeliverable deliverable,
            Workflow promotedContent) {
        VersionedWorkflow promoted = VersionedWorkflow.newBuilder().setWorkflow(promotedContent)
                .setVersion("accepted-" + SHA_A)
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(promotedContent))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1)).build();
        return WorkflowAuthoringLaunchAuthorization.newBuilder().setRequest(launch)
                .setPolicy(protobuf(SHA_B)).setAuthored(deliverable).setPromoted(promoted).build();
    }

    private static ValidationResult validate(Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
    }

    private static void hasRule(ValidationResult result, String id) {
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo(id));
    }

    @Test void completeLaunchIntentAndAuthorizationValidate() {
        assertThat(validate(request()).violations()).isEmpty();
        assertThat(validate(authorization()).violations()).isEmpty();
        assertThat(validate(WorkflowAuthoringLaunchResult.newBuilder().setJobId(LAUNCH_ID)
                .setAuthorization(protobuf(SHA_C)).build()).violations()).isEmpty();
        var limit = protobuf(SHA_D).toBuilder().setSizeBytes(4_194_304L).build();
        assertThat(validate(request().toBuilder().setInput(limit).build()).violations()).isEmpty();
        assertThat(validate(authorization().toBuilder().setPolicy(limit).build()).violations()).isEmpty();
        assertThat(validate(WorkflowAuthoringLaunchResult.newBuilder().setJobId(LAUNCH_ID)
                .setAuthorization(limit).build()).violations()).isEmpty();
    }

    @Test void acceptedCandidateChecksUuidHashesAndAttemptRevisionBounds() {
        var validator = ProtoValidator.forMessageType(WorkflowAcceptedCandidate.getDescriptor());
        assertThat(validator.validate(accepted()).violations()).isEmpty();
        hasRule(validator.validate(accepted().toBuilder().setTaskId("not-a-uuid").build()),
                "string.uuid");
        var badHash = validator.validate(accepted().toBuilder()
                .setCandidateSha256("F".repeat(64)).build());
        hasRule(badHash, "string.sha256_hex");
        var low = validator.validate(accepted().toBuilder().setAttempt(0).build());
        assertThat(low.violations()).anySatisfy(v -> assertThat(v.path()).contains("attempt"));
        var high = validator.validate(accepted().toBuilder().setRevision(1025).build());
        assertThat(high.violations()).anySatisfy(v -> assertThat(v.path()).contains("revision"));
        assertThat(validator.validate(accepted().toBuilder().setAttempt(1024)
                .setRevision(1024).build()).violations()).isEmpty();
    }

    @Test void launchRequestRequiresIdentityAndAcceptanceAndBoundsUnredactedProtobufInput() {
        var validator = ProtoValidator.forMessageType(WorkflowAuthoringLaunchRequest.getDescriptor());
        assertThat(validator.validate(request().toBuilder().clearAcceptance().build()).violations())
                .anySatisfy(v -> assertThat(v.path()).contains("acceptance"));
        assertThat(validator.validate(request().toBuilder().clearInput().build()).violations())
                .anySatisfy(v -> assertThat(v.path()).contains("input"));
        assertThat(validator.validate(request().toBuilder().setLaunchId("bad").build()).violations())
                .anySatisfy(v -> assertThat(v.path()).contains("launch_id"));

        for (ArtifactReference invalid : List.of(
                protobuf(SHA_D).toBuilder().setRedacted(true).build(),
                protobuf(SHA_D).toBuilder().setMediaType("application/json").build(),
                protobuf(SHA_D).toBuilder().setSizeBytes(4_194_305L).build())) {
            hasRule(validator.validate(request().toBuilder().setInput(invalid).build()),
                    "launch-input-protobuf");
        }
    }

    @Test void authorizationPinsPolicyAndPromotedAuthoredWorkflow() {
        var validator = ProtoValidator.forMessageType(
                WorkflowAuthoringLaunchAuthorization.getDescriptor());
        assertThat(validator.validate(authorization().toBuilder().clearRequest().build())
                .violations()).anySatisfy(v -> assertThat(v.path()).contains("request"));
        for (ArtifactReference invalid : List.of(
                protobuf(SHA_B).toBuilder().setRedacted(true).build(),
                protobuf(SHA_B).toBuilder().setMediaType("application/json").build(),
                protobuf(SHA_B).toBuilder().setSizeBytes(4_194_305L).build())) {
            hasRule(validator.validate(authorization().toBuilder().setPolicy(invalid).build()),
                    "launch-policy-protobuf");
        }
        assertThat(validator.validate(authorization(request(), authored, differentWorkflow)
                .toBuilder().build()).violations()).anySatisfy(
                v -> assertThat(v.ruleId()).isEqualTo("launch-promoted-content"));
    }

    @Test void launchResultRequiresJobIdentityAndBoundedAuthorizationEvidence() {
        var validator = ProtoValidator.forMessageType(WorkflowAuthoringLaunchResult.getDescriptor());
        assertThat(validator.validate(WorkflowAuthoringLaunchResult.newBuilder()
                .setAuthorization(protobuf(SHA_C)).build()).violations())
                .anySatisfy(v -> assertThat(v.path()).contains("job_id"));
        for (ArtifactReference invalid : List.of(
                protobuf(SHA_C).toBuilder().setRedacted(true).build(),
                protobuf(SHA_C).toBuilder().setMediaType("application/json").build(),
                protobuf(SHA_C).toBuilder().setSizeBytes(4_194_305L).build())) {
            hasRule(validator.validate(WorkflowAuthoringLaunchResult.newBuilder()
                    .setJobId(LAUNCH_ID).setAuthorization(invalid).build()),
                    "launch-authorization-protobuf");
        }
    }

    @Test void validLookingHashesAndReferencesAreSyntaxOnlyNotProof() {
        // These fabricated but syntactically valid identities and refs pass protobuf rules.
        // Trusted transcript, artifact bytes and accepted candidate matching are handler work.
        assertThat(validate(accepted().toBuilder().setTaskSpecSha256("1".repeat(64))
                .setCandidateSha256("2".repeat(64)).setAcceptedEntrySha256("3".repeat(64))
                .build()).violations()).isEmpty();
        assertThat(validate(request().toBuilder().setInput(protobuf("4".repeat(64)))
                .build()).violations()).isEmpty();
        assertThat(validate(authorization().toBuilder().setPolicy(protobuf("5".repeat(64)))
                .build()).violations()).isEmpty();
    }
}
