package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusResponse;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobState;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobStatus;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchStatusServiceOuterClass;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Native shape contract for read-only launch status; handlers still bind jobs to trusted launch records. */
class WorkflowLaunchStatusContractTest {
    private static final String LAUNCH_ID = "00000000-0000-4000-8000-000000000001";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000002";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);
    private static final String HASH_C = "c".repeat(64);

    @Test
    void everyDeclaredJobStateAndBothNonJobOutcomesValidateAsGeneratedAndDynamic() throws Exception {
        var validator = ProtoValidator.forMessageType(GetWorkflowLaunchStatusResponse.getDescriptor());
        for (WorkflowLaunchJobState state : List.of(
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED,
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_RUNNING,
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_WAITING,
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_COMPLETED,
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_FAILED,
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_DEAD)) {
            assertValidBoth(validator, response().setJob(job(state)).build());
        }
        assertValidBoth(validator, response().setNotAuthorized(true).build());
        assertValidBoth(validator, response().setAuthorizedNotQueued(true).build());

        // Attempt zero is valid while a newly-created job is still queued.
        assertValidBoth(ProtoValidator.forMessageType(WorkflowLaunchJobStatus.getDescriptor()),
                job(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED).toBuilder().setAttempt(0).build());
    }

    @Test
    void outcomeOneofMustBePresentAndBooleanMarkersMustBeExplicitlyTrue() throws Exception {
        var validator = ProtoValidator.forMessageType(GetWorkflowLaunchStatusResponse.getDescriptor());
        assertInvalidBoth(validator, response().build());
        assertInvalidBoth(validator, response().setNotAuthorized(false).build());
        assertInvalidBoth(validator, response().setAuthorizedNotQueued(false).build());
        assertInvalidBoth(ProtoValidator.forMessageType(WorkflowLaunchJobStatus.getDescriptor()),
                WorkflowLaunchJobStatus.getDefaultInstance());
        assertInvalidBoth(ProtoValidator.forMessageType(WorkflowLaunchJobStatus.getDescriptor()),
                job(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_UNSPECIFIED));
        var unknownNumeric = job(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_RUNNING)
                .toBuilder().setStateValue(99).build();
        assertInvalidBoth(ProtoValidator.forMessageType(WorkflowLaunchJobStatus.getDescriptor()), unknownNumeric);
    }

    @Test
    void jobAttemptIsNonnegativeAndTerminalTimestampPresenceIsValidatedByCel() throws Exception {
        var validator = ProtoValidator.forMessageType(WorkflowLaunchJobStatus.getDescriptor());
        var queued = job(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED);
        assertValidBoth(validator, queued.toBuilder().setAttempt(0).build());
        // Lease recovery can increment beyond max_attempts; report this persisted fact as-is.
        assertValidBoth(validator, queued.toBuilder().setAttempt(4).setMaxAttempts(3).build());
        assertInvalidBoth(validator, queued.toBuilder().setAttempt(-1).build());
        assertInvalidBoth(validator, queued.toBuilder().setMaxAttempts(0).build());

        assertRule(queued.toBuilder().setState(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_COMPLETED)
                .build(), "launch-job-terminal-time");
        assertRule(queued.toBuilder().setCompletedAt(time(30)).build(), "launch-job-terminal-time");
        // Timestamps are copied from existing job rows; do not invent an ordering invariant.
        assertValidBoth(validator, queued.toBuilder().setUpdatedAt(time(9)).build());
    }

    @Test
    void responseRequiresTheExistingLaunchRequestAndBindsProjectedJobToItsLaunchId() throws Exception {
        var validator = ProtoValidator.forMessageType(GetWorkflowLaunchStatusResponse.getDescriptor());
        assertInvalidBoth(validator, GetWorkflowLaunchStatusResponse.getDefaultInstance());
        assertInvalidBoth(validator, response().clearRequest().setNotAuthorized(true).build());
        assertRule(response().setJob(job(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED)
                .toBuilder().setJobId(TASK_ID).build()).build(), "launch-status-job-binding");
        assertRule(response().setJob(job(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED)
                .toBuilder().setJobId("not-a-uuid").build()).build(), "string.uuid");

        // This exact request message is reused by launch; status adds no caller-controlled job key.
        assertThat(GetWorkflowLaunchStatusResponse.getDescriptor().findFieldByName("request").getMessageType())
                .isEqualTo(WorkflowAuthoringLaunchRequest.getDescriptor());
        assertThat(WorkflowLaunchStatusServiceOuterClass.getDescriptor().findServiceByName("WorkflowLaunchStatusService")
                .findMethodByName("GetWorkflowLaunchStatus").getInputType())
                .isEqualTo(GetWorkflowLaunchStatusRequest.getDescriptor());
        assertThat(GetWorkflowLaunchStatusRequest.getDescriptor().findFieldByName("request").getMessageType())
                .isEqualTo(WorkflowAuthoringLaunchRequest.getDescriptor());
        assertValidBoth(ProtoValidator.forMessageType(GetWorkflowLaunchStatusRequest.getDescriptor()),
                GetWorkflowLaunchStatusRequest.newBuilder().setRequest(launchRequest()).build());
        assertInvalidBoth(ProtoValidator.forMessageType(GetWorkflowLaunchStatusRequest.getDescriptor()),
                GetWorkflowLaunchStatusRequest.getDefaultInstance());
    }

    @Test
    void jsonSchemaAndOpenApiRetainRuntimeCelRulesAndRequestBinding() throws Exception {
        var json = new ObjectMapper();
        JsonNode schema = json.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(GetWorkflowLaunchStatusResponse.getDescriptor()));
        assertThat(schema.path("required").toString()).contains("request");
        assertThat(schema.path("x-protomolt-cel").toString())
                .contains("launch-status-outcome-required", "launch-status-job-binding");
        JsonNode jobSchema = json.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(WorkflowLaunchJobStatus.getDescriptor()));
        assertThat(jobSchema.path("x-protomolt-cel").toString())
                .contains("launch-job-terminal-time");
        JsonNode requestSchema = json.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(GetWorkflowLaunchStatusRequest.getDescriptor()));
        assertThat(requestSchema.path("required").toString()).contains("request");

        var service = WorkflowLaunchStatusServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowLaunchStatusService");
        var registry = new ProtoRestMethodRegistry();
        service.getMethods().forEach(method -> registry.register(service, method, unused -> {
            throw new AssertionError("OpenAPI generation must not invoke a handler");
        }, ApiTokenRequirement.bearer()));
        JsonNode openApi = json.valueToTree(new ProtoOpenApiGenerator().generate(registry));
        assertThat(openApi.path("paths").has(
                "/grpc-json/WorkflowLaunchStatusService/GetWorkflowLaunchStatus")).isTrue();
        assertThat(openApi.path("components").path("schemas").toString())
                .contains("launch-status-outcome-required", "launch-job-terminal-time");
    }

    private static GetWorkflowStatusResponseBuilder response() {
        return new GetWorkflowStatusResponseBuilder(GetWorkflowLaunchStatusResponse.newBuilder()
                .setRequest(launchRequest()));
    }

    private static WorkflowLaunchJobStatus job(WorkflowLaunchJobState state) {
        var builder = WorkflowLaunchJobStatus.newBuilder().setJobId(LAUNCH_ID).setState(state)
                .setAttempt(1).setMaxAttempts(3).setCreatedAt(time(10)).setUpdatedAt(time(20));
        if (state == WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_COMPLETED
                || state == WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_FAILED
                || state == WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_DEAD) {
            builder.setCompletedAt(time(20));
        }
        return builder.build();
    }

    private static WorkflowAuthoringLaunchRequest launchRequest() {
        return WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(LAUNCH_ID)
                .setAcceptance(WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                        .setTaskSpecSha256(HASH_A).setCandidateSha256(HASH_B).setAcceptedEntrySha256(HASH_C))
                .setInput(ArtifactReference.newBuilder().setSha256(HASH_A)
                        .setMediaType("application/x-protobuf").setSizeBytes(1)).build();
    }

    private static Timestamp time(long seconds) {
        return Timestamp.newBuilder().setSeconds(seconds).build();
    }

    private static void assertRule(com.google.protobuf.Message message, String id) throws Exception {
        var validator = ProtoValidator.forMessageType(message.getDescriptorForType());
        assertThat(validator.validate(message).violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo(id));
        assertThat(validator.validate(dynamic(message)).violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo(id));
    }

    private static DynamicMessage dynamic(com.google.protobuf.Message message) throws Exception {
        return DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
    }

    private static void assertValidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid())
                .as("generated violations: %s", validator.validate(message).violations()).isTrue();
        assertThat(validator.validate(dynamic(message)).valid()).isTrue();
    }

    private static void assertInvalidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid()).isFalse();
        assertThat(validator.validate(dynamic(message)).valid()).isFalse();
    }

    /** Wrapper keeps test response setup readable while allowing generated builders. */
    private record GetWorkflowStatusResponseBuilder(GetWorkflowLaunchStatusResponse.Builder builder) {
        GetWorkflowStatusResponseBuilder setNotAuthorized(boolean value) {
            return new GetWorkflowStatusResponseBuilder(builder.setNotAuthorized(value));
        }
        GetWorkflowStatusResponseBuilder setAuthorizedNotQueued(boolean value) {
            return new GetWorkflowStatusResponseBuilder(builder.setAuthorizedNotQueued(value));
        }
        GetWorkflowStatusResponseBuilder setJob(WorkflowLaunchJobStatus value) {
            return new GetWorkflowStatusResponseBuilder(builder.setJob(value));
        }
        GetWorkflowStatusResponseBuilder clearRequest() {
            return new GetWorkflowStatusResponseBuilder(builder.clearRequest());
        }
        GetWorkflowLaunchStatusResponse build() { return builder.build(); }
    }
}
