package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/** Contributes the accepted-workflow operations to a host's action catalog. */
public final class WorkflowAuthoringActions {
    private WorkflowAuthoringActions() {}

    /** The host supplies trusted operations and registers scoped launch verbs. */
    public static ActionCatalog register(ActionCatalog catalog, WorkflowAuthoringOperations operations) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(operations, "operations");
        return catalog.register(new GetAcceptedAction(operations))
                .register(new LaunchAcceptedAction(operations));
    }

    private abstract static class AuthoringAction implements ProtoAction {
        final WorkflowAuthoringOperations operations;

        AuthoringAction(WorkflowAuthoringOperations operations) {
            this.operations = operations;
        }

        @Override public String requiredScope() { return Scopes.WORKFLOW_LAUNCH; }

        static void validateRequest(Message request, String verb) throws ActionException {
            try {
                WorkflowLaunchValidation.validate(request);
            } catch (IllegalArgumentException invalid) {
                throw new ActionException("invalid-input", verb + " request is invalid");
            }
        }

        static <T extends Message> T validateResult(T result, String verb) throws ActionException {
            if (result == null) {
                throw new ActionException("invalid-upstream-response", verb + " returned no result");
            }
            try {
                WorkflowLaunchValidation.validate(result);
            } catch (IllegalArgumentException invalid) {
                throw new ActionException("invalid-upstream-response", verb + " returned an invalid result");
            }
            return result;
        }

        static ActionException failure(Exception failure) {
            if (failure instanceof WorkflowLaunchConflictException) {
                return new ActionException("workflow-launch-conflict",
                        "launch identifier is already bound to different content");
            }
            if (failure instanceof WorkflowRunner.WorkflowExecutionException execution) {
                if (execution.kind() == WorkflowRunner.FailureKind.DEADLINE) {
                    return new ActionException("workflow-authoring-deadline",
                            "workflow authoring verification timed out");
                }
                if (execution.kind() == WorkflowRunner.FailureKind.GRPC) {
                    ActionException transport = transport(execution.grpcCode());
                    if (transport != null) return transport;
                    return new ActionException("internal-error", "workflow authoring operation failed");
                }
                return new ActionException("workflow-authoring-rejected",
                        "workflow authoring verification rejected the candidate");
            }
            if (failure instanceof StatusException status) {
                ActionException transport = transport(status.getStatus().getCode());
                if (transport != null) return transport;
            }
            if (failure instanceof StatusRuntimeException status) {
                ActionException transport = transport(status.getStatus().getCode());
                if (transport != null) return transport;
            }
            if (failure instanceof IOException) {
                return new ActionException("workflow-authoring-storage-failed",
                        "workflow authoring storage failed");
            }
            if (failure instanceof IllegalArgumentException) {
                return new ActionException("workflow-authoring-rejected",
                        "workflow authoring rejected the request");
            }
            return new ActionException("internal-error", "workflow authoring operation failed");
        }

        private static ActionException transport(Status.Code code) {
            if (code == Status.Code.UNAVAILABLE) {
                return new ActionException("workflow-authoring-unavailable",
                        "workflow authoring dependency is unavailable");
            }
            if (code == Status.Code.DEADLINE_EXCEEDED) {
                return new ActionException("workflow-authoring-deadline",
                        "workflow authoring dependency timed out");
            }
            return null;
        }
    }

    private static final class GetAcceptedAction extends AuthoringAction {
        GetAcceptedAction(WorkflowAuthoringOperations operations) { super(operations); }

        @Override public String name() { return "get-accepted-workflow"; }

        @Override public String description() {
            return "Reads the server-computed identity of a durably accepted workflow candidate; "
                    + "this does not authorize a launch or run fixtures.";
        }

        @Override public Descriptor requestType() { return GetAcceptedWorkflowRequest.getDescriptor(); }

        @Override public Descriptor responseType() { return WorkflowAcceptedCandidate.getDescriptor(); }

        @Override public Message execute(Message input, ActionContext context) throws ActionException {
            GetAcceptedWorkflowRequest request = CatalogContract.as(
                    input, GetAcceptedWorkflowRequest.getDefaultInstance(), name());
            validateRequest(request, name());
            WorkflowAcceptedCandidate result;
            try {
                result = operations.acceptedCandidate(request.getTaskId());
            } catch (Exception failure) {
                throw failure(failure);
            }
            validateResult(result, name());
            if (!result.getTaskId().equals(request.getTaskId())) {
                throw new ActionException("invalid-upstream-response", "accepted workflow names a different task");
            }
            return result;
        }
    }

    private static final class LaunchAcceptedAction extends AuthoringAction {
        LaunchAcceptedAction(WorkflowAuthoringOperations operations) { super(operations); }

        @Override public String name() { return "launch-accepted-workflow"; }

        @Override public String description() {
            return "Independently verifies a durably accepted workflow and launches it under the "
                    + "caller-selected UUID; an exact retry returns its existing job.";
        }

        @Override public Descriptor requestType() { return WorkflowAuthoringLaunchRequest.getDescriptor(); }

        @Override public Descriptor responseType() { return WorkflowAuthoringLaunchResult.getDescriptor(); }

        @Override public Message execute(Message input, ActionContext context) throws ActionException {
            WorkflowAuthoringLaunchRequest request = CatalogContract.as(
                    input, WorkflowAuthoringLaunchRequest.getDefaultInstance(), name());
            validateRequest(request, name());
            WorkflowAuthoringLaunchResult result;
            try {
                result = operations.launch(request);
            } catch (Exception failure) {
                throw failure(failure);
            }
            validateResult(result, name());
            if (!result.getJobId().equals(UUID.fromString(request.getLaunchId()).toString())) {
                throw new ActionException("invalid-upstream-response", "launched workflow names a different job");
            }
            return result;
        }
    }
}
