package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.util.Objects;

/** Optional preparation action, shared by gRPC, JSON and agent transports. */
public final class WorkflowPreparationActions {
    private WorkflowPreparationActions() {}

    public static ActionCatalog register(ActionCatalog catalog, WorkflowPreparationOperations operation) {
        return Objects.requireNonNull(catalog).register(new Prepare(Objects.requireNonNull(operation)));
    }

    private record Prepare(WorkflowPreparationOperations operation) implements ProtoAction {
        @Override public String name() { return "prepare-workflow-candidate"; }
        @Override public String description() { return "Prepare evidence for the current leased workflow task."; }
        @Override public String requiredScope() { return Scopes.WORKFLOW_AUTHOR; }
        @Override public Descriptor requestType() { return PrepareWorkflowCandidateRequest.getDescriptor(); }
        @Override public Descriptor responseType() { return PrepareWorkflowCandidateResponse.getDescriptor(); }

        @Override public Message execute(Message input, ActionContext context) throws ActionException {
            throw new ActionException("permission-denied", "Preparation requires an authenticated task holder");
        }

        @Override public Message execute(Message input, ActionContext context, Caller caller) throws ActionException {
            var request = CatalogContract.as(input, PrepareWorkflowCandidateRequest.getDefaultInstance(), name());
            try {
                WorkflowPreparationValidation.validate(request, WorkflowPreparationValidation.INTENT_MAX);
            } catch (IllegalArgumentException invalid) {
                throw new ActionException("invalid-input", "Invalid preparation request");
            }
            PrepareWorkflowCandidateResponse response;
            try {
                response = operation.prepare(request, caller);
            } catch (WorkflowPreparationException failure) {
                throw failure(failure, context, request);
            } catch (RuntimeException unexpected) {
                throw new ActionException("internal-error", "Preparation failed");
            }
            try {
                WorkflowPreparationValidation.validateResponse(response);
                var binding = response.getBinding();
                if (!binding.getTaskId().equals(request.getTaskId()) || binding.getAttempt() != request.getAttempt()
                        || binding.getRevision() != request.getRevision()
                        || !binding.getPreparationId().equals(request.getPreparationId())
                        || !binding.getSourceSha256().equals(WorkRecords.sha256Hex(
                                request.getExecutableSourceJson().toByteArray()))
                        || !response.getAuthored().getDeliverable().getRunId()
                                .equals("prepare-" + request.getPreparationId())) {
                    throw new IllegalArgumentException();
                }
            } catch (IllegalArgumentException invalid) {
                throw new ActionException("invalid-upstream-response", "Invalid preparation result");
            }
            return response;
        }

        private static ActionException failure(WorkflowPreparationException failure, ActionContext context,
                PrepareWorkflowCandidateRequest request) {
            String code = switch (failure.kind()) {
                case INVALID_INPUT -> "invalid-input";
                case PERMISSION_DENIED -> "permission-denied";
                case INACTIVE -> "workflow-authoring-rejected";
                case CONFLICT -> "workflow-preparation-conflict";
                case TERMINAL_FAILED -> "preparation-attempt-failed";
                case CORRUPT_EVIDENCE -> "workflow-authoring-storage-failed";
                case INVALID_UPSTREAM -> "invalid-upstream-response";
                case UNAVAILABLE -> "workflow-authoring-unavailable";
                case DEADLINE -> "workflow-authoring-deadline";
            };
            ObjectNode details = null;
            if (failure.kind() == WorkflowPreparationException.Kind.TERMINAL_FAILED) {
                try {
                    WorkflowPreparationValidation.validate(failure.failure(), WorkflowPreparationValidation.RESPONSE_MAX);
                    var binding = failure.failure().getBinding();
                    if (!binding.getTaskId().equals(request.getTaskId()) || binding.getAttempt() != request.getAttempt()
                            || binding.getRevision() != request.getRevision()
                            || !binding.getPreparationId().equals(request.getPreparationId())
                            || !binding.getSourceSha256().equals(WorkRecords.sha256Hex(
                                    request.getExecutableSourceJson().toByteArray()))
                            || !failure.failure().getRunId().equals("prepare-" + request.getPreparationId())) {
                        throw new IllegalArgumentException();
                    }
                    details = (ObjectNode) context.objectMapper().readTree(JsonFormat.printer().print(failure.failure()));
                } catch (Exception invalid) {
                    return new ActionException("internal-error", "Invalid preparation failure detail");
                }
            }
            return new ActionException(code, "Workflow preparation did not complete", details);
        }
    }
}
