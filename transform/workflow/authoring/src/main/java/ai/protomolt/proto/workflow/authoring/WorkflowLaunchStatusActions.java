package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Objects;
import java.util.UUID;

/** Read-only status shared by the protocol adapters and scoped browser route. */
public final class WorkflowLaunchStatusActions {
    private WorkflowLaunchStatusActions() {}

    public static ActionCatalog register(ActionCatalog catalog, WorkflowLaunchStatusOperations operations) {
        return Objects.requireNonNull(catalog).register(new StatusAction(Objects.requireNonNull(operations)));
    }

    private record StatusAction(WorkflowLaunchStatusOperations operations) implements ProtoAction {
        @Override public String name() { return "get-workflow-launch-status"; }
        @Override public String description() { return "Read job status for an exact authorized launch intent."; }
        @Override public String requiredScope() { return Scopes.WORKFLOW_LAUNCH; }
        @Override public Descriptor requestType() { return GetWorkflowLaunchStatusRequest.getDescriptor(); }
        @Override public Descriptor responseType() { return GetWorkflowLaunchStatusResponse.getDescriptor(); }

        @Override public Message execute(Message input, ActionContext context) throws ActionException {
            var request = CatalogContract.as(input, GetWorkflowLaunchStatusRequest.getDefaultInstance(), name());
            validate(request, "invalid-input");
            GetWorkflowLaunchStatusResponse response;
            try {
                response = operations.get(request);
            } catch (WorkflowLaunchStatusException failure) {
                String code = switch (failure.kind()) {
                    case INVALID_INPUT -> "invalid-input";
                    case CONFLICT -> "workflow-launch-conflict";
                    case CORRUPT_EVIDENCE -> "invalid-upstream-response";
                    case UNAVAILABLE -> "workflow-authoring-unavailable";
                    case DEADLINE -> "workflow-authoring-deadline";
                };
                throw new ActionException(code, "Workflow launch status is unavailable");
            } catch (RuntimeException unexpected) {
                throw new ActionException("internal-error", "Workflow launch status failed");
            }
            validate(response, "invalid-upstream-response");
            var expected = request.getRequest().toBuilder().setLaunchId(
                    UUID.fromString(request.getRequest().getLaunchId()).toString()).build();
            if (!response.getRequest().equals(expected)) {
                throw new ActionException("invalid-upstream-response", "Workflow launch status has a different binding");
            }
            return response;
        }
    }

    private static void validate(Message message, String code) throws ActionException {
        try {
            WorkflowLaunchValidation.validate(message);
        } catch (RuntimeException invalid) {
            throw new ActionException(code, "Workflow launch status contract is invalid");
        }
    }
}
