package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Objects;

/** Scoped input operations shared by the protocol adapters and console. */
public final class WorkflowLaunchInputActions {
    private static final int MAX_MESSAGE_BYTES = 8 * 1024 * 1024;

    private WorkflowLaunchInputActions() {}

    public static ActionCatalog register(ActionCatalog catalog, WorkflowLaunchInputOperations operations) {
        Objects.requireNonNull(operations);
        return Objects.requireNonNull(catalog).register(new InputAction(operations, false))
                .register(new InputAction(operations, true));
    }

    private record InputAction(WorkflowLaunchInputOperations operations, boolean prepare) implements ProtoAction {
        @Override public String name() {
            return prepare ? "prepare-workflow-launch-input" : "get-workflow-launch-input-contract";
        }
        @Override public String description() {
            return prepare ? "Validate and store input for an accepted workflow without launching it."
                    : "Read the pinned input contract for an accepted workflow.";
        }
        @Override public String requiredScope() { return Scopes.WORKFLOW_LAUNCH; }
        @Override public Descriptor requestType() {
            return prepare ? PrepareWorkflowLaunchInputRequest.getDescriptor()
                    : GetWorkflowLaunchInputContractRequest.getDescriptor();
        }
        @Override public Descriptor responseType() {
            return prepare ? PrepareWorkflowLaunchInputResponse.getDescriptor()
                    : GetWorkflowLaunchInputContractResponse.getDescriptor();
        }

        @Override public Message execute(Message input, ActionContext context) throws ActionException {
            Message request = prepare
                    ? CatalogContract.as(input, PrepareWorkflowLaunchInputRequest.getDefaultInstance(), name())
                    : CatalogContract.as(input, GetWorkflowLaunchInputContractRequest.getDefaultInstance(), name());
            validate(request, "invalid-input");
            Message response;
            try {
                response = prepare ? operations.prepare((PrepareWorkflowLaunchInputRequest) request)
                        : operations.contract((GetWorkflowLaunchInputContractRequest) request);
            } catch (WorkflowLaunchInputException failure) {
                String code = switch (failure.kind()) {
                    case INVALID_INPUT -> "invalid-input";
                    case INACTIVE, INCOMPATIBLE_ARTIFACT -> "workflow-authoring-rejected";
                    case CORRUPT_EVIDENCE -> "invalid-upstream-response";
                    case UNAVAILABLE -> "workflow-authoring-unavailable";
                    case DEADLINE -> "workflow-authoring-deadline";
                };
                throw new ActionException(code, "Workflow launch input operation did not complete");
            } catch (RuntimeException unexpected) {
                throw new ActionException("internal-error", "Workflow launch input operation failed");
            }
            validate(response, "invalid-upstream-response");
            boolean matches;
            if (prepare) {
                matches = ((PrepareWorkflowLaunchInputResponse) response).getAcceptance()
                        .equals(((PrepareWorkflowLaunchInputRequest) request).getAcceptance());
            } else {
                var contract = (GetWorkflowLaunchInputContractResponse) response;
                matches = contract.getAcceptance().equals(
                        ((GetWorkflowLaunchInputContractRequest) request).getAcceptance())
                        && contract.getDescriptors().getSha256().equals(
                                WorkRecords.sha256Hex(contract.getDescriptorSet().toByteArray()));
            }
            if (!matches) throw new ActionException("invalid-upstream-response",
                    "Workflow launch input result differs from the requested binding");
            return response;
        }
    }

    private static void validate(Message message, String code) throws ActionException {
        try {
            WorkflowPreparationValidation.validate(message, MAX_MESSAGE_BYTES);
        } catch (RuntimeException invalid) {
            throw new ActionException(code, "Workflow launch input contract is invalid");
        }
    }
}
