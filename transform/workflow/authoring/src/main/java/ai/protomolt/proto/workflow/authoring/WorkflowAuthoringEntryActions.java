package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.DeliverableContracts;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Objects;

/** Coordinator-scoped entry shared by gRPC, catalog adapters and the console. */
public final class WorkflowAuthoringEntryActions {
    private WorkflowAuthoringEntryActions() {}

    public static ActionCatalog register(ActionCatalog catalog, WorkflowAuthoringEntryOperations operations) {
        Objects.requireNonNull(operations);
        return Objects.requireNonNull(catalog).register(new EntryAction(operations, false))
                .register(new EntryAction(operations, true));
    }

    private record EntryAction(WorkflowAuthoringEntryOperations operations, boolean start) implements ProtoAction {
        @Override public String name() {
            return start ? "start-workflow-authoring" : "get-workflow-authoring-template";
        }
        @Override public String description() {
            return start ? "Start or recover the original configured authoring task offer."
                    : "Read the configured workflow authoring contract and policy identity.";
        }
        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }
        @Override public Descriptor requestType() {
            return start ? StartWorkflowAuthoringRequest.getDescriptor()
                    : GetWorkflowAuthoringTemplateRequest.getDescriptor();
        }
        @Override public Descriptor responseType() {
            return start ? StartWorkflowAuthoringResponse.getDescriptor()
                    : GetWorkflowAuthoringTemplateResponse.getDescriptor();
        }
        @Override public Message execute(Message input, ActionContext context) throws ActionException {
            Message request = start ? CatalogContract.as(input, StartWorkflowAuthoringRequest.getDefaultInstance(), name())
                    : CatalogContract.as(input, GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), name());
            validate(request, "invalid-input");
            Message response;
            try {
                response = start ? operations.start((StartWorkflowAuthoringRequest) request)
                        : operations.template((GetWorkflowAuthoringTemplateRequest) request);
            } catch (WorkflowAuthoringEntryException failure) {
                String code = switch (failure.kind()) {
                    case INVALID_INPUT -> "invalid-input";
                    case CONFLICT -> "workflow-authoring-conflict";
                    case INACTIVE -> "workflow-authoring-rejected";
                    case CORRUPT_EVIDENCE -> "invalid-upstream-response";
                    case UNAVAILABLE -> "workflow-authoring-unavailable";
                    case DEADLINE -> "workflow-authoring-deadline";
                };
                throw new ActionException(code, "Workflow authoring entry did not complete");
            } catch (RuntimeException unexpected) {
                throw new ActionException("internal-error", "Workflow authoring entry failed");
            }
            validate(response, "invalid-upstream-response");
            try {
                if (start) {
                    var result = (StartWorkflowAuthoringResponse) response;
                    if (!result.getRequest().equals(request)
                            || !result.getOffer().getStartBindingSha256().equals(WorkflowAuthoringStartBinding.sha256(
                                    result.getRequest(), result.getOffer().getSpec(), result.getOffer().getLeaseDuration()))) {
                        throw new IllegalArgumentException("start response binding differs");
                    }
                } else {
                    var result = (GetWorkflowAuthoringTemplateResponse) response;
                    var spec = result.getTemplate().getSpec();
                    if (!result.getTemplateSha256().equals(WorkflowAuthoringStartBinding.templateSha256(result.getTemplate()))
                            || !spec.getContract().getTypeName().equals(WorkflowAuthoringDeliverable.getDescriptor().getFullName())) {
                        throw new IllegalArgumentException("template identity differs");
                    }
                    var rendered = DeliverableContracts.rendered(spec.toBuilder().setContract(
                            spec.getContract().toBuilder().clearJsonSchema()).build());
                    if (!rendered.equals(spec)) throw new IllegalArgumentException("template schema differs");
                }
            } catch (RuntimeException invalidResponse) {
                throw new ActionException("invalid-upstream-response", "Workflow authoring entry response has an invalid binding");
            }
            return response;
        }
    }

    private static void validate(Message message, String code) throws ActionException {
        try {
            WorkflowLaunchValidation.validate(message);
        } catch (RuntimeException invalid) {
            throw new ActionException(code, "Workflow authoring entry contract is invalid");
        }
    }
}
