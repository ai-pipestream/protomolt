package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.util.Objects;

/** Optional authenticated task adapters. Registration does not mount a service. */
public final class WorkflowAuthorTaskActions {
    private static final int MAX_BYTES = 16 * 1024 * 1024;
    private static final JsonFormat.TypeRegistry TYPES = JsonFormat.TypeRegistry.newBuilder()
            .add(WorkflowAuthoringDeliverable.getDescriptor()).build();

    private WorkflowAuthorTaskActions() {}

    public static ActionCatalog register(ActionCatalog catalog, WorkflowAuthorTaskOperations operations) {
        Objects.requireNonNull(catalog);
        Objects.requireNonNull(operations);
        for (Operation operation : Operation.values()) catalog.register(new Action(operation, operations));
        return catalog;
    }

    private enum Operation {
        REGISTER("register-workflow-author", RegisterWorkerRequest.getDefaultInstance(), RegisterWorkerResponse.getDefaultInstance()),
        ACCEPT("accept-workflow-task", AcceptTaskRequest.getDefaultInstance(), AcceptTaskResponse.getDefaultInstance()),
        SUBMIT("submit-workflow-candidate", SubmitCandidateRequest.getDefaultInstance(), SubmitCandidateResponse.getDefaultInstance()),
        CONTEXT("get-workflow-author-context", GetWorkflowAuthorContextRequest.getDefaultInstance(), GetWorkflowAuthorContextResponse.getDefaultInstance()),
        EVENTS("read-workflow-author-events", ReadWorkflowAuthorEventsRequest.getDefaultInstance(), ReadWorkflowAuthorEventsResponse.getDefaultInstance());

        final String name;
        final Message request;
        final Message response;
        Operation(String name, Message request, Message response) {
            this.name = name;
            this.request = request;
            this.response = response;
        }
    }

    private record Action(Operation operation, WorkflowAuthorTaskOperations backend) implements ProtoAction {
        @Override public String name() { return operation.name; }
        @Override public String description() { return "Access the authenticated author's assigned workflow task."; }
        @Override public String requiredScope() { return Scopes.WORKFLOW_AUTHOR; }
        @Override public Descriptor requestType() { return operation.request.getDescriptorForType(); }
        @Override public Descriptor responseType() { return operation.response.getDescriptorForType(); }
        @Override public JsonFormat.TypeRegistry typeRegistry(ObjectNode input) { return TYPES; }

        @Override public Message execute(Message input, ActionContext context) throws ActionException {
            throw new ActionException("permission-denied", "Workflow author tasks require caller identity");
        }

        @Override public Message execute(Message input, ActionContext context, Caller caller) throws ActionException {
            if (caller == null || !caller.holds(Scopes.WORKFLOW_AUTHOR)) {
                throw new ActionException("permission-denied", "Workflow author scope is required");
            }
            Message request = CatalogContract.as(input, operation.request, name());
            try {
                WorkflowPreparationValidation.validate(request, MAX_BYTES);
                if (request instanceof SubmitCandidateRequest submission) {
                    validateDeliverable(submission.getCandidate().getResult());
                }
            } catch (Exception invalid) {
                throw new ActionException("invalid-input", "Invalid workflow author task request");
            }
            String worker = switch (request) {
                case RegisterWorkerRequest value -> value.getWorkerId();
                case AcceptTaskRequest value -> value.getWorkerId();
                case SubmitCandidateRequest value -> value.getWorkerId();
                default -> caller.name();
            };
            if (!worker.equals(caller.name())) {
                throw new ActionException("permission-denied", "Worker identity differs from caller");
            }
            Message response;
            try {
                response = switch (request) {
                    case RegisterWorkerRequest value -> backend.register(value, caller);
                    case AcceptTaskRequest value -> backend.accept(value, caller);
                    case SubmitCandidateRequest value -> backend.submit(value, caller);
                    case GetWorkflowAuthorContextRequest value -> backend.context(value, caller);
                    case ReadWorkflowAuthorEventsRequest value -> backend.events(value, caller);
                    default -> throw new IllegalArgumentException("Unsupported task operation");
                };
            } catch (WorkflowPreparationException failure) {
                String code = switch (failure.kind()) {
                    case INVALID_INPUT -> "invalid-input";
                    case PERMISSION_DENIED -> "permission-denied";
                    case INACTIVE -> "workflow-authoring-rejected";
                    case CONFLICT -> "workflow-authoring-conflict";
                    case CORRUPT_EVIDENCE -> "workflow-authoring-storage-failed";
                    case INVALID_UPSTREAM -> "invalid-upstream-response";
                    case UNAVAILABLE -> "workflow-authoring-unavailable";
                    case DEADLINE -> "workflow-authoring-deadline";
                    case TERMINAL_FAILED -> "internal-error";
                };
                throw new ActionException(code, "Workflow author task operation failed");
            } catch (RuntimeException failure) {
                throw new ActionException("internal-error", "Workflow author task operation failed");
            }
            try {
                WorkflowPreparationValidation.validate(response, MAX_BYTES);
                verifyResponse(request, response, caller);
            } catch (Exception invalid) {
                throw new ActionException("invalid-upstream-response", "Invalid workflow author task response");
            }
            return response;
        }
    }

    private static void verifyResponse(Message request, Message response, Caller caller) throws Exception {
        switch (request) {
            case RegisterWorkerRequest value -> {
                var result = (RegisterWorkerResponse) response;
                require(result.getOk() && result.getWorkerId().equals(value.getWorkerId()));
                require(result.getAdmitted() ? !result.getSessionId().isBlank() && result.getReason().isEmpty()
                        : result.getSessionId().isEmpty() && !result.getReason().isBlank());
                WorkflowPreparationValidation.validate(AdmissionDecision.newBuilder()
                        .setAdmitted(result.getAdmitted()).setSessionId(result.getSessionId())
                        .setReason(result.getReason()).build(), MAX_BYTES);
            }
            case AcceptTaskRequest value -> {
                var result = (AcceptTaskResponse) response;
                require(result.getOk() && result.getTaskId().equals(value.getTaskId())
                        && result.getAttempt() == value.getAttempt());
            }
            case SubmitCandidateRequest value -> {
                var result = (SubmitCandidateResponse) response;
                require(result.getOk() && result.getTaskId().equals(value.getTaskId())
                        && result.getAttempt() == value.getCandidate().getAttempt()
                        && result.getRevision() == value.getCandidate().getRevision());
            }
            case GetWorkflowAuthorContextRequest value -> {
                var result = (GetWorkflowAuthorContextResponse) response;
                require(result.getTaskId().equals(value.getTaskId()) && result.getAttempt() == value.getAttempt()
                        && result.getOfferEntry().getWorkerId().equals(caller.name()));
            }
            case ReadWorkflowAuthorEventsRequest value -> {
                var result = (ReadWorkflowAuthorEventsResponse) response;
                require(result.getTaskId().equals(value.getTaskId()) && result.getAttempt() == value.getAttempt()
                        && result.getAfterCursor() == value.getAfterCursor()
                        && result.getEventsCount() <= value.getMaxEvents());
                long previous = value.getAfterCursor();
                for (ObservedEvent event : result.getEventsList()) {
                    require(event.getWorkerId().equals(caller.name()) && event.hasEntry()
                            && event.getEntry().getWorkerId().equals(caller.name()) && event.getCursor() > previous
                            && event.getLane() == event.getEntry().getLane());
                    String task = event.getEntry().hasWorkerFrame()
                            ? event.getEntry().getWorkerFrame().getTaskId()
                            : event.getEntry().getCoordinatorFrame().getTaskId();
                    require(task.equals(value.getTaskId()) && attempt(event.getEntry()) == value.getAttempt());
                    previous = event.getCursor();
                    if (event.getEntry().hasWorkerFrame() && event.getEntry().getWorkerFrame().hasCompletion()) {
                        validateDeliverable(event.getEntry().getWorkerFrame().getCompletion().getResult());
                    }
                }
            }
            default -> throw new IllegalArgumentException("Unsupported request");
        }
    }

    private static void validateDeliverable(Any result) throws Exception {
        require(result.is(WorkflowAuthoringDeliverable.class));
        WorkflowPreparationValidation.validate(result.unpack(WorkflowAuthoringDeliverable.class), MAX_BYTES);
    }

    private static int attempt(TranscriptEntry entry) {
        if (entry.hasWorkerFrame()) {
            var frame = entry.getWorkerFrame();
            return switch (frame.getPayloadCase()) {
                case ACCEPT -> frame.getAccept().getAttempt();
                case REJECT -> frame.getReject().getAttempt();
                case HEARTBEAT -> frame.getHeartbeat().getAttempt();
                case PROGRESS -> frame.getProgress().getAttempt();
                case CHECKPOINT -> frame.getCheckpoint().getAttempt();
                case BLOCKED -> frame.getBlocked().getAttempt();
                case FAILED -> frame.getFailed().getAttempt();
                case CANCELLED -> frame.getCancelled().getAttempt();
                case COMPLETION -> frame.getCompletion().getAttempt();
                default -> 0;
            };
        }
        var frame = entry.getCoordinatorFrame();
        return switch (frame.getPayloadCase()) {
            case OFFER -> frame.getOffer().getAttempt();
            case RENEWAL -> frame.getRenewal().getAttempt();
            case EXPIRED -> frame.getExpired().getAttempt();
            case CANCELLATION -> frame.getCancellation().getAttempt();
            case REVISION_REQUESTED -> frame.getRevisionRequested().getAttempt();
            case ACCEPTED -> frame.getAccepted().getAttempt();
            default -> 0;
        };
    }

    private static void require(boolean condition) {
        if (!condition) throw new IllegalArgumentException("Response binding differs");
    }
}
