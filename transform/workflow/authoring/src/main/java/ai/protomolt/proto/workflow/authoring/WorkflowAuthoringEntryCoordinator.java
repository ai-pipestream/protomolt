package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.TaskStartAdmissionException;
import ai.protomolt.proto.delegation.TaskStartConflictException;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/** The mounted policy selects all authoring authority; a caller supplies only task guidance. */
public final class WorkflowAuthoringEntryCoordinator implements WorkflowAuthoringEntryOperations {
    private final DelegationBridge bridge;
    private final WorkflowAuthoringTemplates templates;

    public WorkflowAuthoringEntryCoordinator(DelegationBridge bridge, ArtifactReference policyReference,
            ArtifactRepository artifacts, int leaseSeconds) {
        this.bridge = Objects.requireNonNull(bridge);
        this.templates = new WorkflowAuthoringTemplates(policyReference, artifacts, leaseSeconds);
    }

    @Override
    public GetWorkflowAuthoringTemplateResponse template(GetWorkflowAuthoringTemplateRequest request) {
        validateRequest(request);
        var template = templates.load();
        var response = GetWorkflowAuthoringTemplateResponse.newBuilder().setTemplate(template)
                .setTemplateSha256(WorkflowAuthoringStartBinding.templateSha256(template)).build();
        return checked(response);
    }

    @Override
    public StartWorkflowAuthoringResponse start(StartWorkflowAuthoringRequest request) {
        validateRequest(request);
        TaskOffer committed;
        try {
            committed = bridge.offerOnce(request.getWorkerId(), request.getTaskId(),
                    offer -> matches(request, offer), () -> initial(request));
        } catch (WorkflowAuthoringEntryException classified) {
            throw classified;
        } catch (TaskStartConflictException conflict) {
            throw failure(WorkflowAuthoringEntryException.Kind.CONFLICT,
                    "task UUID belongs to a different start", conflict);
        } catch (TaskStartAdmissionException inactive) {
            throw failure(WorkflowAuthoringEntryException.Kind.INACTIVE,
                    "selected worker is not currently available", inactive);
        } catch (RuntimeException unavailable) {
            throw failure(deadline(unavailable) ? WorkflowAuthoringEntryException.Kind.DEADLINE
                    : WorkflowAuthoringEntryException.Kind.UNAVAILABLE,
                    "authoring task could not be started", unavailable);
        }
        if (!matches(request, committed)) {
            throw failure(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE,
                    "committed authoring offer has an invalid binding", null);
        }
        return checked(StartWorkflowAuthoringResponse.newBuilder()
                .setRequest(request).setOffer(committed).build());
    }

    private InProcessDelegationCoordinator.InitialOffer initial(StartWorkflowAuthoringRequest request) {
        var current = templates.load();
        if (!WorkflowAuthoringStartBinding.templateSha256(current).equals(request.getTemplateSha256())) {
            throw failure(WorkflowAuthoringEntryException.Kind.INACTIVE,
                    "configured authoring template has changed", null);
        }
        var spec = current.getSpec().toBuilder().setObjective(request.getObjective()).build();
        var lease = com.google.protobuf.Duration.newBuilder().setSeconds(current.getLeaseSeconds()).build();
        String binding;
        try {
            binding = WorkflowAuthoringStartBinding.sha256(request, spec, lease);
        } catch (RuntimeException invalid) {
            throw failure(WorkflowAuthoringEntryException.Kind.INVALID_INPUT,
                    "authoring objective cannot form a valid offer", invalid);
        }
        return new InProcessDelegationCoordinator.InitialOffer(spec,
                java.time.Duration.ofSeconds(current.getLeaseSeconds()), binding);
    }

    private static boolean matches(StartWorkflowAuthoringRequest request, TaskOffer offer) {
        try {
            WorkflowLaunchValidation.validate(offer);
        } catch (RuntimeException corrupt) {
            throw failure(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE,
                    "stored authoring offer is invalid", corrupt);
        }
        try {
            return offer.getAttempt() == 1 && !offer.hasResumeFrom()
                    && offer.getStartBindingSha256().equals(
                            WorkflowAuthoringStartBinding.sha256(request,
                                    offer.getSpec(), offer.getLeaseDuration()));
        } catch (RuntimeException invalid) {
            return false;
        }
    }

    private static void validateRequest(com.google.protobuf.Message message) {
        try {
            WorkflowLaunchValidation.validate(message);
        } catch (RuntimeException invalid) {
            throw failure(WorkflowAuthoringEntryException.Kind.INVALID_INPUT,
                    "authoring entry request is invalid", invalid);
        }
    }

    private static <T extends com.google.protobuf.Message> T checked(T response) {
        try {
            WorkflowLaunchValidation.validate(response);
            return response;
        } catch (RuntimeException invalid) {
            throw failure(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE,
                    "authoring entry response is invalid", invalid);
        }
    }

    static boolean deadline(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof StatusRuntimeException status
                    && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
            if (current instanceof StatusException status
                    && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
            if (current instanceof java.io.InterruptedIOException || current instanceof TimeoutException) return true;
        }
        return false;
    }

    private static WorkflowAuthoringEntryException failure(WorkflowAuthoringEntryException.Kind kind,
            String message, Throwable cause) {
        return new WorkflowAuthoringEntryException(kind, message, cause);
    }
}
