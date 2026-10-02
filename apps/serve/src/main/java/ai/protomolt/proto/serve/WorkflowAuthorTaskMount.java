package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.lifecycle.TranscriptRepository;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthorTaskMutations;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthorTaskOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthorTaskReader;
import ai.protomolt.proto.workflow.authoring.WorkflowPreparationException;
import ai.protomolt.proto.workflow.authoring.v1.*;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.time.Clock;
import java.util.Map;

/** Complete, explicitly bound author task service over the serve process's existing state. */
final class WorkflowAuthorTaskMount {
    private WorkflowAuthorTaskMount() {}

    static ServiceDescriptor service() {
        return WorkflowAuthorTaskServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowAuthorTaskService");
    }

    static Map<String, Map<String, String>> bindings() {
        return Map.of(service().getFullName(), Map.of(
                "RegisterWorkflowAuthor", "register-workflow-author",
                "EnsureWorkflowAuthorRegistration", "ensure-workflow-author-registration",
                "AcceptWorkflowTask", "accept-workflow-task",
                "SubmitWorkflowCandidate", "submit-workflow-candidate",
                "GetWorkflowAuthorContext", "get-workflow-author-context",
                "ReadWorkflowAuthorEvents", "read-workflow-author-events",
                "ReadWorkflowAuthorAssignments", "read-workflow-author-assignments"));
    }

    static WorkflowAuthorTaskOperations operations(DelegationBridge bridge,
            TranscriptRepository transcripts, WorkflowAuthoringMount.Prepared authoring,
            Clock clock) {
        var reader = new WorkflowAuthorTaskReader(transcripts, authoring.artifacts(),
                authoring.policyReference(), clock);
        var mutations = new WorkflowAuthorTaskMutations(bridge, authoring.artifacts(),
                authoring.policyReference(), clock);
        return new WorkflowAuthorTaskOperations() {
            @Override public RegisterWorkerResponse register(RegisterWorkerRequest request, Caller caller)
                    throws WorkflowPreparationException { return mutations.register(request, caller); }
            @Override public EnsureWorkflowAuthorRegistrationResponse ensureRegistration(EnsureWorkflowAuthorRegistrationRequest request, Caller caller)
                    throws WorkflowPreparationException { return mutations.ensureRegistration(request, caller); }
            @Override public AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller)
                    throws WorkflowPreparationException { return mutations.accept(request, caller); }
            @Override public SubmitCandidateResponse submit(SubmitCandidateRequest request, Caller caller)
                    throws WorkflowPreparationException { return mutations.submit(request, caller); }
            @Override public GetWorkflowAuthorContextResponse context(GetWorkflowAuthorContextRequest request, Caller caller)
                    throws WorkflowPreparationException { return reader.context(request, caller); }
            @Override public ReadWorkflowAuthorEventsResponse events(ReadWorkflowAuthorEventsRequest request, Caller caller)
                    throws WorkflowPreparationException { return reader.events(request, caller); }
            @Override public ReadWorkflowAuthorAssignmentsResponse assignments(
                    ReadWorkflowAuthorAssignmentsRequest request, Caller caller)
                    throws WorkflowPreparationException { return reader.assignments(request, caller); }
        };
    }
}
