package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.workflow.RecordSigning;
import ai.protomolt.proto.workflow.authoring.FileSystemWorkflowPreparationRepository;
import ai.protomolt.proto.workflow.authoring.WorkflowCandidatePreparer;
import ai.protomolt.proto.workflow.authoring.WorkflowPreparationOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowSourceTemplateProvider;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/** Operator-selected preparation dependencies, resolved before opening listeners. */
final class WorkflowPreparationMount {
    private WorkflowPreparationMount() {}

    static Prepared prepare(ProtoMoltServe.WorkflowPreparationOptions options,
            WorkflowAuthoringMount.Prepared authoring) {
        return prepare(options, authoring, RecordSigning.fromEnvironment(),
                ServiceLoader.load(WorkflowSourceTemplateProvider.class));
    }

    /** Explicit dependencies keep provider selection and signing startup testable. */
    static Prepared prepare(ProtoMoltServe.WorkflowPreparationOptions options,
            WorkflowAuthoringMount.Prepared authoring, RecordSigning signing,
            Iterable<WorkflowSourceTemplateProvider> providers) {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(authoring, "authoring");
        Objects.requireNonNull(providers, "providers");
        if (signing == null) {
            throw new IllegalStateException("workflow preparation requires receipt signing");
        }
        WorkflowCandidatePreparer.SourceTemplate template = null;
        try {
            for (WorkflowSourceTemplateProvider provider : providers) {
                if (provider == null || provider.id() == null || provider.id().isBlank()) {
                    throw new IllegalStateException("invalid workflow source template provider");
                }
                if (provider.id().equals(options.templateProviderId())) {
                    if (template != null) {
                        throw new IllegalStateException("duplicate workflow source template provider");
                    }
                    template = Objects.requireNonNull(provider.template(),
                            "workflow source template must not be null");
                }
            }
        } catch (ServiceConfigurationError error) {
            throw new IllegalStateException("workflow source template providers are unavailable", error);
        }
        if (template == null) {
            throw new IllegalStateException("workflow source template provider is absent");
        }
        try {
            var ledger = new FileSystemWorkflowPreparationRepository(options.intentDirectory());
            return new Prepared(authoring, signing, template, ledger);
        } catch (IOException failure) {
            throw new IllegalStateException("workflow preparation storage is unavailable", failure);
        }
    }

    record Prepared(WorkflowAuthoringMount.Prepared authoring, RecordSigning signing,
            WorkflowCandidatePreparer.SourceTemplate template,
            FileSystemWorkflowPreparationRepository ledger) {
        WorkflowPreparationOperations operations(TranscriptRepository transcripts) {
            var preparer = new WorkflowCandidatePreparer(transcripts, ledger,
                    authoring.artifacts(), authoring.runs(), authoring.runner(),
                    authoring.actions(), authoring.policyReference(), authoring.trust(),
                    signing, Clock.systemUTC(), template);
            return preparer::prepare;
        }
    }
}
