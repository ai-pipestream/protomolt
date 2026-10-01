package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureServiceGrpc;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import java.util.List;

/** The example's auditable mapping restriction, applied after generic policy preflight. */
public final class AuthoringStarterTemplate {
    private AuthoringStarterTemplate() {}

    /**
     * Keeps the input operation identity for future job inputs, not just test fixtures.
     * This deliberately accepts one source shape; it is not a general CEL equivalence check.
     * Descriptor bytes and service targets remain the trusted policy preflight's responsibility.
     */
    public static void verify(CompiledWorkflow workflow) {
        if (!workflow.validateContract() || workflow.output() != null
                || !workflow.inputType().getFullName().equals(WriteRecordRequest.getDescriptor().getFullName())
                || workflow.steps().size() != 2) {
            throw rejected();
        }
        requireStep(workflow.steps().get(0), "normalize", "NormalizeText",
                List.of("text = input.content"));
        requireStep(workflow.steps().get(1), "write", "WriteRecord",
                List.of("operation_id = input.operation_id", "content = normalize.text"));
    }

    private static void requireStep(CompiledWorkflow.Step step, String name, String method,
            List<String> rules) {
        if (!step.name().equals(name) || step.method() == null
                || !step.method().getFullName().equals(AuthoringFixtureServiceGrpc.SERVICE_NAME + "." + method)
                || step.structured() != null || step.edge() != null || step.fanOut() != null
                || step.external() || !step.completion().isEmpty()
                || step.when() != null && !step.when().isEmpty()
                || !step.celRules().isEmpty() || !step.rules().equals(rules)) {
            throw rejected();
        }
    }

    private static IllegalArgumentException rejected() {
        return new IllegalArgumentException("starter requires normalize then write with the input operation identity");
    }
}
