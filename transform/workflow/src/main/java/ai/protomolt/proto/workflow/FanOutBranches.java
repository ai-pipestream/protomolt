package ai.protomolt.proto.workflow;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.workflow.WorkflowRunner.FailureKind;
import ai.protomolt.proto.workflow.WorkflowRunner.WorkflowExecutionException;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.Status;

import java.util.List;

/**
 * The deterministic parts of a fan-out step: resolving and bounding the items,
 * preparing every branch value before any invocation, the stable branch identity, and
 * the index-ordered collect. Branch execution, cancellation and the failure policy stay
 * in {@link WorkflowRunner}, which owns the threads and the observer ordering.
 */
final class FanOutBranches {

    private FanOutBranches() {
    }

    /** The verdict of one fan-out branch; exactly one of response/summary is null. */
    record BranchOutcome(Message response, String summary, FailureKind kind,
                         Status.Code grpcCode) {

        static BranchOutcome success(Message response) {
            return new BranchOutcome(response, null, null, null);
        }
    }

    /** A branch failure carrying its recorded outcome. */
    static final class BranchFailure extends Exception {
        private final BranchOutcome outcome;

        BranchFailure(BranchOutcome outcome) {
            super(outcome.summary());
            this.outcome = outcome;
        }

        BranchOutcome outcome() {
            return outcome;
        }
    }

    /**
     * Every item prepared before any branch runs: {@code slots[i]} holds the up-front
     * rejection of item i, or null when {@code values[i]} is ready to execute;
     * {@code allValid} is the step's validation evidence.
     */
    record Prepared(BranchOutcome[] slots, Message[] values, boolean allValid) {
    }

    /** The stable identity of branch {@code index} of {@code stepName}. */
    static String branchId(String stepName, int index) {
        return stepName + "#" + index;
    }

    /** The produced message's items, refused when the path is bad or over the cap. */
    static List<Message> items(CompiledWorkflow.Step step, DynamicMessage produced)
            throws WorkflowExecutionException {
        CompiledWorkflow.FanOutSpec fanOut = step.fanOut();
        List<Message> items;
        try {
            items = EdgeFlow.items(produced, fanOut.items());
        } catch (IllegalArgumentException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "fan-out items path rejected: " + e.getMessage(), e);
        }
        if (items.size() > fanOut.maxItems()) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "fan-out produced " + items.size() + " items, over the max_items "
                            + "cap of " + fanOut.maxItems(), null);
        }
        return items;
    }

    /**
     * Projects and validates every item up front, before any invocation: an invalid
     * item is a failed branch that never executes, and the all-items verdict is the
     * step's deterministic validation evidence.
     */
    static Prepared prepare(CompiledWorkflow.Step step, List<Message> items,
                            DescriptorRegistry registry, boolean validateContract) {
        CompiledWorkflow.EdgeSpec edge = step.edge();
        BranchOutcome[] slots = new BranchOutcome[items.size()];
        Message[] branchValues = new Message[items.size()];
        boolean allValid = true;
        for (int i = 0; i < items.size(); i++) {
            Message value = items.get(i);
            if (edge.projectTo() != null) {
                try {
                    value = StepMessages.projectValue(step, edge.projectTo(), value, registry);
                } catch (WorkflowExecutionException e) {
                    slots[i] = new BranchOutcome(null, e.getMessage(), FailureKind.EDGE,
                            null);
                    allValid = false;
                    continue;
                }
            }
            if (edge.validate() || validateContract) {
                ValidationResult result = ProtoValidator
                        .forMessageType(value.getDescriptorForType()).validate(value);
                if (!result.valid()) {
                    slots[i] = new BranchOutcome(null,
                            "branch value failed validation: " + StepMessages.summary(result),
                            FailureKind.VALIDATION, null);
                    allValid = false;
                    continue;
                }
            }
            branchValues[i] = value;
        }
        return new Prepared(slots, branchValues, allValid);
    }

    /** The successful outputs, in index order, collected into the step's collect type. */
    static DynamicMessage collect(CompiledWorkflow.Step step, List<Message> outputs)
            throws WorkflowExecutionException {
        CompiledWorkflow.FanOutSpec fanOut = step.fanOut();
        try {
            return EdgeFlow.collect(fanOut.collectType(), fanOut.collectInto(), outputs);
        } catch (IllegalArgumentException e) {
            throw new WorkflowExecutionException(step.name(), FailureKind.EDGE, null,
                    "fan-out collect rejected: " + e.getMessage(), e);
        }
    }
}
