package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.TaskSpec;

import java.util.List;
import java.util.Objects;

/** Reviews a structurally valid completion candidate against its task. */
@FunctionalInterface
public interface CandidateReviewer {

    /**
     * Reviews one candidate after protocol and acceptance-evidence checks pass.
     *
     * @param context task, worker, and candidate data
     * @return accept, revise, or leave pending for an external reviewer
     * @throws Exception when review infrastructure fails
     */
    ReviewDecision review(ReviewContext context) throws Exception;

    /** Leaves every candidate pending for explicit coordinator review. */
    static CandidateReviewer manual() {
        return context -> ReviewDecision.pending();
    }

    /** Accepts every candidate that passes the protocol checks. */
    static CandidateReviewer acceptAll() {
        return context -> ReviewDecision.accept("acceptance checks verified");
    }

    /** Immutable input to a candidate review. */
    record ReviewContext(String taskId, String workerId, TaskSpec spec,
                         CompletionCandidate candidate) {
        public ReviewContext {
            Objects.requireNonNull(taskId, "taskId");
            Objects.requireNonNull(workerId, "workerId");
            Objects.requireNonNull(spec, "spec");
            Objects.requireNonNull(candidate, "candidate");
        }
    }

    /** A review verdict emitted by the coordinator. */
    sealed interface ReviewDecision permits ReviewDecision.Accept,
            ReviewDecision.Revise, ReviewDecision.Pending {

        /** Accepts the open candidate. */
        static ReviewDecision accept(String verdict) {
            return new Accept(verdict);
        }

        /** Returns the open candidate for another revision. */
        static ReviewDecision revise(String feedback, List<String> failedChecks) {
            return new Revise(feedback, failedChecks);
        }

        /** Leaves the candidate open for an external review action. */
        static ReviewDecision pending() {
            return Pending.INSTANCE;
        }

        /** Acceptance verdict. */
        record Accept(String verdict) implements ReviewDecision {
            public Accept {
                Objects.requireNonNull(verdict, "verdict");
                if (verdict.isBlank()) {
                    throw new IllegalArgumentException("acceptance verdict must not be blank");
                }
            }
        }

        /** Revision verdict. */
        record Revise(String feedback, List<String> failedChecks)
                implements ReviewDecision {
            public Revise {
                Objects.requireNonNull(feedback, "feedback");
                failedChecks = List.copyOf(failedChecks);
                if (feedback.isBlank()) {
                    throw new IllegalArgumentException("revision feedback must not be blank");
                }
            }
        }

        /** External review has not decided yet. */
        enum Pending implements ReviewDecision {
            INSTANCE
        }
    }
}
