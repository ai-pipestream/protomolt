package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.FlowExecutionCheckpoint;
import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;

/** Durable checkpoint and cancellation seam used by the resumable runtime. */
interface FlowRunControl {

    void checkpoint(FlowHistory history, FlowExecutionCheckpoint checkpoint);

    Cancellation cancellation();

    default boolean suspending() {
        return false;
    }

    record Cancellation(boolean requested, String reason, FlowHistory durableHistory) {
        static Cancellation none() {
            return new Cancellation(false, "", FlowHistory.getDefaultInstance());
        }
    }

    static FlowRunControl none() {
        return new FlowRunControl() {
            @Override
            public void checkpoint(FlowHistory history, FlowExecutionCheckpoint checkpoint) {
            }

            @Override
            public Cancellation cancellation() {
                return Cancellation.none();
            }
        };
    }
}
