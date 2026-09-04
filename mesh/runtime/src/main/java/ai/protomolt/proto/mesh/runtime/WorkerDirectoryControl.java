package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.WorkerCapacity;
import ai.protomolt.proto.mesh.runtime.v1.WorkerDrainProgress;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHeartbeat;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;

/** Applies session-fenced worker liveness and capacity to the placement directory. */
public interface WorkerDirectoryControl {

    void heartbeat(WorkerHello hello, WorkerHeartbeat heartbeat);

    void capacity(WorkerHello hello, WorkerCapacity capacity);

    void beginDrain(WorkerHello hello, String reason);

    void drainProgress(WorkerHello hello, WorkerDrainProgress progress);

    static WorkerDirectoryControl none() {
        return NoOp.INSTANCE;
    }

    enum NoOp implements WorkerDirectoryControl {
        INSTANCE;

        @Override
        public void heartbeat(WorkerHello hello, WorkerHeartbeat heartbeat) {
        }

        @Override
        public void capacity(WorkerHello hello, WorkerCapacity capacity) {
        }

        @Override
        public void beginDrain(WorkerHello hello, String reason) {
        }

        @Override
        public void drainProgress(WorkerHello hello, WorkerDrainProgress progress) {
        }
    }
}
