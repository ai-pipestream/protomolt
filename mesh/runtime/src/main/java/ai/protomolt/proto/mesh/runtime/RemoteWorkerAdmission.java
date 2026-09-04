package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;

import java.util.Objects;

/** Admission policy for a demand-driven processor worker stream. */
@FunctionalInterface
public interface RemoteWorkerAdmission {

    Decision admit(WorkerHello hello);

    static RemoteWorkerAdmission allowAll() {
        return ignored -> new Decision(true, "admitted");
    }

    record Decision(boolean admitted, String reason) {
        public Decision {
            Objects.requireNonNull(reason, "reason");
        }
    }
}
