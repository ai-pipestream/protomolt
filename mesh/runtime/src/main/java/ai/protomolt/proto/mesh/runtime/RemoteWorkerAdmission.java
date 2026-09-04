package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;

import java.time.Duration;
import java.util.Objects;

/** Admission policy for a demand-driven processor worker stream. */
@FunctionalInterface
public interface RemoteWorkerAdmission {

    Decision admit(WorkerHello hello);

    static RemoteWorkerAdmission allowAll() {
        return ignored -> new Decision(true, "admitted");
    }

    record Decision(
            boolean admitted,
            String reason,
            long directoryGeneration,
            long directoryEventSequence,
            Duration reconnectGrace) {
        public Decision(boolean admitted, String reason) {
            this(admitted, reason, 0, 0, Duration.ZERO);
        }

        public Decision(
                boolean admitted,
                String reason,
                long directoryGeneration,
                long directoryEventSequence) {
            this(admitted, reason, directoryGeneration, directoryEventSequence,
                    Duration.ZERO);
        }

        public Decision {
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(reconnectGrace, "reconnectGrace");
            if (reconnectGrace.isNegative()) {
                throw new IllegalArgumentException("reconnectGrace must not be negative");
            }
        }
    }
}
