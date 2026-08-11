package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.WorkerHello;

import java.util.Objects;

/** Decides whether a worker hello may open a delegation session. */
@FunctionalInterface
public interface AdmissionPolicy {

    /**
     * Reviews one worker hello.
     *
     * @param hello validated worker metadata
     * @return the admission decision
     */
    Decision admit(WorkerHello hello);

    /** Admits every structurally valid worker. */
    static AdmissionPolicy allowAll() {
        return hello -> Decision.admit();
    }

    /** One admission outcome. */
    record Decision(boolean admitted, String reason) {

        /** Creates an admitted outcome. */
        public static Decision admit() {
            return new Decision(true, "");
        }

        /** Creates a rejected outcome with a bounded explanation. */
        public static Decision reject(String reason) {
            return new Decision(false, Objects.requireNonNull(reason, "reason"));
        }

        public Decision {
            Objects.requireNonNull(reason, "reason");
            if (!admitted && reason.isBlank()) {
                throw new IllegalArgumentException("a rejected worker needs a reason");
            }
        }
    }
}
