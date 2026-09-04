package ai.protomolt.proto.mesh.runtime;

/** Defers upstream acknowledgement until the invocation's downstream work succeeds. */
public interface InvocationSettlement {

    /** Durable delivery id, empty for an in-process invocation. */
    String deliveryId();

    /** Commits successful downstream processing. Idempotent. */
    void settle();

    /** Releases an uncommitted delivery after downstream failure. Idempotent. */
    void release(String reason);

    /** A settlement that needs no durable acknowledgement. */
    static InvocationSettlement local() {
        return LocalSettlement.INSTANCE;
    }

    enum LocalSettlement implements InvocationSettlement {
        INSTANCE;

        @Override
        public String deliveryId() {
            return "";
        }

        @Override
        public void settle() {
        }

        @Override
        public void release(String reason) {
        }
    }
}
