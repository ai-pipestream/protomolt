package ai.protomolt.proto.repo.admission;

/** Host-supplied serialized-byte reservations; independent of storage and transport. */
@FunctionalInterface
public interface DocumentAdmissionReservations {
    /**
     * Reserve before allocation, without waiting while holding another lease.
     * Refusal must throw; it must never return null or an unreserved success.
     * This does not account for parsed descriptors, builders or JVM object overhead.
     */
    Lease reserve(long bytes);

    /** Close after the last consumer releases the bytes. Close must be idempotent and nonthrowing. */
    @FunctionalInterface
    interface Lease extends AutoCloseable {
        @Override void close();
    }
}
