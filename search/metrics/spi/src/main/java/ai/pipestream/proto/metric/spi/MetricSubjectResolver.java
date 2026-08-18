package ai.pipestream.proto.metric.spi;

/**
 * Resolves metric subjects that are not part of the mount's boot-static
 * set — rollup tables, which appear after boot by design. The door
 * consults a resolver only after its static subjects miss, and a
 * {@code null} resolution falls through to the door's unknown-subject
 * refusal, so a resolver never widens what a mount serves silently: what
 * it resolves is self-describing state the mount already owns (a lake
 * table carrying its own declaration), not new configuration.
 */
public interface MetricSubjectResolver {

    /**
     * Resolves one subject name, or returns {@code null} when this
     * resolver does not know it.
     *
     * @param subject the requested mapping subject
     * @return the resolved mapping and the engine that answers it, or
     *         {@code null}
     */
    Resolved resolve(String subject);

    /**
     * One resolved subject.
     *
     * @param mapping the subject's built metric mapping
     * @param executor the engine that answers it
     */
    record Resolved(MetricMapping mapping, MetricExecutor executor) {

        public Resolved {
            if (mapping == null) {
                throw new IllegalArgumentException("mapping must not be null");
            }
            if (executor == null) {
                throw new IllegalArgumentException("executor must not be null");
            }
        }
    }
}
