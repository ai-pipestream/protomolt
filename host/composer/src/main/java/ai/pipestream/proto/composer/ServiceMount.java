package ai.pipestream.proto.composer;

/**
 * A wired but not yet serving role. {@link #start()} is phase two of the
 * module lifecycle: network binds, worker claims, background loops. The
 * composer starts mounts in dependency order and closes them in reverse;
 * {@link #close()} must be idempotent and must stop everything
 * {@code start()} began even when {@code start()} was never called.
 */
public interface ServiceMount extends AutoCloseable {

    /**
     * Begin serving. Called once, after every selected module has wired.
     *
     * @throws Exception startup failures propagate; the composer then
     *         closes every already-started mount in reverse order
     */
    void start() throws Exception;

    /** A mount with nothing to do in phase two. */
    static ServiceMount inert(AutoCloseable resource) {
        return new ServiceMount() {
            @Override
            public void start() {
            }

            @Override
            public void close() throws Exception {
                resource.close();
            }
        };
    }
}
