package ai.pipestream.proto.composer;

import java.util.Map;

/**
 * The shared spine every {@link ServiceModule} wires against: environment,
 * node identity, the channel pivot, and the contribution registry. One
 * instance per boot, owned by the {@link Composer}.
 */
public interface NodeContext {

    /**
     * The configuration environment. Production passes
     * {@link System#getenv()}; tests inject a map. Modules keep their own
     * config-record parsing (the composer adds no config framework).
     */
    Map<String, String> environment();

    /**
     * A short identifier unique to this boot, used to suffix in-process
     * server names so two nodes in one JVM (tests) never collide.
     */
    String nodeId();

    /** The channel pivot between co-mounted and remote roles. */
    Channels channels();

    /** Cross-module contributions collected during the wire phase. */
    Contributions contributions();

    /**
     * Registers an extra resource to close during node shutdown, in
     * reverse registration order, interleaved with the mounts closed
     * after it was registered. For resources a module owns beyond its
     * mount (an extra in-process server, a shared client).
     */
    void onClose(AutoCloseable resource);
}
