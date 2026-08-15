package ai.pipestream.proto.composer;

import io.grpc.ManagedChannel;

/**
 * The monolith/distributed pivot. A module reaches another role with
 * {@link #to(String)} and never knows the topology: when the role is
 * co-mounted on this node it gets the in-process channel the role
 * published during wiring; when the role runs elsewhere it gets a channel
 * to the configured {@code PROTOMOLT_<ROLE>_TARGET}; when neither exists
 * the call fails loudly naming the role and the environment variable.
 *
 * <p>Channels handed out here are owned by the node and closed at
 * shutdown; modules must not shut them down.
 */
public interface Channels {

    /**
     * Publishes this node's in-process endpoint for a role. Called by the
     * role's own module during {@link ServiceModule#wire}; later-wired
     * modules then resolve it via {@link #to}.
     *
     * @param role the publishing module's role name
     * @param inProcessName the started in-process server's name
     */
    void publishInProcess(String role, String inProcessName);

    /**
     * A channel to the given role, co-mounted or remote.
     *
     * @throws ComposerException when the role is neither co-mounted nor
     *         configured with a remote target
     */
    ManagedChannel to(String role);

    /** Whether the role is co-mounted on this node. */
    boolean isLocal(String role);

    /** The environment variable naming a role's remote target. */
    static String targetVariable(String role) {
        return "PROTOMOLT_" + role.toUpperCase(java.util.Locale.ROOT).replace('-', '_') + "_TARGET";
    }
}
