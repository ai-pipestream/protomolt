package ai.protomolt.proto.composer;

import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
     * The target-string prefix naming an in-process endpoint, shared with
     * every service config that accepts {@code inprocess:<name>} targets.
     */
    String IN_PROCESS_PREFIX = "inprocess:";

    /**
     * Role spellings accepted as aliases, mapped to the canonical id.
     * Canonical ids are singular and share the stem of the module's own
     * tree; an operator naming an alias reaches the same role, in a role
     * list or in a {@code PROTOMOLT_<ROLE>_TARGET} variable.
     */
    Map<String, String> ROLE_ALIASES = Map.of(
            "metrics", "metric",
            "parser-text", "parse-text");

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
     * The role's endpoint as a target string, for services that manage
     * their own channels: {@code inprocess:<name>} when co-mounted, the
     * validated {@code PROTOMOLT_<ROLE>_TARGET} value when remote.
     *
     * @throws ComposerException when the role is neither co-mounted nor
     *         configured with a remote target
     */
    String targetOf(String role);

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
        return "PROTOMOLT_" + role.toUpperCase(Locale.ROOT).replace('-', '_') + "_TARGET";
    }

    /**
     * The environment variables a role's remote target is read from, in
     * read order: the canonical {@link #targetVariable(String)} first,
     * then the variable of every {@linkplain #ROLE_ALIASES alias} of the
     * same role. Naming both, the canonical value wins.
     */
    static List<String> targetVariables(String role) {
        List<String> variables = new ArrayList<>();
        variables.add(targetVariable(role));
        for (Map.Entry<String, String> alias : ROLE_ALIASES.entrySet()) {
            if (alias.getValue().equals(role)) {
                variables.add(targetVariable(alias.getKey()));
            }
        }
        return List.copyOf(variables);
    }
}
