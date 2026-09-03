package ai.protomolt.proto.composer;

import java.util.List;
import java.util.function.Supplier;

/**
 * Cross-module contributions, collected during the wire phase and read by
 * their hosts at start. The registry module hosts action contributions on
 * its actions route and workflow contributions in its store; future hosts
 * follow the same shape. Typed by class so the composer depends on no
 * domain module: contributors and hosts share the contribution type, the
 * composer only carries it.
 */
public interface Contributions {

    /**
     * Registers one contribution. Wire-phase only; a contribution made
     * after the wire phase completes is a bug and throws.
     */
    <T> void contribute(Class<T> kind, T contribution);

    /**
     * Every contribution of a kind, in contribution order. Stable across
     * calls; hosts typically read at {@link ServiceMount#start()} time,
     * after all modules have wired.
     */
    <T> List<T> all(Class<T> kind);

    /**
     * The node's single instance of a kind: the first contribution of that
     * kind, or one built by {@code factory} and contributed. For state that
     * must be one per node however many roles use it (the scope-budget
     * ledger), where every module wants the same object and none of them
     * owns it. Wire-phase only, like {@link #contribute}.
     *
     * @param kind the contribution type
     * @param factory builds the instance when the node has none yet
     * @return the node's instance of {@code kind}
     */
    <T> T shared(Class<T> kind, Supplier<T> factory);
}
