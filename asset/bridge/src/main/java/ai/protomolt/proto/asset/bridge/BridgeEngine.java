package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.BridgeKind;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The bridges a host can actually run. The routing rule ({@link Bridges})
 * says which bridges a format APPLIES; this says which of those the running
 * process can execute itself. The distinction is the honest one: a bridge
 * whose extraction rides a parser service is applicable everywhere and
 * executable only where that parser is reachable, and a caller deserves to
 * be told "this applies but runs elsewhere" rather than either silence or a
 * fabricated result.
 */
public final class BridgeEngine {

    private final Map<BridgeKind, Bridge> bridges = new EnumMap<>(BridgeKind.class);

    /**
     * An engine over the given bridges.
     *
     * @param bridges the executable bridges; at most one per kind
     */
    public BridgeEngine(List<Bridge> bridges) {
        for (Bridge bridge : bridges) {
            Bridge previous = this.bridges.put(bridge.kind(), bridge);
            if (previous != null) {
                throw new IllegalArgumentException("two bridges claim " + bridge.kind());
            }
        }
    }

    /**
     * The engine every host can run: the pure-JDK bridges, which need no
     * service beyond the process itself.
     *
     * @return the standard engine
     */
    public static BridgeEngine standard() {
        return new BridgeEngine(List.of(new ContainerMembersBridge()));
    }

    /**
     * The bridge for a kind, when this engine can run it.
     *
     * @param kind the bridge kind
     * @return the bridge, or empty when the kind executes elsewhere
     */
    public Optional<Bridge> forKind(BridgeKind kind) {
        return Optional.ofNullable(bridges.get(kind));
    }

    /** The kinds this engine executes in process. */
    public Set<BridgeKind> executable() {
        return Set.copyOf(bridges.keySet());
    }
}
