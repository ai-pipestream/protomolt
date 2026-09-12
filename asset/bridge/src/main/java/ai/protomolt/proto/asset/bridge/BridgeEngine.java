package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.FormatFact;

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
        return new BridgeEngine(List.of(new ContainerMembersBridge(),
                new DatasetSchemaBridge()));
    }

    /**
     * The bridge for a kind and format, when this engine can run it.
     *
     * @param kind the bridge kind
     * @param format the format of record
     * @return the bridge, or empty when the pair executes elsewhere
     */
    public Optional<Bridge> forKind(BridgeKind kind, FormatFact format) {
        return Optional.ofNullable(bridges.get(kind))
                .filter(bridge -> bridge.handles(format));
    }

    /** The kinds this engine has a bridge for at all. */
    public Set<BridgeKind> executable() {
        return Set.copyOf(bridges.keySet());
    }
}
