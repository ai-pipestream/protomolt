package ai.protomolt.proto.compat;

import java.util.Set;

/**
 * The {@link Impact} sets the diff rules report, named once so that two rules carrying the same
 * consequence cannot drift apart. {@code INFO} is the empty set: the change is worth telling the
 * caller about but breaks nothing.
 */
final class DiffImpacts {

    static final Set<Impact> INFO = Set.of();
    static final Set<Impact> ALL = Set.of(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD,
            Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    static final Set<Impact> WIRE_BOTH_SOURCE =
            Set.of(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD, Impact.SOURCE);
    static final Set<Impact> JSON_BOTH_SOURCE =
            Set.of(Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    static final Set<Impact> JSON_BOTH =
            Set.of(Impact.JSON_BACKWARD, Impact.JSON_FORWARD);

    private DiffImpacts() {
    }
}
