package ai.protomolt.proto.mcp.transport;

import ai.protomolt.proto.actions.Caller;

/**
 * A scoped caller asked to read a resource whose collection requires a scope the
 * caller does not hold (or declares none). The message names the principal and the
 * missing scope — the same refusal shape the action catalog uses — and never the
 * resource contents, its metadata, or any credential.
 */
public final class ResourceAccessDeniedException extends RuntimeException {

    public ResourceAccessDeniedException(Caller caller, String requiredScope) {
        super(message(caller, requiredScope));
    }

    private static String message(Caller caller, String requiredScope) {
        if (requiredScope == null || requiredScope.isBlank()) {
            return "the resource collection serving this URI declares no required scope; "
                    + "a scoped caller cannot read it";
        }
        return "caller '" + caller.name() + "' does not hold '" + requiredScope
                + "', which resources/read requires";
    }
}
