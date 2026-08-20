package ai.pipestream.proto.actions;

import java.util.Set;

/**
 * The resolved identity a request executes as: a named principal and the scopes it holds.
 * Resolution happens once, at the transport edge where the credential is presented; the
 * caller then travels explicitly. The operator form holds every scope and is the identity
 * of the process credential and of local process-boundary surfaces (the CLI, stdio MCP).
 *
 * @param name principal name; never blank, and never a credential
 * @param scopes scopes the principal holds, each from {@link Scopes#VOCABULARY}; empty and
 *        meaningless when {@code unrestricted}
 * @param unrestricted whether this caller holds every scope, present and future
 */
public record Caller(String name, Set<String> scopes, boolean unrestricted) {

    public Caller {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("caller name must not be blank");
        }
        if (unrestricted && !scopes.isEmpty()) {
            throw new IllegalArgumentException(
                    "an unrestricted caller does not enumerate scopes");
        }
        for (String scope : scopes) {
            if (!Scopes.VOCABULARY.contains(scope)) {
                throw new IllegalArgumentException("unknown scope '" + scope
                        + "'; the vocabulary is " + String.join(", ", Scopes.VOCABULARY));
            }
        }
        scopes = Set.copyOf(scopes);
    }

    /** The process-authority caller: every scope, always. */
    public static Caller operator() {
        return new Caller("operator", Set.of(), true);
    }

    /** A named principal holding exactly {@code scopes}. */
    public static Caller scoped(String name, Set<String> scopes) {
        return new Caller(name, scopes, false);
    }

    /** Whether this caller holds {@code scope}. */
    public boolean holds(String scope) {
        return unrestricted || scopes.contains(scope);
    }
}
