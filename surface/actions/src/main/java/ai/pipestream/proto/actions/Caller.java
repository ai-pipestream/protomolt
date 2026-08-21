package ai.pipestream.proto.actions;

import java.util.Map;
import java.util.Set;

/**
 * The resolved identity a request executes as: a named principal, the scopes it holds,
 * and the budgets capping what it may spend there. Resolution happens once, at the
 * transport edge where the credential is presented; the caller then travels explicitly.
 * The operator form holds every scope and is the identity of the process credential and
 * of local process-boundary surfaces (the CLI, stdio MCP).
 *
 * @param name principal name; never blank, and never a credential
 * @param scopes scopes the principal holds, each from {@link Scopes#VOCABULARY}; empty and
 *        meaningless when {@code unrestricted}
 * @param unrestricted whether this caller holds every scope, present and future
 * @param budgets per-scope budgets keyed by a scope the caller holds; a scope with no
 *        entry is unbudgeted, and an unrestricted caller is never budgeted
 */
public record Caller(String name, Set<String> scopes, boolean unrestricted,
                     Map<String, Budget> budgets) {

    /**
     * A budget on one scope. At least one limit must be set — a budget that caps
     * nothing is dead configuration and refused here.
     *
     * @param requestsPerMinute requests per minute across every surface that checks the
     *        scope; zero means unlimited
     * @param maxPayloadBytes largest request payload in bytes on surfaces that know
     *        their payload size; zero means uncapped
     */
    public record Budget(int requestsPerMinute, long maxPayloadBytes) {

        public Budget {
            if (requestsPerMinute < 0 || maxPayloadBytes < 0) {
                throw new IllegalArgumentException("budget limits must not be negative");
            }
            if (requestsPerMinute == 0 && maxPayloadBytes == 0) {
                throw new IllegalArgumentException(
                        "a budget must cap something: set requests per minute or a"
                                + " payload cap");
            }
        }
    }

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
        if (unrestricted && !budgets.isEmpty()) {
            throw new IllegalArgumentException("the operator is never budgeted");
        }
        for (String scope : budgets.keySet()) {
            if (!scopes.contains(scope)) {
                throw new IllegalArgumentException("budgeted scope '" + scope
                        + "' is not one the caller holds");
            }
        }
        budgets = Map.copyOf(budgets);
    }

    /** A caller without budgets. */
    public Caller(String name, Set<String> scopes, boolean unrestricted) {
        this(name, scopes, unrestricted, Map.of());
    }

    /** The process-authority caller: every scope, always. */
    public static Caller operator() {
        return new Caller("operator", Set.of(), true);
    }

    /** A named principal holding exactly {@code scopes}, unbudgeted. */
    public static Caller scoped(String name, Set<String> scopes) {
        return new Caller(name, scopes, false);
    }

    /** A named principal holding exactly {@code scopes} under {@code budgets}. */
    public static Caller scoped(String name, Set<String> scopes,
                                Map<String, Budget> budgets) {
        return new Caller(name, scopes, false, budgets);
    }

    /** Whether this caller holds {@code scope}. */
    public boolean holds(String scope) {
        return unrestricted || scopes.contains(scope);
    }
}
