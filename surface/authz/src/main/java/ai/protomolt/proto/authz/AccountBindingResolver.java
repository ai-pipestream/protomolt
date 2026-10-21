package ai.protomolt.proto.authz;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The tenancy seam: resolves an authenticated scoped caller to the accounts it acts for and
 * the document ACL identities it presents there. Kept apart from {@link CallerResolver},
 * which says who a credential belongs to; this says which accounts that principal may
 * reach, so any credential store (the access policy, OIDC introspection, a JDBC caller
 * store) composes with any binding source.
 *
 * <p>Contract: {@link Optional#empty()} means the principal is bound to no account, and the
 * host refuses it on every account-addressed call. Process authority is never resolved
 * here; it reaches every account. Implementations must be safe for concurrent use.
 */
@FunctionalInterface
public interface AccountBindingResolver {

    /**
     * The binding of an authenticated scoped caller.
     *
     * @param caller the authenticated caller; never process authority
     * @return the caller's binding, or empty when it is bound to no account
     */
    Optional<AccountBinding> bindingOf(AuthenticatedCaller caller);

    /** A resolver binding no principal: every scoped caller is refused by account. */
    static AccountBindingResolver none() {
        return caller -> {
            requireScoped(caller);
            return Optional.empty();
        };
    }

    /**
     * A resolver over a well-formed {@link AccountBindings} document, keyed by principal
     * name exactly as the credential resolver reported it.
     *
     * @param document the binding document; refused when not well formed
     * @return the resolver
     */
    static AccountBindingResolver of(AccountBindings document) {
        AccountBindingPolicies.requireWellFormed(document);
        Map<String, AccountBinding> byPrincipal = new HashMap<>();
        for (AccountBinding binding : document.getBindingsList()) {
            byPrincipal.put(binding.getPrincipal(), binding);
        }
        Map<String, AccountBinding> frozen = Map.copyOf(byPrincipal);
        return caller -> {
            requireScoped(caller);
            return Optional.ofNullable(frozen.get(caller.caller().name()));
        };
    }

    private static void requireScoped(AuthenticatedCaller caller) {
        Objects.requireNonNull(caller, "caller");
        if (caller.caller().unrestricted()) {
            throw new IllegalArgumentException("process authority is never account-bound");
        }
    }
}
