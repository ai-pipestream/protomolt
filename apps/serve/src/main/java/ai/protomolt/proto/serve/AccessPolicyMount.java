package ai.protomolt.proto.serve;

import ai.protomolt.proto.authz.AccessPolicies;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.OidcCallerResolver;
import ai.protomolt.proto.authz.jdbc.JdbcCallerResolver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The caller-resolution family of the serve wiring. The access policy narrows named
 * principals; the operator token keeps every scope. A policy that fails to load or verify
 * refuses startup loudly. The external stores (OIDC introspection, the JDBC caller store)
 * compose behind the policy, first match wins.
 */
final class AccessPolicyMount {

    private AccessPolicyMount() {
    }

    /**
     * The resolved caller chain and the JDBC resolver within it. {@code jdbcCallers} is
     * carried out separately because the serve process owns its lifecycle: it is closed on a
     * failed start and again on shutdown.
     *
     * @param callers the chained resolver, or null when neither a policy nor stores are mounted
     * @param jdbcCallers the JDBC resolver to close, or null when no caller store is mounted
     */
    record Callers(CallerResolver callers, JdbcCallerResolver jdbcCallers) {
    }

    /**
     * Composes the caller resolvers from the access policy and the identity stores: policy
     * first, then OIDC introspection, then the JDBC caller store; a single resolver stands
     * alone and several chain first-match-wins.
     */
    static Callers resolve(Path accessPolicy, ProtoMoltServe.IdentityStoreOptions identityStores) {
        List<CallerResolver> resolvers = new ArrayList<>();
        JdbcCallerResolver jdbcCallers = null;
        if (accessPolicy != null) {
            try {
                resolvers.add(new AccessPolicyCallers(AccessPolicies.load(accessPolicy)));
            } catch (IOException e) {
                throw new IllegalStateException("failed to read the access policy at "
                        + accessPolicy + ": " + e.getMessage(), e);
            }
        }
        if (identityStores != null) {
            if (identityStores.oidcIntrospection() != null) {
                resolvers.add(new OidcCallerResolver(identityStores.oidcIntrospection(),
                        identityStores.oidcClientId(), identityStores.oidcClientSecret()));
            }
            if (identityStores.callerStore() != null) {
                jdbcCallers = new JdbcCallerResolver(identityStores.callerStore());
                resolvers.add(jdbcCallers);
            }
        }
        CallerResolver callers = resolvers.isEmpty() ? null
                : resolvers.size() == 1 ? resolvers.getFirst()
                        : CallerResolver.chain(resolvers);
        return new Callers(callers, jdbcCallers);
    }
}
