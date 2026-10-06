package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import java.util.Optional;

/** Host resolver that can preserve a provisioned credential identity in one lookup. */
@FunctionalInterface
public interface AuthenticatedCallerResolver extends CallerResolver {
    @Override
    Optional<AuthenticatedCaller> resolveAuthenticated(String credential);

    @Override
    default Optional<Caller> resolve(String credential) {
        return resolveAuthenticated(credential).map(AuthenticatedCaller::caller);
    }
}
