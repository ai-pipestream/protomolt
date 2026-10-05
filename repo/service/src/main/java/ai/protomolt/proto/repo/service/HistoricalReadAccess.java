package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.util.Objects;
import java.util.function.Function;

/**
 * Explicit host opt-in for historical transport. Bindings resolve trusted account
 * memberships and ACL identities; request fields and token scopes are not grants.
 * The host owns resolver resources. Calls may resolve concurrently.
 */
public record HistoricalReadAccess(Function<Caller, RepositoryCaller> bindings,
        long responseBudgetBytes, int maxConcurrentCalls) {
    public HistoricalReadAccess {
        Objects.requireNonNull(bindings, "bindings");
        if (responseBudgetBytes <= 0) throw new IllegalArgumentException("Response budget must be positive");
        if (maxConcurrentCalls <= 0) throw new IllegalArgumentException("Historical call limit must be positive");
    }
}
