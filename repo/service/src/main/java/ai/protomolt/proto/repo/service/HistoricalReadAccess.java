package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Explicit host opt-in for historical transport. Bindings resolve trusted account
 * memberships and ACL identities; request fields and token scopes are not grants.
 * The host owns resolver resources. Calls may resolve concurrently. The response
 * byte budget is shared across historical services; the call bound is per service.
 */
public record HistoricalReadAccess(Function<Caller, RepositoryCaller> bindings,
        long responseBudgetBytes, int maxConcurrentCalls,
        Optional<HistoricalMaterializationRepository.Limits> materializationLimits) {
    public HistoricalReadAccess(Function<Caller, RepositoryCaller> bindings, long responseBudgetBytes, int maxConcurrentCalls) {
        this(bindings, responseBudgetBytes, maxConcurrentCalls, Optional.empty());
    }
    public HistoricalReadAccess {
        Objects.requireNonNull(bindings, "bindings");
        Objects.requireNonNull(materializationLimits, "materializationLimits");
        if (responseBudgetBytes <= 0) throw new IllegalArgumentException("Response budget must be positive");
        if (maxConcurrentCalls <= 0) throw new IllegalArgumentException("Historical call limit must be positive");
    }

    /** Explicitly mount selected historical decoding with these maximum work limits. */
    public HistoricalReadAccess withMaterialization(HistoricalMaterializationRepository.Limits limits) {
        return new HistoricalReadAccess(bindings, responseBudgetBytes, maxConcurrentCalls, Optional.of(limits));
    }
}
