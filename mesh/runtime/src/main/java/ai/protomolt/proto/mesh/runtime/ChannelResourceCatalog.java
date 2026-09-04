package ai.protomolt.proto.mesh.runtime;

import java.util.Set;

/** Immutable names of storage and recovery profiles available to flow compilation. */
public record ChannelResourceCatalog(
        Set<String> payloadStores,
        Set<String> transactionalChannels,
        Set<String> retryPolicies,
        Set<String> deadLetterChannels,
        Set<String> retentionPolicies,
        Set<String> legalHoldPolicies) {

    public ChannelResourceCatalog {
        payloadStores = Set.copyOf(payloadStores);
        transactionalChannels = Set.copyOf(transactionalChannels);
        retryPolicies = Set.copyOf(retryPolicies);
        deadLetterChannels = Set.copyOf(deadLetterChannels);
        retentionPolicies = Set.copyOf(retentionPolicies);
        legalHoldPolicies = Set.copyOf(legalHoldPolicies);
    }

    public static ChannelResourceCatalog builtIns() {
        return new ChannelResourceCatalog(Set.of(), Set.of(),
                Set.of("default-retry"), Set.of("default-dead-letter"),
                Set.of(), Set.of());
    }
}
