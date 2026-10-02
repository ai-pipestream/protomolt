package ai.protomolt.proto.registry;

import java.util.Map;

/**
 * Factory for a trusted, installed storage backend, discovered through Java SPI.
 * Constructors and {@link #id()} must not open storage, connect to a network or
 * start background work. Only the explicitly selected provider is opened.
 */
public interface SchemaRegistryStoreProvider {
    /** Unique lowercase identifier using letters, digits and hyphens. */
    String id();

    /**
     * Opens a configured store. Reject missing, unknown and invalid options before
     * acquiring resources. On failure, release any resources already acquired.
     *
     * @param options immutable provider-specific settings
     * @param writeGate caller-selected compatibility gate, or null for no gate
     * @return a store whose lifecycle belongs to the caller
     */
    SchemaRegistryStore open(Map<String, String> options, SchemaRegistryStore.WriteGate writeGate);
}
