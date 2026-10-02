package ai.protomolt.proto.registry;

import java.util.List;
import java.util.Optional;

/**
 * Optional storage capability for typed configuration envelopes.
 * The serving boundary uses {@link ConfigSupport} to validate documents against
 * this registry before writing and before returning a stored document.
 */
public interface ConfigDocumentStore extends SchemaRegistryStore {
    /** Stores an envelope and returns its nonblank, backend-defined version identity. */
    String putConfig(String name, String envelopeJson) throws RegistryStoreException;

    /** Returns an envelope, or empty only when no document exists. */
    Optional<String> config(String name) throws RegistryStoreException;

    /** Returns the stored document's version, or empty when no version is available. */
    Optional<String> configVersion(String name) throws RegistryStoreException;

    /** Returns stored names in ascending order. */
    List<String> configs() throws RegistryStoreException;
}
