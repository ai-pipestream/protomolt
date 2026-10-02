package ai.protomolt.proto.registry;

import java.util.List;
import java.util.Optional;

/**
 * Optional storage capability for named workflow JSON documents.
 * The serving boundary validates workflow semantics before calling this store.
 * This capability does not imply support for immutable promoted workflow versions.
 */
public interface WorkflowDocumentStore extends SchemaRegistryStore {
    /** Stores or replaces a named document; storage failures must propagate. */
    void putWorkflow(String name, String workflowJson) throws RegistryStoreException;

    /** Returns the stored document, or empty only when it is absent. */
    Optional<String> workflow(String name) throws RegistryStoreException;

    /** Returns stored names in ascending order. */
    List<String> workflows() throws RegistryStoreException;
}
