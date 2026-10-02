package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.RegistryStoreException;

import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.io.IOException;

/**
 * The workflow promotion contract backed by the registry's git repository: every promoted
 * version is one committed, immutable {@code workflows/<name>/<version>.pb} alongside the
 * schema subjects, so a workflow's provenance lives in the same reviewable history as the
 * contracts it was checked against.
 *
 * <p>All storage semantics (validation, immutability, idempotent re-promotion, locking) live
 * in {@link GitSchemaRegistryStore}; this adapter only maps the {@link WorkflowVersionRepository}
 * vocabulary onto them.</p>
 */
public final class RegistryWorkflowVersionRepository implements WorkflowVersionRepository {

    private final GitSchemaRegistryStore store;

    /** An adapter over the given store; the caller keeps ownership of the store's lifecycle. */
    public RegistryWorkflowVersionRepository(GitSchemaRegistryStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public Optional<VersionedWorkflow> find(String name, String version) throws IOException {
        try {
            return store.workflow(name, version);
        } catch (RegistryStoreException e) {
            throw new IOException("Failed to read workflow " + name + " version " + version, e);
        }
    }

    @Override
    public List<VersionedWorkflow> versions(String name) throws IOException {
        try {
            return store.workflowVersions(name);
        } catch (RegistryStoreException e) {
            throw new IOException("Failed to list workflow versions for " + name, e);
        }
    }

    @Override
    public void save(VersionedWorkflow workflow) throws IOException {
        try {
            store.putWorkflow(workflow);
        } catch (RegistryStoreException e) {
            throw new IOException("Failed to save workflow version", e);
        }
    }
}
