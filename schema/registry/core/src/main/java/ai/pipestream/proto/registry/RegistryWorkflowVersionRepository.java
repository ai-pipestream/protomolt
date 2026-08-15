package ai.pipestream.proto.registry;

import ai.pipestream.proto.grpc.workflow.WorkflowVersionRepository;
import ai.pipestream.proto.grpc.workflow.v1.VersionedWorkflow;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    public Optional<VersionedWorkflow> find(String name, String version) {
        return store.workflow(name, version);
    }

    @Override
    public List<VersionedWorkflow> versions(String name) {
        return store.workflowVersions(name);
    }

    @Override
    public void save(VersionedWorkflow workflow) {
        store.putWorkflow(workflow);
    }
}
