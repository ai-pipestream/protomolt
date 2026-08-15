package ai.pipestream.proto.grpc.workflow;

import ai.pipestream.proto.grpc.workflow.v1.VersionedWorkflow;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Durable immutable versions of descriptor-grounded gRPC workflows. */
public interface WorkflowVersionRepository {

    /** Finds one exact workflow version. */
    Optional<VersionedWorkflow> find(String name, String version) throws IOException;

    /** Lists all versions of one workflow in repository-defined stable order. */
    List<VersionedWorkflow> versions(String name) throws IOException;

    /** Saves a new immutable version or verifies that an existing version is identical. */
    void save(VersionedWorkflow workflow) throws IOException;
}
