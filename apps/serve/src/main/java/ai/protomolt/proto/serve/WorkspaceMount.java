package ai.protomolt.proto.serve;

import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import java.io.IOException;
import java.nio.file.Path;

/**
 * The filesystem workspaces of the serve wiring: the service-profile workspace backing the
 * service-catalog verbs and MCP resources, and the workflow workspace holding run artifacts
 * and run evidence. Demo mode falls back to a temp-directory workflow workspace so the seeded
 * demo workflow has somewhere to write; a workspace that cannot be opened fails startup loud.
 */
final class WorkspaceMount {

    private WorkspaceMount() {
    }

    /** Opens the service-profile repository at the workspace, or null when none is mounted. */
    static ServiceProfileRepository serviceProfiles(Path serviceWorkspace) {
        if (serviceWorkspace == null) {
            return null;
        }
        try {
            return new FileSystemServiceProfileRepository(serviceWorkspace);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open service workspace at "
                    + serviceWorkspace, e);
        }
    }

    /** The artifact and run-evidence repositories rooted at one workflow workspace. */
    record WorkflowRepositories(ArtifactRepository artifacts, RunEvidenceRepository runEvidence) {
    }

    /**
     * Opens the workflow workspace repositories, or a null-valued pair when no workflow
     * workspace is mounted. Demo mode always has one: an unnamed workspace lives in a temp
     * directory.
     */
    static WorkflowRepositories workflowRepositories(Path workflowWorkspace, boolean demo) {
        if (workflowWorkspace == null && demo) {
            try {
                workflowWorkspace = java.nio.file.Files
                        .createTempDirectory("protomolt-demo-workflows");
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create the demo workflow workspace", e);
            }
        }
        if (workflowWorkspace == null) {
            return new WorkflowRepositories(null, null);
        }
        try {
            return new WorkflowRepositories(
                    new FileSystemArtifactRepository(workflowWorkspace.resolve("artifacts")),
                    new FileSystemRunEvidenceRepository(workflowWorkspace.resolve("runs")));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open workflow workspace at "
                    + workflowWorkspace, e);
        }
    }
}
