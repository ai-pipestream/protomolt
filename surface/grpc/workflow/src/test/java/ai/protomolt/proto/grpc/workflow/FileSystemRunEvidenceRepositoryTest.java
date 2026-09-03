package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemRunEvidenceRepositoryTest {

    @TempDir
    Path directory;

    @Test
    void persistsListsAndReopensEvidence() throws Exception {
        FileSystemRunEvidenceRepository repository =
                new FileSystemRunEvidenceRepository(directory);
        RunEvidence evidence = TestWorkflows.evidence();

        repository.save(evidence);
        repository.save(evidence);

        FileSystemRunEvidenceRepository reopened =
                new FileSystemRunEvidenceRepository(directory);
        assertThat(reopened.find(evidence.getRunId())).contains(evidence);
        assertThat(reopened.list(evidence.getWorkflowName(), 10)).containsExactly(evidence);
        assertThat(reopened.list("other", 10)).isEmpty();
    }

    @Test
    void identitiesAreImmutableAndPathSafe() throws Exception {
        FileSystemRunEvidenceRepository repository =
                new FileSystemRunEvidenceRepository(directory);
        RunEvidence original = TestWorkflows.evidence();
        repository.save(original);

        RunEvidence changed = original.toBuilder().setFailureSummary("changed").build();
        assertThatThrownBy(() -> repository.save(changed))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("immutable");
        assertThatThrownBy(() -> repository.find("../outside"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
