package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.sink.v1.RepositorySinkFailure;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

/** An upload that failed uncertainly and has no recorded outcome yet can always be retried. */
class RepositorySinkFailuresTest {

    @Test
    void everyUncertainCommitFailureWithNoRecordedOutcomeIsRetryable() {
        for (Status.Code code : Status.Code.values()) {
            if (!RepositorySinkFailures.isUncertainCommitFailure(code)) {
                continue;
            }
            RepositorySinkFailure failure =
                    RepositorySinkFailures.commitNotRecorded(code, "reply lost");
            assertThat(failure.getReasonCode()).as(code.name()).isEqualTo("COMMIT_NOT_RECORDED");
            assertThat(failure.getRetryable()).as(code.name()).isTrue();
        }
    }
}
