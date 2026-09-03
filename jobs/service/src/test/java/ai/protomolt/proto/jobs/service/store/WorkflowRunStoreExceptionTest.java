package ai.protomolt.proto.jobs.service.store;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The store layer's failure type: {@code wrap} keeps the SQL cause intact and
 * {@code notFound} names the missing job, so a failure surfaces loud with its
 * context instead of a swallowed default.
 */
class WorkflowRunStoreExceptionTest {

    @Test
    void wrapKeepsTheMessageAndTheCause() {
        SQLException cause = new SQLException("connection reset");
        WorkflowRunStoreException wrapped = WorkflowRunStoreException.wrap("insert failed", cause);
        assertThat(wrapped.getMessage()).isEqualTo("insert failed");
        assertThat(wrapped.getCause()).isSameAs(cause);
        assertThat(wrapped).isInstanceOf(RuntimeException.class);
    }

    @Test
    void notFoundNamesTheMissingJob() {
        UUID jobId = UUID.randomUUID();
        WorkflowRunStoreException missing = WorkflowRunStoreException.notFound(jobId);
        assertThat(missing.getMessage()).isEqualTo("workflow run not found: " + jobId);
        assertThat(missing.getCause()).isNull();
    }
}
