package ai.pipestream.proto.jobs.service.store;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The store layer's failure type: {@code wrap} keeps the SQL cause intact and
 * {@code notFound} names the missing job, so a failure surfaces loud with its
 * context instead of a swallowed default.
 */
class ChainJobStoreExceptionTest {

    @Test
    void wrapKeepsTheMessageAndTheCause() {
        SQLException cause = new SQLException("connection reset");
        ChainJobStoreException wrapped = ChainJobStoreException.wrap("insert failed", cause);
        assertThat(wrapped.getMessage()).isEqualTo("insert failed");
        assertThat(wrapped.getCause()).isSameAs(cause);
        assertThat(wrapped).isInstanceOf(RuntimeException.class);
    }

    @Test
    void notFoundNamesTheMissingJob() {
        UUID jobId = UUID.randomUUID();
        ChainJobStoreException missing = ChainJobStoreException.notFound(jobId);
        assertThat(missing.getMessage()).isEqualTo("chain job not found: " + jobId);
        assertThat(missing.getCause()).isNull();
    }
}
