package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ManagedWriteFailureTest {
    private static void check(Throwable cause, RepositoryException.Code expected) {
        var attempt = UUID.randomUUID();
        var wrapper = new RuntimeException("staging envelope", cause);
        var mapped = RepositoryErrors.managedFailure(attempt, "publication", wrapper);
        assertThat(mapped.code()).isEqualTo(expected);
        assertThat(mapped.getCause()).isSameAs(wrapper);
        assertThat(mapped.getMessage()).contains(attempt.toString(), "publication", "reconcile");
    }
    @Test void knownFailuresKeepTheirMeaningAndAttemptIdentity() {
        check(new RepositoryException(RepositoryException.Code.DEADLINE_EXCEEDED, "expired"), RepositoryException.Code.DEADLINE_EXCEEDED);
        check(new DocumentLedger.RevisionConflictException(), RepositoryException.Code.CONFLICT);
        check(new ai.protomolt.proto.repo.container.ledger.DocumentPartAttemptLedger.FenceException("missing profile"),
                RepositoryException.Code.FAILED_PRECONDITION);
        check(new java.util.concurrent.CancellationException(), RepositoryException.Code.CANCELLED);
        check(new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "unavailable", null), RepositoryException.Code.UNAVAILABLE);
        check(new BlobStore.BlobReadLimitException(1), RepositoryException.Code.DATA_LOSS);
        check(new IllegalArgumentException("invalid plan"), RepositoryException.Code.INVALID_ARGUMENT);
        var budget = new PayloadBudget(1);
        try (var held = budget.reserve(1)) {
            check(catchThrowable(() -> budget.reserve(1)), RepositoryException.Code.RESOURCE_EXHAUSTED);
        }
    }
    @Test void unknownAndCyclicFailuresDoNotBecomeRetryPromises() {
        check(new IllegalStateException("commit acknowledgement lost"), RepositoryException.Code.UNKNOWN);
        var first = new RuntimeException("first");
        var second = new RuntimeException("second", first);
        first.initCause(second);
        check(first, RepositoryException.Code.UNKNOWN);
    }
    @Test void knownDomainEnvelopeWinsOverIncidentalLowLevelCause() {
        check(new RepositoryException(RepositoryException.Code.PERMISSION_DENIED, "denied", new IllegalArgumentException()),
                RepositoryException.Code.PERMISSION_DENIED);
    }
}
