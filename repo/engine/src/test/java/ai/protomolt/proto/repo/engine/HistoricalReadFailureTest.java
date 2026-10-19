package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import jakarta.persistence.PersistenceException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Historical read classification and the regression table for the shared write-path mappings. */
class HistoricalReadFailureTest {
    private static final Map<BlobStoreException.Code, RepositoryException.Code> HISTORICAL = Map.ofEntries(
            Map.entry(BlobStoreException.Code.PERMISSION_DENIED, FAILED_PRECONDITION),
            Map.entry(BlobStoreException.Code.UNAUTHENTICATED, FAILED_PRECONDITION),
            Map.entry(BlobStoreException.Code.UNAVAILABLE, UNAVAILABLE),
            Map.entry(BlobStoreException.Code.DEADLINE_EXCEEDED, UNAVAILABLE),
            Map.entry(BlobStoreException.Code.RESOURCE_EXHAUSTED, RESOURCE_EXHAUSTED),
            Map.entry(BlobStoreException.Code.CANCELLED, CANCELLED),
            Map.entry(BlobStoreException.Code.NOT_FOUND, DATA_LOSS),
            Map.entry(BlobStoreException.Code.DATA_LOSS, DATA_LOSS),
            Map.entry(BlobStoreException.Code.FAILED_PRECONDITION, FAILED_PRECONDITION),
            Map.entry(BlobStoreException.Code.INVALID_ARGUMENT, FAILED_PRECONDITION),
            Map.entry(BlobStoreException.Code.UNIMPLEMENTED, UNSUPPORTED),
            Map.entry(BlobStoreException.Code.ABORTED, CONFLICT),
            Map.entry(BlobStoreException.Code.INTERNAL, INTERNAL),
            Map.entry(BlobStoreException.Code.UNKNOWN, UNKNOWN),
            Map.entry(BlobStoreException.Code.ALREADY_EXISTS, UNKNOWN),
            Map.entry(BlobStoreException.Code.OUT_OF_RANGE, UNKNOWN));

    private static final Map<BlobStoreException.Code, String> MESSAGES = Map.ofEntries(
            Map.entry(BlobStoreException.Code.PERMISSION_DENIED, "Original document backend refused the historical read"),
            Map.entry(BlobStoreException.Code.UNAUTHENTICATED, "Original document backend refused the historical read"),
            Map.entry(BlobStoreException.Code.UNAVAILABLE, "Original document backend is unreachable"),
            Map.entry(BlobStoreException.Code.DEADLINE_EXCEEDED, "Original document backend did not answer in time"),
            Map.entry(BlobStoreException.Code.RESOURCE_EXHAUSTED, "Original document backend read capacity is exhausted"),
            Map.entry(BlobStoreException.Code.CANCELLED, "Historical document read cancelled"),
            Map.entry(BlobStoreException.Code.NOT_FOUND, "Published document part is missing from its original backend"),
            Map.entry(BlobStoreException.Code.DATA_LOSS, "Historical document part is damaged at its original backend"),
            Map.entry(BlobStoreException.Code.FAILED_PRECONDITION, "Original document backend cannot serve the retained revision"),
            Map.entry(BlobStoreException.Code.INVALID_ARGUMENT, "Original document backend rejected the retained object coordinates"),
            Map.entry(BlobStoreException.Code.UNIMPLEMENTED, "Original document backend does not support historical reads"),
            Map.entry(BlobStoreException.Code.ABORTED, "Original document backend aborted the historical read"),
            Map.entry(BlobStoreException.Code.INTERNAL, "Original document backend failed internally"),
            Map.entry(BlobStoreException.Code.UNKNOWN, "Original document backend rejected the historical read"),
            Map.entry(BlobStoreException.Code.ALREADY_EXISTS, "Original document backend rejected the historical read"),
            Map.entry(BlobStoreException.Code.OUT_OF_RANGE, "Original document backend rejected the historical read"));

    /** The write path keeps the provider's own code, except ABORTED and UNIMPLEMENTED, which have repository names. */
    private static RepositoryException.Code writePath(BlobStoreException.Code code) {
        return switch (code) {
            case ABORTED -> CONFLICT;
            case UNIMPLEMENTED -> UNSUPPORTED;
            default -> RepositoryException.Code.valueOf(code.name());
        };
    }

    @Test void everyProviderCodeHasAConstantHistoricalClassificationThatKeepsItsCause() {
        for (var code : BlobStoreException.Code.values()) {
            var provider = new BlobStoreException(code, "s3://bucket/documents/key@version secret=" + code, new IllegalStateException("sdk detail"));
            assertThat(HistoricalReadFailures.translate(provider)).isInstanceOfSatisfying(RepositoryException.class,
                    mapped -> {
                        assertThat(mapped.code()).as(code.name()).isEqualTo(HISTORICAL.get(code));
                        assertThat(mapped.getMessage()).as(code.name()).isEqualTo(MESSAGES.get(code));
                        assertThat(mapped.getMessage()).doesNotContain("s3://", "secret", "bucket");
                        assertThat(mapped.getCause()).isSameAs(provider);
                    });
        }
        assertThat(HISTORICAL).hasSize(BlobStoreException.Code.values().length);
    }

    @Test void corruptionAndMissingVersionsNeverBecomeRetryableOutages() {
        for (var code : Set.of(BlobStoreException.Code.NOT_FOUND, BlobStoreException.Code.DATA_LOSS))
            assertThat(((RepositoryException) HistoricalReadFailures.translate(new BlobStoreException(code, "x", null))).code())
                    .isEqualTo(DATA_LOSS);
        assertThat(((RepositoryException) HistoricalReadFailures.translate(new BlobStoreException(BlobStoreException.Code.UNKNOWN, "x", null))).code())
                .isNotIn(UNAVAILABLE, RESOURCE_EXHAUSTED, DATA_LOSS);
    }

    @Test void resourceAndCapabilityFailuresOutsideTheAdapterAreClassified() {
        var missing = new BlobStore.BlobNotFoundException("blob not found: s3://bucket/key@v", null);
        assertCode(missing, DATA_LOSS, "Published document part is missing from its original backend");
        assertCode(new BlobStore.BlobReadLimitException(7), DATA_LOSS, "Document part exceeds its recorded size");
        assertCode(new UnsupportedOperationException("Redis does not provide object versions"), FAILED_PRECONDITION,
                "Original document backend does not support the retained read");
        assertCode(new CancellationException("worker"), CANCELLED, "Historical document read cancelled");
        var budget = new PayloadBudget(1);
        try (var held = budget.reserve(1)) {
            RuntimeException exhausted = null;
            try { budget.reserve(1); } catch (PayloadBudget.CapacityExceededException failure) { exhausted = failure; }
            assertCode(exhausted, RESOURCE_EXHAUSTED, "Document payload capacity exhausted");
        }
    }

    @Test void repositorySqlInvariantAndArgumentFailuresPassThroughUnchanged() {
        var domain = new RepositoryException(NOT_FOUND, "Document is unavailable");
        assertThat(HistoricalReadFailures.translate(domain)).isSameAs(domain);
        var sql = new PersistenceException("connection lost");
        assertThat(HistoricalReadFailures.translate(sql)).isSameAs(sql);
        var closed = new IllegalStateException("Reader admission is closed");
        assertThat(HistoricalReadFailures.translate(closed)).isSameAs(closed);
        var argument = new IllegalArgumentException("node_id must be a UUID");
        assertThat(HistoricalReadFailures.translate(argument)).isSameAs(argument);
        var generic = new RuntimeException("connection reset by peer");
        assertThat(HistoricalReadFailures.translate(generic)).isSameAs(generic);
    }

    @Test void releaseKeepsThePrimaryFailureAndAttachesACloseFailure() {
        var primary = new RepositoryException(UNAVAILABLE, "Original document backend is unreachable");
        var closeFailure = new IllegalStateException("lease already released");
        var closed = new boolean[1];
        HistoricalReadFailures.release(primary, () -> { closed[0] = true; throw closeFailure; });
        assertThat(closed[0]).isTrue();
        assertThat(primary.getSuppressed()).containsExactly(closeFailure);
        HistoricalReadFailures.release(primary, () -> closed[0] = false);
        assertThat(closed[0]).isFalse();
        HistoricalReadFailures.release(primary, null);
        assertThatThrownBy(() -> HistoricalReadFailures.release(null, () -> { throw closeFailure; })).isSameAs(closeFailure);
        var checked = new IOException("checked close");
        assertThatThrownBy(() -> HistoricalReadFailures.release(null, () -> { throw checked; }))
                .isInstanceOf(IllegalStateException.class).hasCause(checked);
        HistoricalReadFailures.release(primary, () -> { throw primary; });
        assertThat(primary.getSuppressed()).containsExactly(closeFailure);
    }

    @Test void sharedCallMapsArchiveAndProviderFailures() {
        for (var code : BlobStoreException.Code.values()) {
            var provider = new BlobStoreException(code, "provider " + code, null);
            assertThatThrownBy(() -> RepositoryErrors.call(() -> { throw provider; }))
                    .isInstanceOfSatisfying(RepositoryException.class, mapped -> assertThat(mapped.code()).as(code.name()).isEqualTo(writePath(code)));
        }
        assertSharedCall(new BlobStore.BlobNotFoundException("gone", null), NOT_FOUND, "gone");
        assertSharedCall(new IllegalArgumentException("bad plan"), INVALID_ARGUMENT, "bad plan");
        assertSharedCall(new ArchiveLedger.VersionConflictException("version"), CONFLICT, "version");
        assertSharedCall(new ArchiveUploadLedger.FenceException("fence"), CONFLICT, "fence");
        var sql = new PersistenceException("connection lost");
        assertThatThrownBy(() -> RepositoryErrors.call(() -> { throw sql; })).isSameAs(sql);
    }

    private static void assertCode(RuntimeException failure, RepositoryException.Code code, String message) {
        assertThat(HistoricalReadFailures.translate(failure)).isInstanceOfSatisfying(RepositoryException.class, mapped -> {
            assertThat(mapped.code()).isEqualTo(code);
            assertThat(mapped.getMessage()).isEqualTo(message);
            assertThat(mapped.getCause()).isSameAs(failure);
        });
    }

    private static void assertSharedCall(RuntimeException failure, RepositoryException.Code code, String message) {
        assertThatThrownBy(() -> RepositoryErrors.call(() -> { throw failure; })).isInstanceOfSatisfying(RepositoryException.class, mapped -> {
            assertThat(mapped.code()).isEqualTo(code);
            assertThat(mapped.getMessage()).isEqualTo(message);
            assertThat(mapped.getCause()).isSameAs(failure);
        });
    }
}
