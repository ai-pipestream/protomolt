package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The domain failure translation shared by the drive operations:
 * which storage and ledger failures become which {@link RepositoryException} code, that
 * the message and cause survive, and who {@code requireProcessAuthority} refuses.
 */
class RepositoryErrorsTest {

    @Test
    void callReturnsTheOperationResult() {
        assertThat(RepositoryErrors.call(() -> "stored")).isEqualTo("stored");
    }

    @Test
    void blobNotFoundBecomesNotFound() {
        var missing = new BlobStore.BlobNotFoundException("no object at bucket/key", null);
        assertTranslated(() -> { throw missing; }, RepositoryException.Code.NOT_FOUND,
                "no object at bucket/key", missing);
    }

    @Test
    void illegalArgumentBecomesInvalidArgument() {
        var invalid = new IllegalArgumentException("drive name is not a slug");
        assertTranslated(() -> { throw invalid; }, RepositoryException.Code.INVALID_ARGUMENT,
                "drive name is not a slug", invalid);
    }

    @Test
    void illegalArgumentSubclassBecomesInvalidArgument() {
        var invalid = new NumberFormatException("For input string: \"x\"");
        assertTranslated(() -> { throw invalid; }, RepositoryException.Code.INVALID_ARGUMENT,
                "For input string: \"x\"", invalid);
    }

    @Test
    void repositoryExceptionPassesThroughUntouched() {
        var raised = new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "drive is sealed");
        assertThatThrownBy(() -> RepositoryErrors.call(() -> { throw raised; })).isSameAs(raised);
    }

    @Test
    void unmappedFailurePassesThroughUntouched() {
        var unexpected = new IllegalStateException("ledger closed");
        assertThatThrownBy(() -> RepositoryErrors.call(() -> { throw unexpected; })).isSameAs(unexpected);
    }

    @ParameterizedTest
    @MethodSource("providerCodes")
    void blobStoreFailureKeepsTheProviderCodeUnderItsRepositoryName(BlobStoreException.Code provider,
            RepositoryException.Code expected) {
        var failure = new BlobStoreException(provider, "provider said " + provider, new RuntimeException("sdk"));
        assertThat(RepositoryErrors.providerCode(failure)).isEqualTo(expected);
        assertTranslated(() -> { throw failure; }, expected, "provider said " + provider, failure);
    }

    @Test
    void everyProviderCodeHasARepositoryCode() {
        assertThat(providerCodes()).hasSize(BlobStoreException.Code.values().length);
    }

    static Stream<Arguments> providerCodes() {
        return Stream.of(BlobStoreException.Code.values()).map(code -> Arguments.of(
                code, switch (code) {
                    case ABORTED -> RepositoryException.Code.CONFLICT;
                    case UNIMPLEMENTED -> RepositoryException.Code.UNSUPPORTED;
                    case CANCELLED -> RepositoryException.Code.CANCELLED;
                    case UNKNOWN -> RepositoryException.Code.UNKNOWN;
                    case INVALID_ARGUMENT -> RepositoryException.Code.INVALID_ARGUMENT;
                    case DEADLINE_EXCEEDED -> RepositoryException.Code.DEADLINE_EXCEEDED;
                    case NOT_FOUND -> RepositoryException.Code.NOT_FOUND;
                    case ALREADY_EXISTS -> RepositoryException.Code.ALREADY_EXISTS;
                    case PERMISSION_DENIED -> RepositoryException.Code.PERMISSION_DENIED;
                    case RESOURCE_EXHAUSTED -> RepositoryException.Code.RESOURCE_EXHAUSTED;
                    case FAILED_PRECONDITION -> RepositoryException.Code.FAILED_PRECONDITION;
                    case OUT_OF_RANGE -> RepositoryException.Code.OUT_OF_RANGE;
                    case INTERNAL -> RepositoryException.Code.INTERNAL;
                    case UNAVAILABLE -> RepositoryException.Code.UNAVAILABLE;
                    case DATA_LOSS -> RepositoryException.Code.DATA_LOSS;
                    case UNAUTHENTICATED -> RepositoryException.Code.UNAUTHENTICATED;
                }));
    }

    @Test
    void processAuthorityCallerIsAdmitted() {
        RepositoryErrors.requireProcessAuthority(new RepositoryCaller("ingest-process", true));
    }

    @Test
    void nullCallerIsRefused() {
        assertPermissionDenied(null);
    }

    @Test
    void callerWithoutProcessAuthorityIsRefused() {
        assertPermissionDenied(new RepositoryCaller("tenant-user", false, Set.of("account-1"), Set.of()));
    }

    @Test
    void factoriesCarryTheirCodeAndMessage() {
        assertCode(RepositoryErrors.invalidArgument("a"), RepositoryException.Code.INVALID_ARGUMENT, "a");
        assertCode(RepositoryErrors.notFound("b"), RepositoryException.Code.NOT_FOUND, "b");
        assertCode(RepositoryErrors.failedPrecondition("c"), RepositoryException.Code.FAILED_PRECONDITION, "c");
        assertCode(RepositoryErrors.unavailable("d"), RepositoryException.Code.UNAVAILABLE, "d");
        assertCode(RepositoryErrors.aborted("e"), RepositoryException.Code.CONFLICT, "e");
        assertCode(RepositoryErrors.alreadyExists("f"), RepositoryException.Code.ALREADY_EXISTS, "f");
    }

    private static void assertPermissionDenied(RepositoryCaller caller) {
        assertThatThrownBy(() -> RepositoryErrors.requireProcessAuthority(caller))
                .isInstanceOfSatisfying(RepositoryException.class, failure -> assertCode(failure,
                        RepositoryException.Code.PERMISSION_DENIED,
                        "Repository account policy is not configured for this principal"));
    }

    private static void assertTranslated(Supplier<Object> operation, RepositoryException.Code code, String message,
            Throwable cause) {
        assertThatThrownBy(() -> RepositoryErrors.call(operation))
                .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                    assertCode(failure, code, message);
                    assertThat(failure.getCause()).isSameAs(cause);
                });
    }

    private static void assertCode(RepositoryException failure, RepositoryException.Code code, String message) {
        assertThat(failure.code()).isEqualTo(code);
        assertThat(failure.getMessage()).isEqualTo(message);
    }
}
