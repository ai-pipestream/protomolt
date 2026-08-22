package ai.pipestream.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.NodeAddress;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

/** The argument conversions every document RPC runs before it can start work. */
class DocumentRequestsTest {

    private static void assertRefusesNaming(ThrowingCallable call, String... fragments) {
        assertThatThrownBy(call)
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(Status.fromThrowable(t).getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT))
                .hasMessageContainingAll(fragments);
    }

    // --- parts ------------------------------------------------------------------

    @Test
    void partsBecomeASet() {
        assertThat(DocumentRequests.partsOrThrow(
                List.of(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_CHUNKS,
                        DocumentPart.DOCUMENT_PART_CORE), "parts"))
                .containsExactlyInAnyOrder(DocumentPart.DOCUMENT_PART_CORE,
                        DocumentPart.DOCUMENT_PART_CHUNKS);
    }

    @Test
    void noPartsIsAnEmptySetNotAFailure() {
        assertThat(DocumentRequests.partsOrThrow(List.of(), "parts")).isEmpty();
    }

    @Test
    void theUnspecifiedPartIsRefusedUnderTheCallersFieldName() {
        assertRefusesNaming(() -> DocumentRequests.partsOrThrow(
                List.of(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_UNSPECIFIED),
                "parts_written"), "parts_written");
    }

    @Test
    void aPartFromANewerPeerIsRefusedRatherThanDropped() {
        // UNRECOGNIZED means the caller named a part this build does not know. Skipping it
        // would write a document that silently lacks what was asked for.
        assertRefusesNaming(() -> DocumentRequests.partsOrThrow(
                List.of(DocumentPart.UNRECOGNIZED), "parts"), "parts");
    }

    // --- addresses --------------------------------------------------------------

    private static NodeAddress.Builder address() {
        return NodeAddress.newBuilder()
                .setDocId("d").setGraphAddressId("g").setAccountId("a").setGraphId("gr");
    }

    @Test
    void aCompleteAddressPassesThrough() {
        NodeAddress complete = address().build();
        assertThat(DocumentRequests.validateAddress(complete, "address")).isSameAs(complete);
    }

    @Test
    void everySegmentIsRequired() {
        assertRefusesNaming(() -> DocumentRequests.validateAddress(
                address().setDocId("").build(), "address"), "address", "doc_id");
        assertRefusesNaming(() -> DocumentRequests.validateAddress(
                address().setGraphAddressId("").build(), "address"), "graph_address_id");
        assertRefusesNaming(() -> DocumentRequests.validateAddress(
                address().setAccountId("").build(), "address"), "account_id");
        assertRefusesNaming(() -> DocumentRequests.validateAddress(
                address().setGraphId("").build(), "address"), "graph_id");
    }

    @Test
    void aWhitespaceSegmentIsAsAbsentAsAnEmptyOne() {
        assertRefusesNaming(() -> DocumentRequests.validateAddress(
                address().setAccountId("   ").build(), "address"), "account_id");
    }

    @Test
    void anAddressDescribesItselfWithEverySegment() {
        assertThat(DocumentRequests.describe(address().build()))
                .isEqualTo("doc_id=d, graph_address_id=g, account_id=a, graph_id=gr");
    }

    // --- uuids ------------------------------------------------------------------

    @Test
    void aUuidParses() {
        UUID id = UUID.randomUUID();
        assertThat(DocumentRequests.parseUuid(id.toString(), "node_id")).isEqualTo(id);
        assertThat(DocumentRequests.parseUuid("  " + id + "  ", "node_id")).isEqualTo(id);
    }

    @Test
    void anAbsentUuidSaysItIsRequiredAndAMalformedOneShowsWhatItGot() {
        assertRefusesNaming(() -> DocumentRequests.parseUuid(null, "node_id"),
                "node_id", "required");
        assertRefusesNaming(() -> DocumentRequests.parseUuid("   ", "node_id"),
                "node_id", "required");
        assertRefusesNaming(() -> DocumentRequests.parseUuid("not-a-uuid", "node_id"),
                "node_id", "not-a-uuid");
    }

    // --- continuation tokens ----------------------------------------------------

    @Test
    void anAbsentTokenIsTheFirstPage() {
        assertThat(DocumentRequests.parseContinuationToken(null)).isZero();
        assertThat(DocumentRequests.parseContinuationToken("")).isZero();
        assertThat(DocumentRequests.parseContinuationToken("  ")).isZero();
    }

    @Test
    void aTokenIsARowOffset() {
        assertThat(DocumentRequests.parseContinuationToken("0")).isZero();
        assertThat(DocumentRequests.parseContinuationToken(" 42 ")).isEqualTo(42);
        assertThat(DocumentRequests.parseContinuationToken(String.valueOf(Long.MAX_VALUE)))
                .isEqualTo(Long.MAX_VALUE);
    }

    /**
     * A negative offset and an unparseable one fail differently, and the negative case has
     * to survive the catch that handles the unparseable one: it is a refusal raised from
     * inside the same conversion, not a number-format problem.
     */
    @Test
    void aNegativeOffsetIsRefusedAsAnOffsetNotAsANumber() {
        assertRefusesNaming(() -> DocumentRequests.parseContinuationToken("-1"),
                "continuation_token", "non-negative");
    }

    @Test
    void anUnparseableTokenShowsWhatItGot() {
        assertRefusesNaming(() -> DocumentRequests.parseContinuationToken("page-2"),
                "continuation_token", "page-2");
        assertRefusesNaming(() -> DocumentRequests.parseContinuationToken("99999999999999999999"),
                "continuation_token");
    }

    // --- odds and ends ----------------------------------------------------------

    @Test
    void blankBecomesNullSoAnUnsetProtoStringIsAnUnsetColumn() {
        assertThat(DocumentRequests.blankToNull(null)).isNull();
        assertThat(DocumentRequests.blankToNull("")).isNull();
        assertThat(DocumentRequests.blankToNull("  ")).isNull();
        assertThat(DocumentRequests.blankToNull(" x ")).isEqualTo(" x ");
    }

    @Test
    void aMissingBlobIsFoundThroughItsWrappers() {
        BlobStore.BlobNotFoundException missing =
                new BlobStore.BlobNotFoundException("bucket/key is not there", null);
        assertThat(DocumentRequests.hasNotFoundCause(missing)).isTrue();
        assertThat(DocumentRequests.hasNotFoundCause(
                new RuntimeException("wrapped", new IllegalStateException("deeper", missing))))
                .isTrue();
        assertThat(DocumentRequests.hasNotFoundCause(new IOException("unrelated"))).isFalse();
        assertThat(DocumentRequests.hasNotFoundCause(null)).isFalse();
    }

    /**
     * A cause chain that loops has to end the walk. It is not a shape any library sets out
     * to build, but a handler that hangs on one takes a request thread with it, and the
     * guard costs nothing.
     */
    @Test
    void aCyclicCauseChainTerminates() {
        Throwable a = new RuntimeException("a");
        Throwable b = new RuntimeException("b", a);
        a.initCause(b);

        assertThat(DocumentRequests.hasNotFoundCause(a)).isFalse();
    }

    @Test
    void aSelfReferencingCauseTerminates() {
        Throwable self = new RuntimeException("self") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };
        assertThat(DocumentRequests.hasNotFoundCause(self)).isFalse();
    }

    @Test
    void theCurrentTimestampCarriesSecondsAndNanos() {
        var before = java.time.Instant.now();
        var stamp = DocumentRequests.timestampNow();
        var after = java.time.Instant.now();

        var read = java.time.Instant.ofEpochSecond(stamp.getSeconds(), stamp.getNanos());
        assertThat(read).isBetween(before, after);
    }
}
