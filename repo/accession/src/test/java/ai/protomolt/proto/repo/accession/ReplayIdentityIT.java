package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.Any;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §7 Outcomes and retry identity, §15.4 and §15.20: what a retried operation ID returns. */
class ReplayIdentityIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    // A replay answers from the catalog: no upload, no stack read, no new revision or event.
    @Test void aReplayUploadsAndReadsNothing() {
        var address = address(account, "doc-quiet");
        var request = create("op-1", address, document(account, "doc-quiet", "v1")).build();
        var saved = f.saves.save(processAuthority(), request);
        var emptied = f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setOwner(intakeOwner())
                .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setAction(WriteAction.WRITE_ACTION_EMPTY)).build());
        f.stack.reset();

        assertThat(f.saves.save(processAuthority(), request)).isEqualTo(saved.toBuilder().setReplayed(true).build());
        var retriedEmpty = SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setOwner(intakeOwner())
                .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setAction(WriteAction.WRITE_ACTION_EMPTY)).build();
        assertThat(f.saves.save(processAuthority(), retriedEmpty).getAccession()).isEqualTo(emptied.getAccession());
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.stack.opens.get()).isZero();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
        assertThat(f.catalog.events(address)).hasSize(2);
    }

    // Outcomes are unique by account and operation ID, so another account's identical ID is unrelated.
    @Test void operationIdsAreScopedByAccount() {
        var other = "acct-" + UUID.randomUUID();
        var first = f.saves.save(processAuthority(), create("op-shared", address(account, "doc-a"),
                document(account, "doc-a", "a")).build());
        var second = f.saves.save(processAuthority(), create("op-shared", address(other, "doc-b"),
                document(other, "doc-b", "b")).build());
        assertThat(first.getReplayed()).isFalse();
        assertThat(second.getReplayed()).isFalse();
        assertThat(lookup(account, "op-shared").getCommitted().getAccession()).isEqualTo(first.getAccession());
        assertThat(lookup(other, "op-shared").getCommitted().getAccession()).isEqualTo(second.getAccession());
    }

    // The intent normalizes an unspecified validation mode to TYPED, and keeps write order.
    @Test void unspecifiedModeIsTypedButWriteOrderIsIdentity() {
        var address = address(account, "doc-norm");
        var request = create("op-1", address, document(account, "doc-norm", "v1")).build();
        var saved = f.saves.save(processAuthority(), request);
        var typed = request.toBuilder().setValidationMode(ValidationMode.VALIDATION_MODE_TYPED).build();
        assertThat(f.saves.save(processAuthority(), typed).getAccession()).isEqualTo(saved.getAccession());

        var reordered = request.toBuilder().clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS)).addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), reordered)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.ALREADY_EXISTS);
                    var reuse = (OperationReuse) failure.detail().orElseThrow();
                    assertThat(reuse.getRecordedIntentSha256())
                            .isEqualTo(f.catalog.outcome(account, "op-1").orElseThrow().intentSha256());
                    assertThat(reuse.getRequestIntentSha256()).isNotEqualTo(reuse.getRecordedIntentSha256());
                });
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
    }

    // §15.20: replay needs current read access, not a write grant, not even a registered credential.
    @Test void aListedReaderReplaysWithoutAnyGrant() {
        var address = address(account, "doc-reader");
        var request = create("op-1", address, document(account, "doc-reader", "v1", "crawler", "auditor")).build();
        var saved = f.saves.save(processAuthority(), request);
        var auditor = new RepositoryCaller("auditor", false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("auditor").build()));
        var replayed = f.saves.save(auditor, request);
        assertThat(replayed.getReplayed()).isTrue();
        assertThat(replayed.getAccession()).isEqualTo(saved.getAccession());

        var stranger = new RepositoryCaller("stranger", false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("stranger").build()));
        assertThatThrownBy(() -> f.saves.save(stranger, request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
        var elsewhere = new RepositoryCaller("auditor", false, Set.of("acct-" + UUID.randomUUID()),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("auditor").build()));
        assertThatThrownBy(() -> f.saves.save(elsewhere, request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
    }

    // A recorded rejection is terminal for its operation ID: a corrected request needs a new ID.
    @Test void aRejectedOperationIdIsNotReusableForACorrectedRequest() {
        var address = address(account, "doc-fix");
        var invalid = create("op-bad", address, document(account, "doc-fix", "v1").toBuilder()
                .setStructuredData(Any.pack(EmbeddingSetKey.newBuilder().setSegmentationId("s").build())).build()).build();
        assertCode(() -> f.saves.save(processAuthority(), invalid), Status.Code.INVALID_ARGUMENT);
        var corrected = create("op-bad", address, document(account, "doc-fix", "v1")).build();
        assertCode(() -> f.saves.save(processAuthority(), corrected), Status.Code.ALREADY_EXISTS);
        assertThat(f.catalog.outcome(account, "op-bad").orElseThrow().outcome().hasRejection()).isTrue();
        assertThat(f.stack.puts.get()).isZero();

        var fresh = corrected.toBuilder().setOperationId("op-good").build();
        assertThat(f.saves.save(processAuthority(), fresh).getAccession().getRevision()).isEqualTo(1);
    }

    // Concurrent retries of a rejected save record one outcome and both receive it.
    @Test void concurrentRetriesOfARejectionShareOneOutcome() throws Exception {
        var address = address(account, "doc-twin-reject");
        var invalid = create("op-twin", address, document(account, "doc-twin-reject", "v1").toBuilder()
                .setStructuredData(Any.pack(EmbeddingSetKey.newBuilder().setSegmentationId("s").build())).build()).build();
        var results = race(2, index -> f.saves.save(processAuthority(), invalid));
        assertThat(results).allSatisfy(result -> {
            assertThat(result).isInstanceOf(AccessionException.class);
            var failure = (AccessionException) result;
            assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        });
        assertThat(results.stream().map(result -> ((AccessionException) result).detail().orElseThrow()).distinct()).hasSize(1);
        assertThat(f.catalog.current(address)).isEmpty();
    }

    // §7: concurrent same-ID saves with different intent commit once; the other fails as reuse, never ABORTED.
    @Test void concurrentSameIdWithDifferentIntentCommitsOnceAndRefusesTheOther() throws Exception {
        var address = address(account, "doc-twin");
        f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-twin", "v1")).build());
        var results = race(2, index -> f.saves.save(processAuthority(), titleSave("op-twin", address, 1, "title " + index)));
        assertThat(results).filteredOn(result -> result instanceof SaveDocumentResponse).hasSize(1);
        assertThat(results).filteredOn(result -> result instanceof AccessionException).singleElement()
                .satisfies(result -> assertThat(((AccessionException) result).code()).isEqualTo(Status.Code.ALREADY_EXISTS));
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
        assertThat(f.catalog.events(address)).hasSize(2);
    }

    // ------------------------------------------------------------------ helpers

    private GetOperationOutcomeResponse lookup(String accountId, String operation) {
        return f.reads.outcome(processAuthority(), GetOperationOutcomeRequest.newBuilder().setAccountId(accountId)
                .setOperationId(operation).build());
    }

    private static SaveDocumentRequest titleSave(String operation, NodeAddress address, long expected, String title) {
        var core = Document.newBuilder().setDocId(address.getDocId()).setOwnership(ownership(address.getAccountId()))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title)).build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }

    private static void assertCode(Runnable call, Status.Code code) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(code));
    }

    @FunctionalInterface private interface Indexed { Object call(int index) throws Exception; }

    private static List<Object> race(int count, Indexed call) throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var start = new CountDownLatch(1);
        try {
            var futures = new ArrayList<Future<Object>>();
            for (int i = 0; i < count; i++) {
                int index = i;
                futures.add(executor.submit((Callable<Object>) () -> {
                    start.await();
                    try {
                        return call.call(index);
                    } catch (AccessionException failure) {
                        return failure;
                    }
                }));
            }
            start.countDown();
            var results = new ArrayList<Object>();
            for (var future : futures) results.add(future.get());
            return results;
        } finally {
            executor.shutdownNow();
        }
    }
}
