package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The v2 prerequisites 6 and 7 of the archive over v2 documents: a save under
 * {@code UNCHANGED_POLICY_RECORD_OUTCOME} that changes nothing records only its
 * operation outcome, bound to the expected revision; and every recorded outcome
 * keeps the request metadata of the save that recorded it, rejections included.
 */
class UnchangedOutcomeIT {
    private AccessionFixture f;
    private String account;
    private NodeAddress address;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-unchanged");
    }

    @AfterEach void close() { f.close(); }

    private Document document() {
        return AccessionFixture.document(account, "doc-unchanged", "title");
    }

    private SaveDocumentRequest.Builder again(String operation, long expected, Document document) {
        return create(operation, address, document).setExpected(at(expected))
                .setUnchanged(UnchangedPolicy.UNCHANGED_POLICY_RECORD_OUTCOME).putMetadata("intent", operation);
    }

    private RecordedOutcome outcome(String operation) {
        return f.catalog.outcome(account, operation).orElseThrow().outcome();
    }

    @Test void aSaveThatChangesNothingRecordsOnlyItsOutcome() {
        var first = f.saves.save(processAuthority(), create("op-1", address, document())
                .putMetadata("intent", "first").build());
        assertThat(outcome("op-1").getRequestMetadataMap()).containsExactly(java.util.Map.entry("intent", "first"));
        f.stack.reset();

        var unchanged = f.saves.save(processAuthority(), again("op-2", 1, document()).build());
        assertThat(unchanged.getUnchanged()).isTrue();
        assertThat(unchanged.getReplayed()).isFalse();
        assertThat(unchanged.getAccession()).isEqualTo(first.getAccession());
        assertThat(f.stack.puts.get()).as("no fragment or blob is written").isZero();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
        assertThat(f.catalog.events(address)).hasSize(1);
        var recorded = outcome("op-2");
        assertThat(recorded.getUnchanged()).isEqualTo(first.getAccession());
        assertThat(recorded.hasAccession()).isFalse();
        assertThat(recorded.getRequestMetadataMap()).containsEntry("intent", "op-2");

        var retry = f.saves.save(processAuthority(), again("op-2", 1, document()).build());
        assertThat(retry.getReplayed()).isTrue();
        assertThat(retry.getUnchanged()).isTrue();
        assertThat(retry.getAccession()).isEqualTo(first.getAccession());

        // The policy is part of the intent: the same ID without it is another request.
        var withoutPolicy = again("op-2", 1, document()).clearUnchanged().build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), withoutPolicy))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.ALREADY_EXISTS));
    }

    @Test void anyDifferenceOrNoPolicyMakesARevision() {
        f.saves.save(processAuthority(), create("op-1", address, document()).build());
        var plain = f.saves.save(processAuthority(), again("op-2", 1, document()).clearUnchanged().build());
        assertThat(plain.getUnchanged()).isFalse();
        assertThat(plain.getAccession().getRevision()).isEqualTo(2);

        var changedCore = f.saves.save(processAuthority(), again("op-3", 2,
                AccessionFixture.document(account, "doc-unchanged", "another title")).build());
        assertThat(changedCore.getUnchanged()).isFalse();
        assertThat(changedCore.getAccession().getRevision()).isEqualTo(3);

        var otherBlob = document().toBuilder().setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder()
                .addBlob(Blob.newBuilder().setBlobId("b1").setData(ByteString.copyFromUtf8("other bytes")))));
        var changedBlob = f.saves.save(processAuthority(), again("op-4", 3, otherBlob.build()).build());
        assertThat(changedBlob.getUnchanged()).isFalse();
        assertThat(changedBlob.getAccession().getRevision()).isEqualTo(4);

        // A DELETE or EMPTY write is never unchanged, even of an equal part.
        var emptied = again("op-5", 4, otherBlob.clearParserResults().build())
                .clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS))
                .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setAction(WriteAction.WRITE_ACTION_EMPTY))
                .build();
        assertThat(f.saves.save(processAuthority(), emptied).getAccession().getRevision()).isEqualTo(5);
        assertThat(f.catalog.events(address)).hasSize(5);
    }

    /**
     * Pinned for prerequisite 6: with CORE and PARSED present at revision 1, a save
     * at expected revision 1 whose CORE PUT is identical but which also deletes or
     * empties PARSED changes part state, so it commits revision 2 and never takes
     * the unchanged path, even under {@code UNCHANGED_POLICY_RECORD_OUTCOME}.
     */
    @Test void aDeleteBesideAnIdenticalPutCommitsARevision() {
        assertPartStateChangeCommits(WriteAction.WRITE_ACTION_DELETE, "obsolete", PartState.PART_STATE_DELETED);
    }

    @Test void anEmptyBesideAnIdenticalPutCommitsARevision() {
        assertPartStateChangeCommits(WriteAction.WRITE_ACTION_EMPTY, "", PartState.PART_STATE_EMPTY);
    }

    private void assertPartStateChangeCommits(WriteAction action, String reason, PartState state) {
        var first = f.saves.save(processAuthority(), create("op-1", address, document()).build());
        assertThat(first.getAccession().getManifest().getPartsList()).filteredOn(p ->
                p.getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED).singleElement()
                .satisfies(p -> assertThat(p.getState()).isEqualTo(PartState.PART_STATE_PRESENT));
        var coreOnly = document().toBuilder().clearBlobBag().clearParserResults().build();

        // Control: the identical CORE PUT on its own is unchanged, so only the PARSED write below differs.
        var control = f.saves.save(processAuthority(), again("op-control", 1, coreOnly)
                .clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build());
        assertThat(control.getUnchanged()).isTrue();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);

        var request = again("op-2", 1, coreOnly).clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_PARSED)).setAction(action).setDeletedReason(reason))
                .build();
        var saved = f.saves.save(processAuthority(), request);
        assertThat(saved.getUnchanged()).as("deduplicated").isFalse();
        assertThat(saved.getReplayed()).isFalse();
        assertThat(saved.getAccession().getRevision()).isEqualTo(2);
        assertThat(saved.getAccession().getManifest().getPartsList()).filteredOn(p ->
                p.getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED).singleElement()
                .satisfies(p -> assertThat(p.getState()).isEqualTo(state));
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
        assertThat(f.catalog.events(address)).hasSize(2);
        var recorded = outcome("op-2");
        assertThat(recorded.hasUnchanged()).isFalse();
        assertThat(recorded.getAccession().getRevision()).isEqualTo(2);

        var retry = f.saves.save(processAuthority(), request);
        assertThat(retry.getReplayed()).isTrue();
        assertThat(retry.getUnchanged()).isFalse();
        assertThat(retry.getAccession().getRevision()).isEqualTo(2);
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
    }

    @Test void aStaleExpectedRevisionIsAbortedAndRecordsNothing() {
        f.saves.save(processAuthority(), create("op-1", address, document()).build());
        f.saves.save(processAuthority(), again("op-2", 1, AccessionFixture.document(account, "doc-unchanged", "v2")).build());
        // Equal to revision 1, but revision 2 is current.
        assertThatThrownBy(() -> f.saves.save(processAuthority(), again("op-3", 1, document()).build()))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.ABORTED));
        assertThat(f.catalog.outcome(account, "op-3")).isEmpty();
    }

    @Test void aRecordedRejectionKeepsItsRequestMetadata() {
        var unknownContent = document().toBuilder()
                .setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder().addField(4000,
                        com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        var request = create("op-rejected", address, unknownContent).putMetadata("intent", "rejected").build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        var recorded = outcome("op-rejected");
        assertThat(recorded.getRejection().getReason()).isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
        assertThat(recorded.getRequestMetadataMap()).containsExactly(java.util.Map.entry("intent", "rejected"));
        assertThat(List.of(recorded)).allSatisfy(o -> assertThat(DocumentSaves.VALIDATOR.validate(o).valid()).isTrue());
    }
}
