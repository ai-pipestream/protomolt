package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import io.grpc.Status;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SliceOneFollowupsIT {
    @Test void emptyAndDeletedPartsKeepHistoryAndRequireValidReasons() throws Exception {
        try (var f = new AccessionFixture()) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "parts");
            var original = document(account, "parts", "original");
            f.saves.save(processAuthority(), create("create", address, original).build());
            f.stack.reset();
            var empty = f.saves.save(processAuthority(), change("empty", address, 1,
                    DocumentPart.DOCUMENT_PART_PARSED, WriteAction.WRITE_ACTION_EMPTY, ""));
            assertThat(empty.getAccession().getManifest().getPartsList()).filteredOn(p ->
                    p.getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED).singleElement().satisfies(p -> {
                        assertThat(p.getState()).isEqualTo(PartState.PART_STATE_EMPTY);
                        assertThat(p.getContentIncluded()).isFalse();
                    });
            assertThat(f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build())
                    .getDocument().getParserResultsCount()).isZero();
            assertThat(f.catalog.events(address).get(1).getAccessioned().getChanges(0).getKind())
                    .isEqualTo(ChangeKind.CHANGE_KIND_EMPTIED);
            var deleted = f.saves.save(processAuthority(), change("delete", address, 2,
                    DocumentPart.DOCUMENT_PART_PARSED, WriteAction.WRITE_ACTION_DELETE, "obsolete"));
            assertThat(deleted.getAccession().getManifest().getPartsList()).filteredOn(p ->
                    p.getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED).singleElement().satisfies(p -> {
                        assertThat(p.getState()).isEqualTo(PartState.PART_STATE_DELETED);
                        assertThat(p.getDeletedReason()).isEqualTo("obsolete");
                    });
            assertThat(f.catalog.events(address).get(2).getAccessioned().getChanges(0).getKind())
                    .isEqualTo(ChangeKind.CHANGE_KIND_DELETED);
            assertThat(f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                    .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getDocument()).isEqualTo(original);
            assertThat(f.stack.puts.get()).isZero();
            // A PUT with a reason carries real PARSED content, so the delete-reason rule is the only fault.
            var parsedContent = Document.newBuilder().setDocId("parts").putParserResults("tika",
                    original.getParserResultsOrThrow("tika")).build();
            var refusals = new java.util.LinkedHashMap<SaveDocumentRequest, String>();
            refusals.put(change("bad-delete", address, 3, DocumentPart.DOCUMENT_PART_PARSED, WriteAction.WRITE_ACTION_DELETE, ""),
                    "delete-reason");
            refusals.put(change("bad-empty", address, 3, DocumentPart.DOCUMENT_PART_PARSED, WriteAction.WRITE_ACTION_EMPTY, "reason"),
                    "delete-reason");
            refusals.put(change("bad-put", address, 3, DocumentPart.DOCUMENT_PART_PARSED, WriteAction.WRITE_ACTION_PUT, "reason")
                    .toBuilder().setDocument(parsedContent).build(), "delete-reason");
            refusals.put(change("empty-core", address, 3, DocumentPart.DOCUMENT_PART_CORE, WriteAction.WRITE_ACTION_EMPTY, ""),
                    "needs a PRESENT CORE part");
            refusals.put(change("delete-core", address, 3, DocumentPart.DOCUMENT_PART_CORE, WriteAction.WRITE_ACTION_DELETE, "reason"),
                    "needs a PRESENT CORE part");
            for (var refusal : refusals.entrySet()) {
                var request = refusal.getKey();
                assertThatThrownBy(() -> f.saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class, e -> {
                    assertThat(e.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(((SaveRejection) e.detail().orElseThrow()).getReason()).isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                    assertThat(e.getMessage()).as(request.getOperationId()).contains(refusal.getValue());
                });
                assertThat(f.catalog.outcome(account, request.getOperationId())).isEmpty();
                assertThat(f.stack.puts.get()).isZero();
            }
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(3);
        }
    }

    @Test void differentDocumentSourceIsUnrecordedAndCorrectedRequestCanCommit() throws Exception {
        try (var f = new AccessionFixture()) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "source");
            f.saves.save(processAuthority(), create("create", address, document(account, "source", "original")).build());
            f.stack.reset();
            var source = PartSource.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                    .setFromAddress(address(account, "different")).setFromRevision(1);
            var request = SaveDocumentRequest.newBuilder().setOperationId("copy").setTarget(DocumentTarget.newBuilder().setAddress(address))
                    .setExpected(at(1)).setOwner(intakeOwner()).addSources(source).build();
            assertThatThrownBy(() -> f.saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class, e -> {
                assertThat(e.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                assertThat(((SaveRejection) e.detail().orElseThrow()).getReason()).isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
            });
            assertThat(f.catalog.outcome(account, "copy")).isEmpty();
            assertThat(f.stack.puts.get()).isZero();
            var corrected = request.toBuilder().setSources(0, source.setFromAddress(address)).build();
            assertThat(f.saves.save(processAuthority(), corrected).getAccession().getRevision()).isEqualTo(2);
            assertThat(f.stack.puts.get()).isZero();
        }
    }

    @Test void selfNarrowingAclBlocksReadsWritesAndReplayButPreservesAuthorizedConfirmation() throws Exception {
        try (var f = new AccessionFixture()) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "acl");
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "writer");
            f.intakeGrant(account, credential, "ds-1", true);
            var caller = writer(account, "writer", credential, 1);
            f.saves.save(caller, create("create", address, document(account, "acl", "original", "writer", "reader")).build());
            var old = f.catalog.manifest(address, 1).orElseThrow();
            var request = SaveDocumentRequest.newBuilder().setOperationId("narrow").setTarget(DocumentTarget.newBuilder().setAddress(address))
                    .setExpected(at(1)).setOwner(intakeOwner()).addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                    .setDocument(Document.newBuilder().setDocId("acl").setOwnership(ownership(account, "reader"))).build();
            var committed = f.saves.save(caller, request);
            f.stack.reset();
            denied(() -> f.saves.save(caller, request));
            denied(() -> f.saves.save(caller, request.toBuilder().setOperationId("new-write").setExpected(at(2)).build()));
            denied(() -> f.reads.getDocument(caller, GetDocumentRequest.newBuilder().setAddress(address).build()));
            denied(() -> f.reads.getDocument(caller, GetDocumentRequest.newBuilder().setAddress(address)
                    .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()));
            denied(() -> f.reads.getManifest(caller, GetManifestRequest.newBuilder().setAddress(address).build()));
            denied(() -> f.reads.listRevisions(caller, ListRevisionsRequest.newBuilder().setAddress(address).setPageSize(10).build()));
            var lookup = GetOperationOutcomeRequest.newBuilder().setAccountId(account).setOperationId("narrow").build();
            denied(() -> f.reads.outcome(caller, lookup));
            assertThat(f.reads.outcome(processAuthority(), lookup).getCommitted().getAccession()).isEqualTo(committed.getAccession());
            var reader = writer(account, "reader", UUID.randomUUID(), 1);
            for (long revision : new long[]{1, 2}) {
                assertThat(f.reads.getDocument(reader, GetDocumentRequest.newBuilder().setAddress(address)
                        .setRevision(RevisionSelector.newBuilder().setRevision(revision)).build()).getManifest().getRevision()).isEqualTo(revision);
            }
            assertThat(f.catalog.manifest(address, 1).orElseThrow()).isEqualTo(old);
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
            assertThat(f.catalog.outcome(account, "new-write")).isEmpty();
            assertThat(f.stack.puts.get()).isZero();
        }
    }

    private static SaveDocumentRequest change(String id, NodeAddress address, long revision, DocumentPart part, WriteAction action, String reason) {
        return SaveDocumentRequest.newBuilder().setOperationId(id).setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(revision)).setOwner(intakeOwner()).addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(part)).setAction(action).setDeletedReason(reason)).build();
    }

    private static void denied(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(AccessionException.class,
                e -> assertThat(e.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
    }
}
