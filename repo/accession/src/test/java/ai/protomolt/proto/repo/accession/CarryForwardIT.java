package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.ByteString;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * §5 and §15.1: a part that a save does not name is carried by reference through
 * any number of later revisions, without being read or written again, and keeps
 * the revision, owner, digest and schema binding it was written with.
 */
class CarryForwardIT {
    private static final DocumentPart CORE = DocumentPart.DOCUMENT_PART_CORE;
    private static final DocumentPart BLOBS = DocumentPart.DOCUMENT_PART_BLOBS;
    private static final DocumentPart PARSED = DocumentPart.DOCUMENT_PART_PARSED;

    private AccessionFixture f;
    private String account;
    private NodeAddress address;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-carry");
    }

    @AfterEach void close() { f.close(); }

    @Test void partsCarryByReferenceAcrossFiveRevisions() {
        var original = document(account, "doc-carry", "v1");
        f.saves.save(processAuthority(), create("op-1", address, original).build());
        var r1 = manifest(1);
        f.stack.reset();

        // r2: PARSED replaced. CORE and BLOBS carry from r1.
        var docling = Document.newBuilder().putParserResults("docling", ParserResult.newBuilder().setParserName("docling")
                .setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build();
        save("op-2", 2, docling, put(PARSED));
        var r2 = manifest(2);
        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertCarried(r2, r1, CORE, BLOBS);

        // r3: BLOBS emptied. Nothing uploaded; CORE carries from r1 and PARSED from r2.
        save("op-3", 3, null, write(BLOBS, WriteAction.WRITE_ACTION_EMPTY));
        var r3 = manifest(3);
        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertCarried(r3, r1, CORE);
        assertCarried(r3, r2, PARSED);
        assertThat(entry(r3, BLOBS).getEntry().getState()).isEqualTo(PartState.PART_STATE_EMPTY);
        assertThat(entry(r3, BLOBS).hasRef()).isFalse();

        // r4: CORE replaced without structured data. PARSED still carries from r2, the empty BLOBS from r3.
        var core = Document.newBuilder().setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v4")).build();
        save("op-4", 4, core, put(CORE));
        var r4 = manifest(4);
        assertThat(f.stack.puts.get()).isEqualTo(2);
        assertCarried(r4, r2, PARSED);
        assertCarried(r4, r3, BLOBS);

        // r5: BLOBS written again after being emptied is an addition.
        var blobs = Document.newBuilder().setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("b2")
                .setData(ByteString.copyFromUtf8("second raw bytes")))).build();
        save("op-5", 5, blobs, put(BLOBS));
        var r5 = manifest(5);
        assertThat(f.stack.puts.get()).isEqualTo(3);
        assertCarried(r5, r4, CORE);
        assertCarried(r5, r2, PARSED);
        assertThat(f.stack.opens.get()).as("no save read a carried part").isZero();

        // The events name only what changed in each revision.
        var events = f.catalog.events(address);
        assertThat(events).extracting(DocumentEvent::getRevision).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertChanges(events.get(1), PARSED, ChangeKind.CHANGE_KIND_REPLACED);
        assertChanges(events.get(2), BLOBS, ChangeKind.CHANGE_KIND_EMPTIED);
        assertChanges(events.get(3), CORE, ChangeKind.CHANGE_KIND_REPLACED);
        assertChanges(events.get(4), BLOBS, ChangeKind.CHANGE_KIND_ADDED);

        // The CORE occurrence of structured_data carries while CORE does, and leaves with it.
        for (var carried : new StoredManifest[]{r2, r3})
            assertThat(carried.getValidation().getResolvedOccurrencesList())
                    .isEqualTo(r1.getValidation().getResolvedOccurrencesList()).isNotEmpty();
        assertThat(r4.getValidation().getResolvedOccurrencesList()).isEmpty();

        // Exact reads compose each revision from the parts it references.
        var atR3 = read(3);
        assertThat(atR3.getSearchMetadata()).isEqualTo(original.getSearchMetadata());
        assertThat(atR3.getStructuredData()).isEqualTo(original.getStructuredData());
        assertThat(atR3.hasBlobBag()).isFalse();
        assertThat(atR3.getParserResultsMap()).isEqualTo(docling.getParserResultsMap());
        var atR5 = read(5);
        assertThat(atR5.getSearchMetadata().getTitle()).isEqualTo("v4");
        assertThat(atR5.hasStructuredData()).isFalse();
        assertThat(atR5.getBlobBag()).isEqualTo(blobs.getBlobBag());
        assertThat(atR5.getParserResultsMap()).isEqualTo(docling.getParserResultsMap());
        assertThat(read(1)).isEqualTo(original);
    }

    // Storage keys are never derived from content: rewriting identical bytes is a new object and a REPLACED change.
    @Test void rewritingIdenticalBytesIsANewObjectWithTheSameDigest() {
        var original = document(account, "doc-carry", "v1");
        f.saves.save(processAuthority(), create("op-1", address, original).build());
        var parsed = Document.newBuilder().putAllParserResults(original.getParserResultsMap()).build();
        save("op-2", 2, parsed, put(PARSED));
        var before = entry(manifest(1), PARSED);
        var after = entry(manifest(2), PARSED);
        assertThat(after.getEntry().getSha256()).isEqualTo(before.getEntry().getSha256());
        assertThat(after.getRef().getKey()).isNotEqualTo(before.getRef().getKey());
        assertThat(after.getEntry().getWrittenInRevision()).isEqualTo(2);
        assertChanges(f.catalog.events(address).get(1), PARSED, ChangeKind.CHANGE_KIND_REPLACED);
        assertThat(manifest(2).getValidation().getCandidateSha256())
                .as("the candidate is the same parts in the same states")
                .isEqualTo(manifest(1).getValidation().getCandidateSha256());
    }

    // ------------------------------------------------------------------ helpers

    private void save(String operation, long revision, Document content, PartWrite write) {
        var request = SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(revision - 1))
                .addWrites(write).setOwner(intakeOwner());
        if (content != null) request.setDocument(content);
        assertThat(f.saves.save(processAuthority(), request.build()).getAccession().getRevision()).isEqualTo(revision);
    }

    private static PartWrite write(DocumentPart part, WriteAction action) {
        return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(part)).setAction(action).build();
    }

    private StoredManifest manifest(long revision) { return f.catalog.manifest(address, revision).orElseThrow(); }

    private Document read(long revision) {
        return f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(revision)).build()).getDocument();
    }

    private static StoredPartEntry entry(StoredManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart() == part)
                .findFirst().orElseThrow();
    }

    /** The part is the one {@code from} recorded: same reference, digest, writer, revision and schema binding. */
    private static void assertCarried(StoredManifest manifest, StoredManifest from, DocumentPart... parts) {
        for (var part : parts) {
            assertThat(entry(manifest, part)).as(part + " in revision " + manifest.getRevision()).isEqualTo(entry(from, part));
            var binding = manifest.getValidation().getPartSchemasList().stream()
                    .filter(schema -> schema.getPart().getPart() == part).toList();
            var original = from.getValidation().getPartSchemasList().stream()
                    .filter(schema -> schema.getPart().getPart() == part).toList();
            assertThat(binding).isEqualTo(original);
        }
    }

    private static void assertChanges(DocumentEvent event, DocumentPart part, ChangeKind kind) {
        assertThat(event.getAccessioned().getChangesList()).singleElement().satisfies(change -> {
            assertThat(change.getSelector().getPart()).isEqualTo(part);
            assertThat(change.getKind()).isEqualTo(kind);
        });
    }
}
