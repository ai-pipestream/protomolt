package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import io.grpc.Status;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * §8 and §15.13: part bytes are verified against their stack reference before they
 * are decoded or reused. Truncation and corruption are in DocumentSavesIT; these
 * cover a missing object, valid bytes from another object, appended bytes, a
 * corrupt source during a save, and a stack this host does not have.
 */
class VerifiedReadFailuresIT {
    private AccessionFixture f;
    private String account;
    private NodeAddress address;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-verify");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-verify", "v1")).build());
    }

    @AfterEach void close() { f.close(); }

    // Only the parts a read selects are fetched, so one lost object fails exactly the reads that need it.
    @Test void aMissingObjectIsDataLossForTheReadsThatSelectIt() {
        delete(ref(1, DocumentPart.DOCUMENT_PART_BLOBS).getKey());
        assertDataLoss(() -> read(null));
        assertDataLoss(() -> read(DocumentPart.DOCUMENT_PART_BLOBS));
        f.stack.reset();
        var parsed = read(DocumentPart.DOCUMENT_PART_PARSED);
        assertThat(parsed.getDocument().getParserResultsMap()).containsKey("tika");
        assertThat(f.stack.opened).containsOnlyKeys(ref(1, DocumentPart.DOCUMENT_PART_PARSED).getKey());
        f.stack.reset();
        assertThat(f.reads.getManifest(processAuthority(), GetManifestRequest.newBuilder().setAddress(address).build())
                .getManifest().getPartsCount()).isEqualTo(3);
        assertThat(f.stack.opens.get()).as("manifest inspection fetches no payloads").isZero();
    }

    // A well-formed fragment under the wrong key decodes cleanly; only the digest catches it.
    @Test void validBytesOfAnotherRevisionAreDataLoss() {
        var core = ai.protomolt.proto.repo.v1.Document.newBuilder().setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v2")).build();
        f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());
        overwrite(ref(1, DocumentPart.DOCUMENT_PART_CORE).getKey(), object(ref(2, DocumentPart.DOCUMENT_PART_CORE).getKey()));
        assertDataLoss(() -> f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()));
        assertThat(read(DocumentPart.DOCUMENT_PART_CORE).getDocument().getSearchMetadata().getTitle()).isEqualTo("v2");
    }

    @Test void appendedBytesAreDataLoss() {
        var key = ref(1, DocumentPart.DOCUMENT_PART_PARSED).getKey();
        var original = object(key);
        var longer = java.util.Arrays.copyOf(original, original.length + 4);
        overwrite(key, longer);
        assertDataLoss(() -> read(DocumentPart.DOCUMENT_PART_PARSED));
    }

    // A sourced part is read and verified during the save; a lost source fails the save and records nothing.
    @Test void aLostSourceObjectFailsTheSaveAsDataLossWithoutAnOutcome() {
        delete(ref(1, DocumentPart.DOCUMENT_PART_PARSED).getKey());
        f.stack.reset();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-source")
                .setTarget(DocumentTarget.newBuilder().setAddress(address.toBuilder().setGraphId("derived:" + account)))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setOwnership(ownership(account))
                        .setSearchMetadata(SearchMetadata.newBuilder().setTitle("derived")))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setFromAddress(address).setFromRevision(1))
                .setOwner(intakeOwner()).build();
        assertDataLoss(() -> f.saves.save(processAuthority(), request));
        assertThat(f.catalog.outcome(account, "op-source")).isEmpty();
        assertThat(f.stack.puts.get()).as("the source is verified before any upload").isZero();
    }

    // A revision whose stack this host does not configure is unavailable here, not lost.
    @Test void aStackThisHostDoesNotHaveIsUnavailableButTheManifestStillReads() {
        var elsewhere = new BlobStoreStack("stack-" + UUID.randomUUID(), new S3BlobStore(AccessionFixture.s3()), BUCKET,
                f.prefix);
        var drives = new DocumentSaves.Drives("default", Map.of("default", elsewhere));
        var reads = new DocumentReads(f.catalog, drives, limits(), bytes -> () -> {}, Clock.systemUTC());
        assertThatThrownBy(() -> reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .build())).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(reads.getManifest(processAuthority(), GetManifestRequest.newBuilder().setAddress(address).build())
                .getManifest().getRevision()).isEqualTo(1);
    }

    // ------------------------------------------------------------------ helpers

    private GetDocumentResponse read(DocumentPart part) {
        var request = GetDocumentRequest.newBuilder().setAddress(address);
        if (part != null) request.setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder().setPart(part)));
        return f.reads.getDocument(processAuthority(), request.build());
    }

    private StackRef ref(long revision, DocumentPart part) {
        return f.catalog.manifest(address, revision).orElseThrow().getPartsList().stream()
                .filter(p -> p.getEntry().getSelector().getPart() == part).findFirst().orElseThrow().getRef();
    }

    private static void assertDataLoss(Runnable call) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.DATA_LOSS));
    }

    private static void delete(String key) {
        AccessionFixture.s3().deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(key).build());
    }

    private static void overwrite(String key, byte[] bytes) {
        AccessionFixture.s3().putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromBytes(bytes));
    }

    private static byte[] object(String key) {
        return AccessionFixture.s3().getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build()).asByteArray();
    }
}
