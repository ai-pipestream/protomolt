package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import io.grpc.Status;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * §15.18 and §15.22: authority that lapses after the save was authorized but before
 * its commit transaction. The upload case for grant revocation and rotation is in
 * DocumentSavesIT; these cover credential revocation, saves that upload nothing,
 * and a second grant that would still cover the owner.
 */
class RevocationBeforeCommitIT {
    private AccessionFixture f;
    private String account;
    private UUID credential;
    private NodeAddress address;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "chunker");
        address = address(account, "doc-lapse");
        f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-lapse", "v1", "chunker")).build());
        f.stack.reset();
    }

    @AfterEach void close() { f.close(); }

    @Test void credentialRevokedDuringUploadDeniesTheCommit() {
        f.stepGrant(account, credential, "chunker", false);
        var caller = writer(account, "chunker", credential, 1);
        f.stack.afterPut = () -> f.catalog.revokeCredential(ISSUER, credential);
        var request = titleSave("op-1", "v2");
        assertDenied(() -> f.saves.save(caller, request));
        assertThat(f.stack.puts.get()).as("the upload happened").isEqualTo(1);
        assertUnchanged("op-1");
        f.stack.afterPut = () -> {};
        assertDenied(() -> f.saves.save(caller, request));
        assertThat(f.stack.puts.get()).as("a revoked credential is refused before upload").isEqualTo(1);
    }

    // A sourced-only save uploads nothing; the source read is the window between authorization and commit.
    @Test void grantRevokedWhileASourceIsReadDeniesTheCommit() {
        var grant = f.stepGrant(account, credential, "chunker", false);
        var caller = writer(account, "chunker", credential, 1);
        f.saves.save(processAuthority(), titleSave("op-1", "v2").toBuilder().setOwner(intakeOwner()).build());
        f.stack.reset();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(2))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setFromAddress(address).setFromRevision(1))
                .setOwner(stepOwner("chunker")).build();
        f.stack.afterOpen = () -> f.catalog.revokeGrant(grant.getGrantId());
        assertDenied(() -> f.saves.save(caller, request));
        assertThat(f.stack.opens.get()).isEqualTo(1);
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.catalog.outcome(account, "op-2")).isEmpty();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
    }

    // §15.22: no live substitution. The commit rechecks the grant it was authorized under, so
    // revoking it fails the save even though another grant covers the owner; a retry selects that one.
    @Test void revokingTheSelectedGrantIsNotRescuedByAnotherAtCommit() {
        var first = installStepGrant("grant-a-" + UUID.randomUUID());
        var second = installStepGrant("grant-b-" + UUID.randomUUID());
        var caller = writer(account, "chunker", credential, 1);
        var request = titleSave("op-1", "v2");
        f.stack.afterPut = () -> f.catalog.revokeGrant(first.getGrantId());
        assertDenied(() -> f.saves.save(caller, request));
        assertUnchanged("op-1");

        f.stack.afterPut = () -> {};
        var saved = f.saves.save(caller, request);
        assertThat(saved.getAccession().getRevision()).isEqualTo(2);
        assertThat(saved.getAccession().getValidation().getWriterGrant().getWriterGrantId()).isEqualTo(second.getGrantId());
        assertThat(saved.getAccession().getValidation().getWriterGrant().getGrantSnapshot()).isEqualTo(second.toByteString());
    }

    // §15.23: overlapping grants select deterministically, by unsigned UTF-8 grant ID.
    @Test void overlappingGrantsSelectTheLowestGrantId() {
        var suffix = UUID.randomUUID();
        var later = installStepGrant("grant-z-" + suffix);
        var earlier = installStepGrant("grant-a-" + suffix);
        var caller = writer(account, "chunker", credential, 1);
        var saved = f.saves.save(caller, titleSave("op-1", "v2"));
        assertThat(saved.getAccession().getValidation().getWriterGrant().getWriterGrantId())
                .isEqualTo(earlier.getGrantId()).isNotEqualTo(later.getGrantId());
    }

    // ------------------------------------------------------------------ helpers

    private WriterGrant installStepGrant(String grantId) {
        var grant = WriterGrant.newBuilder().setGrantId(grantId).setAccountId(account)
                .setCredential(WriterCredential.newBuilder().setIssuer(ISSUER).setCredentialId(credential.toString()))
                .setStep(StepWriterScope.newBuilder().setModuleId("chunker")).build();
        f.catalog.installGrant(grant);
        return grant;
    }

    private SaveDocumentRequest titleSave(String operation, String title) {
        var core = Document.newBuilder().setDocId(address.getDocId()).setOwnership(ownership(account, "chunker"))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title)).build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(stepOwner("chunker")).build();
    }

    private void assertUnchanged(String operation) {
        assertThat(f.catalog.outcome(account, operation)).as("an unauthorized attempt claims nothing").isEmpty();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
        assertThat(f.catalog.events(address)).hasSize(1);
    }

    private static void assertDenied(Runnable call) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(AccessionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
        });
    }
}
