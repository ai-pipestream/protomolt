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
import com.google.protobuf.Any;
import io.grpc.Status;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * §7 and §15.20: "Current access is still checked" when a recorded outcome is
 * returned. Rejection details and intent digests require the same current access
 * boundary; rejected creates additionally retain their authenticated creator.
 */
class RejectionOutcomeAccessIT {
    private AccessionFixture f;
    private String account;
    private NodeAddress address;
    private RepositoryCaller crawler;
    private RepositoryCaller outsider;
    private SaveDocumentRequest invalid;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-private");
        f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-private", "v1", "crawler")).build());
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "crawler");
        f.intakeGrant(account, credential, "ds-1", false);
        crawler = writer(account, "crawler", credential, 1);
        outsider = new RepositoryCaller("outsider", false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("outsider").build()));
        var core = Document.newBuilder().setDocId("doc-private").setOwnership(ownership(account, "crawler"))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("confidential title"))
                .setStructuredData(Any.pack(EmbeddingSetKey.newBuilder().setSegmentationId("s").build())).build();
        invalid = SaveDocumentRequest.newBuilder().setOperationId("op-invalid")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
        assertThatThrownBy(() -> f.saves.save(crawler, invalid)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(((SaveRejection) failure.detail().orElseThrow()).getViolationsList()).isNotEmpty());
        assertThatThrownBy(() -> f.reads.getManifest(outsider, GetManifestRequest.newBuilder().setAddress(address).build()))
                .as("the outsider cannot read the document").isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
    }

    @AfterEach void close() { f.close(); }

    @Test void anOutcomeLookupDoesNotShowAnotherDocumentsRejectionToACallerOutsideItsAcl() {
        var lookup = GetOperationOutcomeRequest.newBuilder().setAccountId(account).setOperationId("op-invalid").build();
        assertThat(f.reads.outcome(crawler, lookup).getCommitted().getRejection().getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThatThrownBy(() -> f.reads.outcome(outsider, lookup)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
    }

    @Test void aReplayedRejectionNeedsCurrentReadAccessLikeAReplayedAccession() {
        assertThatThrownBy(() -> f.saves.save(crawler, invalid)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> f.saves.save(outsider, invalid)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(failure.detail()).isEmpty();
                });
    }
    @Test void rejectedCreateUsesCredentialIdentityAndCurrentGenerationWithoutRequiringTheGrant() {
        var binding = crawler.credentialBinding().orElseThrow();
        var grant = f.intakeGrant(account, binding.credentialId(), "ds-1", true);
        var target = address(account, "never-created");
        var request = create("op-create-reject", target,
                invalid.getDocument().toBuilder().setDocId(target.getDocId()).build())
                .clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build();
        assertRejected(crawler, request);
        assertThat(f.catalog.current(target)).isEmpty();
        assertLookup(crawler, request);
        assertLookup(processAuthority(), request);
        assertRejected(processAuthority(), request);
        f.catalog.revokeGrant(grant.getGrantId());
        assertRejected(crawler, request);
        assertLookup(crawler, request);

        // A principal name is not a credential identity, nor is an account membership.
        var otherId = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, otherId, "crawler");
        assertDenied(writer(account, "crawler", otherId, 1), request);
        assertDenied(outsider, request);
        var otherIssuer = new RepositoryCaller(crawler.principalName(), false, crawler.accountIds(), crawler.identities(),
                java.util.Optional.of(new ai.protomolt.proto.repo.spi.RepositoryCredentialBinding(
                        "another-issuer", binding.credentialId(), 1)));
        assertDenied(otherIssuer, request);

        f.catalog.rotateCredential(ISSUER, binding.credentialId());
        assertDenied(crawler, request);
        var rotated = writer(account, "crawler", binding.credentialId(), 2);
        assertLookup(rotated, request);
        assertRejected(rotated, request);
        f.catalog.revokeCredential(ISSUER, binding.credentialId());
        assertDenied(rotated, request);
        assertLookup(processAuthority(), request);
    }

    @Test void subsequentlyCreatedTargetUsesItsCurrentAclInsteadOfCreatorIdentity() {
        var binding = crawler.credentialBinding().orElseThrow();
        f.intakeGrant(account, binding.credentialId(), "ds-1", true);
        var target = address(account, "later-created");
        var request = create("op-later-reject", target,
                invalid.getDocument().toBuilder().setDocId(target.getDocId()).build())
                .clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build();
        assertRejected(crawler, request);
        f.saves.save(processAuthority(), create("op-later-create", target,
                document(account, target.getDocId(), "private", "outsider")).build());
        assertDenied(crawler, request);
        assertLookup(outsider, request);
        assertRejected(outsider, request);
    }

    @Test void unboundLegacyRejectionIsProcessAuthorityOnly() throws Exception {
        try (var connection = java.sql.DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var update = connection.prepareStatement("INSERT INTO catalog_operation_outcomes (account_id, operation_id, intent_sha256, outcome, recorded_at) SELECT account_id, 'op-legacy', intent_sha256, outcome, recorded_at FROM catalog_operation_outcomes WHERE account_id=? AND operation_id=?")) {
            update.setString(1, account);
            update.setString(2, "op-invalid");
            assertThat(update.executeUpdate()).isEqualTo(1);
        }
        var legacy = invalid.toBuilder().setOperationId("op-legacy").build();
        assertDenied(crawler, legacy);
        assertLookup(processAuthority(), legacy);
        assertRejected(processAuthority(), legacy);
    }

    @Test void rejectedCreateByProcessAuthorityHasNoImplicitCredentialOwner() {
        var target = address(account, "process-reject");
        var request = create("op-process-reject", target,
                invalid.getDocument().toBuilder().setDocId(target.getDocId()).build())
                .clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build();
        assertRejected(processAuthority(), request);
        assertLookup(processAuthority(), request);
        assertDenied(crawler, request);
    }

    @Test void deniedCallerCannotUseDifferentIntentToReadRecordedDigest() {
        var changed = invalid.toBuilder().setDocument(invalid.getDocument().toBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("changed"))).build();
        assertDenied(outsider, changed);
        assertThatThrownBy(() -> f.saves.save(crawler, changed)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.ALREADY_EXISTS));
    }

    private void assertLookup(RepositoryCaller caller, SaveDocumentRequest request) {
        assertThat(f.reads.outcome(caller, GetOperationOutcomeRequest.newBuilder().setAccountId(account)
                .setOperationId(request.getOperationId()).build()).getCommitted().getRejection().getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
    }

    private void assertRejected(RepositoryCaller caller, SaveDocumentRequest request) {
        assertThatThrownBy(() -> f.saves.save(caller, request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    private void assertDenied(RepositoryCaller caller, SaveDocumentRequest request) {
        assertThatThrownBy(() -> f.saves.save(caller, request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> { assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED); assertThat(failure.detail()).isEmpty(); });
        assertThatThrownBy(() -> f.reads.outcome(caller, GetOperationOutcomeRequest.newBuilder().setAccountId(account)
                .setOperationId(request.getOperationId()).build())).isInstanceOfSatisfying(AccessionException.class,
                failure -> { assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED); assertThat(failure.detail()).isEmpty(); });
    }

}
