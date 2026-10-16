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
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * §7 and §15.20: "Current access is still checked" when a recorded outcome is
 * returned. For an accession it is. A recorded rejection of a save to an existing
 * document goes to any caller bound to the account, including one the document's
 * ACL excludes, with its violations, field paths and rule messages.
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
}
