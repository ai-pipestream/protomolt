package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * §15.18: "unauthorized attempts cannot claim an operation ID", with commit and
 * revocation exercised in both orderings. The commit transaction rechecks the
 * grant under a lock; recording a rejection does not, so a grant revoked after
 * authorization and before the rejection is recorded still claims the ID.
 */
class RejectionAfterRevocationIT {
    private static final String UNREGISTERED = "type.googleapis.com/unregistered.Thing";

    @Test void aGrantRevokedBeforeARejectionIsRecordedClaimsNoOperationId() throws Exception {
        try (var f = new AccessionFixture()) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "doc-revoked");
            f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-revoked", "v1", "chunker")).build());
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "chunker");
            var grant = f.stepGrant(account, credential, "chunker", false);
            var caller = writer(account, "chunker", credential, 1);

            // The registry lookup runs after authorization and before the outcome is recorded.
            var revoke = new AtomicReference<Runnable>(() -> f.catalog.revokeGrant(grant.getGrantId()));
            var registered = resolver();
            DocumentSchemaAdmission.Resolver revoking = selection -> {
                if (UNREGISTERED.equals(selection.typeUrl())) revoke.getAndSet(() -> {}).run();
                return registered.select(selection);
            };
            var saves = new DocumentSaves(f.catalog, f.drives, revoking, limits(), bytes -> () -> {}, Clock.systemUTC());

            var core = Document.newBuilder().setDocId("doc-revoked").setOwnership(ownership(account, "chunker"))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v2"))
                    .setStructuredData(Any.newBuilder().setTypeUrl(UNREGISTERED).setValue(ByteString.copyFromUtf8("x"))).build();
            var request = SaveDocumentRequest.newBuilder().setOperationId("op-1")
                    .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(core)
                    .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(stepOwner("chunker")).build();

            assertThatThrownBy(() -> saves.save(caller, request)).isInstanceOfSatisfying(AccessionException.class,
                    failure -> assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
            assertThat(f.catalog.outcome(account, "op-1")).as("the revoked writer claimed the operation ID").isEmpty();
        }
    }
}
