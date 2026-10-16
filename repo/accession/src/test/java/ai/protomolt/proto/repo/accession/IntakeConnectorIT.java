package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.SaveRejection;
import io.grpc.Status;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression from Fable's slice 1 review, issue #332. */
class IntakeConnectorIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void processAuthorityIntakeOwnerMustNameTheDocumentsConnector() {
        var address = address(account, "doc-connector");
        var document = document(account, "doc-connector", "c").toBuilder()
                .setOwnership(ownership(account).toBuilder().setConnectorId("conn-1")).build();
        var otherConnector = SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder()
                .setDatasourceId("ds-1").setConnectorId("conn-2")).build();

        assertThatThrownBy(() -> f.saves.save(processAuthority(),
                create("op-connector", address, document).setOwner(otherConnector).build()))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                            .isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
                });
        assertThat(f.catalog.outcome(account, "op-connector")).isEmpty();
        assertThat(f.catalog.current(address)).isEmpty();
    }

    @Test void namedConnectorNeedsAMatchAndDeniedOperationCanBeCorrected() {
        for (boolean documentHasConnector : new boolean[] {false, true}) {
            var address = address(account, "connector-" + documentHasConnector);
            var own = ownership(account).toBuilder();
            if (documentHasConnector) own.setConnectorId("conn-1");
            var doc = document(account, address.getDocId(), "connector").toBuilder().setOwnership(own).build();
            var mismatched = SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder()
                    .setDatasourceId("ds-1").setConnectorId("conn-2")).build();
            var request = create("connector-" + documentHasConnector, address, doc).setOwner(mismatched).build();
            int puts = f.stack.puts.get();
            assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
                        assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                                .isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
                    });
            assertThat(f.stack.puts.get()).isEqualTo(puts);
            assertThat(f.catalog.outcome(account, request.getOperationId())).isEmpty();
            assertThat(f.catalog.current(address)).isEmpty();
            var matched = IntakeOrigin.newBuilder().setDatasourceId("ds-1");
            if (documentHasConnector) matched.setConnectorId("conn-1");
            var corrected = request.toBuilder().setOwner(SaveOwner.newBuilder().setIntake(matched)).build();
            var result = f.saves.save(processAuthority(), corrected);
            assertThat(result.getAccession().getRevision()).isEqualTo(1);
            assertThat(result.getAccession().getOwner()).isEqualTo(corrected.getOwner());
        }
    }

    @Test void omittedOwnerConnectorCanWriteADocumentWithAConnector() {
        var address = address(account, "optional-connector");
        var doc = document(account, address.getDocId(), "optional").toBuilder()
                .setOwnership(ownership(account).toBuilder().setConnectorId("conn-1")).build();
        assertThat(f.saves.save(processAuthority(), create("optional", address, doc).build())
                .getAccession().getRevision()).isEqualTo(1);
    }

}
