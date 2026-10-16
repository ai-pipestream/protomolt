package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import io.grpc.Status;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * #332 from the outside, on saves that do not write CORE: the connector is the stored
 * document's, and the owner check runs before the revision check and before upload.
 */
class IntakeConnectorEdgesIT {
    private AccessionFixture f;
    private String account;
    private NodeAddress address;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-conn");
        var doc = document(account, "doc-conn", "v1").toBuilder()
                .setOwnership(ownership(account).toBuilder().setConnectorId("conn-1")).build();
        f.saves.save(processAuthority(), create("op-0", address, doc).setOwner(owner("conn-1")).build());
        f.stack.reset();
    }

    @AfterEach void close() { f.close(); }

    @Test void aParsedOnlySaveNamingAnotherConnectorIsDeniedAndClaimsNothing() {
        var request = parsedSave("op-1", 1, owner("conn-2"));
        assertDenied(request);
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.catalog.outcome(account, "op-1")).isEmpty();

        var matched = request.toBuilder().setOwner(owner("conn-1")).build();
        var saved = f.saves.save(processAuthority(), matched);
        assertThat(saved.getAccession().getRevision()).isEqualTo(2);
        assertThat(saved.getAccession().getManifest().getPartsList()).filteredOn(p ->
                p.getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED).singleElement()
                .satisfies(p -> assertThat(p.getOwner()).isEqualTo(owner("conn-1")));
        var unnamed = f.saves.save(processAuthority(), parsedSave("op-2", 2, intakeOwner()));
        assertThat(unnamed.getAccession().getRevision()).as("an owner without a connector still writes").isEqualTo(3);
    }

    // The owner is checked before the expected revision, so a stale request learns nothing about the document.
    @Test void aMismatchedConnectorIsDeniedEvenWithAStaleExpectedRevision() {
        assertDenied(parsedSave("op-stale", 7, owner("conn-2")));
        assertThat(f.catalog.outcome(account, "op-stale")).isEmpty();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), parsedSave("op-stale", 7, owner("conn-1"))))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.ABORTED));
    }

    // A matching connector cannot be used to change the stored one with a CORE PUT.
    @Test void aCorePutCannotMoveTheDocumentToTheOwnersConnector() {
        var core = ai.protomolt.proto.repo.v1.Document.newBuilder()
                .setOwnership(ownership(account).toBuilder().setConnectorId("conn-2")).build();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-move")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(owner("conn-2")).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(f.catalog.outcome(account, "op-move")).isEmpty();
        assertThat(f.catalog.manifest(address, 1).orElseThrow().getOwnership().getConnectorId()).isEqualTo("conn-1");
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
    }

    private SaveDocumentRequest parsedSave(String operation, long expected, SaveOwner owner) {
        var parsed = ai.protomolt.proto.repo.v1.Document.newBuilder().putParserResults("docling", ParserResult.newBuilder()
                .setParserName("docling").setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected)).setDocument(parsed)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).setOwner(owner).build();
    }

    private static SaveOwner owner(String connector) {
        return SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-1").setConnectorId(connector))
                .build();
    }

    private void assertDenied(SaveDocumentRequest request) {
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                            .isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
                });
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
    }
}
