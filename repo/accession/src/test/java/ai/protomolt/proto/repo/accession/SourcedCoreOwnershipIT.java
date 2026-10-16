package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartSource;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.SaveRejection;
import io.grpc.Status;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Decision 47 regressions, based on Fable’s ReviewRound2IT evidence. */
class SourcedCoreOwnershipIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    // §5 and decision 30: the revision's ownership governs access. A CORE part sourced from an
    // earlier revision must not return an ACL other than the one the revision is checked against.
    @Test void sourcedCoreReturnsTheOwnershipItsRevisionIsGovernedBy() {
        var address = address(account, "doc-core-source");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-core-source", "v1", "crawler")).build());
        var widened = ownership(account, "crawler", "reader");
        f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1))
                .setDocument(Document.newBuilder().setDocId("doc-core-source").setOwnership(widened)
                        .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v2")))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());

        int puts = f.stack.puts.get();
        var sourced = f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-3")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(2))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setFromAddress(address).setFromRevision(1))
                .setOwner(intakeOwner()).build());
        assertThat(sourced.getAccession().getRevision()).isEqualTo(3);

        assertThat(f.stack.puts.get()).isEqualTo(puts);
        assertThat(f.catalog.manifest(address, 2).orElseThrow().getOwnership()).isEqualTo(widened);
        var governing = f.catalog.manifest(address, 3).orElseThrow().getOwnership();
        assertThat(governing).isEqualTo(ownership(account, "crawler"));
        var reader = new RepositoryCaller("reader", false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("reader").build()));
        assertThatThrownBy(() -> f.reads.getDocument(reader, GetDocumentRequest.newBuilder().setAddress(address).build()))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
        var crawler = new RepositoryCaller("crawler", false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("crawler").build()));
        var read = f.reads.getDocument(crawler, GetDocumentRequest.newBuilder().setAddress(address).build());
        assertThat(read.getManifest().getRevision()).as("the remaining reader is admitted by the stored ownership").isEqualTo(3);
        assertThat(read.getDocument().getOwnership()).as("the returned ownership is the one access was checked against")
                .isEqualTo(governing);
    }

    @Test void sourcedCoreCannotChangeDatasourceOrConnector() {
        for (boolean connector : new boolean[] {false, true}) {
            String docId = "fixed-" + connector;
            var target = address(account, docId);
            var source = target.toBuilder().setGraphAddressId("other").build();
            var changed = ownership(account).toBuilder();
            if (connector) changed.setConnectorId("other-connector");
            else changed.setDatasourceId("other-datasource");
            var origin = IntakeOrigin.newBuilder().setDatasourceId(changed.getDatasourceId());
            if (changed.hasConnectorId()) origin.setConnectorId(changed.getConnectorId());
            f.saves.save(processAuthority(), create("target-" + connector, target,
                    document(account, docId, "target")).build());
            f.saves.save(processAuthority(), create("source-" + connector, source,
                    document(account, docId, "source").toBuilder().setOwnership(changed).build())
                    .setOwner(SaveOwner.newBuilder().setIntake(origin)).build());
            int puts = f.stack.puts.get();
            var request = SaveDocumentRequest.newBuilder().setOperationId("copy-" + connector)
                    .setTarget(DocumentTarget.newBuilder().setAddress(target)).setExpected(at(1))
                    .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder()
                            .setPart(DocumentPart.DOCUMENT_PART_CORE)).setFromAddress(source).setFromRevision(1))
                    .setOwner(intakeOwner()).build();
            assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                                .isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                    });
            assertThat(f.stack.puts.get()).isEqualTo(puts);
            assertThat(f.catalog.current(target).orElseThrow().revision()).isEqualTo(1);
            assertThat(f.catalog.outcome(account, request.getOperationId())).isEmpty();
        }
    }

    @Test void sourcedCoreRequiresCurrentWriteAclAndActiveWriterGrant() {
        var target = address(account, "auth-core");
        f.saves.save(processAuthority(), create("auth-create", target,
                document(account, "auth-core", "original", "crawler")).build());
        var request = SaveDocumentRequest.newBuilder().setOperationId("auth-source")
                .setTarget(DocumentTarget.newBuilder().setAddress(target)).setExpected(at(1))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)).setFromAddress(target).setFromRevision(1))
                .setOwner(intakeOwner()).build();
        var outsiderId = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, outsiderId, "outsider");
        f.intakeGrant(account, outsiderId, "ds-1", false);
        var outsider = writer(account, "outsider", outsiderId, 1);
        int opens = f.stack.opens.get();
        assertThatThrownBy(() -> f.saves.save(outsider, request))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "crawler");
        var crawler = writer(account, "crawler", credential, 1);
        assertThatThrownBy(() -> f.saves.save(crawler, request))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
        assertThat(f.stack.opens.get()).isEqualTo(opens);
        assertThat(f.catalog.outcome(account, request.getOperationId())).isEmpty();
        assertThat(f.catalog.current(target).orElseThrow().revision()).isEqualTo(1);
        var grant = f.intakeGrant(account, credential, "ds-1", false);
        int puts = f.stack.puts.get();
        var saved = f.saves.save(crawler, request);
        assertThat(saved.getAccession().getRevision()).isEqualTo(2);
        assertThat(saved.getAccession().getValidation().getWriterGrant().getWriterGrantId())
                .isEqualTo(grant.getGrantId());
        assertThat(f.stack.puts.get()).isEqualTo(puts);
        assertThat(f.stack.opens.get()).isEqualTo(opens + 1);
        assertThat(f.saves.save(crawler, request)).isEqualTo(saved.toBuilder().setReplayed(true).build());
        assertThat(f.stack.opens.get()).isEqualTo(opens + 1);
    }
}
