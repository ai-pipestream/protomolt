package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Items 1, 2 and 4 of the review of 1a240b18f. */
class ReviewFixesIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    // Item 1: a retry with map entries in another order is a replay, not OperationReuse.
    @Test void reorderedMapEntriesGiveTheSameContentDigestAndReplay() {
        var address = address(account, "doc-maps");
        var first = withFields(document(account, "doc-maps", "m"), "alpha", "beta", "gamma");
        var second = withFields(document(account, "doc-maps", "m"), "gamma", "alpha", "beta");
        assertThat(first.toByteArray()).as("the client bytes really differ").isNotEqualTo(second.toByteArray());

        var saved = f.saves.save(processAuthority(), create("op-maps", address, first).build());
        var retried = f.saves.save(processAuthority(), create("op-maps", address, second).build());
        assertThat(retried.getReplayed()).isTrue();
        assertThat(retried.getAccession()).isEqualTo(saved.getAccession());
        var read = f.reads.getDocument(processAuthority(), ai.protomolt.proto.repo.v2.GetDocumentRequest.newBuilder()
                .setAddress(address).build());
        assertThat(read.getDocument().getSearchMetadata().getCustomFields().getFieldsMap())
                .isEqualTo(first.getSearchMetadata().getCustomFields().getFieldsMap());
        assertThat(read.getDocument().getParserResultsMap()).isEqualTo(first.getParserResultsMap());
    }

    // Item 2: a resolver that answers with the wrong asset is a host failure, not a recorded rejection.
    @Test void aMisbehavingRegistryFailsWithoutRecordingAnOutcome() {
        DocumentSchemaAdmission.Resolver wrong = selection ->
                BuiltinDocumentSchema.definition(ai.protomolt.proto.repo.semantic.v1.Segmentation.getDescriptor());
        var saves = new DocumentSaves(f.catalog, f.drives, wrong, limits(), bytes -> () -> {}, Clock.systemUTC());
        var address = address(account, "doc-host");
        var request = create("op-host", address, document(account, "doc-host", "h")).build();
        assertThatThrownBy(() -> saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.INTERNAL);
                    assertThat(failure.detail()).isEmpty();
                });
        assertThat(f.catalog.outcome(account, "op-host")).as("the operation ID is still usable").isEmpty();
        assertThat(f.catalog.current(address)).isEmpty();
        assertThat(f.saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
    }

    // Item 2: a host reservation refusal or schema-binding budget exhaustion surfaces as RESOURCE_EXHAUSTED, unrecorded.
    @Test void anExhaustedHostReservationIsNotARecordedRejection() {
        var saves = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), bytes -> {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(
                    ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED, "admission memory exhausted");
        }, Clock.systemUTC());
        var address = address(account, "doc-budget");
        assertThatThrownBy(() -> saves.save(processAuthority(), create("op-budget", address,
                document(account, "doc-budget", "b")).build())).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.catalog.outcome(account, "op-budget")).isEmpty();

        var oneBindingOnly = new DocumentSchemaAdmission.Limits(16, 64L * 1024 * 1024, 64, 4L * 1024 * 1024, 1,
                32L * 1024 * 1024, 64 * 1024 * 1024);
        var bindingExhausted = new DocumentSaves(f.catalog, f.drives, resolver(), oneBindingOnly,
                bytes -> () -> {}, Clock.systemUTC());
        assertThatThrownBy(() -> bindingExhausted.save(processAuthority(), create("op-binding-budget", address,
                document(account, "doc-budget", "b")).build())).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.catalog.outcome(account, "op-binding-budget")).isEmpty();
    }

    // Item 4: a carried part keeps the schema binding it was admitted under.
    @Test void carriedPartsKeepTheirRetainedSchemaBinding() {
        var address = address(account, "doc-binding");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-binding", "v1")).build());
        var original = f.catalog.manifest(address, 1).orElseThrow().getValidation().getPartSchemasList();

        // The same descriptor under another registered identity, as a later host would retain it.
        var compiled = BuiltinDocumentSchema.definition();
        var renamed = new DocumentSchemaAdmission.Definition(compiled.metadata().toBuilder()
                .setTypeUrl("type.test/" + Document.getDescriptor().getFullName()).build(),
                compiled.descriptors(), compiled.source());
        var later = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC(), renamed);
        var parsed = Document.newBuilder().putParserResults("docling", ParserResult.newBuilder()
                .setParserName("docling").setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build();
        later.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(parsed)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).setOwner(intakeOwner()).build());

        var bindings = f.catalog.manifest(address, 2).orElseThrow().getValidation().getPartSchemasList();
        assertThat(bindings).hasSize(3);
        for (var part : new DocumentPart[]{DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS})
            assertThat(binding(bindings, part)).isEqualTo(binding(original, part));
        assertThat(binding(bindings, DocumentPart.DOCUMENT_PART_PARSED).getFragmentSchema().getTypeUrl())
                .startsWith("type.test/");
        assertThat(binding(original, DocumentPart.DOCUMENT_PART_PARSED).getFragmentSchema().getTypeUrl())
                .startsWith("type.googleapis.com/");
    }

    // Decision 30: any authorized writer may change the ACL; account, datasource and connector are fixed.
    @Test void anyAuthorizedWriterMayChangeTheAclButNotTheDocumentIdentity() {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "crawler");
        f.intakeGrant(account, credential, "ds-1", true);
        f.stepGrant(account, credential, "enricher", false);
        var crawler = writer(account, "crawler", credential, 1);
        var address = address(account, "doc-acl");
        var first = f.saves.save(crawler, create("op-1", address, document(account, "doc-acl", "v1", "crawler")).build());
        assertThat(first.getAccession().getRevision()).isEqualTo(1);

        var widened = f.saves.save(crawler, corePut("op-2", address, 1, ownership(account, "crawler", "reader")));
        assertThat(widened.getAccession().getRevision()).isEqualTo(2);
        var stepped = f.saves.save(crawler, corePut("op-3", address, 2, ownership(account, "crawler", "reader", "auditor"))
                .toBuilder().setOwner(stepOwner("enricher")).build());
        assertThat(stepped.getAccession().getRevision()).isEqualTo(3);
        assertThat(f.catalog.manifest(address, 3).orElseThrow().getOwnership().getSecurity().getPermissionsCount()).isEqualTo(6);
        assertThat(f.catalog.manifest(address, 1).orElseThrow().getOwnership().getSecurity().getPermissionsCount()).isEqualTo(2);
        var reader = new ai.protomolt.proto.repo.spi.RepositoryCaller("reader", false, java.util.Set.of(account),
                java.util.Set.of(ai.protomolt.proto.repo.v1.Principal.newBuilder().setIdentityType("service")
                        .setIdentity("reader").build()));
        assertThat(f.reads.getManifest(reader, ai.protomolt.proto.repo.v2.GetManifestRequest.newBuilder()
                .setAddress(address).build()).getManifest().getRevision()).isEqualTo(3);

        var otherDatasource = ownership(account, "crawler").toBuilder().setDatasourceId("ds-2").build();
        var withConnector = ownership(account, "crawler").toBuilder().setConnectorId("s3").build();
        for (var caller : new ai.protomolt.proto.repo.spi.RepositoryCaller[]{crawler, processAuthority()}) {
            for (var changed : new ai.protomolt.proto.repo.v1.OwnershipContext[]{otherDatasource, withConnector}) {
                assertThatThrownBy(() -> f.saves.save(caller, corePut("op-x", address, 3, changed)))
                        .isInstanceOfSatisfying(AccessionException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                            assertThat(((ai.protomolt.proto.repo.v2.SaveRejection) failure.detail().orElseThrow()).getReason())
                                    .isEqualTo(ai.protomolt.proto.repo.v2.RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                        });
            }
        }
        assertThat(f.catalog.outcome(account, "op-x")).isEmpty();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(3);
    }

    // Decision 31: creation needs allow_create; derived_from is lineage only.
    @Test void derivedFromGrantsNoCreationAuthority() {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "summarizer");
        var caller = writer(account, "summarizer", credential, 1);
        var source = address(account, "doc-source");
        f.saves.save(processAuthority(), create("op-0", source, document(account, "doc-source", "s", "summarizer")).build());
        var derived = address(account, "doc-derived");
        var request = create("op-1", derived, document(account, "doc-derived", "d", "summarizer"))
                .setOwner(stepOwner("summarizer"))
                .addDerivedFrom(ai.protomolt.proto.repo.v2.SourceRevision.newBuilder().setAddress(source).setRevision(1)).build();

        f.stepGrant(account, credential, "summarizer", false);
        assertThatThrownBy(() -> f.saves.save(caller, request)).isInstanceOfSatisfying(AccessionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(((ai.protomolt.proto.repo.v2.SaveRejection) failure.detail().orElseThrow()).getReason())
                    .isEqualTo(ai.protomolt.proto.repo.v2.RejectionReason.REJECTION_REASON_WRITE_DENIED);
        });
        assertThat(f.catalog.outcome(account, "op-1")).isEmpty();

        var creator = f.stepGrant(account, credential, "summarizer", true);
        var saved = f.saves.save(caller, request);
        assertThat(saved.getAccession().getValidation().getWriterGrant().getWriterGrantId()).isEqualTo(creator.getGrantId());
        assertThat(saved.getAccession().getDerivedFromList()).singleElement()
                .satisfies(lineage -> assertThat(lineage.getRevision()).isEqualTo(1));
    }

    @Test void revokedSourceReadAccessPreventsSourcingAndLineageFromHistoricalRevision() {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "summarizer");
        f.stepGrant(account, credential, "summarizer", true);
        var caller = writer(account, "summarizer", credential, 1);

        var source = address(account, "doc-shared");
        f.saves.save(processAuthority(), create("op-source-1", source,
                document(account, "doc-shared", "rev1", "summarizer")).build());
        // Revision 2 revokes summarizer's read access on the source document.
        f.saves.save(processAuthority(), corePut("op-source-2", source, 1, ownership(account, "other-reader")));

        var target = source.toBuilder().setGraphId("derived:" + account).build();
        var coreOnly = Document.newBuilder().setDocId("doc-shared")
                .setOwnership(ownership(account, "summarizer"))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("derived")).build();
        var withSourcedPart = SaveDocumentRequest.newBuilder().setOperationId("op-sourced-revoked")
                .setTarget(DocumentTarget.newBuilder().setAddress(target))
                .setExpected(ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setNone(true))
                .setDocument(coreOnly).addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addSources(ai.protomolt.proto.repo.v2.PartSource.newBuilder()
                        .setSelector(ai.protomolt.proto.repo.v2.PartSelector.newBuilder()
                                .setPart(DocumentPart.DOCUMENT_PART_PARSED))
                        .setFromAddress(source).setFromRevision(1))
                .setOwner(stepOwner("summarizer")).build();
        assertThatThrownBy(() -> f.saves.save(caller, withSourcedPart)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(((ai.protomolt.proto.repo.v2.SaveRejection) failure.detail().orElseThrow()).getReason())
                            .isEqualTo(ai.protomolt.proto.repo.v2.RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND);
                });

        var withLineage = SaveDocumentRequest.newBuilder().setOperationId("op-lineage-revoked")
                .setTarget(DocumentTarget.newBuilder().setAddress(target))
                .setExpected(ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setNone(true))
                .setDocument(coreOnly).addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addDerivedFrom(ai.protomolt.proto.repo.v2.SourceRevision.newBuilder()
                        .setAddress(source).setRevision(1))
                .setOwner(stepOwner("summarizer")).build();
        assertThatThrownBy(() -> f.saves.save(caller, withLineage)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(((ai.protomolt.proto.repo.v2.SaveRejection) failure.detail().orElseThrow()).getReason())
                            .isEqualTo(ai.protomolt.proto.repo.v2.RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND);
                });
    }

    private static SaveDocumentRequest corePut(String operation, ai.protomolt.proto.repo.v1.NodeAddress address,
            long expected, ai.protomolt.proto.repo.v1.OwnershipContext ownership) {
        var core = Document.newBuilder().setDocId(address.getDocId()).setOwnership(ownership)
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("acl " + operation)).build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }

    private static ai.protomolt.proto.repo.v2.PartSchemaBinding binding(
            java.util.List<ai.protomolt.proto.repo.v2.PartSchemaBinding> bindings, DocumentPart part) {
        return bindings.stream().filter(b -> b.getPart().getPart() == part).findFirst().orElseThrow();
    }

    private static Document withFields(Document document, String... keys) {
        var fields = Struct.newBuilder();
        var parsers = document.toBuilder();
        for (var key : keys) {
            fields.putFields(key, Value.newBuilder().setStringValue("value of " + key).build());
            parsers.putParserResults(key, ParserResult.newBuilder().setParserName(key).setParserVersion("1")
                    .setStatus(ParseStatus.PARSE_STATUS_OK).build());
        }
        return parsers.setSearchMetadata(SearchMetadata.newBuilder(document.getSearchMetadata()).setCustomFields(fields)).build();
    }
}
