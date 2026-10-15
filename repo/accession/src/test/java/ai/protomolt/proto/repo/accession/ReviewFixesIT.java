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

    // Item 2: a host reservation refusal surfaces as RESOURCE_EXHAUSTED, unrecorded.
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
