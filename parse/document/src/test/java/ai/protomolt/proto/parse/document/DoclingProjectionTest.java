package ai.protomolt.proto.parse.document;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.parse.document.v1.BaseTextItem;
import ai.protomolt.proto.parse.document.v1.CodeItem;
import ai.protomolt.proto.parse.document.v1.Document;
import ai.protomolt.proto.parse.document.v1.DocumentOrigin;
import ai.protomolt.proto.parse.document.v1.PageItem;
import ai.protomolt.proto.parse.document.v1.SectionHeaderItem;
import ai.protomolt.proto.parse.document.v1.TextItem;
import ai.protomolt.proto.parse.document.v1.TextItemBase;
import ai.protomolt.proto.parse.document.v1.TitleItem;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

class DoclingProjectionTest {

    private static Document doclingFixture() {
        return Document.newBuilder()
                .setSchemaName("docling_document_v2")
                .setVersion("2.0.0")
                .setName("opinion-42.pdf")
                .setOrigin(
                        DocumentOrigin.newBuilder()
                                .setMimetype("application/pdf")
                                .setFilename("opinion-42.pdf")
                                .setUri("s3://court/opinion-42.pdf")
                                .setBinaryHash(42L))
                .addTexts(
                        BaseTextItem.newBuilder()
                                .setTitle(
                                        TitleItem.newBuilder()
                                                .setBase(
                                                        TextItemBase.newBuilder()
                                                                .setSelfRef("#/texts/0")
                                                                .setText("Habeas Corpus Petition"))))
                .addTexts(
                        BaseTextItem.newBuilder()
                                .setSectionHeader(
                                        SectionHeaderItem.newBuilder()
                                                .setLevel(1)
                                                .setBase(
                                                        TextItemBase.newBuilder()
                                                                .setSelfRef("#/texts/1")
                                                                .setText("I. Background"))))
                .addTexts(
                        BaseTextItem.newBuilder()
                                .setText(
                                        TextItem.newBuilder()
                                                .setBase(
                                                        TextItemBase.newBuilder()
                                                                .setSelfRef("#/texts/2")
                                                                .setText("The petitioner seeks relief."))))
                .addTexts(
                        BaseTextItem.newBuilder()
                                .setCode(CodeItem.newBuilder().setSelfRef("#/texts/3").setText("42 U.S.C. § 1983")))
                .putPages(1, PageItem.getDefaultInstance())
                .putPages(2, PageItem.getDefaultInstance())
                .build();
    }

    @Test
    void forwardProjectionExtractsFieldsWithProvenance() {
        DoclingProjection.Projected projected = DoclingProjection.toParserDocument(doclingFixture());

        var fields = projected.document().getExtractedFields().getFieldsMap();
        assertThat(fields.get(DoclingProjection.FIELD_TITLE).getStringValue())
                .isEqualTo("Habeas Corpus Petition");
        assertThat(fields.get(DoclingProjection.FIELD_BODY).getStringValue())
                .isEqualTo(
                        "Habeas Corpus Petition\nI. Background\nThe petitioner seeks relief.\n42 U.S.C. § 1983");
        assertThat(fields.get(DoclingProjection.FIELD_PAGE_COUNT).getNumberValue()).isEqualTo(2.0);
        assertThat(fields.get(DoclingProjection.FIELD_MIME_TYPE).getStringValue())
                .isEqualTo("application/pdf");
        assertThat(fields.get(DoclingProjection.FIELD_FILENAME).getStringValue())
                .isEqualTo("opinion-42.pdf");
        assertThat(fields.get(DoclingProjection.FIELD_SOURCE_URI).getStringValue())
                .isEqualTo("s3://court/opinion-42.pdf");

        assertThat(projected.provenance())
                .containsEntry(DoclingProjection.FIELD_TITLE, "texts[0].title.text")
                .containsEntry(DoclingProjection.FIELD_BODY, "texts[*].text")
                .containsEntry(DoclingProjection.FIELD_PAGE_COUNT, "pages")
                .containsEntry(DoclingProjection.FIELD_MIME_TYPE, "origin.mimetype")
                .containsEntry(DoclingProjection.FIELD_FILENAME, "origin.filename")
                .containsEntry(DoclingProjection.FIELD_SOURCE_URI, "origin.uri");
    }

    @Test
    void titleFallsBackToDocumentNameWithProvenance() {
        Document unnamedTitle =
                Document.newBuilder().setName("fallback-name.docx").build();
        DoclingProjection.Projected projected = DoclingProjection.toParserDocument(unnamedTitle);
        assertThat(
                        projected
                                .document()
                                .getExtractedFields()
                                .getFieldsMap()
                                .get(DoclingProjection.FIELD_TITLE)
                                .getStringValue())
                .isEqualTo("fallback-name.docx");
        assertThat(projected.provenance()).containsEntry(DoclingProjection.FIELD_TITLE, "name");
    }

    @Test
    void roundTripIsLossless() {
        Document original = doclingFixture();
        DoclingProjection.Projected projected = DoclingProjection.toParserDocument(original);
        Document unpacked =
                DoclingProjection.fromParserDocument(projected.document()).orElseThrow();
        assertThat(unpacked.toByteArray()).isEqualTo(original.toByteArray());
    }

    @Test
    void reverseDirectionRejectsForeignShapesQuietlyAndCorruptShapesLoudly() {
        assertThat(DoclingProjection.fromParserDocument(ParserDocument.getDefaultInstance()))
                .isEmpty();
        ParserDocument foreign =
                ParserDocument.newBuilder().setShape(Any.pack(StringValue.of("not a document"))).build();
        assertThat(DoclingProjection.fromParserDocument(foreign)).isEmpty();

        ParserDocument corrupt =
                ParserDocument.newBuilder()
                        .setShape(
                                Any.newBuilder()
                                        .setTypeUrl("type.googleapis.com/ai.protomolt.proto.parse.document.v1.Document")
                                        .setValue(com.google.protobuf.ByteString.copyFromUtf8("garbage-not-a-messageÿ")))
                        .build();
        assertThatThrownBy(() -> DoclingProjection.fromParserDocument(corrupt))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void applyToFillsSearchMetadataFromExtractedFields() {
        DoclingProjection.Projected projected = DoclingProjection.toParserDocument(doclingFixture());
        SearchMetadata.Builder metadata = SearchMetadata.newBuilder();
        DoclingProjection.applyTo(projected, metadata);
        assertThat(metadata.getTitle()).isEqualTo("Habeas Corpus Petition");
        assertThat(metadata.getBody()).contains("The petitioner seeks relief.");
        assertThat(metadata.getSourceMimeType()).isEqualTo("application/pdf");
        assertThat(metadata.getSourceUri()).isEqualTo("s3://court/opinion-42.pdf");
        assertThat(metadata.getMetadataMap()).containsEntry("page_count", "2");
    }

    @Test
    void emptyExtractionProducesNoPhantomFields() {
        DoclingProjection.Projected projected =
                DoclingProjection.toParserDocument(Document.getDefaultInstance());
        assertThat(projected.document().getExtractedFields().getFieldsMap()).isEmpty();
        assertThat(projected.provenance()).isEmpty();
        SearchMetadata.Builder metadata = SearchMetadata.newBuilder();
        DoclingProjection.applyTo(projected, metadata);
        assertThat(metadata.hasTitle()).isFalse();
        assertThat(metadata.hasBody()).isFalse();
    }

    @Test
    void vendoredValueSemantics() {
        // Guard against silent divergence from the fleet copy: the schema
        // identity fields the fleet pins must survive the round trip.
        Document doc = doclingFixture();
        assertThat(doc.getSchemaName()).isEqualTo("docling_document_v2");
        DoclingProjection.Projected projected = DoclingProjection.toParserDocument(doc);
        assertThat(
                        DoclingProjection.fromParserDocument(projected.document())
                                .orElseThrow()
                                .getSchemaName())
                .isEqualTo("docling_document_v2");
    }

    @Test
    void nullInputIsRejectedLoudly() {
        assertThatThrownBy(() -> DoclingProjection.toParserDocument(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
