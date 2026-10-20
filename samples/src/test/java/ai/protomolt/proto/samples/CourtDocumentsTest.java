package ai.protomolt.proto.samples;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CourtDocumentsTest {

    @Test
    void missingEmbeddingInputIsNotRecordedAsEmptyText() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> CourtDocuments.withEmbedding(
                Document.getDefaultInstance(), null, new float[]{1f}, "model"))
                .isInstanceOf(NullPointerException.class).hasMessage("embeddedText");
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void mapsAnOpinionLineToADocument() throws Exception {
        Document document = CourtDocuments.toDocument(opinion("""
                {
                  "opinion_id": "1",
                  "cluster_id": "42",
                  "case_name": "United States v. Davila-Gonzalez",
                  "date_filed": "2010-02-10",
                  "judges": "Lipez, Circuit Judge",
                  "nature_of_suit": "",
                  "precedential_status": "Published",
                  "docket_id": "1970215",
                  "opinion_type": "010combined",
                  "author": "",
                  "plain_text": "The defendant appeals his sentence."
                }
                """));

        assertThat(document.getDocId()).isEqualTo(UUID.nameUUIDFromBytes(
                "courtlistener|1".getBytes(StandardCharsets.UTF_8)).toString());
        var metadata = document.getSearchMetadata();
        assertThat(metadata.getTitle()).isEqualTo("United States v. Davila-Gonzalez");
        assertThat(metadata.getBody()).isEqualTo("The defendant appeals his sentence.");
        assertThat(metadata.getDocumentType()).isEqualTo("court-opinion");
        assertThat(metadata.getLanguage()).isEqualTo("en");
        // author is blank, so the judges column is the fallback
        assertThat(metadata.getAuthor()).isEqualTo("Lipez, Circuit Judge");
        assertThat(metadata.getSourceUri()).isEqualTo(
                "https://www.courtlistener.com/opinion/42/united-states-v-davila-gonzalez/");
        Timestamp expectedCreation = Timestamp.newBuilder()
                .setSeconds(LocalDate.parse("2010-02-10").atStartOfDay(ZoneOffset.UTC).toEpochSecond())
                .build();
        assertThat(metadata.getCreationDate()).isEqualTo(expectedCreation);
        assertThat(metadata.getMetadataMap())
                .containsEntry("precedential_status", "Published")
                .containsEntry("opinion_type", "010combined")
                .containsEntry("docket_id", "1970215")
                .doesNotContainKey("nature_of_suit"); // blank values are skipped
        assertThat(document.getOwnership().getAccountId()).isEqualTo("samples");
        assertThat(document.getOwnership().getDatasourceId()).isEqualTo("courtlistener");
    }

    @Test
    void docIdIsDeterministic() {
        assertThat(CourtDocuments.docId(7))
                .isEqualTo(CourtDocuments.docId(7))
                .isNotEqualTo(CourtDocuments.docId(8));
    }

    @Test
    void prefersAuthorOverJudges() throws Exception {
        Document document = CourtDocuments.toDocument(opinion("""
                {
                  "opinion_id": "2",
                  "cluster_id": "3",
                  "case_name": "Forsyth v. Spencer",
                  "date_filed": "2010-03-01",
                  "judges": "Some Panel",
                  "author": "Torruella, Circuit Judge",
                  "plain_text": "Habeas corpus petition."
                }
                """));

        assertThat(document.getSearchMetadata().getAuthor())
                .isEqualTo("Torruella, Circuit Judge");
    }

    @Test
    void skipsBlankOptionalFields() throws Exception {
        Document document = CourtDocuments.toDocument(opinion("""
                {
                  "opinion_id": "5",
                  "cluster_id": "6",
                  "case_name": "",
                  "date_filed": "",
                  "judges": "",
                  "author": "",
                  "plain_text": ""
                }
                """));

        var metadata = document.getSearchMetadata();
        assertThat(metadata.hasTitle()).isFalse();
        assertThat(metadata.hasBody()).isFalse();
        assertThat(metadata.hasAuthor()).isFalse();
        assertThat(metadata.hasCreationDate()).isFalse();
        assertThat(metadata.getMetadataMap()).isEmpty();
    }

    @Test
    void slugLowercasesAndDashesNonAlphanumerics() {
        assertThat(CourtDocuments.slug("United States v. Davila-Gonzalez"))
                .isEqualTo("united-states-v-davila-gonzalez");
        assertThat(CourtDocuments.slug("ANSYS, Inc. v. Computational Dynamics North America"))
                .isEqualTo("ansys-inc-v-computational-dynamics-north-america");
        assertThat(CourtDocuments.slug("Morrissette v. Teledyne Princeton, Inc."))
                .isEqualTo("morrissette-v-teledyne-princeton-inc");
    }

    @Test
    void attachesValidSegmentationAndEmbeddingSet() throws Exception {
        Document base = CourtDocuments.toDocument(opinion("""
                {
                  "opinion_id": "10",
                  "cluster_id": "20",
                  "case_name": "Smith v. Jones",
                  "plain_text": "Opinion text."
                }
                """));
        assertThat(CourtDocuments.firstDocumentEmbedding(base)).isEmpty();

        float[] vector = new float[] {0.25f, -0.5f, 0.75f};
        String expectedText = CourtDocumentIndexSample.embedText(base);
        assertThat(expectedText).isEqualTo("Smith v. Jones\nOpinion text.");
        byte[] expectedBytes = expectedText.getBytes(StandardCharsets.UTF_8);
        String expectedSha256 = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(expectedBytes));

        Document embedded = CourtDocuments.withEmbedding(base, vector, "model2vec-sample");

        assertThat(ValidationResult.validate(embedded).valid()).isTrue();
        assertThat(CourtDocuments.firstDocumentEmbedding(embedded)).containsExactly(vector);
        assertThat(embedded.getSearchMetadata().getSegmentationsCount()).isEqualTo(1);
        assertThat(embedded.getSearchMetadata().getEmbeddingSetsCount()).isEqualTo(1);

        var segmentation = embedded.getSearchMetadata().getSegmentations(0);
        assertThat(segmentation.getSource().getDerived().getText()).isEqualTo(expectedText);
        assertThat(segmentation.getSource().getBinding().getUtf8Length())
                .isEqualTo(expectedBytes.length);
        assertThat(segmentation.getSource().getBinding().getResolvedTextSha256())
                .isEqualTo(expectedSha256);
        assertThat(segmentation.getSegmentsCount()).isEqualTo(1);
        var segment = segmentation.getSegments(0);
        assertThat(segment.getText()).isEqualTo(expectedText);
        assertThat(segment.getStart()).isZero();
        assertThat(segment.getEnd()).isEqualTo(expectedBytes.length);
    }

    @Test
    void repeatedEmbeddingReplacesOnlyItsOwnEntriesAndPreservesMetadata() {
        var original = CourtDocuments.withEmbedding(Document.newBuilder().setDocId("doc").build(),
                "original", new float[]{1f, 2f}, "first-model");
        var metadata = original.getSearchMetadata();
        var otherSegmentation = metadata.getSegmentations(0).toBuilder().setSegmentationId("other").build();
        var otherEmbedding = metadata.getEmbeddingSets(0).toBuilder()
                .setEmbeddingSetId("other").setSegmentationId("other").build();
        var before = original.toBuilder().setSearchMetadata(metadata.toBuilder()
                .addSegmentations(0, otherSegmentation).addEmbeddingSets(0, otherEmbedding)
                .setTitle("preserved").putMetadata("key", "value")
                .setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(999, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(42).build())
                        .build())).build();
        var after = CourtDocuments.withEmbedding(before, "replacement", new float[]{3f, 4f}, "second-model");
        var result = after.getSearchMetadata();
        assertThat(result.getSegmentationsCount()).isEqualTo(2);
        assertThat(result.getEmbeddingSetsCount()).isEqualTo(2);
        assertThat(result.getSegmentations(0)).isEqualTo(otherSegmentation);
        assertThat(result.getEmbeddingSets(0)).isEqualTo(otherEmbedding);
        assertThat(result.toBuilder().clearSegmentations().clearEmbeddingSets().build())
                .isEqualTo(before.getSearchMetadata().toBuilder().clearSegmentations().clearEmbeddingSets().build());
        var replacement = result.getSegmentations(1);
        assertThat(replacement.getSegmentationId()).isEqualTo(CourtDocuments.EMBEDDING_RESULT_ID);
        assertThat(replacement.getSource().getDerived().getDerivationId()).isEqualTo("document-embedding-source");
        assertThat(replacement.getSource().getDerived().getText()).isEqualTo("replacement");
        assertThat(replacement.getContentDigest()).isNotEqualTo(metadata.getSegmentations(0).getContentDigest());
        assertThat(result.getEmbeddingSets(1).getSegmentationDigest()).isEqualTo(replacement.getContentDigest());
        assertThat(result.getEmbeddingSets(1).getModel()).isEqualTo("second-model");
        assertThat(result.getEmbeddingSets(1).getVectors(0).getValuesList()).containsExactly(3f, 4f);
        assertThat(ValidationResult.validate(after).valid()).isTrue();
    }

    private static JsonNode opinion(String json) throws Exception {
        return JSON.readTree(json);
    }
}
