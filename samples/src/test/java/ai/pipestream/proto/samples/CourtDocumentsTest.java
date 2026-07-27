package ai.pipestream.proto.samples;

import ai.pipestream.proto.repo.v1.Document;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CourtDocumentsTest {

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

    private static JsonNode opinion(String json) throws Exception {
        return JSON.readTree(json);
    }
}
