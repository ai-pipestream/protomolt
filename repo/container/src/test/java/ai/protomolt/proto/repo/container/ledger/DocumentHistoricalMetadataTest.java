package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentHistoricalMetadataTest {
    private static final String SNAPSHOT = """
            {"row_kind":"PIPELINE","cluster_id":null,"account_id":"account","datasource_id":"source",
             "connector_id":null,"content_type":"","filename":null,"security":{},
             "delete_source_blobs_on_settle":false,"source_blob_delete_reason":null,"crawl_id":null,
             "created_at_epoch_micros":9007199254740993.000000,"updated_at_epoch_micros":-1}
            """;

    @Test void preservesExactMicrosNullsAndEmptyStrings() {
        var value = DocumentHistoricalMetadata.decode(1, SNAPSHOT, "account").getKnown();
        assertThat(value.getCreatedAtEpochMicros()).isEqualTo(9007199254740993L);
        assertThat(value.getUpdatedAtEpochMicros()).isEqualTo(-1);
        assertThat(value.hasConnectorId()).isFalse();
        assertThat(value.hasFilename()).isFalse();
        assertThat(value.hasContentType()).isTrue();
        assertThat(value.getContentType()).isEmpty();
        assertThat(DocumentHistoricalMetadata.decode(1, SNAPSHOT.replace("9007199254740993.000000", "9223372036854775807"),
                "account").getKnown().getCreatedAtEpochMicros()).isEqualTo(Long.MAX_VALUE);
    }

    @Test void corruptionNeverBecomesUnknownOrRounded() {
        for (var bad : new String[] {
                SNAPSHOT.replace("9007199254740993.000000", "9007199254740993.1"),
                SNAPSHOT.replace("9007199254740993.000000", "9223372036854775808"),
                SNAPSHOT.replace("9007199254740993.000000", "\"123\""),
                SNAPSHOT.replace("\"connector_id\":null,", ""),
                SNAPSHOT.replace("\"connector_id\":null", "\"connector_id\":42"),
                SNAPSHOT.replace("\"security\":{}", "\"security\":{\"unknownField\":true}"),
                SNAPSHOT.replace("\"security\":{}", "\"security\":null"),
                SNAPSHOT.replace("false", "\"false\""),
                SNAPSHOT.replace("PIPELINE", "OTHER"),
                SNAPSHOT.replace("\"cluster_id\":null", "\"cluster_id\":null,\"extra\":0"),
                SNAPSHOT.replace("false", "true"), "null", "[]", "{" }) {
            assertThatThrownBy(() -> DocumentHistoricalMetadata.decode(1, bad, "account"))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
        }
        assertThatThrownBy(() -> DocumentHistoricalMetadata.decode(1, SNAPSHOT, "other"))
                .isInstanceOfSatisfying(RepositoryException.class,
                        e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
    }

    @Test void unsupportedVersionAndCapacityAreExplicit() {
        assertThatThrownBy(() -> DocumentHistoricalMetadata.decode(2, SNAPSHOT, "account"))
                .isInstanceOfSatisfying(RepositoryException.class,
                        e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        assertThatThrownBy(() -> DocumentHistoricalMetadata.decode(1, "x".repeat(DocumentHistoricalMetadata.MAX_BYTES + 1), "account"))
                .isInstanceOfSatisfying(RepositoryException.class,
                        e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
    }
}
