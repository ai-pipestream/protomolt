package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** The scan reaches string fields at any depth, in lists and maps, and reads no bytes field. */
class ExternalEvidenceTextScanTest {

    private static final String TOKEN = "Bearer abcdefghijklmnopqrstuvwxyz0123456789";

    @Test
    void namesTheFirstFieldCarryingACredentialAtAnyDepth() {
        ExternalProviderEvidence clean = ExternalProviderEvidence.newBuilder()
                .setDiagnosticCode("OK").setExternalObjectId("object-a").build();
        assertThat(ExternalEvidenceTextScan.firstCredentialField(clean, "evidence")).isEmpty();

        assertThat(ExternalEvidenceTextScan.firstCredentialField(
                        clean.toBuilder().setExternalVersion(TOKEN).build(), "evidence"))
                .contains("evidence.external_version");

        // Nested inside a repeated message field.
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .addHistoricalLinks(ExternalLink.newBuilder().setExternalObjectId("object-a"))
                .addHistoricalLinks(ExternalLink.newBuilder().setExternalObjectId(TOKEN))
                .build();
        assertThat(ExternalEvidenceTextScan.firstCredentialField(ledger, "ledger"))
                .contains("ledger.historical_links[1].external_object_id");

        // Inside a map value.
        ExternalSystemBinding labelled = ExternalSystemBinding.newBuilder()
                .putLabels("team", "search")
                .putLabels("note", TOKEN)
                .build();
        assertThat(ExternalEvidenceTextScan.firstCredentialField(labelled, "binding"))
                .hasValueSatisfying(path -> assertThat(path).startsWith("binding.labels["));
    }

    @Test
    void bytesFieldsAreNotRead() {
        ExternalProviderEvidence withBytes = ExternalProviderEvidence.newBuilder()
                .setSignedWorkRecordBytes(ByteString.copyFrom(TOKEN, StandardCharsets.UTF_8))
                .build();
        assertThat(ExternalEvidenceTextScan.firstCredentialField(withBytes, "evidence")).isEmpty();
    }
}
