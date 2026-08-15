package ai.pipestream.proto.acquire.pull;

import ai.pipestream.proto.repo.v1.ChecksumType;
import ai.pipestream.proto.repo.v1.DocIdDerivationMethod;
import ai.pipestream.proto.repo.v1.Document;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The stable-identity wrap: deterministic ids, blob integrity, and refusals by name. */
class PullDocumentsTest {

    @Test
    void docIdIsDeterministicPerSourceItemAndDistinctAcrossAxes() {
        String same = PullDocuments.docId("s3-pull", "ds-1", "s3://bucket/a.txt");
        assertThat(PullDocuments.docId("s3-pull", "ds-1", "s3://bucket/a.txt")).isEqualTo(same);
        assertThat(PullDocuments.docId("s3-pull", "ds-1", "s3://bucket/b.txt")).isNotEqualTo(same);
        assertThat(PullDocuments.docId("s3-pull", "ds-2", "s3://bucket/a.txt")).isNotEqualTo(same);
        assertThat(PullDocuments.docId("jdbc-pull", "ds-1", "s3://bucket/a.txt")).isNotEqualTo(same);
    }

    @Test
    void changedContentKeepsTheSameDocIdSoUpdatesReplaceInsteadOfDuplicating() {
        Document first = PullDocuments.document("s3-pull", "ds-1", "s3://bucket/a.txt",
                ByteString.copyFromUtf8("v1"), "a.txt", "text/plain");
        Document second = PullDocuments.document("s3-pull", "ds-1", "s3://bucket/a.txt",
                ByteString.copyFromUtf8("v2 with different content"), "a.txt", "text/plain");
        assertThat(second.getDocId()).isEqualTo(first.getDocId());
        assertThat(second.getBlobBag().getBlob().getChecksum())
                .isNotEqualTo(first.getBlobBag().getBlob().getChecksum());
    }

    @Test
    void wrapCarriesBlobIntegrityProvenanceAndDerivation() throws Exception {
        byte[] payload = "row payload".getBytes();
        Document document = PullDocuments.document("jdbc-pull", "ds-9", "id=42",
                ByteString.copyFrom(payload), "42.json", "application/json");

        var blob = document.getBlobBag().getBlob();
        assertThat(blob.getBlobId()).isEqualTo(document.getDocId());
        assertThat(blob.getData().toByteArray()).isEqualTo(payload);
        assertThat(blob.getSizeBytes()).isEqualTo(payload.length);
        assertThat(blob.getChecksumType()).isEqualTo(ChecksumType.CHECKSUM_TYPE_SHA256);
        assertThat(blob.getChecksum()).isEqualTo(HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(payload)));
        assertThat(blob.getFilename()).isEqualTo("42.json");
        assertThat(blob.getMimeType()).isEqualTo("application/json");

        assertThat(document.getSearchMetadata().getSourceMimeType())
                .isEqualTo("application/json");
        assertThat(document.getOwnership().getConnectorId()).isEqualTo("jdbc-pull");
        assertThat(document.getOwnership().getAccountId())
                .as("intake stamps account identity from the key; the wrap must not")
                .isEmpty();
        assertThat(document.getDocIdDerivation().getMethod())
                .isEqualTo(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CALLER_PROVIDED);
        assertThat(document.getDocIdDerivation().getSourceValue()).isEqualTo("id=42");
    }

    @Test
    void missingIdentityIsRefusedByName() {
        ByteString data = ByteString.copyFromUtf8("x");
        assertThatThrownBy(() -> PullDocuments.document(" ", "ds", "key", data, "", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connectorId");
        assertThatThrownBy(() -> PullDocuments.document("c", "", "key", data, "", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("datasourceId");
        assertThatThrownBy(() -> PullDocuments.document("c", "ds", null, data, "", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sourceKey");
    }

    @Test
    void watermarkOnlyAdvancesThroughAnUnbrokenPrefixOfSuccesses() {
        PullReport.Accumulator acc = new PullReport.Accumulator("start");
        acc.success(false, "m1");
        acc.success(true, "m2");
        acc.failure("item three broke");
        acc.success(false, "m4");
        PullReport report = acc.report();

        assertThat(report.submitted()).isEqualTo(2);
        assertThat(report.deduplicated()).isEqualTo(1);
        assertThat(report.failed()).isEqualTo(1);
        assertThat(report.errors()).containsExactly("item three broke");
        assertThat(report.watermark())
                .as("the failed item must stay ahead of the watermark and retry next pull")
                .isEqualTo("m2");
    }
}
