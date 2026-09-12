package ai.protomolt.proto.asset.catalog;

import ai.protomolt.proto.asset.v1.AssetCatalogRow;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.ContentClass;
import ai.protomolt.proto.asset.v1.ContentProfile;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ParquetDataset;
import ai.protomolt.proto.asset.v1.PdfDocument;
import ai.protomolt.proto.asset.v1.QualityScore;
import ai.protomolt.proto.asset.v1.TarArchive;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The projection from what the archive stores to one flat catalog row. */
class AssetCatalogRowsTest {

    @Test
    void aVerifiedAssetProjectsItsStateFormatAndTenancy() {
        AssetCatalogRow row = AssetCatalogRows.of(
                entry("bundle.tar", Classification.newBuilder()
                        .setState(ClassificationState.CLASSIFICATION_STATE_VERIFIED)
                        .setDeclared(tar())
                        .setIdentified(tar())
                        .setClassifiedAt(Timestamps.fromSeconds(1_700_000_000L))
                        .build()),
                manifest(present("original", 1000, null)));

        assertThat(row.getAccountId()).isEqualTo("acct");
        assertThat(row.getArchive()).isEqualTo("papers");
        assertThat(row.getEntryId()).isEqualTo("doc-1");
        assertThat(row.getClassificationState()).isEqualTo("VERIFIED");
        assertThat(row.getFormatKind()).isEqualTo("tar");
        assertThat(row.getVerified()).isTrue();
        assertThat(row.getSizeBytes()).isEqualTo(1000);
        assertThat(row.getRenditionCount()).isEqualTo(1);
        assertThat(row.getClassifiedAt().getSeconds()).isEqualTo(1_700_000_000L);
    }

    @Test
    void aDeclaredAssetTakesItsFormatFromTheClaimAndAnIdentifiedOneFromTheBytes() {
        assertThat(AssetCatalogRows.of(
                entry("f.parquet", Classification.newBuilder()
                        .setState(ClassificationState.CLASSIFICATION_STATE_DECLARED)
                        .setDeclared(FormatFact.newBuilder().setParquet(
                                ParquetDataset.newBuilder().setFilename("f.parquet")))
                        .build()),
                manifest()).getFormatKind()).isEqualTo("parquet");

        assertThat(AssetCatalogRows.of(
                entry("paper.pdf", Classification.newBuilder()
                        .setState(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED)
                        .setIdentified(FormatFact.newBuilder().setPdf(
                                PdfDocument.newBuilder().setFilename("paper.pdf")))
                        .build()),
                manifest()).getFormatKind()).isEqualTo("pdf");
    }

    @Test
    void anAssetWithNoSingleFormatGroupsUnderItsOwnBlankFacet() {
        // UNCLASSIFIED has no format, and CONFLICTED has two. Both are real
        // catalog answers, and neither may be filed under a format kind.
        AssetCatalogRow unclassified = AssetCatalogRows.of(
                entry("mystery.bin", Classification.newBuilder()
                        .setState(ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED)
                        .build()),
                manifest());
        assertThat(unclassified.getClassificationState()).isEqualTo("UNCLASSIFIED");
        assertThat(unclassified.getFormatKind()).isEmpty();

        AssetCatalogRow conflicted = AssetCatalogRows.of(
                entry("liar.tar", Classification.newBuilder()
                        .setState(ClassificationState.CLASSIFICATION_STATE_CONFLICTED)
                        .setDeclared(tar())
                        .setIdentified(FormatFact.newBuilder().setPdf(
                                PdfDocument.newBuilder().setFilename("liar.pdf")))
                        .build()),
                manifest());
        assertThat(conflicted.getClassificationState()).isEqualTo("CONFLICTED");
        assertThat(conflicted.getFormatKind()).isEmpty();
        assertThat(conflicted.getVerified()).isFalse();
    }

    @Test
    void derivedRenditionsAreCountedAsSuchAndMarkTheAssetBridged() {
        AssetCatalogRow row = AssetCatalogRows.of(
                entry("bundle.tar", verifiedTar()),
                manifest(present("original", 900, null),
                        present("members", 100, null),
                        present("schema", 50, null)));

        assertThat(row.getRenditionCount()).isEqualTo(3);
        assertThat(row.getDerivedRenditions()).isEqualTo(2);
        assertThat(row.getBridged()).isTrue();
        assertThat(row.getSizeBytes()).isEqualTo(1050);
    }

    @Test
    void anAssetNothingHasBridgedSaysSo() {
        AssetCatalogRow row = AssetCatalogRows.of(entry("bundle.tar", verifiedTar()),
                manifest(present("original", 10, null)));

        assertThat(row.getDerivedRenditions()).isZero();
        assertThat(row.getBridged()).isFalse();
    }

    @Test
    void aMeasuredQualityRidesTheRowAndAnUnmeasuredOneIsNotZero() {
        AssetCatalogRow scored = AssetCatalogRows.of(
                entry("scan.png", verifiedTar()),
                manifest(present("original", 500, null),
                        present("ocr-text", 40, profile(ContentClass.CONTENT_CLASS_OCR_TEXT,
                                0.42))));

        assertThat(scored.getContentClass()).isEqualTo("OCR_TEXT");
        assertThat(scored.getContentQuality()).isEqualTo(0.42);
        assertThat(scored.getQualityMeasured()).isTrue();

        AssetCatalogRow unscored = AssetCatalogRows.of(entry("bundle.tar", verifiedTar()),
                manifest(present("original", 500, null)));
        assertThat(unscored.getContentQuality())
                .isEqualTo(AssetCatalogRows.UNMEASURED_QUALITY);
        assertThat(unscored.getQualityMeasured()).isFalse();
        assertThat(unscored.getContentClass()).isEmpty();
    }

    @Test
    void theBestMeasuredQualityWins() {
        AssetCatalogRow row = AssetCatalogRows.of(entry("scan.pdf", verifiedTar()),
                manifest(present("text", 10, profile(
                                ContentClass.CONTENT_CLASS_INFORMATIONAL_TEXT, 0.3)),
                        present("ocr-text", 10, profile(
                                ContentClass.CONTENT_CLASS_OCR_TEXT, 0.8))));

        assertThat(row.getContentQuality()).isEqualTo(0.8);
    }

    @Test
    void aTombstoneCountsForNothingBecauseItsBytesAreGone() {
        RenditionManifestEntry deleted = RenditionManifestEntry.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setState(RenditionState.RENDITION_STATE_DELETED)
                .setSizeBytes(9_000)
                .setDeletedReason("RTBF")
                .build();

        AssetCatalogRow row = AssetCatalogRows.of(entry("gone.tar", verifiedTar()),
                manifest(deleted, present("members", 100, null)));

        assertThat(row.getRenditionCount()).isEqualTo(1);
        assertThat(row.getSizeBytes()).isEqualTo(100);
    }

    // ------------------------------------------------------------------

    private static Classification verifiedTar() {
        return Classification.newBuilder()
                .setState(ClassificationState.CLASSIFICATION_STATE_VERIFIED)
                .setDeclared(tar())
                .setIdentified(tar())
                .build();
    }

    private static FormatFact tar() {
        return FormatFact.newBuilder()
                .setTar(TarArchive.newBuilder().setFilename("bundle.tar")).build();
    }

    private static EntryInfo entry(String filename, Classification classification) {
        return EntryInfo.newBuilder()
                .setAddress(EntryAddress.newBuilder()
                        .setAccountId("acct").setArchive("papers").setEntryId("doc-1"))
                .setEntryUuid("6b3f2a5c-0000-4000-8000-000000000001")
                .setFilename(filename)
                .setCurrentVersion(3)
                .setUpdatedAt(Timestamps.fromSeconds(1_700_000_500L))
                .setClassification(classification)
                .build();
    }

    private static VersionManifest manifest(RenditionManifestEntry... renditions) {
        VersionManifest.Builder manifest = VersionManifest.newBuilder().setVersion(3);
        for (RenditionManifestEntry rendition : renditions) {
            manifest.addRenditions(rendition);
        }
        return manifest.build();
    }

    private static RenditionManifestEntry present(String name, long size,
                                                  ContentProfile profile) {
        RenditionManifestEntry.Builder rendition = RenditionManifestEntry.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName(name))
                .setState(RenditionState.RENDITION_STATE_PRESENT)
                .setSizeBytes(size);
        if (profile != null) {
            rendition.setContentProfile(profile);
        }
        return rendition.build();
    }

    private static ContentProfile profile(ContentClass contentClass, double score) {
        return ContentProfile.newBuilder()
                .setContentClass(contentClass)
                .setQuality(QualityScore.newBuilder().setScore(score))
                .build();
    }
}
