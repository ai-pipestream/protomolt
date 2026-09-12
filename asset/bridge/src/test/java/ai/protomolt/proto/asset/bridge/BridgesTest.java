package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.AvroDataset;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.ContainerMembers;
import ai.protomolt.proto.asset.v1.DatasetSchema;
import ai.protomolt.proto.asset.v1.DelimitedTable;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.GzipFile;
import ai.protomolt.proto.asset.v1.HeaderPresence;
import ai.protomolt.proto.asset.v1.JsonDocument;
import ai.protomolt.proto.asset.v1.NdjsonDataset;
import ai.protomolt.proto.asset.v1.ParquetDataset;
import ai.protomolt.proto.asset.v1.PdfDocument;
import ai.protomolt.proto.asset.v1.RasterImage;
import ai.protomolt.proto.asset.v1.TarArchive;
import ai.protomolt.proto.asset.v1.ZipArchive;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The routing rule: which bridges a characterized format applies, and which
 * classification states let it apply any at all.
 */
class BridgesTest {

    @Test
    void containersListTheirMembers() {
        assertThat(Bridges.applicableTo(tar())).containsExactly(
                BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS);
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setZip(ZipArchive.newBuilder().setFilename("bundle.zip")).build()))
                .containsExactly(BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS);
    }

    @Test
    void selfDescribingDatasetsBridgeToTheirSchemaOnly() {
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setParquet(ParquetDataset.newBuilder().setFilename("f.parquet")).build()))
                .containsExactly(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA);
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setAvro(AvroDataset.newBuilder().setFilename("f.avro")).build()))
                .containsExactly(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA);
    }

    @Test
    void textTablesBridgeToBothASchemaAndANormalizedDataset() {
        FormatFact csv = FormatFact.newBuilder().setDelimited(DelimitedTable.newBuilder()
                .setFilename("rows.csv").setDelimiter(",")
                .setHeader(HeaderPresence.HEADER_PRESENCE_PRESENT)).build();
        assertThat(Bridges.applicableTo(csv)).containsExactly(
                BridgeKind.BRIDGE_KIND_DATASET_SCHEMA,
                BridgeKind.BRIDGE_KIND_TABULAR_DATASET);
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setNdjson(NdjsonDataset.newBuilder().setFilename("rows.ndjson")).build()))
                .containsExactly(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA,
                        BridgeKind.BRIDGE_KIND_TABULAR_DATASET);
    }

    @Test
    void documentsBridgeToProseAndImagesToOcr() {
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setPdf(PdfDocument.newBuilder().setFilename("paper.pdf")).build()))
                .containsExactly(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT);
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setImage(RasterImage.newBuilder().setFilename("scan.png")).build()))
                .containsExactly(BridgeKind.BRIDGE_KIND_OCR_TEXT);
    }

    @Test
    void aFormatNothingConsumesBridgesToNothing() {
        // Not an oversight: no tool in the tree derives a rendition from a
        // lone gzip file or a JSON document, so the rule claims none.
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setGzip(GzipFile.newBuilder().setFilename("blob.gz")).build())).isEmpty();
        assertThat(Bridges.applicableTo(FormatFact.newBuilder()
                .setJson(JsonDocument.newBuilder().setFilename("doc.json")).build())).isEmpty();
    }

    @Test
    void onlyStatesThatNameOneFormatAreBridgeable() {
        assertThat(Bridges.bridgeable(ClassificationState.CLASSIFICATION_STATE_DECLARED)).isTrue();
        assertThat(Bridges.bridgeable(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED))
                .isTrue();
        assertThat(Bridges.bridgeable(ClassificationState.CLASSIFICATION_STATE_VERIFIED)).isTrue();
        assertThat(Bridges.bridgeable(ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED))
                .isFalse();
        assertThat(Bridges.bridgeable(ClassificationState.CLASSIFICATION_STATE_CONFLICTED))
                .isFalse();
        assertThat(Bridges.bridgeable(ClassificationState.CLASSIFICATION_STATE_UNSPECIFIED))
                .isFalse();
    }

    @Test
    void theFormatOfRecordIsTheDeclarationWhenOneStands() {
        Classification declared = Classification.newBuilder()
                .setState(ClassificationState.CLASSIFICATION_STATE_DECLARED)
                .setDeclared(tar()).build();
        assertThat(Bridges.formatOfRecord(declared)).isEqualTo(tar());

        Classification identified = Classification.newBuilder()
                .setState(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED)
                .setIdentified(tar()).build();
        assertThat(Bridges.formatOfRecord(identified)).isEqualTo(tar());
    }

    @Test
    void aConflictedOrUnclassifiedAssetHasNoFormatOfRecord() {
        Classification conflicted = Classification.newBuilder()
                .setState(ClassificationState.CLASSIFICATION_STATE_CONFLICTED)
                .setDeclared(tar())
                .setIdentified(FormatFact.newBuilder()
                        .setPdf(PdfDocument.newBuilder().setFilename("x.pdf")).build())
                .build();
        assertThat(Bridges.formatOfRecord(conflicted)).isNull();
        assertThat(Bridges.formatOfRecord(Classification.newBuilder()
                .setState(ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED).build()))
                .isNull();
    }

    @Test
    void everyKindNamesItsWellKnownRenditionAndPinsItsShape() {
        assertThat(Bridges.renditionName(BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS))
                .isEqualTo("members");
        assertThat(Bridges.renditionName(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA))
                .isEqualTo("schema");
        assertThat(Bridges.renditionName(BridgeKind.BRIDGE_KIND_TABULAR_DATASET))
                .isEqualTo("dataset");
        assertThat(Bridges.renditionName(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT))
                .isEqualTo("text");
        assertThat(Bridges.renditionName(BridgeKind.BRIDGE_KIND_OCR_TEXT))
                .isEqualTo("ocr-text");
        assertThat(Bridges.renditionName(BridgeKind.BRIDGE_KIND_CONVERSATION))
                .isEqualTo("conversation");
        assertThatThrownBy(() -> Bridges.renditionName(BridgeKind.BRIDGE_KIND_UNSPECIFIED))
                .isInstanceOf(IllegalArgumentException.class);

        // The pin comes from the generated descriptor, so it cannot drift
        // from the message the bytes actually decode as.
        assertThat(Bridges.schemaSubject(BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS))
                .isEqualTo(ContainerMembers.getDescriptor().getFullName())
                .isEqualTo("ai.protomolt.proto.asset.v1.ContainerMembers");
        assertThat(Bridges.schemaSubject(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA))
                .isEqualTo(DatasetSchema.getDescriptor().getFullName());
        assertThat(Bridges.mediaType(BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS))
                .isEqualTo("application/x-protobuf");
        assertThat(Bridges.mediaType(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT))
                .isEqualTo("text/plain");
    }

    @Test
    void theStandardEngineRunsOnlyWhatNeedsNoOtherService() {
        BridgeEngine engine = BridgeEngine.standard();
        assertThat(engine.executable()).containsExactlyInAnyOrder(
                BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS,
                BridgeKind.BRIDGE_KIND_DATASET_SCHEMA);
        assertThat(engine.forKind(BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS, tar()))
                .isPresent();
        assertThat(engine.forKind(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT, tar())).isEmpty();
    }

    @Test
    void executabilityIsPerFormatNotJustPerKind() {
        BridgeEngine engine = BridgeEngine.standard();
        FormatFact ndjson = FormatFact.newBuilder()
                .setNdjson(NdjsonDataset.newBuilder().setFilename("rows.ndjson")).build();
        FormatFact parquet = FormatFact.newBuilder()
                .setParquet(ParquetDataset.newBuilder().setFilename("f.parquet")).build();

        // One kind, two formats, two answers: the schema bridge reads NDJSON
        // and does not read a Parquet footer.
        assertThat(engine.forKind(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA, ndjson)).isPresent();
        assertThat(engine.forKind(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA, parquet)).isEmpty();
    }

    @Test
    void everyDeferrableKindNamesWhatItNeeds() {
        FormatFact parquet = FormatFact.newBuilder()
                .setParquet(ParquetDataset.newBuilder().setFilename("f.parquet")).build();
        assertThat(Bridges.deferralReason(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA, parquet))
                .contains("Parquet reader");
        assertThat(Bridges.deferralReason(BridgeKind.BRIDGE_KIND_TABULAR_DATASET, parquet))
                .contains("Parquet emitter");
        assertThat(Bridges.deferralReason(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT, tar()))
                .contains("parser service");
        assertThat(Bridges.deferralReason(BridgeKind.BRIDGE_KIND_OCR_TEXT, tar()))
                .contains("parser service");
    }

    @Test
    void theDerivedNamesAreExactlyWhatTheBridgesWrite() {
        // The list a consumer excludes when picking a primary rendition comes
        // from BridgeKind itself, so it cannot fall out of step.
        assertThat(Bridges.derivedName("members")).isTrue();
        assertThat(Bridges.derivedName("schema")).isTrue();
        assertThat(Bridges.derivedName("ocr-text")).isTrue();
        assertThat(Bridges.derivedName("original")).isFalse();
        assertThat(Bridges.derivedName("zip-file")).isFalse();
    }

    private static FormatFact tar() {
        return FormatFact.newBuilder()
                .setTar(TarArchive.newBuilder().setFilename("bundle.tar")).build();
    }
}
