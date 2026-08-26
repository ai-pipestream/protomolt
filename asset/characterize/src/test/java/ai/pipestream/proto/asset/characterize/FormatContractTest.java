package ai.pipestream.proto.asset.characterize;

import ai.pipestream.proto.asset.v1.ContentClass;
import ai.pipestream.proto.asset.v1.ContentProfile;
import ai.pipestream.proto.asset.v1.DelimitedTable;
import ai.pipestream.proto.asset.v1.GzipFile;
import ai.pipestream.proto.asset.v1.HeaderPresence;
import ai.pipestream.proto.asset.v1.ObjectStoreOrigin;
import ai.pipestream.proto.asset.v1.QualityScore;
import ai.pipestream.proto.asset.v1.TarArchive;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contract's rules exercised through the real validator: the strict
 * grammars refuse what they name, the gzip/tar boundary holds, a table
 * without its parameters is not a table, an origin without its
 * coordinates is not an origin, and OCR text without a measured quality
 * score is refused.
 */
class FormatContractTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static ValidationResult validate(Message message) {
        return VALIDATOR.validate(message);
    }

    @Test
    void theTarGrammarRefusesAZipName() {
        assertThat(validate(TarArchive.newBuilder().setFilename("docs.tar").build()).valid())
                .isTrue();
        assertThat(validate(TarArchive.newBuilder().setFilename("docs.tgz").build()).valid())
                .isTrue();
        ValidationResult wrong =
                validate(TarArchive.newBuilder().setFilename("docs.zip").build());
        assertThat(wrong.valid()).isFalse();
    }

    @Test
    void aTarGzNamesATarNotAGzipFile() {
        assertThat(validate(GzipFile.newBuilder().setFilename("dump.sql.gz").build()).valid())
                .isTrue();
        ValidationResult tarGz =
                validate(GzipFile.newBuilder().setFilename("docs.tar.gz").build());
        assertThat(tarGz.valid()).isFalse();
        assertThat(tarGz.violations()).anySatisfy(v ->
                assertThat(v.ruleId()).isEqualTo("gzip.not_a_tar"));
    }

    @Test
    void aTableWithoutItsParametersIsNotATable() {
        assertThat(validate(DelimitedTable.newBuilder()
                .setFilename("rows.csv")
                .setDelimiter(",")
                .setHeader(HeaderPresence.HEADER_PRESENCE_PRESENT)
                .build()).valid()).isTrue();
        assertThat(validate(DelimitedTable.newBuilder()
                .setFilename("rows.csv")
                .setHeader(HeaderPresence.HEADER_PRESENCE_PRESENT)
                .build()).valid()).as("no delimiter").isFalse();
        assertThat(validate(DelimitedTable.newBuilder()
                .setFilename("rows.csv")
                .setDelimiter(",")
                .build()).valid()).as("unstated header presence").isFalse();
    }

    @Test
    void anOriginWithoutItsCoordinatesIsRefusedByName() {
        assertThat(validate(ObjectStoreOrigin.newBuilder()
                .setBucket("landing")
                .setObjectKey("in/report.pdf")
                .build()).valid()).isTrue();
        ValidationResult missing = validate(ObjectStoreOrigin.newBuilder()
                .setBucket("landing").build());
        assertThat(missing.valid()).isFalse();
        assertThat(missing.violations()).anySatisfy(v ->
                assertThat(v.path()).contains("object_key"));
    }

    @Test
    void ocrTextWithoutAMeasuredQualityScoreIsRefused() {
        assertThat(validate(ContentProfile.newBuilder()
                .setContentClass(ContentClass.CONTENT_CLASS_OCR_TEXT)
                .setQuality(QualityScore.newBuilder().setScore(0.82))
                .build()).valid()).isTrue();
        ValidationResult unmeasured = validate(ContentProfile.newBuilder()
                .setContentClass(ContentClass.CONTENT_CLASS_OCR_TEXT)
                .build());
        assertThat(unmeasured.valid()).isFalse();
        assertThat(unmeasured.violations()).anySatisfy(v ->
                assertThat(v.ruleId()).isEqualTo("content.ocr_measures_quality"));
    }
}
