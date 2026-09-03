package ai.protomolt.proto.asset.characterize;

import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.DelimitedTable;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.HeaderPresence;
import ai.protomolt.proto.asset.v1.PdfDocument;
import ai.protomolt.proto.asset.v1.PlainText;
import ai.protomolt.proto.asset.v1.TarArchive;
import ai.protomolt.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The state machine's one resolution point, and the shape pin: every
 * classification the resolver produces validates against the contract's
 * own per-state message rules — the machine can never mint a state its
 * fields do not support.
 */
class ClassificationsTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final Instant NOW = Instant.ofEpochSecond(1_750_000_000);

    private static FormatFact tar(String name) {
        return FormatFact.newBuilder()
                .setTar(TarArchive.newBuilder().setFilename(name)).build();
    }

    private static Characterizer.Identification none() {
        return new Characterizer.Identification(null, List.of());
    }

    private static Characterizer.Identification of(FormatFact fact) {
        return new Characterizer.Identification(fact, List.of());
    }

    private static Classification resolve(FormatFact declared,
                                          Characterizer.Identification id) {
        Classification classification =
                Classifications.resolve(declared, id, null, null, NOW);
        assertThat(VALIDATOR.validate(classification).valid())
                .as(classification + " must satisfy its own per-state rules")
                .isTrue();
        return classification;
    }

    @Test
    void nothingKnownIsUnclassifiedNeverADefault() {
        assertThat(resolve(null, none()).getState())
                .isEqualTo(ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED);
    }

    @Test
    void aLoneDeclarationStandsAsDeclared() {
        Classification c = resolve(tar("a.tar"), none());
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_DECLARED);
        assertThat(c.hasDeclared()).isTrue();
        assertThat(c.hasIdentified()).isFalse();
    }

    @Test
    void aLoneIdentificationStandsAsIdentified() {
        Classification c = resolve(null, of(tar("a.tar")));
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED);
        assertThat(c.hasIdentified()).isTrue();
    }

    @Test
    void agreementIsVerified() {
        Classification c = resolve(tar("a.tar"), of(tar("a.tar")));
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_VERIFIED);
        assertThat(c.hasDeclared()).isTrue();
        assertThat(c.hasIdentified()).isTrue();
    }

    @Test
    void aGeneralizedConclusionLetsTheClaimStand() {
        // Delimited tables sniff as plain text: the identification is a
        // generalization, not a refutation, so the declaration stands and
        // the generalized conclusion is not stored as a finding.
        FormatFact delimited = FormatFact.newBuilder()
                .setDelimited(DelimitedTable.newBuilder()
                        .setFilename("rows.csv")
                        .setDelimiter(",")
                        .setHeader(HeaderPresence.HEADER_PRESENCE_PRESENT))
                .build();
        Characterizer.Identification text = Characterizer.identify(
                "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8), "rows.csv");
        Classification c = resolve(delimited,
                text.identified() ? text : of(FormatFact.newBuilder()
                        .setText(PlainText.newBuilder()).build()));
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_DECLARED);
        assertThat(c.hasIdentified()).isFalse();
    }

    @Test
    void aContradictionIsConflictedWithBothFactsKept() {
        FormatFact pdf = FormatFact.newBuilder()
                .setPdf(PdfDocument.newBuilder().setFilename("a.pdf")).build();
        Classification c = resolve(tar("a.tar"), of(pdf));
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_CONFLICTED);
        assertThat(c.getDeclared().getFormatCase()).isEqualTo(FormatFact.FormatCase.TAR);
        assertThat(c.getIdentified().getFormatCase()).isEqualTo(FormatFact.FormatCase.PDF);
    }

    @Test
    void ooxmlInsideZipDoesNotConflict() {
        FormatFact word = FormatFact.newBuilder()
                .setWord(ai.protomolt.proto.asset.v1.WordDocument.newBuilder()
                        .setFilename("r.docx"))
                .build();
        FormatFact zip = FormatFact.newBuilder()
                .setZip(ai.protomolt.proto.asset.v1.ZipArchive.newBuilder()).build();
        assertThat(FormatCompatibility.contradicts(word, zip)).isFalse();
        assertThat(FormatCompatibility.contradicts(zip, word)).isTrue();
    }
}
