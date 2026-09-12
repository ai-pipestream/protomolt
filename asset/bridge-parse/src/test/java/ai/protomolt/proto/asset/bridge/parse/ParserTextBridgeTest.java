package ai.protomolt.proto.asset.bridge.parse;

import ai.protomolt.proto.asset.bridge.Bridge;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.ContentClass;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.HtmlDocument;
import ai.protomolt.proto.asset.v1.PdfDocument;
import ai.protomolt.proto.asset.v1.PlainText;
import ai.protomolt.proto.asset.v1.RasterImage;
import ai.protomolt.proto.asset.v1.TarArchive;
import ai.protomolt.proto.parse.service.ParserClient;
import ai.protomolt.proto.parse.text.TextParserService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The parser-backed bridges, driven over a real in-process
 * {@code ParserPluginService} rather than a mock of one, so the wire idiom
 * the bridge depends on is the idiom under test.
 */
class ParserTextBridgeTest {

    static Server parserServer;
    static ManagedChannel channel;
    static ParserExtraction extraction;

    @BeforeAll
    static void boot() throws IOException {
        parserServer = InProcessServerBuilder.forName("bridge-parser")
                .addService(new TextParserService())
                .directExecutor()
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("bridge-parser").build();
        extraction = new PluginExtraction(new ParserClient(channel), Duration.ofSeconds(30));
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        parserServer.shutdownNow();
    }

    @Test
    void theProseBridgeReturnsWhatTheParserRecovered() throws IOException {
        Bridge.Derivation derivation = ParserTextBridge.prose(extraction).derive(
                bytes("# A title\n\nA paragraph of ordinary prose worth extracting.\n"),
                context(html()));

        String text = new String(derivation.content(), StandardCharsets.UTF_8);
        assertThat(text).contains("A paragraph of ordinary prose worth extracting.");
        assertThat(derivation.profile().getContentClass())
                .isEqualTo(ContentClass.CONTENT_CLASS_INFORMATIONAL_TEXT);
        // Prose carries no score: nothing about it was measured.
        assertThat(derivation.profile().hasQuality()).isFalse();
        assertThat(derivation.degraded()).isFalse();
    }

    @Test
    void theOcrBridgeAlwaysCarriesAMeasuredScore() throws IOException {
        Bridge.Derivation derivation = ParserTextBridge.ocr(extraction).derive(
                bytes("Recovered lines of perfectly ordinary text from a scanned page.\n"),
                context(image()));

        assertThat(derivation.profile().getContentClass())
                .isEqualTo(ContentClass.CONTENT_CLASS_OCR_TEXT);
        assertThat(derivation.profile().hasQuality()).isTrue();
        assertThat(derivation.profile().getQuality().getScore()).isGreaterThan(0.5);
        assertThat(derivation.profile().getQuality().getDimensionsList()).hasSize(3);
    }

    @Test
    void aDocumentWithNoProseSaysSoInsteadOfPretending() throws IOException {
        Bridge.Derivation derivation = ParserTextBridge.prose(silent()).derive(
                bytes("irrelevant"), context(pdf()));

        assertThat(derivation.content()).isEmpty();
        assertThat(derivation.degraded()).isTrue();
        assertThat(derivation.warnings()).contains(ParserTextBridge.NO_PROSE);
        // The class is still stated: the bridge ran, and found nothing.
        assertThat(derivation.profile().getContentClass())
                .isEqualTo(ContentClass.CONTENT_CLASS_INFORMATIONAL_TEXT);
    }

    @Test
    void anOcrPassThatRecoveredNothingScoresZeroAndReportsIt() throws IOException {
        Bridge.Derivation derivation = ParserTextBridge.ocr(silent()).derive(
                bytes("irrelevant"), context(image()));

        assertThat(derivation.profile().getQuality().getScore()).isZero();
        assertThat(derivation.warnings())
                .contains("the parser recovered no text from this scan");
    }

    @Test
    void aParserFailureFailsTheBridgeInTheParsersOwnWords() {
        ParserExtraction broken = (original, context) ->
                ParserExtraction.Result.failure("the parser fell over reading page 3");

        assertThatThrownBy(() -> ParserTextBridge.prose(broken)
                .derive(bytes("x"), context(pdf())))
                .isInstanceOf(IOException.class)
                .hasMessage("the parser fell over reading page 3");
    }

    @Test
    void theParsersOwnWarningsSurviveIntoTheDerivation() throws IOException {
        ParserExtraction degraded = (original, context) -> ParserExtraction.Result.of(
                "Most of the document came back.", "grparse", "1.2.3",
                List.of("page 4 was unreadable"));

        Bridge.Derivation derivation = ParserTextBridge.prose(degraded)
                .derive(bytes("x"), context(pdf()));

        assertThat(derivation.warnings()).containsExactly("page 4 was unreadable");
    }

    @Test
    void eachBridgeReadsOnlyTheFormatsItsRoutingSends() {
        Bridge prose = ParserTextBridge.prose(extraction);
        assertThat(prose.handles(pdf())).isTrue();
        assertThat(prose.handles(html())).isTrue();
        assertThat(prose.handles(image())).isFalse();
        assertThat(prose.handles(tar())).isFalse();
        assertThat(prose.kind()).isEqualTo(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT);

        Bridge ocr = ParserTextBridge.ocr(extraction);
        assertThat(ocr.handles(image())).isTrue();
        // A scanned PDF escalates to OCR, so the bridge must read one.
        assertThat(ocr.handles(pdf())).isTrue();
        assertThat(ocr.handles(text())).isFalse();
        assertThat(ocr.kind()).isEqualTo(BridgeKind.BRIDGE_KIND_OCR_TEXT);
    }

    // ------------------------------------------------------------------

    /** A parser that succeeds and recovers nothing: the scanned-page case. */
    private static ParserExtraction silent() {
        return (original, context) ->
                ParserExtraction.Result.of("", "silent-parser", "0.0.1", List.of());
    }

    private static ByteArrayInputStream bytes(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static Bridge.Context context(FormatFact format) {
        return new Bridge.Context(format, "document.txt");
    }

    private static FormatFact pdf() {
        return FormatFact.newBuilder()
                .setPdf(PdfDocument.newBuilder().setFilename("paper.pdf")).build();
    }

    private static FormatFact html() {
        return FormatFact.newBuilder()
                .setHtml(HtmlDocument.newBuilder().setFilename("page.html")).build();
    }

    private static FormatFact image() {
        return FormatFact.newBuilder()
                .setImage(RasterImage.newBuilder().setFilename("scan.png")).build();
    }

    private static FormatFact text() {
        return FormatFact.newBuilder()
                .setText(PlainText.newBuilder().setFilename("notes.txt")).build();
    }

    private static FormatFact tar() {
        return FormatFact.newBuilder()
                .setTar(TarArchive.newBuilder().setFilename("bundle.tar")).build();
    }
}
