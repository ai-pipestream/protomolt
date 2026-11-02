package ai.protomolt.proto.skos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.types.ConceptScheme;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SkosMainTest {
    @TempDir Path directory;

    @Test void fileVerbsRoundTripBinaryProtobufAndRefuseOverwriting() throws Exception {
        Path input = directory.resolve("source.ttl");
        Path binary = directory.resolve("scheme.pb");
        Path xml = directory.resolve("export.rdf");
        Path binaryAgain = directory.resolve("again.pb");
        Files.writeString(input, """
                @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
                <https://example.org/scheme> a skos:ConceptScheme .
                <https://example.org/a> a skos:Concept;
                    skos:inScheme <https://example.org/scheme>; skos:prefLabel "Alpha"@en .
                """);
        run("import", "turtle", input, binary);
        var scheme = ConceptScheme.parseFrom(Files.readAllBytes(binary));
        assertThat(scheme.getConcepts(0).getPrefLabel(0).getText()).isEqualTo("Alpha");
        run("export", "rdfxml", binary, xml);
        run("import", "rdfxml", xml, binaryAgain);
        assertThat(ConceptScheme.parseFrom(Files.readAllBytes(binaryAgain))).isEqualTo(scheme);
        assertThatThrownBy(() -> run("export", "rdfxml", binary, input))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        assertThat(Files.readString(input)).startsWith("@prefix");
        try (var files = Files.list(directory)) {
            assertThat(files.map(p -> p.getFileName().toString()).toList())
                    .containsExactlyInAnyOrder("source.ttl", "scheme.pb", "export.rdf", "again.pb");
        }
    }

    @Test void conversionFailurePublishesNoOutputAndCleansStaging() throws Exception {
        Path source = directory.resolve("bad.ttl");
        Path target = directory.resolve("bad.pb");
        Files.writeString(source, "not Turtle");
        assertThatThrownBy(() -> run("import", "turtle", source, target))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(target).doesNotExist();
        try (var files = Files.list(directory)) {
            assertThat(files.toList()).containsExactly(source);
        }
    }

    private void run(String verb, String format, Path input, Path output) throws Exception {
        SkosMain.main(new String[] {verb, "subject", "https://example.org/scheme", format,
                input.toString(), output.toString()});
    }
}
