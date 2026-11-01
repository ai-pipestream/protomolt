package ai.protomolt.proto.skos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptNotation;
import ai.protomolt.proto.types.ConceptScheme;
import com.google.protobuf.UnknownFieldSet;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SkosConverterTest {
    @TempDir Path directory;
    private static final String PREFIX = """
            @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
            @prefix ex: <https://example.org/> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix dct: <http://purl.org/dc/terms/> .
            ex:scheme a skos:ConceptScheme; skos:prefLabel "Subjects"@en .
            """;
    private static final String SIMPLE = PREFIX + """
            ex:a a skos:Concept; skos:inScheme ex:scheme; skos:prefLabel "Alpha"@en .
            """;
    private static final ConceptRef EXTERNAL = ConceptRef.newBuilder()
            .setSchemeId("other").setConceptId("remote").build();
    private final SkosConverter converter = new SkosConverter("subject", "https://example.org/scheme",
            Map.of("https://other.example/concept", EXTERNAL));

    private ConceptScheme read(String text) {
        return converter.read(bytes(text), SkosConverter.Format.TURTLE);
    }

    private static InputStream bytes(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    @Test void importsStableUuid5IdentityAndPreservesNotationSourceOrder() {
        var scheme = read(SIMPLE + """
                ex:a skos:notation "second"^^<urn:notation:code>, "001", "first"^^<urn:notation:code> .
                """);
        var concept = scheme.getConcepts(0);
        assertThat(concept.getId()).isEqualTo("bd172ad6-0e60-5be0-af2c-27044386d223");
        assertThat(concept.getIri()).isEqualTo("https://example.org/a");
        assertThat(concept.getNotationList()).extracting(n -> n.getValue())
                .containsExactly("second", "001", "first");
        assertThat(concept.getNotation(0).getDatatypeIri()).isEqualTo("urn:notation:code");
        assertThat(concept.getNotation(1).getDatatypeIri()).isEmpty();
        assertThat(read(SIMPLE).getConcepts(0).getId()).isEqualTo(concept.getId());
    }

    @Test void roundTripsBothRdfFormatsWithLabelsMappingsAndRetirement() throws Exception {
        var original = read(SIMPLE + """
                ex:a skos:prefLabel "Alphabète"@fr; skos:altLabel "First"@en;
                  skos:hiddenLabel "Alfa"@en; skos:definition "First letter."@en;
                  skos:scopeNote "Use for letters."@en; skos:notation "001"^^<urn:notation:code>;
                  skos:exactMatch <https://other.example/concept>;
                  skos:closeMatch <https://other.example/concept>;
                  skos:broadMatch <https://other.example/concept>;
                  skos:narrowMatch <https://other.example/concept>;
                  skos:relatedMatch <https://other.example/concept> .
                ex:b a skos:Concept; skos:inScheme ex:scheme; skos:prefLabel "Beta"@en; skos:broader ex:a .
                ex:c a skos:Concept; skos:inScheme ex:scheme; skos:prefLabel "Old"@en;
                  owl:deprecated true; dct:isReplacedBy ex:b; skos:related ex:a .
                """);
        for (var format : SkosConverter.Format.values()) {
            var output = new ByteArrayOutputStream();
            converter.write(original, output, format);
            var result = converter.read(new ByteArrayInputStream(output.toByteArray()), format);
            assertThat(result.getConceptsList()).hasSize(3);
            var a = result.getConceptsList().stream().filter(c -> c.getIri().endsWith("/a")).findFirst().orElseThrow();
            var expected = original.getConcepts(0);
            assertThat(a.getPrefLabelList()).containsExactlyInAnyOrderElementsOf(expected.getPrefLabelList());
            assertThat(a.getAltLabelList()).isEqualTo(expected.getAltLabelList());
            assertThat(a.getHiddenLabelList()).isEqualTo(expected.getHiddenLabelList());
            assertThat(a.getDefinitionList()).isEqualTo(expected.getDefinitionList());
            assertThat(a.getScopeNoteList()).isEqualTo(expected.getScopeNoteList());
            assertThat(a.getNotationList()).isEqualTo(expected.getNotationList());
            assertThat(a.getExactMatchList()).containsExactly(EXTERNAL);
            assertThat(a.getCloseMatchList()).containsExactly(EXTERNAL);
            assertThat(a.getBroadMatchList()).containsExactly(EXTERNAL);
            assertThat(a.getNarrowMatchList()).containsExactly(EXTERNAL);
            assertThat(a.getRelatedMatchList()).containsExactly(EXTERNAL);
            var b = result.getConceptsList().stream().filter(c -> c.getIri().endsWith("/b")).findFirst().orElseThrow();
            var c = result.getConceptsList().stream().filter(v -> v.getIri().endsWith("/c")).findFirst().orElseThrow();
            assertThat(b.getBroaderList()).containsExactly(a.getId());
            assertThat(c.getDeprecated()).isTrue();
            assertThat(c.getReplacedByList()).containsExactly(b.getId());
            assertThat(c.getRelatedList()).containsExactly(a.getId());
            assertThat(ConceptScheme.parseFrom(result.toByteArray())).isEqualTo(result);
        }
    }

    @Test void readsNarrowerAsBroaderAndRefusesInconsistentDualDirections() {
        String concepts = SIMPLE + """
                ex:b a skos:Concept; skos:inScheme ex:scheme; skos:prefLabel "Beta"@en .
                """;
        assertThat(read(concepts + "ex:a skos:narrower ex:b .").getConcepts(1).getBroaderList())
                .containsExactly(SkosConverter.conceptId("https://example.org/a"));
        assertThatThrownBy(() -> read(concepts + "ex:a skos:broader ex:b; skos:narrower ex:b ."))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("broader/narrower");
    }

    @Test void refusesUnsupportedRdfAndUnresolvedMappingsByName() {
        for (String statement : new String[] {
                "ex:a skos:editorialNote \"hidden\"@en .",
                "ex:a <https://unknown.example/predicate> \"hidden\" .",
                "ex:collection a skos:Collection .",
                "ex:a skos:exactMatch <https://unknown.example/concept> .",
                "ex:a skos:broader _:blank .",
                "ex:a skos:prefLabel \"No language\" .",
                "ex:a skos:inScheme ex:anotherScheme ."}) {
            assertThatThrownBy(() -> read(SIMPLE + statement))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("SKOS");
        }
    }

    @Test void refusesInvalidLabelsAndHierarchyWithExistingRuleNames() {
        assertThatThrownBy(() -> read(SIMPLE + "ex:a skos:prefLabel \"Again\"@en ."))
                .hasMessageContaining("concept_scheme.pref_label_language");
        assertThatThrownBy(() -> read(SIMPLE + "ex:a skos:broader ex:a ."))
                .hasMessageContaining("concept_scheme.broader_cycle");
        assertThatThrownBy(() -> read(SIMPLE + "ex:a skos:broader ex:missing ."))
                .hasMessageContaining("missing");
    }

    @Test void exportRefusesMissingIriUnknownFieldsAndInvalidHierarchyBeforeWriting() {
        var valid = read(SIMPLE);
        var unknown = UnknownFieldSet.newBuilder().addField(991,
                UnknownFieldSet.Field.newBuilder().addVarint(3).build()).build();
        for (var invalid : new ConceptScheme[] {
                valid.toBuilder().setConcepts(0, valid.getConcepts(0).toBuilder().clearIri()).build(),
                valid.toBuilder().setUnknownFields(unknown).build(),
                valid.toBuilder().setConcepts(0, valid.getConcepts(0).toBuilder().setUnknownFields(unknown)).build(),
                valid.toBuilder().setConcepts(0, valid.getConcepts(0).toBuilder().addBroader(valid.getConcepts(0).getId())).build()
        }) {
            var output = new ByteArrayOutputStream();
            assertThatThrownBy(() -> converter.write(invalid, output, SkosConverter.Format.TURTLE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(output.size()).isZero();
        }
    }

    @Test void malformedRdfAndIoFailureRemainFailures() {
        assertThatThrownBy(() -> read(SIMPLE + "ex:a skos:prefLabel ["))
                .isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(RuntimeException.class);
        InputStream broken = new InputStream() {
            @Override public int read() throws IOException { throw new IOException("source unavailable"); }
        };
        assertThatThrownBy(() -> converter.read(broken, SkosConverter.Format.TURTLE))
                .isInstanceOf(java.io.UncheckedIOException.class)
                .hasStackTraceContaining("source unavailable");
        OutputStream brokenOutput = new OutputStream() {
            @Override public void write(int value) throws IOException { throw new IOException("sink unavailable"); }
        };
        assertThatThrownBy(() -> converter.write(read(SIMPLE), brokenOutput, SkosConverter.Format.TURTLE))
                .hasStackTraceContaining("sink unavailable");
    }

    @Test void rdfXmlExternalEntitiesDoNotBecomeLabels() throws Exception {
        Path sentinel = directory.resolve("external.txt");
        Files.writeString(sentinel, "External content must not be imported");
        String xml = """
                <?xml version="1.0"?>
                <!DOCTYPE rdf:RDF [<!ENTITY secret SYSTEM "%s">]>
                <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                         xmlns:skos="http://www.w3.org/2004/02/skos/core#">
                  <skos:ConceptScheme rdf:about="https://example.org/scheme"/>
                  <skos:Concept rdf:about="https://example.org/a">
                    <skos:inScheme rdf:resource="https://example.org/scheme"/>
                    <skos:prefLabel xml:lang="en">&secret;</skos:prefLabel>
                  </skos:Concept>
                </rdf:RDF>
                """.formatted(sentinel.toUri());
        assertThatThrownBy(() -> converter.read(bytes(xml), SkosConverter.Format.RDF_XML))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void exportRefusesAmbiguityInsteadOfCollapsingProtobufValues() {
        var valid = read(SIMPLE);
        for (var bad : new ConceptScheme[] {
                valid.toBuilder().setConcepts(0, valid.getConcepts(0).toBuilder()
                        .addNotation(ConceptNotation.newBuilder().setValue("one")
                                .setDatatypeIri("http://www.w3.org/2001/XMLSchema#string"))).build(),
                valid.toBuilder().setConcepts(0, valid.getConcepts(0).toBuilder()
                        .addExactMatch(EXTERNAL).addExactMatch(EXTERNAL)).build()
        }) {
            var output = new ByteArrayOutputStream();
            assertThatThrownBy(() -> converter.write(bad, output, SkosConverter.Format.RDF_XML))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(output.size()).isZero();
        }
        assertThatThrownBy(() -> new SkosConverter("subject", "https://example.org/scheme",
                Map.of("https://other.example/one", EXTERNAL, "https://other.example/two", EXTERNAL)))
                .hasMessageContaining("ambiguous");
        assertThatThrownBy(() -> new SkosConverter("subject", "https://example.org/scheme",
                Map.of("https://example.org/scheme", EXTERNAL)))
                .hasMessageContaining("scheme IRI");
    }

    @Test void exportNeedsAnExplicitIriForEveryCrossSchemeMapping() {
        var scheme = read(SIMPLE + "ex:a skos:exactMatch <https://other.example/concept> .");
        var withoutBindings = new SkosConverter("subject", "https://example.org/scheme", Map.of());
        var output = new ByteArrayOutputStream();
        assertThatThrownBy(() -> withoutBindings.write(scheme, output, SkosConverter.Format.TURTLE))
                .hasMessageContaining("unresolved export mapping other/remote");
        assertThat(output.size()).isZero();
    }

    @Test void malformedUtf8CannotBecomeReplacementText() throws Exception {
        for (byte[] invalid : new byte[][] {{(byte) 0x80}, {(byte) 0xc0, (byte) 0xaf},
                {(byte) 0xed, (byte) 0xa0, (byte) 0x80}}) {
            var input = new ByteArrayOutputStream();
            input.write((PREFIX + "ex:a a skos:Concept; skos:inScheme ex:scheme; skos:prefLabel \"a")
                    .getBytes(StandardCharsets.UTF_8));
            input.write(invalid);
            input.write("b\"@en .".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> converter.read(new ByteArrayInputStream(input.toByteArray()), SkosConverter.Format.TURTLE))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("malformed UTF-8");
        }
    }

    @Test void exportRefusesUnpairedSurrogatesBeforeWriting() {
        var valid = read(SIMPLE);
        for (String invalid : new String[] {"high\ud800end", "low\udc00end"}) {
            var label = valid.getConcepts(0).getPrefLabel(0).toBuilder().setText(invalid);
            var withLabel = valid.toBuilder().setConcepts(0, valid.getConcepts(0).toBuilder().setPrefLabel(0, label)).build();
            var withNotation = valid.toBuilder().setConcepts(0, valid.getConcepts(0).toBuilder()
                    .addNotation(ConceptNotation.newBuilder().setValue(invalid))).build();
            for (var format : SkosConverter.Format.values()) {
                for (var scheme : new ConceptScheme[] {withLabel, withNotation}) {
                    var output = new ByteArrayOutputStream();
                    assertThatThrownBy(() -> converter.write(scheme, output, format))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unpaired UTF-16");
                    assertThat(output.size()).isZero();
                }
            }
        }
    }

    @Test void identitiesUseResolvedRdfIrisAndExportNeverChangesThem() {
        var resolved = read(PREFIX + """
                <https://example.org/a/../b> a skos:Concept;
                    skos:inScheme ex:scheme; skos:prefLabel "Beta"@en .
                """);
        assertThat(resolved.getConcepts(0).getIri()).isEqualTo("https://example.org/b");
        assertThat(resolved.getConcepts(0).getId()).isEqualTo(SkosConverter.conceptId("https://example.org/b"));
        assertThat(SkosConverter.conceptId("https://example.org/%41"))
                .isNotEqualTo(SkosConverter.conceptId("https://example.org/A"));
        assertThatThrownBy(() -> SkosConverter.conceptId("https://example.org/a/../b"))
                .hasMessageContaining("IRI is not resolved");
        var nonResolved = resolved.toBuilder().setConcepts(0,
                resolved.getConcepts(0).toBuilder().setIri("https://example.org/a/../b")).build();
        var datatype = resolved.toBuilder().setConcepts(0, resolved.getConcepts(0).toBuilder()
                .addNotation(ConceptNotation.newBuilder().setValue("code")
                        .setDatatypeIri("https://example.org/a/../datatype"))).build();
        for (var scheme : new ConceptScheme[] {nonResolved, datatype}) {
            var output = new ByteArrayOutputStream();
            assertThatThrownBy(() -> converter.write(scheme, output, SkosConverter.Format.TURTLE))
                    .hasMessageContaining("IRI is not resolved");
            assertThat(output.size()).isZero();
        }
    }

    @Test void numericNotationLexicalFormAndSupplementaryUnicodeSurviveBothFormats() {
        var original = read(PREFIX + """
                ex:a a skos:Concept; skos:inScheme ex:scheme;
                    skos:prefLabel "Letter 🙂"@EN;
                    skos:notation "001"^^<http://www.w3.org/2001/XMLSchema#integer> .
                """);
        // RDF language tags compare case-insensitively; Jena emits lowercase tags.
        assertThat(original.getConcepts(0).getPrefLabel(0).getLanguage()).isEqualTo("en");
        for (var format : SkosConverter.Format.values()) {
            var output = new ByteArrayOutputStream();
            converter.write(original, output, format);
            var roundTrip = converter.read(new ByteArrayInputStream(output.toByteArray()), format);
            assertThat(roundTrip).isEqualTo(original);
            assertThat(roundTrip.getConcepts(0).getNotation(0).getValue()).isEqualTo("001");
        }
    }
}
