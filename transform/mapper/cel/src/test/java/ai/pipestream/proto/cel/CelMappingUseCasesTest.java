package ai.pipestream.proto.cel;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Creative CEL mapping scenarios beyond the engine filter/selector pin tests.
 */
class CelMappingUseCasesTest {

    private DescriptorRegistry registry;
    private ProtoFieldMapperImpl fieldMapper;
    private CelEvaluator evaluator;
    private CelProtoMapper mapper;

    @BeforeEach
    void setUp() {
        registry = DescriptorRegistry.create();
        registry.registerFile(CelFixtures.FILE);
        fieldMapper = new ProtoFieldMapperImpl(registry);
        evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(CelFixtures.DOCUMENT)
                .addMessageType(CelFixtures.INFO)
                .addMessageType(Struct.getDescriptor())
                .addVar("input")
                .build());
        mapper = new CelProtoMapper(fieldMapper, evaluator);
    }

    @Test
    void multiRulePipelineNormalizesAndDefaults() throws Exception {
        var doc = CelFixtures.doc("  Hello  ");
        doc.setField(CelFixtures.DOCUMENT.findFieldByName("language"), "");
        mapper.map(doc, List.of(
                new CelMappingRule("", "input.title", "body"),
                new CelMappingRule("input.language == ''", "'und'", "language"),
                new CelMappingRule("", null, "tier", List.of("tier = \"standard\""))
        ));
        var built = doc.build();
        assertEquals("  Hello  ", built.getField(CelFixtures.DOCUMENT.findFieldByName("body")));
        assertEquals("und", built.getField(CelFixtures.DOCUMENT.findFieldByName("language")));
        assertEquals("standard", built.getField(CelFixtures.DOCUMENT.findFieldByName("tier")));
    }

    @Test
    void conditionalEnrichmentWritesTierFromScore() throws Exception {
        var doc = CelFixtures.doc("doc");
        doc.setField(CelFixtures.DOCUMENT.findFieldByName("score"), 0.92);
        mapper.map(doc, List.of(
                new CelMappingRule("input.score >= 0.9", "'gold'", "tier"),
                new CelMappingRule("input.score < 0.9", "'silver'", "tier")
        ));
        assertEquals("gold", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("tier")));
    }

    @Test
    void coalesceCandidatesPreferPresentPath() throws Exception {
        var doc = CelFixtures.doc("headline");
        assertTrue(mapper.mapFirstCandidate(doc, List.of(
                new CelMappingRule("", "input.missing", "body"),
                new CelMappingRule("", "input.title", "body")
        )));
        assertEquals("headline", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void textRulesSeedStructThenCelReadsIt() throws Exception {
        var doc = CelFixtures.doc("t");
        mapper.map(doc, List.of(
                new CelMappingRule("", null, "metadata", List.of(
                        "metadata.source = \"tika\"",
                        "metadata.pages = 3"
                )),
                new CelMappingRule("input.metadata.source == 'tika'", "'parsed'", "tier")
        ));
        assertEquals("parsed", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("tier")));
        assertEquals("tika", fieldMapper.getValue(doc.build(), "metadata.source"));
    }

    @Test
    void appendTagsInStructViaTextFallback() throws Exception {
        var doc = CelFixtures.doc("t");
        mapper.map(doc, List.of(new CelMappingRule("", null, "metadata", List.of(
                "metadata.tags += \"nlp\"",
                "metadata.tags += \"cel\""
        ))));
        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) fieldMapper.getValue(doc.build(), "metadata.tags");
        assertEquals(List.of("nlp", "cel"), tags);
    }

    @Test
    void clearThenReassign() throws Exception {
        var doc = CelFixtures.doc("keep");
        doc.setField(CelFixtures.DOCUMENT.findFieldByName("body"), "old");
        mapper.map(doc, List.of(
                new CelMappingRule("", null, "body", List.of("-body")),
                new CelMappingRule("", "input.title", "body")
        ));
        assertEquals("keep", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void ternarySelectorChoosesLabel() throws Exception {
        var doc = CelFixtures.doc("t");
        doc.setField(CelFixtures.DOCUMENT.findFieldByName("language"), "en");
        mapper.map(doc, List.of(new CelMappingRule(
                "",
                "input.language == 'en' ? 'English' : 'Other'",
                "tier")));
        assertEquals("English", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("tier")));
    }

    @Test
    void numericComparisonFilter() throws Exception {
        var low = CelFixtures.doc("a");
        low.setField(CelFixtures.DOCUMENT.findFieldByName("score"), 0.1);
        mapper.map(low, List.of(new CelMappingRule("input.score > 0.5", "'hot'", "tier")));
        assertEquals("", low.build().getField(CelFixtures.DOCUMENT.findFieldByName("tier")));

        var high = CelFixtures.doc("b");
        high.setField(CelFixtures.DOCUMENT.findFieldByName("score"), 0.8);
        mapper.map(high, List.of(new CelMappingRule("input.score > 0.5", "'hot'", "tier")));
        assertEquals("hot", high.build().getField(CelFixtures.DOCUMENT.findFieldByName("tier")));
    }

    @Test
    void independentEvaluatorsDoNotShareCache() {
        CelEvaluator a = new CelEvaluator(CelEnvironmentFactory.builder().addVar("input").build());
        CelEvaluator b = new CelEvaluator(CelEnvironmentFactory.builder().addVar("input").build());
        a.evaluateValue("1 + 1", Map.of());
        assertEquals(1, a.cacheSize());
        assertEquals(0, b.cacheSize());
    }

    @Test
    void textRuleFallbackChainsProgressively() throws Exception {
        var doc = CelFixtures.doc("Alpha");
        mapper.map(doc, List.of(new CelMappingRule("", null, "body", List.of(
                "body = title",
                "language = \"en\"",
                "tier = body"
        ))));
        var built = doc.build();
        assertEquals("Alpha", built.getField(CelFixtures.DOCUMENT.findFieldByName("body")));
        assertEquals("en", built.getField(CelFixtures.DOCUMENT.findFieldByName("language")));
        assertEquals("Alpha", built.getField(CelFixtures.DOCUMENT.findFieldByName("tier")));
    }

    @Test
    void unicodeAndEmojiInCelStrings() throws Exception {
        var doc = CelFixtures.doc("文档");
        mapper.map(doc, List.of(new CelMappingRule(
                "",
                "input.title + ' 🚀'",
                "body")));
        assertEquals("文档 🚀", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void blankFilterMeansApply() throws Exception {
        var doc = CelFixtures.doc("t");
        assertTrue(mapper.tryMap(doc, new CelMappingRule("   ", "'yes'", "body")));
        assertEquals("yes", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void selectorWritesNestedMessageField() throws Exception {
        var doc = CelFixtures.doc("t");
        doc.setField(CelFixtures.DOCUMENT.findFieldByName("info"),
                DynamicMessage.newBuilder(CelFixtures.INFO)
                        .setField(CelFixtures.INFO.findFieldByName("version"), "0")
                        .build());
        mapper.map(doc, List.of(new CelMappingRule("", "'2.0'", "info.version")));
        assertEquals("2.0", fieldMapper.getValue(doc.build(), "info.version"));
    }

    @Test
    void mixedSkipAndApplyRules() throws Exception {
        var doc = CelFixtures.doc("src");
        mapper.map(doc, List.of(
                new CelMappingRule("false", "'no'", "body"),
                new CelMappingRule("true", "input.title", "body"),
                new CelMappingRule("false", "'nope'", "language"),
                new CelMappingRule("", "'en'", "language")
        ));
        var built = doc.build();
        assertEquals("src", built.getField(CelFixtures.DOCUMENT.findFieldByName("body")));
        assertEquals("en", built.getField(CelFixtures.DOCUMENT.findFieldByName("language")));
    }

    @Test
    void allCandidatesFailReturnsFalse() throws Exception {
        var doc = CelFixtures.doc("t");
        assertFalse(mapper.mapFirstCandidate(doc, List.of(
                new CelMappingRule("false", "'a'", "body"),
                new CelMappingRule("", "input.missing", "body")
        )));
        assertEquals("", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void streamAndInputBindingsTogether() throws Exception {
        Struct stream = Struct.newBuilder()
                .putFields("tenant", Value.newBuilder().setStringValue("acme").build())
                .build();
        var bound = new CelProtoMapper(fieldMapper,
                new CelEvaluator(CelEnvironmentFactory.builder()
                        .addMessageType(CelFixtures.DOCUMENT)
                        .addMessageType(Struct.getDescriptor())
                        .addVar("input")
                        .addVar("stream")
                        .build()),
                "input",
                Map.of("stream", stream));
        var doc = CelFixtures.doc("Hello");
        bound.map(doc, List.of(new CelMappingRule(
                "stream.tenant == 'acme'",
                "input.title + ' @' + stream.tenant",
                "body")));
        assertEquals("Hello @acme", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void tryMapReturnsFalseOnFilterWithoutMutating() throws Exception {
        var doc = CelFixtures.doc("t");
        doc.setField(CelFixtures.DOCUMENT.findFieldByName("body"), "keep");
        assertFalse(mapper.tryMap(doc, new CelMappingRule("input.title == 'other'", "'x'", "body")));
        assertEquals("keep", doc.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
}
