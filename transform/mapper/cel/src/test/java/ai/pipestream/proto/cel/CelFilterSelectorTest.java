package ai.pipestream.proto.cel;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CelFilterSelectorTest {
    private CelProtoMapper mapper;
    @BeforeEach void setUp() {
        var registry = DescriptorRegistry.create(); registry.registerFile(CelFixtures.FILE);
        var evaluator = new CelEvaluator(CelEnvironmentFactory.builder().addMessageType(CelFixtures.DOCUMENT).addVar("input").build());
        mapper = new CelProtoMapper(new ProtoFieldMapperImpl(registry), evaluator);
    }
    @Test void filterFalseSkipsTheRule() throws Exception {
        var document = CelFixtures.doc("title"); mapper.map(document, List.of(new CelMappingRule("false", "'x'", "body")));
        assertEquals("", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
    @Test void filterTrueAndBlankApply() throws Exception {
        var document = CelFixtures.doc("title"); mapper.map(document, List.of(new CelMappingRule("", "'x'", "body")));
        assertEquals("x", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
    @Test void laterRulesSeeEarlierRulesEffects() throws Exception {
        var document = CelFixtures.doc("title"); mapper.map(document, List.of(
                new CelMappingRule("", "'first'", "body"), new CelMappingRule("input.body == 'first'", "'second'", "language")));
        assertEquals("second", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("language")));
    }
    @Test void filteredCandidateFallsBackToTheNext() throws Exception {
        var document = CelFixtures.doc("title");
        assertTrue(mapper.mapFirstCandidate(document, List.of(new CelMappingRule("false", "'x'", "body"), new CelMappingRule("true", "'ok'", "body"))));
        assertEquals("ok", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
    @Test void selectorExtractsByCelExpression() throws Exception {
        var document = CelFixtures.doc("hello"); mapper.map(document, List.of(new CelMappingRule("", "input.title + '-world'", "body")));
        assertEquals("hello-world", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
    @Test void selectorReachesNestedMessage() throws Exception {
        var document = CelFixtures.doc("title");
        document.setField(CelFixtures.DOCUMENT.findFieldByName("info"), com.google.protobuf.DynamicMessage.newBuilder(CelFixtures.INFO)
                .setField(CelFixtures.INFO.findFieldByName("version"), "v1").build());
        mapper.map(document, List.of(new CelMappingRule("", "input.info.version", "body")));
        assertEquals("v1", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
    @Test void selectorMissFallsBackToNextCandidate() throws Exception {
        var document = CelFixtures.doc("title");
        assertTrue(mapper.mapFirstCandidate(document, List.of(new CelMappingRule("", "input.nope", "body"), new CelMappingRule("", "'ok'", "body"))));
        assertEquals("ok", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
    @Test void invalidFilterThrowsInStrictMap() {
        var document = CelFixtures.doc("title");
        assertThrows(CelEvaluationException.class,
                () -> mapper.map(document, List.of(new CelMappingRule("input.title ==", "'x'", "body"))));
    }
    @Test void invalidFilterSkipsCandidateInSoftMode() throws Exception {
        var document = CelFixtures.doc("title");
        assertTrue(mapper.mapFirstCandidate(document, List.of(
                new CelMappingRule("input.title ==", "'x'", "body"), new CelMappingRule("", "'ok'", "body"))));
        assertEquals("ok", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
    @Test void extraBindingIsVisibleToFilters() throws Exception {
        var registry = DescriptorRegistry.create(); registry.registerFile(CelFixtures.FILE);
        var evaluator = new CelEvaluator(CelEnvironmentFactory.builder().addMessageType(CelFixtures.DOCUMENT).addMessageType(Struct.getDescriptor()).addVar("input").addVar("stream").build());
        Struct stream = Struct.newBuilder().putFields("stream_id", Value.newBuilder().setStringValue("preview-1").build()).build();
        var bound = new CelProtoMapper(new ProtoFieldMapperImpl(registry), evaluator, "input", Map.of("stream", stream));
        var document = CelFixtures.doc("title"); bound.map(document, List.of(new CelMappingRule("stream.stream_id == 'preview-1'", "'yes'", "body")));
        assertEquals("yes", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }
}
