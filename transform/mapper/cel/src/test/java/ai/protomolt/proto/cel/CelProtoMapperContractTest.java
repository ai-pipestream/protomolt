package ai.protomolt.proto.cel;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link CelProtoMapper} null/strictness/candidate contract beyond the use-case coverage. */
class CelProtoMapperContractTest {

    private CelProtoMapper mapper;

    @BeforeEach
    void setUp() {
        var registry = DescriptorRegistry.create();
        registry.registerFile(CelFixtures.FILE);
        var evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(CelFixtures.DOCUMENT).addVar("input").build());
        mapper = new CelProtoMapper(new ProtoFieldMapperImpl(registry), evaluator);
    }

    @Test
    void nullRuleListIsANoOp() throws Exception {
        var document = CelFixtures.doc("t");
        mapper.map(document, null);
        assertEquals("", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void nullCandidateListReturnsFalse() throws Exception {
        var document = CelFixtures.doc("t");
        assertFalse(mapper.mapFirstCandidate(document, null));
    }

    @Test
    void ruleWithNeitherSelectorNorFallbackDoesNotApply() throws Exception {
        var document = CelFixtures.doc("t");
        assertFalse(mapper.tryMap(document, new CelMappingRule("", null, "body")));
        mapper.map(document, List.of(new CelMappingRule("", null, "body")));
        assertEquals("", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void tryMapSoftensSelectorRuntimeFailure() throws Exception {
        var document = CelFixtures.doc("t");
        assertFalse(mapper.tryMap(document, new CelMappingRule("", "1 / 0", "body")));
        assertEquals("", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void strictMapPropagatesSelectorRuntimeFailure() {
        var document = CelFixtures.doc("t");
        CelEvaluationException e = assertThrows(CelEvaluationException.class,
                () -> mapper.map(document, List.of(new CelMappingRule("", "1 / 0", "body"))));
        assertFalse(e instanceof CelCompilationException);
    }

    @Test
    void strictMapPropagatesSelectorCompileFailure() {
        var document = CelFixtures.doc("t");
        assertThrows(CelCompilationException.class,
                () -> mapper.map(document, List.of(new CelMappingRule("", "input.title +(", "body"))));
    }

    @Test
    void pathFailurePropagatesEvenFromTryMap() {
        // The selector succeeds; only the write fails. Documented as the one throwing path.
        var document = CelFixtures.doc("t");
        assertThrows(MappingException.class,
                () -> mapper.tryMap(document, new CelMappingRule("", "'x'", "nosuchfield")));
    }

    @Test
    void tryMapReturnsTrueWhenTheRuleApplies() throws Exception {
        var document = CelFixtures.doc("t");
        assertTrue(mapper.tryMap(document, new CelMappingRule("", "'x'", "body")));
        assertEquals("x", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void rootBindingReplacesAnExtraBindingWithTheSameName() throws Exception {
        var registry = DescriptorRegistry.create();
        registry.registerFile(CelFixtures.FILE);
        var evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(CelFixtures.DOCUMENT).addVar("input").build());
        // The "input" extra binding would break evaluation if it survived; the target message wins.
        var bound = new CelProtoMapper(new ProtoFieldMapperImpl(registry), evaluator, "input",
                Map.of("input", "garbage"));
        var document = CelFixtures.doc("t");
        bound.map(document, List.of(new CelMappingRule("input.title == 't'", "'yes'", "body")));
        assertEquals("yes", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void nullExtraBindingsAreTreatedAsEmpty() throws Exception {
        var registry = DescriptorRegistry.create();
        registry.registerFile(CelFixtures.FILE);
        var evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(CelFixtures.DOCUMENT).addVar("input").build());
        var bound = new CelProtoMapper(new ProtoFieldMapperImpl(registry), evaluator, "input", null);
        var document = CelFixtures.doc("t");
        bound.map(document, List.of(new CelMappingRule("", "'ok'", "body")));
        assertEquals("ok", document.build().getField(CelFixtures.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void selectorNumberConvertsToTheTargetFieldType() throws Exception {
        var document = CelFixtures.doc("t");
        mapper.map(document, List.of(new CelMappingRule("", "40 + 2", "score")));
        assertEquals(42.0d, document.build().getField(CelFixtures.DOCUMENT.findFieldByName("score")));
    }

    @Test
    void nullRuleInTheListIsRejected() {
        var document = CelFixtures.doc("t");
        var rules = new java.util.ArrayList<CelMappingRule>();
        rules.add(null);
        assertThrows(NullPointerException.class, () -> mapper.map(document, rules));
    }
}
