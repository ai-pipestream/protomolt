package ai.protomolt.proto.mapper;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ProtoFieldMapper#map} reads from a separate source message while mutating the target,
 * parses the whole rule list before executing, and wraps non-mapping failures with the rule.
 */
class MapSourceTargetTest {

    private final DescriptorRegistry registry = DescriptorRegistry.create();
    private final ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(registry);

    @Test
    void mapReadsFromSourceAndWritesToTarget() throws Exception {
        var source = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "s").build();
        var target = TestDescriptors.document();
        mapper.map(source, target, List.of("body = title", "language = \"en\""));
        assertEquals("s", mapper.getValue(target, "body"));
        assertEquals("en", mapper.getValue(target, "language"));
    }

    @Test
    void mapLeavesTheSourceMessageUntouched() throws Exception {
        var source = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "s").build();
        var target = TestDescriptors.document();
        mapper.map(source, target, List.of("body = title", "-title"));
        assertEquals(source, TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "s").build());
        // The clear rule applied to the target, not the (already empty) source.
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("title")));
    }

    @Test
    void clearRuleClearsTargetField() throws Exception {
        var source = TestDescriptors.document().build();
        var target = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "existing");
        mapper.map(source, target, List.of("-title"));
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("title")));
    }

    @Test
    void emptyRuleListLeavesTargetUntouched() throws Exception {
        var source = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "s").build();
        var target = TestDescriptors.document();
        mapper.map(source, target, List.of());
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void nullAndBlankRuleStringsAreSkipped() throws Exception {
        var source = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "s").build();
        var target = TestDescriptors.document();
        mapper.map(source, target, Arrays.asList(null, "   ", "body = title"));
        assertEquals("s", mapper.getValue(target, "body"));
    }

    @Test
    void rulesAreAllParsedBeforeAnyIsExecuted() {
        // A syntax error anywhere in the list aborts before the first rule mutates the target.
        var source = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "s").build();
        var target = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.map(source, target, List.of("body = title", "!!!invalid")));
        assertTrue(e.getMessage().contains("Invalid rule syntax"), e.getMessage());
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("body")));
    }

    @Test
    void nonMappingFailureIsWrappedWithTheOffendingRule() {
        var source = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "not-a-number").build();
        var target = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.map(source, target, List.of("score = title")));
        assertTrue(e.getMessage().contains("Failed to execute rule"), e.getMessage());
        assertTrue(e.getMessage().contains("score = title"), e.getMessage());
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void mappingFailurePropagatesUnwrapped() {
        var source = TestDescriptors.document().build();
        var target = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.map(source, target, List.of("body = nosuchfield")));
        assertTrue(e.getMessage().contains("Field 'nosuchfield' not found"), e.getMessage());
    }

    @Test
    void accessorsReturnTheConstructorWiredCollaborators() {
        assertSame(registry, mapper.getDescriptorRegistry());
        assertNotNull(mapper.getAnyHandler());
        assertSame(registry, mapper.getAnyHandler().getDescriptorRegistry());
    }
}
