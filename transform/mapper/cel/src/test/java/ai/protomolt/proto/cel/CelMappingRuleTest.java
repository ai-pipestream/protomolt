package ai.protomolt.proto.cel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Record contract of {@link CelMappingRule}: null handling and defensive copies. */
class CelMappingRuleTest {

    @Test
    void nullTargetPathIsRejected() {
        assertThrows(NullPointerException.class,
                () -> new CelMappingRule(null, "'x'", null));
    }

    @Test
    void nullFallbackDefaultsToEmptyList() {
        var rule = new CelMappingRule(null, "'x'", "body", null);
        assertTrue(rule.textRuleFallback().isEmpty());
    }

    @Test
    void threeArgumentConstructorHasEmptyFallback() {
        var rule = new CelMappingRule("true", "'x'", "body");
        assertEquals(List.of(), rule.textRuleFallback());
    }

    @Test
    void fallbackListIsCopiedDefensively() {
        var mutable = new ArrayList<>(List.of("a = b"));
        var rule = new CelMappingRule(null, null, "body", mutable);
        mutable.add("c = d");
        assertEquals(List.of("a = b"), rule.textRuleFallback());
    }

    @Test
    void fallbackListIsImmutable() {
        var rule = new CelMappingRule(null, null, "body", List.of("a = b"));
        assertThrows(UnsupportedOperationException.class,
                () -> rule.textRuleFallback().add("c = d"));
    }

    @Test
    void accessorsReturnConstructorValues() {
        var rule = new CelMappingRule("input.x", "input.y", "body", List.of("a = b"));
        assertEquals("input.x", rule.filterExpression());
        assertEquals("input.y", rule.selectorExpression());
        assertEquals("body", rule.targetPath());
        assertEquals(List.of("a = b"), rule.textRuleFallback());
    }
}
