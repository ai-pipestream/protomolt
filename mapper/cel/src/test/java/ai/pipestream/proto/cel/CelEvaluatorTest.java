package ai.pipestream.proto.cel;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CelEvaluatorTest {
    @Test
    void evaluatesBooleanTrueAndFalse() {
        CelEvaluator evaluator = new CelEvaluator();
        assertTrue(evaluator.evaluateBoolean("true", Map.of()));
        assertFalse(evaluator.evaluateBoolean("false", Map.of()));
    }
    @Test void evaluateBooleanFailsSoftly() {
        assertFalse(new CelEvaluator().evaluateBoolean("not valid(", Map.of()));
    }
    @Test void evaluateBooleanOrFailRejectsNonBoolean() {
        assertThrows(CelEvaluationException.class, () -> new CelEvaluator().evaluateBooleanOrFail("'value'", Map.of()));
    }
    @Test void evaluatesArithmeticAndStrings() {
        CelEvaluator evaluator = new CelEvaluator();
        assertEquals(3L, evaluator.evaluateValue("1 + 2", Map.of()));
        assertEquals("ab", evaluator.evaluateValue("'a' + 'b'", Map.of()));
    }
    @Test void warmupCachesExpressionsWithVirtualThreads() {
        CelEvaluator evaluator = new CelEvaluator();
        evaluator.warmup(List.of("1 + 1", "2 + 2", "true"));
        assertEquals(3, evaluator.cacheSize());
    }
    @Test void clearCacheRemovesPrograms() {
        CelEvaluator evaluator = new CelEvaluator();
        evaluator.evaluateValue("1", Map.of()); evaluator.clearCache();
        assertEquals(0, evaluator.cacheSize());
    }
    @Test void blankExpressionThrows() {
        assertThrows(CelEvaluationException.class, () -> new CelEvaluator().evaluateValue(" ", Map.of()));
    }
    @Test void compileFailureIsTypedAsCompilationException() {
        CelEvaluator evaluator = new CelEvaluator();
        assertThrows(CelCompilationException.class, () -> evaluator.evaluateValue("not valid(", Map.of()));
        assertThrows(CelCompilationException.class, () -> evaluator.precompile("not valid("));
    }
    @Test void compileFailureIsCachedAndNotRecompiled() {
        CelEvaluator evaluator = new CelEvaluator();
        CelCompilationException first = assertThrows(CelCompilationException.class,
                () -> evaluator.evaluateValue("not valid(", Map.of()));
        CelCompilationException second = assertThrows(CelCompilationException.class,
                () -> evaluator.evaluateValue("not valid(", Map.of()));
        assertSame(first, second, "cached compile failure should be rethrown, not recompiled");
        evaluator.clearCache();
        CelCompilationException third = assertThrows(CelCompilationException.class,
                () -> evaluator.evaluateValue("not valid(", Map.of()));
        assertNotSame(first, third, "clearCache should also clear cached compile failures");
    }
    @Test void runtimeFailureIsNotACompilationException() {
        CelEvaluator evaluator = new CelEvaluator();
        CelEvaluationException e = assertThrows(CelEvaluationException.class,
                () -> evaluator.evaluateValue("1 / 0", Map.of()));
        assertFalse(e instanceof CelCompilationException);
        assertFalse(evaluator.evaluateBoolean("1 / 0 == 1", Map.of()));
    }
    /**
     * Warmup is precompilation, so callers that catch {@link CelCompilationException} to tell a
     * broken expression from a runtime failure must see the same type they get from precompile.
     */
    @Test void warmupReportsACompileFailureAsACompilationException() {
        CelEvaluator evaluator = new CelEvaluator();
        CelCompilationException e = assertThrows(CelCompilationException.class,
                () -> evaluator.warmup(List.of("1 + 1", "not valid(")));
        assertTrue(e.getMessage().contains("not valid("));
    }
    @Test void precompileCachesProgramWithoutEvaluating() {
        CelEvaluator evaluator = new CelEvaluator();
        evaluator.precompile("1 + 1");
        assertEquals(1, evaluator.cacheSize());
    }
}
