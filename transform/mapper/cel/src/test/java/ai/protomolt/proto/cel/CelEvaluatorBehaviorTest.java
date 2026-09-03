package ai.protomolt.proto.cel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CelEvaluator} behavior beyond the compile/failure pins: bindings, blank input,
 * warmup edge cases, concurrent evaluation, and cache eviction.
 */
class CelEvaluatorBehaviorTest {

    @Test
    void nullExpressionThrows() {
        assertThrows(CelEvaluationException.class,
                () -> new CelEvaluator().evaluateValue(null, Map.of()));
    }

    @Test
    void emptyExpressionThrows() {
        assertThrows(CelEvaluationException.class,
                () -> new CelEvaluator().evaluateValue("", Map.of()));
    }

    @Test
    void nullBindingsAreTreatedAsEmpty() {
        assertEquals(2L, new CelEvaluator().evaluateValue("1 + 1", null));
    }

    @Test
    void boundVariablesAreVisibleToTheExpression() {
        CelEvaluator evaluator = new CelEvaluator(
                CelEnvironmentFactory.builder().addVar("input").build());
        assertEquals(42L, evaluator.evaluateValue("input * 2", Map.of("input", 21L)));
    }

    @Test
    void undeclaredVariableIsACompileFailure() {
        CelEvaluator evaluator = new CelEvaluator();
        assertThrows(CelCompilationException.class,
                () -> evaluator.evaluateValue("nosuchvar + 1", Map.of()));
    }

    @Test
    void evaluateBooleanOrFailPropagatesCompileFailure() {
        CelEvaluator evaluator = new CelEvaluator();
        assertThrows(CelCompilationException.class,
                () -> evaluator.evaluateBooleanOrFail("true &&", Map.of()));
    }

    @Test
    void repeatedEvaluationReusesTheCachedProgram() {
        CelEvaluator evaluator = new CelEvaluator();
        evaluator.evaluateValue("1 + 1", Map.of());
        evaluator.evaluateValue("1 + 1", Map.of());
        assertEquals(1, evaluator.cacheSize());
    }

    @Test
    void distinctExpressionsAreCachedSeparately() {
        CelEvaluator evaluator = new CelEvaluator();
        evaluator.evaluateValue("1 + 1", Map.of());
        evaluator.evaluateValue("2 + 2", Map.of());
        assertEquals(2, evaluator.cacheSize());
    }

    @Test
    void warmupWithNullOrEmptyIsANoOp() {
        CelEvaluator evaluator = new CelEvaluator();
        evaluator.warmup(null);
        evaluator.warmup(List.of());
        assertEquals(0, evaluator.cacheSize());
    }

    @Test
    void warmupSkipsNullExpressions() {
        CelEvaluator evaluator = new CelEvaluator();
        evaluator.warmup(Arrays.asList("1", null, "2"));
        assertEquals(2, evaluator.cacheSize());
    }

    @Test
    void concurrentEvaluationIsConsistent() throws Exception {
        CelEvaluator evaluator = new CelEvaluator(
                CelEnvironmentFactory.builder().addVar("input").build());
        int threads = 16;
        List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            long value = i;
            tasks.add(() -> evaluator.evaluateValue("input + 1", Map.of("input", value)));
            tasks.add(() -> evaluator.evaluateValue("40 + 2", Map.of()));
        }
        try (var executor = Executors.newFixedThreadPool(threads)) {
            List<Future<Object>> futures = new ArrayList<>();
            for (Callable<Object> task : tasks) {
                futures.add(executor.submit(task));
            }
            for (int i = 0; i < futures.size(); i++) {
                Object expected = i % 2 == 0 ? (i / 2) + 1L : 42L;
                assertEquals(expected, futures.get(i).get());
            }
        }
        assertEquals(2, evaluator.cacheSize());
    }

    @Test
    void fullProgramCacheIsClearedAndRebuilt() {
        // Documented eviction semantics: a full cache is dropped, then the new expression is cached.
        CelEvaluator evaluator = new CelEvaluator();
        for (int i = 0; i < 1024; i++) {
            evaluator.evaluateValue(String.valueOf(i), Map.of());
        }
        assertEquals(1024, evaluator.cacheSize());
        evaluator.evaluateValue("1024", Map.of());
        assertEquals(1, evaluator.cacheSize());
    }

    @Test
    void evaluateBooleanOnMissingVariableReturnsFalse() {
        CelEvaluator evaluator = new CelEvaluator();
        assertFalse(evaluator.evaluateBoolean("input > 1", Map.of()));
    }
}
