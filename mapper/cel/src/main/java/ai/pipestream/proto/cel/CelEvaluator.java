package ai.pipestream.proto.cel;

import dev.cel.bundle.Cel;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.runtime.CelRuntime;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/** Thread-safe, cached CEL expression compiler and evaluator. */
public final class CelEvaluator {
    private final Cel cel;
    private final ConcurrentHashMap<String, CelRuntime.Program> programs = new ConcurrentHashMap<>();

    public CelEvaluator() {
        this(CelEnvironmentFactory.builder().addVar("input").build());
    }

    public CelEvaluator(Cel cel) {
        this.cel = Objects.requireNonNull(cel, "cel");
    }

    public boolean evaluateBoolean(String expression, Map<String, Object> bindings) {
        try {
            return evaluateBooleanOrFail(expression, bindings);
        } catch (CelEvaluationException ignored) {
            return false;
        }
    }

    public boolean evaluateBooleanOrFail(String expression, Map<String, Object> bindings) {
        Object value = evaluateValue(expression, bindings);
        if (value instanceof Boolean result) {
            return result;
        }
        throw new CelEvaluationException("CEL expression did not return a boolean: " + expression);
    }

    public Object evaluateValue(String expression, Map<String, Object> bindings) {
        try {
            return compile(expression).eval(bindings == null ? Map.of() : bindings);
        } catch (Exception e) {
            if (e instanceof CelEvaluationException evaluationException) {
                throw evaluationException;
            }
            throw new CelEvaluationException("Failed to evaluate CEL expression: " + expression, e);
        }
    }

    public void warmup(Collection<String> expressions) {
        if (expressions == null || expressions.isEmpty()) {
            return;
        }
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            expressions.stream()
                    .filter(Objects::nonNull)
                    .map(expression -> executor.submit(() -> compile(expression)))
                    .toList()
                    .forEach(future -> {
                        try {
                            future.get();
                        } catch (Exception e) {
                            throw new CelEvaluationException("Failed to warm CEL cache", e);
                        }
                    });
        }
    }

    public void clearCache() {
        programs.clear();
    }

    public int cacheSize() {
        return programs.size();
    }

    private CelRuntime.Program compile(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new CelEvaluationException("CEL expression must not be blank");
        }
        return programs.computeIfAbsent(expression, this::compileProgram);
    }

    private CelRuntime.Program compileProgram(String expression) {
        try {
            var validation = cel.compile(expression);
            if (validation.hasError()) {
                throw new CelCompilationException("Invalid CEL expression: " + validation.getErrorString());
            }
            CelAbstractSyntaxTree ast = validation.getAst();
            return cel.createProgram(ast);
        } catch (Exception e) {
            if (e instanceof CelEvaluationException evaluationException) {
                throw evaluationException;
            }
            throw new CelEvaluationException("Failed to compile CEL expression: " + expression, e);
        }
    }
}
