package ai.pipestream.proto.cel;

import dev.cel.bundle.Cel;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.runtime.CelRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Thread-safe, cached CEL expression compiler and evaluator.
 *
 * <p>Both successful compilations and compile <em>failures</em> are cached (bounded), so a
 * repeatedly evaluated broken expression is compiled and WARN-logged only once instead of
 * per message. Compile failures surface as {@link CelCompilationException}, runtime failures
 * as plain {@link CelEvaluationException}.</p>
 */
public final class CelEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(CelEvaluator.class);

    /**
     * Upper bound for the program and compile-failure caches. When a cache is full and an
     * unseen expression arrives, the cache is cleared and rebuilt (simple and allocation-free
     * compared to LRU bookkeeping; expression sets are small and stable in practice).
     */
    private static final int MAX_CACHE_SIZE = 1024;

    private final Cel cel;
    private final ConcurrentHashMap<String, CelRuntime.Program> programs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CelCompilationException> compileFailures = new ConcurrentHashMap<>();

    public CelEvaluator() {
        this(CelEnvironmentFactory.builder().addVar("input").build());
    }

    public CelEvaluator(Cel cel) {
        this.cel = Objects.requireNonNull(cel, "cel");
    }

    /**
     * Evaluates a boolean expression, returning {@code false} on any failure.
     *
     * <p>Compile failures are logged at WARN once (when first compiled and cached);
     * runtime evaluation failures are logged at DEBUG.</p>
     */
    public boolean evaluateBoolean(String expression, Map<String, Object> bindings) {
        try {
            return evaluateBooleanOrFail(expression, bindings);
        } catch (CelCompilationException e) {
            // Already WARN-logged once when the failure was cached.
            return false;
        } catch (CelEvaluationException e) {
            LOG.debug("CEL boolean expression failed at runtime, treating as false: {} ({})",
                    expression, e.getMessage());
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
        CelRuntime.Program program = compile(expression);
        try {
            return program.eval(bindings == null ? Map.of() : bindings);
        } catch (Exception e) {
            if (e instanceof CelEvaluationException evaluationException) {
                throw evaluationException;
            }
            throw new CelEvaluationException("Failed to evaluate CEL expression: " + expression, e);
        }
    }

    /**
     * Compiles (and caches) the expression without evaluating it.
     *
     * @throws CelCompilationException when the expression does not compile in this
     *         evaluator's environment
     */
    public void precompile(String expression) {
        compile(expression);
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
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CelEvaluationException("Interrupted while warming CEL cache", e);
                        } catch (ExecutionException e) {
                            // Warmup is precompilation, so a broken expression must still be a
                            // CelCompilationException here, exactly as precompile reports it.
                            if (e.getCause() instanceof CelEvaluationException cause) {
                                throw cause;
                            }
                            throw new CelEvaluationException("Failed to warm CEL cache", e.getCause());
                        }
                    });
        }
    }

    public void clearCache() {
        programs.clear();
        compileFailures.clear();
    }

    public int cacheSize() {
        return programs.size();
    }

    private CelRuntime.Program compile(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new CelEvaluationException("CEL expression must not be blank");
        }
        CelRuntime.Program cached = programs.get(expression);
        if (cached != null) {
            return cached;
        }
        CelCompilationException cachedFailure = compileFailures.get(expression);
        if (cachedFailure != null) {
            throw cachedFailure;
        }
        CelRuntime.Program program;
        try {
            program = compileProgram(expression);
        } catch (CelCompilationException e) {
            LOG.warn("CEL expression failed to compile (failure cached, not retried): {} ({})",
                    expression, e.getMessage());
            if (compileFailures.size() >= MAX_CACHE_SIZE) {
                compileFailures.clear();
            }
            compileFailures.putIfAbsent(expression, e);
            throw e;
        }
        if (programs.size() >= MAX_CACHE_SIZE) {
            programs.clear();
        }
        CelRuntime.Program existing = programs.putIfAbsent(expression, program);
        return existing != null ? existing : program;
    }

    private CelRuntime.Program compileProgram(String expression) {
        try {
            var validation = cel.compile(expression);
            if (validation.hasError()) {
                throw new CelCompilationException("Invalid CEL expression: " + validation.getErrorString());
            }
            CelAbstractSyntaxTree ast = validation.getAst();
            return cel.createProgram(ast);
        } catch (CelCompilationException e) {
            throw e;
        } catch (Exception e) {
            throw new CelCompilationException("Failed to compile CEL expression: " + expression, e);
        }
    }
}
