package ai.pipestream.proto.metadata;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelValidation;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import dev.cel.bundle.Cel;
import dev.cel.common.types.StructTypeReference;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Extracts named metadata values from a protobuf message with CEL selectors.
 *
 * <p>The message descriptor is used to build a typed CEL environment for {@code input},
 * so selector compile errors (typos, unknown fields) surface eagerly before any selector
 * is evaluated. Each selector is validated once per descriptor (results are cached), and
 * is also precompiled in the injected evaluator's own environment so an environment
 * mismatch (e.g. an evaluator environment lacking {@code input}) surfaces eagerly as an
 * invalid-selector error instead of failing later at evaluation time; the precompiled
 * program is cached by the evaluator and reused for every evaluation. Selector failures
 * are reported as {@link IllegalStateException}s carrying the selector name and
 * expression text.
 */
public final class MetadataExtractor {

    /** Bound for the per-descriptor validation environment cache. */
    private static final int MAX_ENVIRONMENTS = 64;

    /** Bound for the per-descriptor set of already-validated selector expressions. */
    private static final int MAX_VALIDATED_EXPRESSIONS = 1024;

    private final CelEvaluator evaluator;
    private final ConcurrentHashMap<Descriptor, ValidationEnvironment> validationEnvironments =
            new ConcurrentHashMap<>();

    private record ValidationEnvironment(Cel cel, Set<String> validatedExpressions) {
    }

    public MetadataExtractor(CelEvaluator evaluator) {
        this.evaluator = Objects.requireNonNull(evaluator, "evaluator");
    }

    public Map<String, Object> extract(
            Descriptor descriptor, Message message, Map<String, String> selectors) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(message, "message");
        if (selectors == null || selectors.isEmpty()) {
            return Map.of();
        }
        validateSelectors(descriptor, selectors);
        Map<String, Object> bindings = Map.of("input", message);
        if (selectors.size() == 1) {
            var selector = selectors.entrySet().iterator().next();
            return Map.of(selector.getKey(), evaluate(selector.getKey(), selector.getValue(), bindings));
        }
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Map<String, java.util.concurrent.Future<Object>> futures = new LinkedHashMap<>();
            selectors.forEach((name, expression) ->
                    futures.put(name, executor.submit(() -> evaluate(name, expression, bindings))));
            Map<String, Object> result = new LinkedHashMap<>();
            for (var entry : futures.entrySet()) {
                try {
                    result.put(entry.getKey(), entry.getValue().get());
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof IllegalStateException failure) {
                        throw failure;
                    }
                    throw selectorFailure(entry.getKey(), selectors.get(entry.getKey()), e.getCause());
                } catch (Exception e) {
                    throw selectorFailure(entry.getKey(), selectors.get(entry.getKey()), e);
                }
            }
            return Map.copyOf(result);
        }
    }

    /**
     * Compiles every selector once against a typed environment (typos surface with field-level
     * diagnostics) and once in the evaluator's own environment (whose cached program is what
     * {@link #extract} evaluates), so errors surface before evaluation. Validation results are
     * cached per descriptor: subsequent {@code extract} calls with already-validated selectors
     * do not recompile anything.
     */
    private void validateSelectors(Descriptor descriptor, Map<String, String> selectors) {
        if (validationEnvironments.size() >= MAX_ENVIRONMENTS
                && !validationEnvironments.containsKey(descriptor)) {
            validationEnvironments.clear();
        }
        ValidationEnvironment environment = validationEnvironments.computeIfAbsent(descriptor, d ->
                new ValidationEnvironment(
                        CelEnvironmentFactory.builder()
                                .addMessageType(d)
                                .addVar("input", StructTypeReference.create(d.getFullName()))
                                .build(),
                        ConcurrentHashMap.newKeySet()));
        selectors.forEach((name, expression) -> {
            if (environment.validatedExpressions().contains(expression)) {
                return;
            }
            CelValidation.Result result = CelValidation.validate(environment.cel(), expression);
            if (!result.valid()) {
                throw new IllegalStateException("Invalid metadata selector '" + name
                        + "' (" + expression + "): " + String.join("; ", result.errors()));
            }
            // The evaluator environment may differ from the typed validation environment;
            // compile there too (cached, reused by evaluation) so a mismatch surfaces now.
            try {
                evaluator.precompile(expression);
            } catch (CelEvaluationException e) {
                throw new IllegalStateException("Invalid metadata selector '" + name
                        + "' (" + expression + "): does not compile in the evaluator environment: "
                        + e.getMessage(), e);
            }
            if (environment.validatedExpressions().size() >= MAX_VALIDATED_EXPRESSIONS) {
                environment.validatedExpressions().clear();
            }
            environment.validatedExpressions().add(expression);
        });
    }

    private Object evaluate(String name, String expression, Map<String, Object> bindings) {
        try {
            return evaluator.evaluateValue(expression, bindings);
        } catch (CelEvaluationException e) {
            throw selectorFailure(name, expression, e);
        }
    }

    private static IllegalStateException selectorFailure(String name, String expression, Throwable cause) {
        return new IllegalStateException(
                "Failed to extract metadata selector '" + name + "' (" + expression + ")", cause);
    }
}
