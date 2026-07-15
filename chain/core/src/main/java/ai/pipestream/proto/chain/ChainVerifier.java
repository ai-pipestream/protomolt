package ai.pipestream.proto.chain;

import ai.pipestream.proto.shapes.RuleChecker;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Verifies a chain without running it — {@code check-chain}'s engine. Step names must be
 * unique identifiers (they become scope variables); every method must be unary; each step's
 * gate and request mapping is checked by {@link RuleChecker} against exactly the scope that
 * step will see ({@code input} plus every prior step's response); the output mapping is
 * checked against the full scope. A gated ({@code when}) step's output is legitimately in
 * that scope because the runner binds a skipped step's name to its output type's default
 * instance — the static scope and the runtime value map always agree. A chain that
 * verifies cannot fail on a type error at run time — only on live-service behavior.
 */
public final class ChainVerifier {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** One problem: {@code step} is empty for chain-level findings. */
    public record Finding(String step, String kind, String error) {
    }

    public List<Finding> verify(ChainDefinition chain) {
        List<Finding> findings = new ArrayList<>();
        RuleChecker checker = new RuleChecker();
        Map<String, Descriptor> scope = new LinkedHashMap<>();
        scope.put("input", chain.inputType());

        for (ChainDefinition.Step step : chain.steps()) {
            if (!IDENTIFIER.matcher(step.name()).matches()
                    || step.name().equals("input") || step.name().equals("target")) {
                findings.add(new Finding(step.name(), "chain",
                        "step name must be an identifier other than 'input'/'target'"));
                continue;
            }
            if (scope.containsKey(step.name())) {
                findings.add(new Finding(step.name(), "chain", "duplicate step name"));
                continue;
            }
            if (step.method().isClientStreaming() || step.method().isServerStreaming()) {
                findings.add(new Finding(step.name(), "method",
                        step.method().getFullName() + " is not unary; chains call unary "
                                + "methods (streaming is a later phase)"));
            }
            List<String> gates = step.when() == null || step.when().isBlank()
                    ? List.of() : List.of(step.when());
            for (RuleChecker.Finding finding : checker.checkScoped(scope,
                    step.method().getInputType(), step.rules(), step.celRules(), gates)) {
                findings.add(new Finding(step.name(),
                        finding.kind().equals("filter") ? "when" : finding.kind(),
                        finding.error() + " (" + finding.rule() + ")"));
            }
            scope.put(step.name(), step.method().getOutputType());
        }

        if (chain.output() != null) {
            for (RuleChecker.Finding finding : checker.checkScoped(scope,
                    chain.output().type(), chain.output().rules(),
                    chain.output().celRules(), List.of())) {
                findings.add(new Finding("", "output",
                        finding.error() + " (" + finding.rule() + ")"));
            }
        }
        return findings;
    }
}
