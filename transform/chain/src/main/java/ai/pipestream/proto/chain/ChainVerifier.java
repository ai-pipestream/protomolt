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
 * Structured-generation steps check statically instead: the spec must be bare (no
 * gate, rules, deadline, validation flag, or completion mode), the model id non-blank,
 * the attempt cap within the coordinator's bound, and the target type's file reachable
 * from the chain's files so compile and replay fingerprint over it.
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
            if (step.structured() != null) {
                verifyStructured(chain, step, findings);
                scope.put(step.name(), step.structured().targetType());
                continue;
            }
            if (step.method().isClientStreaming() || step.method().isServerStreaming()) {
                findings.add(new Finding(step.name(), "method",
                        step.method().getFullName() + " is not unary; chains call unary "
                                + "methods (streaming is a later phase)"));
            }
            if (!step.completion().isEmpty()
                    && !ChainDefinition.Step.COMPLETION_EXTERNAL.equals(step.completion())) {
                findings.add(new Finding(step.name(), "completion",
                        "completion must be '' (invoke) or 'external'; got '"
                                + step.completion() + "'"));
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

    /**
     * A structured step has no gRPC method: it must be a bare generation spec whose
     * target type the chain's file set can fingerprint and replay can resolve. Gates,
     * mapping rules, deadlines, validation flags, and completion modes all belong to
     * the gRPC lane and are rejected here.
     */
    private static void verifyStructured(ChainDefinition chain, ChainDefinition.Step step,
                                         List<Finding> findings) {
        ChainDefinition.StructuredSpec spec = step.structured();
        if (spec.model().isBlank()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured step model must not be blank"));
        }
        if (spec.maxAttempts() < 0 || spec.maxAttempts() > 3) {
            findings.add(new Finding(step.name(), "structured",
                    "structured step maxAttempts must be between 0 and 3; got "
                            + spec.maxAttempts()));
        }
        if (step.when() != null && !step.when().isBlank()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps do not support when gates"));
        }
        if (!step.rules().isEmpty() || !step.celRules().isEmpty()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps declare no mapping rules; the coordinator fills "
                            + "the target type directly"));
        }
        if (step.validate()) {
            findings.add(new Finding(step.name(), "structured",
                    "validate is meaningless on a structured step; the coordinator "
                            + "always validates its output"));
        }
        if (step.deadlineMs() != 0) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps do not support per-step deadlines"));
        }
        if (!step.completion().isEmpty()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps do not support completion modes"));
        }
        if (!reachable(chain.files(), spec.targetType().getFile())) {
            findings.add(new Finding(step.name(), "structured",
                    "target type " + spec.targetType().getFullName() + "'s file "
                            + spec.targetType().getFile().getFullName()
                            + " is not reachable from the chain's files; compile and "
                            + "replay fingerprint over the chain's file set"));
        }
    }

    /** True when {@code file} is one of {@code files} or a transitive import of one. */
    private static boolean reachable(List<com.google.protobuf.Descriptors.FileDescriptor> files,
                                     com.google.protobuf.Descriptors.FileDescriptor file) {
        for (com.google.protobuf.Descriptors.FileDescriptor candidate : files) {
            if (candidate.equals(file) || reachable(candidate.getDependencies(), file)) {
                return true;
            }
        }
        return false;
    }
}
