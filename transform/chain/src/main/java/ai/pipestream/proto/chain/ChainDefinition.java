package ai.pipestream.proto.chain;

import ai.pipestream.proto.cel.CelMappingRule;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.util.List;
import java.util.Objects;

/**
 * A resolved chain: serial gRPC calls whose requests are mapped from the chain scope —
 * {@code input} plus every prior step's response under the step's name. Descriptors are
 * already resolved (from a registry, reflection, or inline sources); the JSON-envelope
 * parsing lives with the verbs.
 *
 * @param deadlineMs the whole chain's budget; per-step deadlines nest inside it
 * @param output the output mapping, or null to return the last step's response
 */
public record ChainDefinition(String name, List<FileDescriptor> files, Descriptor inputType,
                              long deadlineMs, List<Step> steps, Output output) {

    public ChainDefinition {
        Objects.requireNonNull(inputType, "inputType");
        files = List.copyOf(files);
        steps = List.copyOf(steps);
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("A chain needs at least one step");
        }
        if (deadlineMs <= 0) {
            deadlineMs = 30_000;
        }
    }

    /**
     * One serial call. {@code when} is an optional boolean CEL gate (a false skips the
     * step); {@code validate} runs the response's declared validation rules before the
     * chain proceeds.
     *
     * <p>A step is exactly one of two kinds. A gRPC step ({@link #grpc}) carries a
     * non-null {@code method} and a null {@code structured}. A structured-generation
     * step ({@link #structured}) carries a non-null {@code structured} and a null
     * {@code method}: it fills one message type with a catalog model through the
     * structured-generation coordinator instead of invoking gRPC, so its
     * {@code when}, {@code rules}, {@code celRules}, {@code validate},
     * {@code deadlineMs}, and {@code completion} are always empty/zero - the
     * verifier rejects a structured step that carries any of them.</p>
     *
     * @param method the unary method a gRPC step invokes; null exactly when
     *        {@code structured} is set
     * @param structured the structured-generation specification, or null for a gRPC
     *        step
     * @param completion how the step obtains its response: {@code ""} invokes
     *        {@code method} on {@code target}; {@code "external"} parks the job until
     *        {@code complete-step} supplies the response (the human-in-the-loop lane).
     *        External steps only execute as jobs; synchronous {@code run-chain} rejects
     *        them. Any other value fails verification.
     * @param edge the typed edge feeding this step, or null when the request is mapped
     *        by the top-level {@code rules}/{@code celRules}. An edge owns request
     *        mapping: the top-level rule lists must be empty when it is set.
     * @param fanOut the bounded fan-out over a repeated field of the edge's produced
     *        message, or null for a single invocation. Requires {@code edge}.
     */
    public record Step(String name, String target, boolean tls, MethodDescriptor method,
                       String when, List<String> rules, List<CelMappingRule> celRules,
                       boolean validate, long deadlineMs, String completion,
                       StructuredSpec structured, EdgeSpec edge, FanOutSpec fanOut) {

        /** {@link #completion()} value marking a step as externally completed. */
        public static final String COMPLETION_EXTERNAL = "external";

        /** The deterministic dependency anchor name shared by every structured step. */
        public static final String STRUCTURED_DEPENDENCY = "structured-generation";

        public Step {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(target, "target");
            if (structured == null) {
                Objects.requireNonNull(method, "method");
            } else if (method != null) {
                throw new IllegalArgumentException(
                        "a step carries a method or a structured spec, never both");
            }
            if (fanOut != null && edge == null) {
                throw new IllegalArgumentException(
                        "fan-out requires an edge; the items resolve against the "
                                + "edge's produced message");
            }
            rules = List.copyOf(rules);
            celRules = List.copyOf(celRules);
            completion = completion == null ? "" : completion;
        }

        /**
         * Source- and binary-compatible constructor for the pre-edge step shape:
         * no typed edge and no fan-out.
         */
        public Step(String name, String target, boolean tls, MethodDescriptor method,
                    String when, List<String> rules, List<CelMappingRule> celRules,
                    boolean validate, long deadlineMs, String completion,
                    StructuredSpec structured) {
            this(name, target, tls, method, when, rules, celRules, validate, deadlineMs,
                    completion, structured, null, null);
        }

        /**
         * Source- and binary-compatible constructor for the original gRPC-only step
         * shape. New callers may prefer {@link #grpc}, but existing integrations do
         * not need to change merely because structured steps were added.
         */
        public Step(String name, String target, boolean tls, MethodDescriptor method,
                    String when, List<String> rules, List<CelMappingRule> celRules,
                    boolean validate, long deadlineMs, String completion) {
            this(name, target, tls, method, when, rules, celRules, validate, deadlineMs,
                    completion, null, null, null);
        }

        /**
         * A gRPC step: one unary call whose request is mapped from the chain scope.
         * This is the canonical-constructor shape with {@code structured} null.
         */
        public static Step grpc(String name, String target, boolean tls,
                                MethodDescriptor method, String when, List<String> rules,
                                List<CelMappingRule> celRules, boolean validate,
                                long deadlineMs, String completion) {
            return new Step(name, target, tls, method, when, rules, celRules, validate,
                    deadlineMs, completion, null);
        }

        /**
         * A structured-generation step: the coordinator fills {@code targetType} with
         * the catalog model {@code model}, bounded to {@code maxAttempts} attempts
         * (0 applies the coordinator default). The step anchors its dependency under
         * {@link #STRUCTURED_DEPENDENCY}; the chain's files must carry the target
         * type's descriptor so compile and replay fingerprint over it.
         */
        public static Step structured(String name, Descriptor targetType, String model,
                                      int maxAttempts) {
            return new Step(name, STRUCTURED_DEPENDENCY, false, null, null, List.of(),
                    List.of(), false, 0, "",
                    new StructuredSpec(targetType, model, maxAttempts));
        }

        /** True when the step parks the job and waits for {@code complete-step}. */
        public boolean external() {
            return COMPLETION_EXTERNAL.equals(completion);
        }
    }

    /**
     * The typed edge feeding a {@link Step}: which scope sources it reads, the message
     * type it produces by mapping them with the existing scoped text and CEL rule
     * dialects, and the projection and validation applied before the value reaches the
     * step. The produced (or projected) value is the step input: for a gRPC step
     * without fan-out it is the method request; for a structured step it packs as the
     * generation's grounding; with fan-out each item of {@link FanOutSpec#items()}
     * feeds one branch.
     *
     * @param sources the ordered scope names the edge reads: {@code "input"} or prior
     *        step names; never empty
     * @param produceType the message type the edge produces by mapping the joined scope
     * @param rules scoped text mapping rules over the declared sources
     * @param celRules typed CEL mapping rules over the declared sources
     * @param projectTo a projection-annotated target type the produced (or per-branch
     *        item) value is projected to before validation and delivery, or null
     * @param validate whether the final value re-validates against its declared rules
     *        before the step executes; rejection happens before any invocation
     */
    public record EdgeSpec(List<String> sources, Descriptor produceType, List<String> rules,
                           List<CelMappingRule> celRules, Descriptor projectTo,
                           boolean validate) {

        public EdgeSpec {
            sources = List.copyOf(sources);
            if (sources.isEmpty()) {
                throw new IllegalArgumentException("an edge reads at least one source");
            }
            Objects.requireNonNull(produceType, "produceType");
            rules = List.copyOf(rules);
            celRules = List.copyOf(celRules);
        }
    }

    /** What one failed fan-out branch does to its step. */
    public enum BranchFailurePolicy {
        /** The first branch failure fails the step; remaining branches are abandoned. */
        FAIL_FAST,
        /** Failed branches are recorded; the collect succeeds with the survivors. */
        CONTINUE
    }

    /**
     * Bounded fan-out over a repeated field of the edge's produced message: each item
     * executes one branch under a stable identity ({@code "<step>#<index>"}), bounded
     * by the item and concurrency caps, and the branch outputs collect in index order
     * into one repeated field of the collect type.
     *
     * @param items dotted path of the repeated message field inside the produced
     *        message whose items each execute one branch
     * @param maxItems the item cap; more items reject the step before execution
     * @param maxConcurrency the in-flight branch cap
     * @param failurePolicy what one failed branch does to the step
     * @param collectType the message type the collected branch outputs form
     * @param collectInto the repeated field of {@code collectType} the ordered branch
     *        outputs land in; its element type must equal the branch output type
     */
    public record FanOutSpec(String items, int maxItems, int maxConcurrency,
                             BranchFailurePolicy failurePolicy, Descriptor collectType,
                             String collectInto) {

        public FanOutSpec {
            Objects.requireNonNull(items, "items");
            Objects.requireNonNull(failurePolicy, "failurePolicy");
            Objects.requireNonNull(collectType, "collectType");
            Objects.requireNonNull(collectInto, "collectInto");
        }
    }

    /**
     * The structured-generation specification of a {@link Step}: the message type to
     * fill, the catalog model id to fill it with, and the attempt cap.
     *
     * @param targetType the descriptor of the message type the coordinator fills
     * @param model the catalog model id; must declare the structured-output capability
     * @param maxAttempts the attempt cap; 0 applies the coordinator default, and the
     *        value never exceeds the coordinator's hard cap of 3
     */
    public record StructuredSpec(Descriptor targetType, String model, int maxAttempts) {

        public StructuredSpec {
            Objects.requireNonNull(targetType, "targetType");
            Objects.requireNonNull(model, "model");
        }
    }

    /** The chain's output shape and the scoped rules that populate it. */
    public record Output(Descriptor type, List<String> rules, List<CelMappingRule> celRules) {

        public Output {
            Objects.requireNonNull(type, "type");
            rules = List.copyOf(rules);
            celRules = List.copyOf(celRules);
        }
    }

    /** Resolves {@code package.Service/Method} across the chain's files. */
    public static MethodDescriptor resolveMethod(List<FileDescriptor> files, String qualified) {
        int slash = qualified.indexOf('/');
        if (slash <= 0 || slash == qualified.length() - 1) {
            throw new IllegalArgumentException(
                    "method must be 'package.Service/Method'; got '" + qualified + "'");
        }
        String serviceName = qualified.substring(0, slash);
        String methodName = qualified.substring(slash + 1);
        for (FileDescriptor file : files) {
            for (ServiceDescriptor service : file.getServices()) {
                if (service.getFullName().equals(serviceName)) {
                    MethodDescriptor method = service.findMethodByName(methodName);
                    if (method != null) {
                        return method;
                    }
                }
            }
        }
        throw new IllegalArgumentException(
                "method '" + qualified + "' not found in the chain's schema");
    }
}
