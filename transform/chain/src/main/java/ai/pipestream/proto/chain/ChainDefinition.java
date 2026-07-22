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
     */
    public record Step(String name, String target, boolean tls, MethodDescriptor method,
                       String when, List<String> rules, List<CelMappingRule> celRules,
                       boolean validate, long deadlineMs) {

        public Step {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(method, "method");
            rules = List.copyOf(rules);
            celRules = List.copyOf(celRules);
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
