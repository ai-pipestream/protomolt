package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.spi.ValidationRuleSources;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Renders the prose argument contract for a role from the command catalog's
 * request descriptors. Operates on descriptors only: nothing here sees
 * credentials, provider processes or session state.
 */
final class AgentCommandContract {

    /** How deep the prose contract expands nested request messages. */
    private static final int CONTRACT_DEPTH = 3;

    /** The rule readers the validator and the schema generator both run on. */
    private static final List<ValidationRuleSource> RULE_SOURCES =
            ValidationRuleSources.defaults();

    private AgentCommandContract() {
    }

    /**
     * The argument contract for a role in prose, rendered from the same request
     * descriptors the schema and the validator read.
     *
     * <p>The prompt states the shape a command takes and the rules a message declares
     * about itself; both come from the descriptor, so neither can drift from what the
     * host will accept.
     */
    static String commandContract(AgentRole role) {
        StringBuilder out = new StringBuilder(role == AgentRole.WORKER
                ? "Worker argument contract, from the delegation request messages: "
                : "Coordinator argument contract, from the delegation request messages: ");
        Set<String> rules = new LinkedHashSet<>();
        boolean first = true;
        for (String tool : AgentCommands.tools(role)) {
            if (!first) {
                out.append("; ");
            }
            first = false;
            out.append(tool).append('=');
            if (AgentCommands.ACK.equals(tool)) {
                out.append("{reason}");
            } else {
                out.append(shape(AgentCommands.REQUESTS.get(tool),
                        AgentCommands.pinnedFields(role, tool),
                        new HashSet<>(), 0, rules));
            }
        }
        out.append('.');
        if (!rules.isEmpty()) {
            out.append(" Rules the messages declare: ")
                    .append(String.join("; ", rules)).append('.');
        }
        return out.append(" Use exactly these field names and no others.").toString();
    }

    /** One message as its member names, expanding the messages it carries. */
    private static String shape(Descriptor descriptor, Set<String> omit, Set<String> seen,
                                int depth, Set<String> rules) {
        rules.addAll(messageRules(descriptor));
        if (depth >= CONTRACT_DEPTH || !seen.add(descriptor.getFullName())) {
            return "{...}";
        }
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (FieldDescriptor field : descriptor.getFields()) {
            String name = field.getJsonName();
            if (depth == 0 && omit.contains(name)) {
                continue;
            }
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append(name);
            String nested = "";
            if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                nested = choices(field);
            } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && !field.isMapField()
                    && !field.getMessageType().getFullName().startsWith("google.protobuf.")) {
                nested = shape(field.getMessageType(), Set.of(), seen, depth + 1, rules);
            }
            out.append(field.isRepeated() ? "[" + nested + "]" : nested);
        }
        seen.remove(descriptor.getFullName());
        return out.append('}').toString();
    }

    /** An enum field's declared values, less the unspecified zero no request may send. */
    private static String choices(FieldDescriptor field) {
        List<String> names = new ArrayList<>();
        field.getEnumType().getValues().stream()
                .filter(value -> value.getNumber() != 0)
                .forEach(value -> names.add(value.getName()));
        return "(" + String.join("|", names) + ")";
    }

    /** What a message states about itself as a whole, in the words of its own rules. */
    private static List<String> messageRules(Descriptor descriptor) {
        List<String> stated = new ArrayList<>();
        for (ValidationRuleSource source : RULE_SOURCES) {
            MessageConstraints constraints = source.messageConstraints(descriptor)
                    .orElse(null);
            if (constraints == null) {
                continue;
            }
            for (CelConstraint rule : constraints.cel()) {
                if (!rule.message().isBlank()) {
                    stated.add(rule.message());
                }
            }
        }
        return stated;
    }
}
