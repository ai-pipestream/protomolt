package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * The {@code check-chain} verb: verify a chain without executing anything — methods
 * resolve and are unary, step names are sound scope variables, and every gate, mapping
 * rule, and CEL expression type-checks against exactly the scope its step will see. The
 * lint gate for consoles, CI, and registration.
 */
public final class CheckChainAction implements ProtoAction {

    @Override
    public String name() {
        return "check-chain";
    }

    @Override
    public String description() {
        return "Statically verifies a chain definition: every step's method resolves and "
                + "is unary, step names are valid scope variables, 'when' gates are boolean "
                + "CEL, and every mapping rule and CEL expression type-checks against the "
                + "scope that step will see ('input' plus prior steps' responses). A chain "
                + "that checks clean cannot fail on a type error at run time.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = RunChainAction.baseSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("chain", RunChainAction.chainSchema());
        schema.putArray("required").add("chain");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        ArrayNode findingsNode = result.putArray("findings");
        JsonNode chainNode = input.get("chain");
        if (!(chainNode instanceof ObjectNode chain)) {
            result.put("ok", false);
            finding(findingsNode, "", "chain", "'chain' object is required");
            return result;
        }
        ChainDefinition definition;
        try {
            definition = ChainJson.parse(chain, context);
        } catch (ChainJson.ChainParseException e) {
            result.put("ok", false);
            finding(findingsNode, e.step, "chain", e.getMessage());
            return result;
        }
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(definition);
        for (ChainVerifier.Finding entry : findings) {
            finding(findingsNode, entry.step(), entry.kind(), entry.error());
        }
        result.put("ok", findings.isEmpty());
        return result;
    }

    private static void finding(ArrayNode findings, String step, String kind, String error) {
        ObjectNode node = findings.addObject();
        node.put("step", step);
        node.put("kind", kind);
        node.put("error", error);
    }
}
