package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;

import java.util.List;

/**
 * The {@code run-chain} verb: execute an inline chain — serial typed gRPC calls, each
 * request mapped from the chain input and every prior step's response. The chain is
 * statically verified first; execution failures return {@code ok=false} with the failing
 * step, never a stack trace.
 */
public final class RunChainAction implements ProtoAction {

    private final ChainRunner runner;
    private final ChainRepository repository;

    public RunChainAction() {
        this(new ChainRunner(), null);
    }

    /** Injectable runner — the channel-factory seam for tests and TLS policy. */
    public RunChainAction(ChainRunner runner) {
        this(runner, null);
    }

    /** With a repository, {@code chainName} resolves stored chains. */
    public RunChainAction(ChainRunner runner, ChainRepository repository) {
        this.runner = runner;
        this.repository = repository;
    }

    @Override
    public String name() {
        return "run-chain";
    }

    @Override
    public String description() {
        return "Executes a chain: serial unary gRPC calls where each step's request is "
                + "mapped (rules + CEL) from the chain 'input' and every prior step's "
                + "response, gates ('when') skip steps, 'validate' checks responses against "
                + "their declared rules, and deadlines nest. The chain is verified before "
                + "anything runs. Returns the composed output (the last response, or the "
                + "'output' mapping's message).";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = baseSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("chain", chainSchema());
        properties.putObject("chainName")
                .put("type", "string")
                .put("description", "A stored chain to run instead of an inline 'chain' — "
                        + "registered via the registry's chains endpoint.");
        properties.putObject("input")
                .put("type", "object")
                .put("description", "The chain input, as proto3 JSON of the chain's "
                        + "inputType.");
        schema.putArray("required").add("input");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        JsonNode chainNode = input.get("chain");
        JsonNode nameNode = input.get("chainName");
        if (chainNode == null && nameNode != null && nameNode.isTextual()) {
            if (repository == null) {
                result.put("ok", false);
                result.put("error", "No chain repository is mounted; run with an inline "
                        + "'chain' or start a server with a registry");
                return result;
            }
            chainNode = repository.chain(nameNode.asText()).orElse(null);
            if (chainNode == null) {
                result.put("ok", false);
                result.put("error", "No stored chain named '" + nameNode.asText() + "'");
                return result;
            }
        }
        JsonNode inputNode = input.get("input");
        if (!(chainNode instanceof ObjectNode chain) || !(inputNode instanceof ObjectNode)) {
            result.put("ok", false);
            result.put("error", "'chain' (or 'chainName') and 'input' objects are required");
            return result;
        }
        ChainDefinition definition;
        try {
            definition = ChainJson.parse(chain, context);
        } catch (ChainJson.ChainParseException e) {
            result.put("ok", false);
            result.put("failedStep", e.step);
            result.put("error", e.getMessage());
            return result;
        }
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(definition);
        if (!findings.isEmpty()) {
            ChainVerifier.Finding first = findings.get(0);
            result.put("ok", false);
            result.put("failedStep", first.step());
            result.put("error", "chain does not verify (" + findings.size() + " finding"
                    + (findings.size() == 1 ? "" : "s") + "); first: [" + first.kind() + "] "
                    + first.error());
            return result;
        }
        DynamicMessage message;
        try {
            message = context.transcoder()
                    .fromJsonDynamic(inputNode.toString(), definition.inputType());
        } catch (MalformedProtobufJsonException e) {
            result.put("ok", false);
            result.put("error", "'input' is not valid proto3 JSON for "
                    + definition.inputType().getFullName() + ": " + e.getMessage());
            return result;
        }
        ChainRunner.Result outcome;
        try {
            outcome = runner.run(definition, message);
        } catch (ChainRunner.ChainExecutionException e) {
            result.put("ok", false);
            result.put("failedStep", e.step());
            result.put("error", e.getMessage());
            return result;
        }
        result.put("ok", true);
        result.put("outputType", outcome.output().getDescriptorForType().getFullName());
        try {
            result.set("output", context.objectMapper()
                    .readTree(context.transcoder().toJson(outcome.output())));
        } catch (JsonProcessingException e) {
            result.put("ok", false);
            result.put("error", "failed to render the chain output: " + e.getMessage());
            return result;
        }
        ArrayNode steps = result.putArray("steps");
        for (ChainRunner.StepOutcome step : outcome.steps()) {
            ObjectNode node = steps.addObject();
            node.put("name", step.name());
            node.put("skipped", step.skipped());
        }
        return result;
    }

    static ObjectNode baseSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }

    /** The chain-definition schema shared by run-chain and check-chain. */
    static ObjectNode chainSchema() {
        ObjectNode chain = JsonNodeFactory.instance.objectNode();
        chain.put("type", "object");
        chain.put("description", "The chain definition: a schema declaring every step's "
                + "service, an inputType, and serial steps whose requests are mapped from "
                + "'input' plus prior steps' responses (by step name). Steps: {name, "
                + "target, method, tls?, when? (bool CEL gate), rules?, celRules?, "
                + "validate?, deadlineMs?}. Optional output: {type, rules, celRules}; "
                + "without it the last response returns.");
        return chain;
    }
}
